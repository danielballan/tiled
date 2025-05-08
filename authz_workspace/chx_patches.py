import collections
import copy
import functools
import itertools
from pathlib import Path

import dask
import databroker.v1
import h5py
import hdf5plugin
import numpy
import packaging.version
import pandas
import xarray
from area_detector_handlers.eiger import EigerHandler
from databroker.mongo_normalized import (
    BlueskyEventStream,
    BlueskyRun,
    DatasetFromDocuments,
)
from databroker.mongo_normalized import Tree as _Tree
from databroker.mongo_normalized import _fill

try:
    from databroker.mongo_normalized import default_validate_shape
except ImportError:
    # for databroker < 2.0.0v32
    from databroker.mongo_normalized import _validate_shape as default_validate_shape

import event_model
from event_model import compose_datum, compose_descriptor

from tiled.utils import OneShotCachedMap

try:
    from tiled.readers.xarray import DatasetAdapter
except ImportError:
    from tiled.adapters.xarray import DatasetAdapter

# Importing this has some side-effects that resolves this error:
# OSError: Can't read data (required filter 'HDF5 lz4 filter;
# see http://www.hdfgroup.org/services/contributions.html' is not registered)
# But we do not use it below.
del hdf5plugin

PLURAL_EVENT_COUNTERS = (
    (
        packaging.version.parse(event_model.__version__)
        >= packaging.version.parse("1.19.3")
    )
    or
    # some bug in the packaging
    (
        packaging.version.parse(event_model.__version__)
        == packaging.version.parse("0.0.0")
    )
)

# This is doing standard dtype_str patching.
# Nothing terribly special or scary is happening here.


DEFAULT_DTYPE = {
    "BCam_image": "|u1",
    "eiger1m_single_image": "<u2",
    "eiger4m_single_image": "<u4",
    "OAV_image": "|u1",
    "xray_eye1_image": "|u1",
    "xray_eye2_image": "<u2",
    "xray_eye3_image": "|u1",
    "xray_eye4_image": "|u1",
}


def patch_descriptor(doc):
    if "eiger1m_single_image" in doc["data_keys"]:
        doc["data_keys"]["eiger1m_single_image"].setdefault("dtype_str", "<u2")
    for field, default_dtype in DEFAULT_DTYPE.items():
        if field in doc["data_keys"]:
            doc["data_keys"][field].setdefault("dtype_str", default_dtype)
    return doc


# do the root map early to unbreak CHX specific code that is doing path munging
def patch_resource(doc):
    if doc.get("root", "") == "/nsls2/xf11id1":
        doc["root"] = "/nsls2/data/chx/legacy"
    return doc


class PatchedBlueskyRun(BlueskyRun):
    # Inject some extra Datum documents.
    def __init__(self, *args, extra_datums, **kwargs):
        super().__init__(*args, **kwargs)
        self._resource_lookup = {d["datum_id"]: d["resource"] for d in extra_datums}
        self._datum_lookup = collections.defaultdict(list)
        self._extra_datums = extra_datums
        for d in extra_datums:
            self._datum_lookup[d["resource"]].append(d)

    def new_variation(self, *args, **kwargs):
        res = super().new_variation(*args, extra_datums=self._extra_datums, **kwargs)
        res._resource_lookup = self._resource_lookup
        res._datum_lookup = self._datum_lookup
        return res

    def lookup_resource_for_datum(self, datum_id):
        if datum_id in self._resource_lookup:
            return self._resource_lookup[datum_id]
        return super().lookup_resource_for_datum(datum_id)

    def get_datum_for_resource(self, resource_uid):
        res = super().get_datum_for_resource(resource_uid)
        return list(res) + self._datum_lookup[resource_uid]


class PatchedHeader(databroker.v1.Header):
    # Inject extra streams for Eiger metadata.
    def data(self, field, stream_name="primary", fill=True):
        "Wrap dask.array in PIMS."
        from databroker._legacy_images import Images

        results = super().data(field, stream_name=stream_name, fill=fill)
        if "eiger" in field:
            for item in results:
                images = Images(data_array=xarray.DataArray(item))
                images.md = (
                    self.table(
                        stream_name=f"{field}_metadata_patched_in_at_runtime", fill=True
                    )
                    .iloc[0]
                    .to_dict()
                )
                yield images
        else:
            yield from results


databroker.v1.Header = PatchedHeader


# server side caching
class DatasetFromLocal(DatasetFromDocuments):
    def __init__(
        self,
        *,
        run,
        stream_name,
        event_descriptors,
        root_map,
        sub_dict,
        validate_shape,
        events,
        datums,
    ):
        super().__init__(
            run=run,
            stream_name=stream_name,
            cutoff_seq_num=2,
            event_descriptors=event_descriptors,
            event_collection=None,  # Below we overrride the methods that would use this.
            root_map=root_map,
            sub_dict=sub_dict,
            validate_shape=validate_shape,
        )
        self._events = events
        self._table = pandas.DataFrame(
            [ev[sub_dict] for ev in events], index=[ev["seq_num"] for ev in events]
        )
        self._datums_by_resource = collections.defaultdict(list)
        self._datums_by_uid = {}
        for d in datums:
            self._datums_by_resource[d["resource"]].append(d)
            self._datums_by_uid[d["datum_id"]] = d

    def _get_columns(self, keys, slices):
        if slices is None:
            min_seq_num = 1
            max_seq_num = 1 + len(self._events)
        else:
            slice_ = slices[0]
            min_seq_num = 1 + slice_.start
            max_seq_num = 1 + slice_.stop

        to_stack = self._inner_get_columns(tuple(keys), min_seq_num, max_seq_num)

        result = {}
        for key, value in to_stack.items():
            array = numpy.stack(value)
            if slices:
                sliced_array = array[(..., *slices[1:])]
            else:
                sliced_array = array
            result[key] = sliced_array

        return result

    @functools.lru_cache(maxsize=1024)
    def _inner_get_columns(self, keys, min_seq_num, max_seq_num):
        columns = {
            key: self._table[key].loc[min_seq_num : 1 + max_seq_num]  # noqa: E203
            for key in keys
        }
        # IMPORTANT: Access via self.metadata so that transforms are applied.
        descriptors = self.metadata()["descriptors"]
        descriptor_uids = [doc["uid"] for doc in descriptors]
        # The `data_keys` in a series of Event Descriptor documents with the
        # same `name` MUST be alike, so we can just use the first one.
        data_keys = [descriptors[0]["data_keys"][key] for key in keys]
        is_externals = ["external" in data_key for data_key in data_keys]
        expected_shapes = [tuple(data_key["shape"] or []) for data_key in data_keys]

        # If data is external, we now have a column of datum_ids, and we need
        # to look up the data that they reference.
        to_stack = collections.defaultdict(list)
        # Any arbitrary valid descriptor uid will work; we just need to satisfy
        # the Filler with our mocked Event below. So we pick the first one.
        descriptor_uid = descriptor_uids[0]
        for key, expected_shape, is_external in zip(
            keys, expected_shapes, is_externals
        ):
            column = columns[key]
            if is_external:
                filled_column = []
                for datum_id in column:
                    # HACK to adapt Filler which is designed to consume whole,
                    # streamed documents, to this column-based access mode.
                    mock_event = {
                        "data": {key: datum_id},
                        "descriptor": descriptor_uid,
                        "uid": "PLACEHOLDER",
                        "filled": {key: False},
                    }
                    filled_mock_event = _fill(
                        self._run.filler,
                        mock_event,
                        # we have an in-memory version of all the datum already
                        lambda datum_uid: self._datums_by_uid[datum_id]["resource"],
                        # let it fallback to the normal behavior
                        self._run.get_resource,
                        # we have an in-memory mapping
                        lambda resource_uid: self._datums_by_resource[resource_uid],
                        last_datum_id=None,
                    )
                    filled_data = filled_mock_event["data"][key]
                    validated_filled_data = default_validate_shape(
                        key, filled_data, expected_shape
                    )
                    filled_column.append(validated_filled_data)
                to_stack[key].extend(filled_column)
            else:
                to_stack[key].extend(column)

        return to_stack

    @functools.lru_cache(maxsize=1024)
    def _get_time_coord(self, slice_params):
        if slice_params is None:
            min_seq_num = 1
            max_seq_num = self._cutoff_seq_num
        else:
            min_seq_num = 1 + slice_params[0]
            max_seq_num = 1 + slice_params[1]
        times = [
            ev["time"]
            for ev in self._events[min_seq_num - 1 : max_seq_num]  # noqa: E203
        ]
        return numpy.array(times)


class BlueskyEventStreamLocal(BlueskyEventStream):
    def __init__(
        self, *args, run, events, event_collection=None, cutoff_seq_num=2, **kwargs
    ):
        # The base class requires an event_collection but it won't be used,
        # so we pass None.
        super().__init__(
            *args, cutoff_seq_num=2, run=run, event_collection=None, **kwargs
        )
        self._events = events

    def new_variation(self, **kwargs):
        res = super().new_variation(
            events=self._events,
            **kwargs,
        )
        return res

    def iter_descriptors_and_events(self):
        for descriptor in sorted(
            self.metadata()["descriptors"], key=lambda d: d["time"]
        ):
            yield ("descriptor", descriptor)
            for ev in self._events:
                if ev["descriptor"] == descriptor["uid"]:
                    yield ("event", ev)


class PatchedTree(_Tree):
    def _build_extra_stream(self, *, run_start, descriptors, datums, events):
        stream_name = descriptors[0]["name"]
        run_start_uid = run_start["uid"]
        run = self[run_start_uid]
        ts = collections.defaultdict(list)
        for ev in events:
            for k, v in ev["timestamps"].items():
                ts[k].append(v)

        mapping = OneShotCachedMap(
            {
                # The construction of DatasetFromLocal must be deferred in this
                # lazy OneShotCachedMap because we
                # haven't finished constructing `run` at this point.
                "data": lambda: DatasetFromLocal(
                    run=run,
                    stream_name=stream_name,
                    event_descriptors=descriptors,
                    root_map=self.root_map,
                    sub_dict="data",
                    events=events,
                    datums=datums,
                    validate_shape=self.validate_shape,
                ),
                # transpose the time stamps
                "timestamps": lambda: DatasetAdapter(xarray.Dataset(ts)),
                "config": lambda: DatasetAdapter(xarray.Dataset({})),
                "config_timestamps": lambda: DatasetAdapter(xarray.Dataset({})),
            }
        )

        metadata = {"descriptors": descriptors, "stream_name": stream_name}
        return BlueskyEventStreamLocal(
            mapping,
            serializer=self._serializer,
            clear_from_cache=lambda: self._clear_from_cache(run_start_uid),
            metadata=metadata,
            events=events,
            run=run,
        )

    def _build_run(self, run_start_doc):
        "This should not be called directly, even internally. Use _get_run."
        # Instantiate a BlueskyRun for this run_start_doc.
        uid = run_start_doc["uid"]
        # This may be None; that's fine.
        run_stop_doc = self._get_stop_doc(uid)
        stream_names = self._event_descriptor_collection.distinct(
            "name",
            {"run_start": uid},
        )
        mapping = {}
        for stream_name in stream_names:
            mapping[stream_name] = functools.partial(
                self._build_event_stream,
                run_start_uid=uid,
                stream_name=stream_name,
                is_complete=(run_stop_doc is not None),
            )
        primary_stream = mapping.get("primary", None)
        all_datums = []
        if primary_stream is not None:
            ...
            # get the resource(s) in that the primary stream references
            resources = list(self._resource_collection.find({"run_start": uid}))

            # TODO pull this from the right key in the descriptors in the primary stream
            desc = next(
                self._event_descriptor_collection.find(
                    {"run_start": uid, "name": "primary"}
                )
            )
            eiger_keys = {
                k: v
                for k, v in desc["data_keys"].items()
                if "eiger" in k.lower() and len(v["shape"]) > 0
            }
            for field, eiger_key in eiger_keys.items():
                image_shape = eiger_key["shape"]
                for res in resources:
                    if "eiger" not in res["spec"].lower():
                        # for anything that is not an eiger, bail
                        continue
                    # count how many datum exist
                    current_datum_count = self._datum_collection.count_documents(
                        {"resource": res["uid"]}
                    )
                    datum = next(self._datum_collection.find({"resource": res["uid"]}))
                    seq_id = datum["datum_kwargs"]["seq_id"]
                    datums = {}
                    counter = itertools.count(current_datum_count + 1)
                    data_keys = format_data_keys(image_shape[1:])
                    for k in data_keys:
                        datums[k] = compose_datum(
                            resource=res,
                            counter=counter,
                            # this assumes that pulling the metadata from the first file
                            # is good enough for all of them!  In the original code we are
                            # constantly re-reading an over-writing the values.
                            datum_kwargs={"metadata_key": k, "seq_id": seq_id},
                        )

                    # Deal with backward-incompatible argument name change in
                    # https://github.com/bluesky/event-model/pull/257
                    if PLURAL_EVENT_COUNTERS:
                        kwargs = {"event_counters": {}}
                    else:
                        kwargs = {"event_counter": {}}
                    desc_bundle = compose_descriptor(
                        start=run_start_doc,
                        streams={},
                        name=f"{field}_metadata_patched_in_at_runtime",
                        data_keys=data_keys,
                        **kwargs,
                    )
                    ts = run_start_doc["time"]
                    ev = desc_bundle.compose_event(
                        data={k: v["datum_id"] for k, v in datums.items()},
                        timestamps={k: ts for k in datums},
                        filled={k: False for k in datums},
                    )
                    mapping[desc_bundle.descriptor_doc["name"]] = functools.partial(
                        self._build_extra_stream,
                        run_start=run_start_doc,
                        descriptors=[desc_bundle.descriptor_doc],
                        datums=list(datums.values()),
                        events=[ev],
                    )
                    all_datums.extend(datums.values())

        return PatchedBlueskyRun(
            OneShotCachedMap(mapping),
            metadata={"start": run_start_doc, "stop": run_stop_doc},
            serializer=self.get_serializer(),
            clear_from_cache=lambda: self._clear_from_cache(uid),
            handler_registry=self.handler_registry,
            transforms=copy.copy(self.transforms),
            root_map=copy.copy(self.root_map),
            datum_collection=self._datum_collection,
            resource_collection=self._resource_collection,
            extra_datums=all_datums,
        )


def format_data_keys(image_shape):
    data_keys = {
        "y_pixel_size": {
            "dtype": "number",
            "dtype_str": "<f8",
            "source": "eiger_file:entry/instrument/detector/y_pixel_size",
            "shape": [],
            "external": "FILESTORE:",
        },
        "x_pixel_size": {
            "dtype": "number",
            "dtype_str": "<f8",
            "source": "eiger_file:entry/instrument/detector/x_pixel_size",
            "shape": [],
            "external": "FILESTORE:",
        },
        "detector_distance": {
            "dtype": "number",
            "dtype_str": "<f8",
            "source": "eiger_file:entry/instrument/detector/detector_distance",
            "shape": [],
            "external": "FILESTORE:",
        },
        "incident_wavelength": {
            "dtype": "number",
            "dtype_str": "<f8",
            "source": "eiger_file:entry/instrument/beam/incident_wavelength",
            "shape": [],
            "external": "FILESTORE:",
        },
        "frame_time": {
            "dtype": "number",
            "dtype_str": "<f8",
            "source": "eiger_file:entry/instrument/detector/frame_time",
            "shape": [],
            "external": "FILESTORE:",
        },
        "beam_center_x": {
            "dtype": "number",
            "dtype_str": "<f8",
            "source": "eiger_file:entry/instrument/detector/beam_center_x",
            "shape": [],
            "external": "FILESTORE:",
        },
        "beam_center_y": {
            "dtype": "number",
            "dtype_str": "<f8",
            "source": "eiger_file:entry/instrument/detector/beam_center_y",
            "shape": [],
            "external": "FILESTORE:",
        },
        "count_time": {
            "dtype": "number",
            "dtype_str": "<f8",
            "source": "eiger_file:entry/instrument/detector/count_time",
            "shape": [],
            "external": "FILESTORE:",
        },
        "pixel_mask": {
            "dtype": "integer",
            "dtype_str": "<u4",
            "source": "eiger_file:entry/instrument/detector/detectorSpecific/pixel_mask",
            "shape": image_shape,
            "external": "FILESTORE:",
        },
        "binary_mask": {
            "dtype": "integer",
            "dtype_str": "|b1",
            "source": "eiger_file:computed_from_pixel_mask",
            "shape": image_shape,
            "external": "FILESTORE:",
        },
    }
    return data_keys


class MetaDataAwareEiger(EigerHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._md_cache = None

    def __call__(self, seq_id, frame_num=None, *, metadata_key=None):
        """
        This returns data contained in the file.

        Parameters
        ----------
        seq_id: int
            The sequence id of the data

        frame_num: int or None
            If not None, return the frame_num'th image from this
            3D array. Useful for when an event is one image rather
            than a stack. (Editor's note: It's not clear what this original
            docstring was supposed to mean. Is it *ever* not None?)

        Returns
        -------
            A dask array
        """
        master_path = Path(f"{self._file_prefix}_{seq_id}_master.h5").absolute()
        try:
            file = self._files[master_path]
        except KeyError:
            file = h5py.File(master_path, "r")
            self._files[master_path] = file

        # if we are asking for not the primary data
        if metadata_key is not None:
            if frame_num is not None:
                raise RuntimeError("this is impossible")
            if self._md_cache is None:
                data_keys = format_data_keys((2, 2))
                md = {k: file[v][()] for k, v in self.EIGER_MD_LAYOUT.items()}
                for k, v in list(md.items()):
                    if data_keys.get(k)["dtype_str"] == "<f8":
                        md[k] = v.astype("<f8")
                # the pixel mask from the eiger contains:
                # 1  -- gap
                # 2  -- dead
                # 4  -- under-responsive
                # 8  -- over-responsive
                # 16 -- noisy
                pixel_mask = md["pixel_mask"]
                md["pixel_mask"] = dask.array.from_array(pixel_mask)
                # pixel_mask[pixel_mask>0] = 1
                # pixel_mask[pixel_mask==0] = 2
                # pixel_mask[pixel_mask==1] = 0
                # pixel_mask[pixel_mask==2] = 1
                md["binary_mask"] = dask.array.from_array(pixel_mask == 0)
                md["framerate"] = 1.0 / float(md["frame_time"])
                self._md_cache = md
            return self._md_cache[metadata_key]

        # the primary resurrts
        try:
            # Eiger firmware v1.3.0 and onwards
            entry = file["entry"]["data"]
        except KeyError:
            # Older firmwares
            entry = file["entry"]

        # Each 'master' file references multiple 'data' files.
        # We just need to know many there are, but here we make
        # a sorted list of their names because it can be handy
        # when things break and we need to debug.
        data_files = sorted([key for key in entry.keys() if key.startswith("data")])

        to_concatenate = []
        for k in data_files:
            dataset = entry[k]
            da = dask.array.from_array(dataset)
            to_concatenate.append(da)
        stack = dask.array.concatenate(to_concatenate)
        if frame_num is None:
            return stack
        else:
            return stack[frame_num % self.images_per_file]


null = None
