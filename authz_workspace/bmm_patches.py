import numpy
from PIL import Image


class BMM_JPEG_HANDLER:
    def __init__(self, resource_path):
        # resource_path is really a template string with a %d in it
        self._template = resource_path

    def __call__(self, index=0, point_number=None):
        if point_number is not None:
            filepath = self._template % point_number
        else:
            filepath = self._template % index
        return numpy.asarray(Image.open(filepath))


def patch_descriptor(doc):
    # Add more specific numpy-style data type, "dtype_str", if not present.
    if "usbcam1_image" in doc["data_keys"]:
        doc["data_keys"]["usbcam1_image"]["dtype_str"] = "|u1"
    if "usbcam2_image" in doc["data_keys"]:
        doc["data_keys"]["usbcam2_image"]["dtype_str"] = "|u1"
    if "xascam_image" in doc["data_keys"]:
        doc["data_keys"]["xascam_image"]["dtype_str"] = "|u1"
    if "xrdcam_image" in doc["data_keys"]:
        doc["data_keys"]["xrdcam_image"]["dtype_str"] = "|u1"
    if "anacam_image" in doc["data_keys"]:
        doc["data_keys"]["anacam_image"]["dtype_str"] = "|u1"
    for i in range(1, 5):
        if f"4-element SDD_channel0{i}" in doc["data_keys"]:
            doc["data_keys"][f"4-element SDD_channel0{i}"]["dtype_str"] = "<f8"

    return doc
