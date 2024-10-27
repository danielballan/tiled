"""Add ragged structure

Revision ID: 58b9954f1241
Revises: ed3a4223a600
Create Date: 2024-10-27 17:11:17.262891

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "58b9954f1241"
down_revision = "ed3a4223a600"
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()

    if connection.engine.dialect.name == "postgresql":
        with op.get_context().autocommit_block():
            op.execute(
                sa.text(
                    "ALTER TYPE structurefamily ADD VALUE IF NOT EXISTS 'ragged' AFTER 'container'"
                )
            )


def downgrade():
    # This _could_ be implemented but we will wait for a need since we are
    # still in beta releases.
    raise NotImplementedError
