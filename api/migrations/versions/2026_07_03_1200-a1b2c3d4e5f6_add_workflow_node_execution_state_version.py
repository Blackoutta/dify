"""add workflow node execution state version

Revision ID: a1b2c3d4e5f6
Revises: 6b5f9f8b1a2c
Create Date: 2026-07-03 12:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

revision = "a1b2c3d4e5f6"
down_revision = "6b5f9f8b1a2c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("workflow_node_executions", schema=None) as batch_op:
        batch_op.add_column(sa.Column("state_version", sa.BigInteger(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("workflow_node_executions", schema=None) as batch_op:
        batch_op.drop_column("state_version")
