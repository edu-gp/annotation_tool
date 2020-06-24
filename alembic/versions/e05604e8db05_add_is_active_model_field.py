"""add_is_active_model_field

Revision ID: e05604e8db05
Revises: 3360fe3522aa
Create Date: 2020-06-16 18:08:15.324002

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e05604e8db05'
down_revision = '3360fe3522aa'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('model', schema=None) as batch_op:
        batch_op.add_column(sa.Column('is_active', sa.Boolean(), nullable=True))

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('model', schema=None) as batch_op:
        batch_op.drop_column('is_active')

    # ### end Alembic commands ###