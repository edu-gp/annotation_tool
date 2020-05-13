"""AnnotationGuide data column

Revision ID: 1dbed6f30db0
Revises: 19687a7ba1e1
Create Date: 2020-05-12 08:47:54.847498

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1dbed6f30db0'
down_revision = '19687a7ba1e1'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('annotation_guide', schema=None) as batch_op:
        batch_op.add_column(sa.Column('data', sa.JSON(), nullable=True))
        batch_op.drop_column('url')

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('annotation_guide', schema=None) as batch_op:
        batch_op.add_column(sa.Column('url', sa.TEXT(), nullable=True))
        batch_op.drop_column('data')

    # ### end Alembic commands ###