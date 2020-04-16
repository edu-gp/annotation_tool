"""remove fake_field

Revision ID: 1f534cffb049
Revises: 044204823448
Create Date: 2020-04-16 22:07:35.299253

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1f534cffb049'
down_revision = '044204823448'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('label') as batch_op:
        batch_op.drop_column('fake_field')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('label', sa.Column('fake_field', sa.VARCHAR(length=64), nullable=True))
    # ### end Alembic commands ###
