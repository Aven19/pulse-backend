"""update plans table

Revision ID: 0035
Revises: 0034
Create Date: 2023-09-18 17:50:29.771063

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0035'
down_revision = '0034'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('plans', sa.Column('monthly_price', sa.Numeric(), nullable=True))
    op.add_column('plans', sa.Column('yearly_price', sa.Numeric(), nullable=True))
    op.add_column('plans', sa.Column('discount', sa.Numeric(), nullable=True))
    op.drop_column('plans', 'price')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('plans', sa.Column('price', sa.NUMERIC(), autoincrement=False, nullable=True))
    op.drop_column('plans', 'discount')
    op.drop_column('plans', 'yearly_price')
    op.drop_column('plans', 'monthly_price')
    # ### end Alembic commands ###
