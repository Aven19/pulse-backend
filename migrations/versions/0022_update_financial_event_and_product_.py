"""Update Financial Event and Product performance Table

Revision ID: 0022
Revises: 0021
Create Date: 2023-08-04 00:56:54.430700

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0022'
down_revision = '0021'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('az_financial_event', sa.Column('seller_sku', sa.String(length=255), nullable=True))
    op.add_column('az_product_performance', sa.Column('summary_analysis', sa.JSON(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('az_product_performance', 'summary_analysis')
    op.drop_column('az_financial_event', 'seller_sku')
    # ### end Alembic commands ###