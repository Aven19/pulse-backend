"""Create Amazon Order Item Table

Revision ID: 0013
Revises: 0012
Create Date: 2023-07-06 12:38:34.307699

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0013'
down_revision = '0012'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('az_order_item',
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.Column('selling_partner_id', sa.String(length=255), nullable=True),
    sa.Column('amazon_order_id', sa.String(length=255), nullable=True),
    sa.Column('product_info_number_of_items', sa.Integer(), nullable=True),
    sa.Column('item_tax_currency_code', sa.String(length=20), nullable=True),
    sa.Column('item_tax_amount', sa.Numeric(), nullable=True),
    sa.Column('quantity_shipped', sa.Integer(), nullable=True),
    sa.Column('item_price_currency_code', sa.String(length=20), nullable=True),
    sa.Column('item_price_amount', sa.Numeric(), nullable=True),
    sa.Column('asin', sa.String(length=255), nullable=True),
    sa.Column('seller_sku', sa.String(length=255), nullable=True),
    sa.Column('title', sa.String(length=255), nullable=True),
    sa.Column('is_gift', sa.Boolean(), nullable=True),
    sa.Column('is_transparency', sa.Boolean(), nullable=True),
    sa.Column('quantity_ordered', sa.Integer(), nullable=True),
    sa.Column('promotion_discount_tax_currency_code', sa.String(length=20), nullable=True),
    sa.Column('promotion_discount_tax_amount', sa.Numeric(), nullable=True),
    sa.Column('promotion_ids', sa.String(length=255), nullable=True),
    sa.Column('promotion_discount_currency_code', sa.String(length=20), nullable=True),
    sa.Column('promotion_discount_amount', sa.Numeric(), nullable=True),
    sa.Column('order_item_id', sa.String(length=255), nullable=True),
    sa.Column('created_at', sa.BigInteger(), nullable=False),
    sa.Column('updated_at', sa.BigInteger(), nullable=True),
    sa.Column('deleted_at', sa.BigInteger(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('az_order_item')
    # ### end Alembic commands ###