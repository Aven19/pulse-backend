"""Added account_id and asp_id columsn to table

Revision ID: 0020
Revises: 0019
Create Date: 2023-07-25 20:18:58.308184

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0020'
down_revision = '0019'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('az_fba_replacement', sa.Column('account_id', sa.String(length=36), nullable=False))
    op.add_column('az_fba_replacement', sa.Column('asp_id', sa.String(length=255), nullable=True))
    op.add_column('az_fba_return', sa.Column('account_id', sa.String(length=36), nullable=False))
    op.add_column('az_fba_return', sa.Column('asp_id', sa.String(length=255), nullable=True))
    op.add_column('az_item_master', sa.Column('asp_id', sa.String(length=255), nullable=True))
    op.add_column('az_ledger_summary', sa.Column('account_id', sa.String(length=36), nullable=False))
    op.add_column('az_ledger_summary', sa.Column('asp_id', sa.String(length=255), nullable=True))
    op.add_column('az_order_report', sa.Column('asp_id', sa.String(length=255), nullable=True))
    op.add_column('az_report', sa.Column('asp_id', sa.String(length=255), nullable=True))
    op.add_column('az_return', sa.Column('account_id', sa.String(length=36), nullable=False))
    op.add_column('az_return', sa.Column('asp_id', sa.String(length=255), nullable=True))
    op.add_column('az_sales_traffic_asin', sa.Column('account_id', sa.String(length=36), nullable=True))
    op.add_column('az_sales_traffic_asin', sa.Column('asp_id', sa.String(length=255), nullable=True))
    op.add_column('az_sales_traffic_summary', sa.Column('account_id', sa.String(length=36), nullable=True))
    op.add_column('az_sales_traffic_summary', sa.Column('asp_id', sa.String(length=255), nullable=True))
    op.add_column('az_settlement', sa.Column('account_id', sa.String(length=36), nullable=False))
    op.add_column('az_settlement', sa.Column('asp_id', sa.String(length=255), nullable=True))
    op.add_column('az_settlement_v2', sa.Column('account_id', sa.String(length=36), nullable=True))
    op.add_column('az_settlement_v2', sa.Column('asp_id', sa.String(length=255), nullable=True))
    op.add_column('az_sponsored_brand', sa.Column('account_id', sa.String(length=36), nullable=False))
    op.add_column('az_sponsored_display', sa.Column('account_id', sa.String(length=36), nullable=False))
    op.add_column('az_sponsored_product', sa.Column('account_id', sa.String(length=36), nullable=False))
    op.add_column('az_stock_transfer_adhoc', sa.Column('account_id', sa.String(length=36), nullable=False))
    op.add_column('az_stock_transfer_adhoc', sa.Column('asp_id', sa.String(length=255), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('az_stock_transfer_adhoc', 'asp_id')
    op.drop_column('az_stock_transfer_adhoc', 'account_id')
    op.drop_column('az_sponsored_product', 'account_id')
    op.drop_column('az_sponsored_display', 'account_id')
    op.drop_column('az_sponsored_brand', 'account_id')
    op.drop_column('az_settlement_v2', 'asp_id')
    op.drop_column('az_settlement_v2', 'account_id')
    op.drop_column('az_settlement', 'asp_id')
    op.drop_column('az_settlement', 'account_id')
    op.drop_column('az_sales_traffic_summary', 'asp_id')
    op.drop_column('az_sales_traffic_summary', 'account_id')
    op.drop_column('az_sales_traffic_asin', 'asp_id')
    op.drop_column('az_sales_traffic_asin', 'account_id')
    op.drop_column('az_return', 'asp_id')
    op.drop_column('az_return', 'account_id')
    op.drop_column('az_report', 'asp_id')
    op.drop_column('az_order_report', 'asp_id')
    op.drop_column('az_ledger_summary', 'asp_id')
    op.drop_column('az_ledger_summary', 'account_id')
    op.drop_column('az_item_master', 'asp_id')
    op.drop_column('az_fba_return', 'asp_id')
    op.drop_column('az_fba_return', 'account_id')
    op.drop_column('az_fba_replacement', 'asp_id')
    op.drop_column('az_fba_replacement', 'account_id')
    # ### end Alembic commands ###
