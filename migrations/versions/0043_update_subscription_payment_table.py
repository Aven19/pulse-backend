"""update subscription payment table

Revision ID: 0043
Revises: 0042
Create Date: 2023-09-29 09:12:36.754777

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '0043'
down_revision = '0042'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('payment',
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.Column('user_id', sa.BigInteger(), nullable=False),
    sa.Column('account_id', sa.BigInteger(), nullable=False),
    sa.Column('amount', sa.Numeric(), nullable=True),
    sa.Column('status', sa.String(length=50), nullable=True),
    sa.Column('verified', sa.String(length=20), nullable=True),
    sa.Column('currency', sa.String(length=20), nullable=True),
    sa.Column('reference_order_id', sa.String(length=255), nullable=True),
    sa.Column('reference_payment_id', sa.String(length=255), nullable=True),
    sa.Column('reference_subscription_id', sa.String(length=255), nullable=True),
    sa.Column('request', sa.JSON(), nullable=True),
    sa.Column('response', sa.JSON(), nullable=True),
    sa.Column('payment_mode', sa.String(length=50), nullable=True),
    sa.Column('created_at', sa.BigInteger(), nullable=True),
    sa.Column('updated_at', sa.BigInteger(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.drop_table('payments')
    op.add_column('subscription', sa.Column('reference_subscription_id', sa.String(length=50), nullable=False))
    op.alter_column('subscription', 'payment_id',
               existing_type=sa.BIGINT(),
               nullable=True)
    op.drop_column('subscription', 'user_id')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('subscription', sa.Column('user_id', sa.BIGINT(), autoincrement=False, nullable=True))
    op.alter_column('subscription', 'payment_id',
               existing_type=sa.BIGINT(),
               nullable=False)
    op.drop_column('subscription', 'reference_subscription_id')
    op.create_table('payments',
    sa.Column('id', sa.BIGINT(), autoincrement=True, nullable=False),
    sa.Column('user_id', sa.BIGINT(), autoincrement=False, nullable=False),
    sa.Column('account_id', sa.BIGINT(), autoincrement=False, nullable=False),
    sa.Column('amount', sa.NUMERIC(), autoincrement=False, nullable=True),
    sa.Column('verified', sa.VARCHAR(length=20), nullable=True),
    sa.Column('currency', sa.VARCHAR(length=20), autoincrement=False, nullable=True),
    sa.Column('subscription_id', sa.VARCHAR(length=255), autoincrement=False, nullable=True),
    sa.Column('request', postgresql.JSON(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('response', postgresql.JSON(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('payment_mode', sa.VARCHAR(length=50), autoincrement=False, nullable=True),
    sa.Column('created_at', sa.BIGINT(), autoincrement=False, nullable=True),
    sa.Column('updated_at', sa.BIGINT(), autoincrement=False, nullable=True),
    sa.PrimaryKeyConstraint('id', name='payments_pkey')
    )
    op.drop_table('payment')
    # ### end Alembic commands ###