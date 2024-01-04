"""Update subscription and payment table

Revision ID: 0046
Revises: 0045
Create Date: 2023-10-04 00:56:05.067571

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0046'
down_revision = '0045'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('payment', 'account_id',
               existing_type=sa.BIGINT(),
               type_=sa.String(length=36),
               existing_nullable=False)
    op.alter_column('subscription', 'account_id',
               existing_type=sa.BIGINT(),
               type_=sa.String(length=36),
               existing_nullable=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('subscription', 'account_id',
               existing_type=sa.String(length=36),
               type_=sa.BIGINT(),
               existing_nullable=False)
    op.alter_column('payment', 'account_id',
               existing_type=sa.String(length=36),
               type_=sa.BIGINT(),
               existing_nullable=False)
    # ### end Alembic commands ###