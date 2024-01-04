"""update account and user_account table

Revision ID: 0049
Revises: 0048
Create Date: 2023-10-10 13:23:21.863757

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0049'
down_revision = '0048'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('account', sa.Column('deactivated_at', sa.BigInteger(), nullable=True))
    op.add_column('user_account', sa.Column('deactivated_at', sa.BigInteger(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('user_account', 'deactivated_at')
    op.drop_column('account', 'deactivated_at')
    # ### end Alembic commands ###