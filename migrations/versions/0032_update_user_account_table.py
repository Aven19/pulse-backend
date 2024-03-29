"""update_user_account_table

Revision ID: 0032
Revises: 0031
Create Date: 2023-09-16 18:01:27.537529

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '0032'
down_revision = '0031'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('user_account', sa.Column('brand', postgresql.ARRAY(sa.String()), nullable=False, server_default='{}'))
    op.add_column('user_account', sa.Column('category', postgresql.ARRAY(sa.String()), nullable=False, server_default='{}'))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('user_account', 'category')
    op.drop_column('user_account', 'brand')
    # ### end Alembic commands ###
