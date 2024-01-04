"""update user invite table

Revision ID: 0031
Revises: 0030
Create Date: 2023-09-16 17:58:24.629495

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '0031'
down_revision = '0030'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('user_invite', sa.Column('invited_by_user_id', sa.Integer(), nullable=False))
    op.add_column('user_invite', sa.Column('invited_by_account_id', sa.Integer(), nullable=False))
    op.add_column('user_invite', sa.Column('brand', postgresql.ARRAY(sa.String()), nullable=False, server_default='{}'))
    op.add_column('user_invite', sa.Column('category', postgresql.ARRAY(sa.String()), nullable=False, server_default='{}'))
    op.drop_column('user_invite', 'invited_by')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('user_invite', sa.Column('invited_by', sa.VARCHAR(length=255), autoincrement=False, nullable=False))
    op.drop_column('user_invite', 'category')
    op.drop_column('user_invite', 'brand')
    op.drop_column('user_invite', 'invited_by_account_id')
    op.drop_column('user_invite', 'invited_by_user_id')

    # ### end Alembic commands ###