"""Initial Migrations

Revision ID: 0001
Revises:
Create Date: 2023-07-06 12:31:47.360485

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('account',
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.Column('uuid', sa.String(length=36), nullable=False),
    sa.Column('legal_name', sa.String(length=100), nullable=True),
    sa.Column('display_name', sa.String(length=100), nullable=True),
    sa.Column('primary_user_id', sa.BigInteger(), nullable=True),
    sa.Column('asp_id', sa.String(length=100), nullable=True),
    sa.Column('asp_credentials', sa.JSON(), nullable=True),
    sa.Column('fsp_id', sa.String(length=100), nullable=True),
    sa.Column('fsp_credentials', sa.JSON(), nullable=True),
    sa.Column('ssp_id', sa.String(length=100), nullable=True),
    sa.Column('ssp_credentials', sa.JSON(), nullable=True),
    sa.Column('az_ads_credentials', sa.JSON(), nullable=True),
    sa.Column('created_at', sa.BigInteger(), nullable=True),
    sa.Column('updated_at', sa.BigInteger(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('uuid')
    )
    op.create_table('attachment',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('entity_type', sa.BigInteger(), nullable=False),
    sa.Column('sub_entity_type', sa.BigInteger(), nullable=True),
    sa.Column('entity_id', sa.BigInteger(), nullable=True),
    sa.Column('sub_entity_id', sa.BigInteger(), nullable=True),
    sa.Column('name', sa.String(length=100), nullable=False),
    sa.Column('path', sa.Text(), nullable=False),
    sa.Column('size', sa.Text(), nullable=False),
    sa.Column('description', sa.Text(), nullable=True),
    sa.Column('created_at', sa.BigInteger(), nullable=False),
    sa.Column('updated_at', sa.BigInteger(), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('queue_task',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('owner_id', sa.BigInteger(), nullable=False),
    sa.Column('account_id', sa.String(length=36), nullable=False),
    sa.Column('queue_name', sa.Text(), nullable=False),
    sa.Column('status', sa.Integer(), nullable=False),
    sa.Column('input_attachment_id', sa.BigInteger(), nullable=True),
    sa.Column('output_attachment_id', sa.BigInteger(), nullable=True),
    sa.Column('created_at', sa.BigInteger(), nullable=False),
    sa.Column('entity_type', sa.BigInteger(), nullable=True),
    sa.Column('param', sa.Text(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('user',
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.Column('first_name', sa.String(length=100), nullable=True),
    sa.Column('last_name', sa.String(length=100), nullable=True),
    sa.Column('email', sa.String(length=255), nullable=True),
    sa.Column('password', sa.String(length=255), nullable=True),
    sa.Column('google_auth', sa.JSON(), nullable=True),
    sa.Column('amazon_auth', sa.JSON(), nullable=True),
    sa.Column('last_login_at', sa.BigInteger(), nullable=True),
    sa.Column('created_at', sa.BigInteger(), nullable=True),
    sa.Column('updated_at', sa.BigInteger(), nullable=True),
    sa.Column('deactivated_at', sa.BigInteger(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('email')
    )
    op.create_table('user_account',
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.Column('user_id', sa.BigInteger(), nullable=True),
    sa.Column('account_id', sa.BigInteger(), nullable=True),
    sa.Column('created_at', sa.BigInteger(), nullable=True),
    sa.Column('updated_at', sa.BigInteger(), nullable=True),
    sa.ForeignKeyConstraint(['account_id'], ['account.id'], ),
    sa.ForeignKeyConstraint(['user_id'], ['user.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('user_account')
    op.drop_table('user')
    op.drop_table('queue_task')
    op.drop_table('attachment')
    op.drop_table('account')
    # ### end Alembic commands ###
