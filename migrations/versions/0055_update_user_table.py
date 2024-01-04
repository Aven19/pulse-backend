"""Update user table

Revision ID: 0055
Revises: 0054
Create Date: 2023-10-30 15:26:39.258183

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '0055'
down_revision = '0054'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("UPDATE public.user SET first_name = 'Suhrid', last_name = 'Thacker' WHERE email = 'aven.mathias@bombaysoftwares.com';")

def downgrade():
    op.execute("UPDATE public.user SET first_name = '', last_name = '' WHERE email = 'aven.mathias@bombaysoftwares.com';")
