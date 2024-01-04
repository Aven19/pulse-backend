"""Update payment and susbscription table

Revision ID: 0057
Revises: 0056
Create Date: 2023-10-30 15:26:39.258183

"""
# from app import db
import sqlalchemy as sa
# from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = '0057'
down_revision = '0056'
branch_labels = None
depends_on = None


def upgrade():
    # tables = [
    #     'subscription',
    #     'payment'
    # ]

    # for table in tables:
    #     db.session.execute(
    #         text(f'TRUNCATE TABLE "{table}" RESTART IDENTITY CASCADE'))

    # db.session.commit()
    pass

def downgrade():
    pass
