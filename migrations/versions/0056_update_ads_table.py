"""Update ads table

Revision ID: 0056
Revises: 0055
Create Date: 2023-10-30 15:26:39.258183

"""
import json

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0056'
down_revision = '0055'
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()
    # Get user.id from public.user where email = 'aven.mathias@bombaysoftwares.com'
    user_id = connection.execute(
        sa.text("SELECT id FROM public.user WHERE email = 'aven.mathias@bombaysoftwares.com'")
    ).scalar()

    if user_id:
        # Get az_ads_account_info from account table where primary_user_id = user.id
        az_ads_account_info = connection.execute(
            sa.text(f'SELECT az_ads_account_info FROM account WHERE primary_user_id = {user_id}')
        ).scalar()

        if az_ads_account_info:
            # Process and update az_ads_account_info
            response = []
            for az_ad_profile in az_ads_account_info:
                az_ad_profile['created_by'] = user_id
                response.append(az_ad_profile)

            json_az_ads_account_info = json.dumps(response)

            # Update az_ads_account_info in the account table
            connection.execute(
                sa.text('UPDATE account SET az_ads_account_info = :az_ads_account_info WHERE primary_user_id = :user_id')
                .bindparams(az_ads_account_info=None, user_id=user_id)
            )
            connection.execute(
                sa.text('UPDATE account SET az_ads_account_info = :az_ads_account_info WHERE primary_user_id = :user_id')
                .bindparams(az_ads_account_info=json_az_ads_account_info, user_id=user_id)
            )

def downgrade():
    pass
