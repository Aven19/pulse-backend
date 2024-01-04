"""This file used to define custom commands.
    ex. python manage.py seed_default_category
"""
from app import app
from app import COGNITO_CLIENT
from app import config_data
from app import db
from app.helpers.constants import ResponseMessageKeys
from app.models.user import User
import click
from flask_migrate import Migrate
from flask_migrate import MigrateCommand
from flask_script import Manager
migrate = Migrate(app=app, db=db)
# keeps track of all the commands and handles how they are called from the command line
manager = Manager(app)
manager.add_command('db', MigrateCommand)  # type: ignore  # noqa: FKA100


@manager.command
def create_super_user():
    """ This command is used for creating superuser(admin). """
    try:
        user_details = User.get_by_email(
            config_data.get('SUPERADMIN.PRIMARY_EMAIL'))
        if not user_details:
            # Add User to Database
            user_details = User(email_id=config_data.get(
                'SUPERADMIN.PRIMARY_EMAIL'), password=config_data.get('SUPERADMIN.PIN'))
            user_details.save()

        # Add User in Cognito User Pool
        COGNITO_CLIENT.admin_create_user(
            UserPoolId=config_data.get('COGNITO_USER_POOL_ID'),
            Username=config_data.get('SUPERADMIN.PRIMARY_EMAIL'),
            UserAttributes=[
                {'Name': 'email',
                    'Value': config_data.get('SUPERADMIN.PRIMARY_EMAIL')},
            ],
            ForceAliasCreation=True,
            MessageAction='SUPPRESS',
            DesiredDeliveryMediums=[
                'EMAIL',
            ],
        )

        # Set User Password
        COGNITO_CLIENT.admin_set_user_password(
            UserPoolId=config_data.get('COGNITO_USER_POOL_ID'),
            Username=config_data.get('SUPERADMIN.PRIMARY_EMAIL'),
            Password=config_data.get('SUPERADMIN.PIN'),
            Permanent=True | False,
        )

        click.echo(ResponseMessageKeys.USER_ADDED_SUCCESSFULLY.value)
        return None

    except COGNITO_CLIENT.exceptions.UsernameExistsException as exception_error:
        raise click.UsageError('\n%s:%s' % (
            ResponseMessageKeys.USER_ALREADY_EXIST.value, exception_error))

    except Exception as exception_error:
        raise click.UsageError('\n%s:%s' % (
            ResponseMessageKeys.FAILED.value, exception_error))


@manager.command
def truncate_tables():
    """This command is used to truncate all the mentioned tables in the database and reset auto-increment IDs."""

    from sqlalchemy import text

    # tables = ['user', 'account', 'user_account']
    tables = [
        # 'az_report',
        # 'queue_task'
        # 'az_item_master',
        # 'az_sales_traffic_summary',
        # 'az_sales_traffic_asin',
        # 'az_order_report',
        # 'az_settlement_v2',
        # 'az_financial_event',
        # 'az_product_performance'
        # 'az_ledger_summary',
        # 'az_fba_returns',
        # 'az_fba_reimbursements',
        # 'az_fba_customer_shipment_sales',
        # 'az_sponsored_brand',
        # 'az_sponsored_display',
        # 'az_sponsored_product',
        # 'subscription',
        # 'plan',
        # 'payment'
    ]

    for table in tables:
        db.session.execute(
            text(f'TRUNCATE TABLE "{table}" RESTART IDENTITY CASCADE'))

    db.session.commit()

    # delete_all_cognito_users()


def delete_all_cognito_users():
    """Delete all user's from cognito"""

    from app import COGNITO_CLIENT

    # List all users in the user pool
    response = COGNITO_CLIENT.list_users(
        UserPoolId=config_data.get('COGNITO_USER_POOL_ID'))

    # Delete each user
    for user in response['Users']:
        username = user['Username']
        COGNITO_CLIENT.admin_delete_user(UserPoolId=config_data.get(
            'COGNITO_USER_POOL_ID'), Username=username)


@manager.command
def update_postal_code_master_zones():
    """This command is used to update postal code master zones."""

    from sqlalchemy import text

    # Update records with the specified conditions
    query = text(
        'UPDATE postal_code_master '
        'SET state_name = :new_state, zone = :new_zone '
        "WHERE state_name IN ('UTTAR PRADESH - zone A', 'UTTAR PRADESH - zone B')"
    )

    # Execute the update query
    db.session.execute(query, {'new_state': 'UTTAR PRADESH', 'new_zone': 'North zone'})  # type: ignore  # noqa: FKA100

    # Commit the transaction
    db.session.commit()


@manager.command
def get_count_for_tables():
    """Get the count of records for each table."""

    from sqlalchemy import text

    tables = ['queue_task', 'az_report', 'az_item_master',
              'az_sales_traffic_summary', 'az_sales_traffic_asin', 'az_order_report',
              'az_settlement_v2', 'az_financial_event', 'az_product_performance', 'az_ledger_summary']
    counts = {}

    for table in tables:
        result = db.session.execute(text(f'SELECT COUNT(ID) FROM "{table}"'))
        count = result.fetchone()[0]
        counts[table] = count

    return counts


@manager.command
def del_report_by_type():
    """This command is used to truncate all the mentioned tables in the database and reset auto-increment IDs."""

    from sqlalchemy import text
    from app.helpers.constants import ASpReportType

    table = 'az_report'
    report_type = ASpReportType.SALES_TRAFFIC_REPORT.value

    # Use a placeholder for the report_type value to avoid SQL injection
    query = text(f'DELETE FROM "{table}" WHERE type = :report_type')

    # Execute the query with the parameter
    db.session.execute(query, {'report_type': report_type})  # type: ignore  # noqa: FKA100

    db.session.commit()


@manager.command
def update_ads_account_cred():
    """This command updates specific columns in the 'account' table to NULL."""

    from sqlalchemy import text

    table = 'account'

    columns_to_update = ['az_ads_profile_ids',
                         'az_ads_account_info', 'az_ads_credentials']

    query = text(f'UPDATE "{table}" SET '
                 + ', '.join(f'{col} = NULL' for col in columns_to_update))

    db.session.execute(query)

    db.session.commit()


@manager.command
def add_free_trial_subscription():
    """Function to test worker create amazon reports"""

    from app.helpers.constants import EcommPulsePlanName
    from app.models.subscription import Subscription
    from app.models.user import User
    from app.models.account import Account
    from app.models.plan import Plan
    from datetime import datetime
    from datetime import timedelta
    from app.helpers.constants import SubscriptionStates

    email_id = 'aven.mathias@bombaysoftwares.com'

    user = User.get_by_email(email_id)

    if user:
        user_id = user.id
        account = Account.get_by_user_id(primary_user_id=user_id)
        if account:
            account_id = account.uuid

            trial_plan_value = EcommPulsePlanName.ECOMM_FREE_TRAIL.value

            free_subscription = Subscription.get_by_reference_subscription_id(
                account_id=account_id, reference_subscription_id=trial_plan_value)

            if free_subscription is None:
                """Check for Free Plan"""
                get_free_plan = Plan.get_by_reference_plan_id(
                    reference_plan_id=trial_plan_value)
                if get_free_plan:
                    start_date = datetime.now()
                    end_date = start_date + timedelta(days=10)
                    start_date_iso = start_date.isoformat()
                    end_date_iso = end_date.isoformat()
                    Subscription.add_update(account_id=account_id, reference_subscription_id=get_free_plan.reference_plan_id, plan_id=get_free_plan.id, payment_id=0,
                                            status=SubscriptionStates.ACTIVE.value, start_date=start_date_iso, end_date=end_date_iso)


@manager.command
def create_test_report():
    """Function to test worker create amazon reports"""

    from workers.asp_report_worker import AspReportWorker

    AspReportWorker.create_reports()


if __name__ == '__main__':
    manager.run()
