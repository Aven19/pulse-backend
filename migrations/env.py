import logging
from logging.config import fileConfig

from alembic import context
from flask import current_app

from app.models import user  # noqa nosort
from app.models import account  # noqa nosort
from app.models import user_account  # noqa nosort
from app.models import attachment  # noqa nosort
from app.models import queue_task  # noqa nosort
from app.models import az_report  # noqa nosort
from app.models import az_item_master  # noqa nosort
from app.models import az_order_report  # noqa nosort
from app.models import az_settlement_v2  # noqa nosort
from app.models import az_sales_traffic_asin  # noqa nosort
from app.models import az_sales_traffic_summary  # noqa nosort
from app.models import az_return  # noqa nosort
from app.models import az_settlement # noqa nosort
from app.models import az_ledger_summary  # noqa nosort
from app.models import az_stock_transfer_adhoc  # noqa nosort
from app.models import az_orders # noqa nosort
from app.models import az_order_items # noqa nosort
from app.models import az_fba_replacement  # noqa nosort
from app.models import az_sponsored_brand  # noqa nosort
from app.models import az_sponsored_display  # noqa nosort
from app.models import az_sponsored_product  # noqa nosort
from app.models import az_financial_event  # noqa nosort
from app.models import az_product_performance  # noqa nosort
from app.models import postal_code_master  # noqa nosort
from app.models import az_fba_returns  # noqa nosort
from app.models import az_fba_reimbursements  # noqa nosort
from app.models import az_fba_customer_shipment_sales  # noqa nosort
from app.models import az_performance_zone  # noqa nosort
from app.models import user_invite # noqa nosort
from app.models import plan # noqa nosort
from app.models import subscription # noqa nosort
from app.models import payment # noqa nosort


# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)
logger = logging.getLogger('alembic.env')


def get_engine():
    try:
        # this works with Flask-SQLAlchemy<3 and Alchemical
        return current_app.extensions['migrate'].db.get_engine()
    except TypeError:
        # this works with Flask-SQLAlchemy>=3
        return current_app.extensions['migrate'].db.engine


def get_engine_url():
    try:
        return get_engine().url.render_as_string(hide_password=False).replace(
            '%', '%%')
    except AttributeError:
        return str(get_engine().url).replace('%', '%%')


# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
config.set_main_option('sqlalchemy.url', get_engine_url())
target_db = current_app.extensions['migrate'].db

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def get_metadata():
    if hasattr(target_db, 'metadatas'):
        return target_db.metadatas[None]
    return target_db.metadata


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option('sqlalchemy.url')
    context.configure(
        url=url, target_metadata=get_metadata(), literal_binds=True
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """

    # this callback is used to prevent an auto-migration from being generated
    # when there are no changes to the schema
    # reference: http://alembic.zzzcomputing.com/en/latest/cookbook.html
    def process_revision_directives(context, revision, directives):
        if getattr(config.cmd_opts, 'autogenerate', False):
            script = directives[0]
            if script.upgrade_ops.is_empty():
                directives[:] = []
                logger.info('No changes in schema detected.')

    connectable = get_engine()

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=get_metadata(),
            process_revision_directives=process_revision_directives,
            **current_app.extensions['migrate'].configure_args
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
