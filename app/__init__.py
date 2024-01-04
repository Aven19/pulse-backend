"""This file initializes Application."""
from functools import partial
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import sys

from app.helpers.constants import HttpStatusCode
from app.helpers.constants import QueueName
from app.helpers.constants import ResponseMessageKeys
import boto3
from botocore.client import Config
from cloudwatch import cloudwatch
from cryptography.fernet import Fernet
from flask import Flask
from flask import jsonify
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter import RequestLimit
from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy
from flask_swagger_ui import get_swaggerui_blueprint
import razorpay
import redis
from rq import Queue
from rq_scheduler import Scheduler
import yaml

# base_dir = os.path.dirname(os.path.abspath(__file__))

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)
media_dir = os.path.join(base_dir, 'media')  # type: ignore  # noqa: FKA100
if not os.path.exists(media_dir):
    os.makedirs(media_dir)

# Initialising the configuration variable
environment = os.environ.get('APP_ENV')
ENVIRONMENT_LIST = ['prod', 'staging', 'dev']
AWS_BUCKET = None
if environment in ENVIRONMENT_LIST:
    config_data = yaml.safe_load(
        boto3.client(
            's3').get_object(
            Bucket=f'ep-backend-{environment}-config',
            Key=f'config-{environment}.yaml'
        ).get('Body'))
    AWS_BUCKET = f'ep-backend-{environment}-config'
else:
    with open(file='config/config.yaml', mode='r') as config_file:
        config_data = yaml.load(config_file, Loader=yaml.FullLoader)

S3_RESOURCE = boto3.resource(
    's3',
    config=Config(signature_version='s3v4')
)

# Initializing logging configuration object
formatter = logging.Formatter(
    '%(asctime)s: %(levelname)s {%(filename)s:%(lineno)d} -> %(message)s'
)
logger = logging.getLogger(__name__)

if environment in ENVIRONMENT_LIST:
    handler = cloudwatch.CloudwatchHandler(
        region=config_data.get('CLOUD_WATCH_REGION'),
        log_group=config_data.get('LOG_GROUP'),
        log_stream=config_data.get('LOG_STREAM'))
    handler.setFormatter(formatter)
    logger.addHandler(handler)
else:
    handler = TimedRotatingFileHandler(
        config_data.get('LOG_FILE_PATH'),
        when='midnight',
        interval=1,
        backupCount=7)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

logger.setLevel(logging.INFO)
logger.error = partial(logger.error, exc_info=True)
logger.info = partial(logger.info, exc_info=True)
logger.warning = partial(logger.warning, exc_info=True)
logger.exception = partial(logger.exception, exc_info=True)

fernet_key = Fernet(config_data.get('APP_SECRET').encode('utf-8'))


def ratelimit_handler(request_limit: RequestLimit) -> tuple:
    """
        This method will create custom json response for error 429 (Too Many Requests).
    """
    limit_string = request_limit.limit.limit
    time_limit = str(limit_string).split('per')[1]
    # Here, we get limit value in string like 1 per 30 seconds so, for getting time limit we are splitting above string.
    return jsonify({'status': False,
                    'message': ResponseMessageKeys.PLEASE_TRY_AFTER_SECONDS.value.format(time_limit)}), HttpStatusCode.TOO_MANY_REQUESTS.value


COGNITO_CLIENT = boto3.client(
    'cognito-idp', region_name=config_data.get('COGNITO_REGION')
)

# razorpay client
RAZORPAY_CLIENT = razorpay.Client(
    auth=(config_data.get('RAZORPAY').get('KEY_ID'), config_data.get('RAZORPAY').get('KEY_SECRET')))
RAZORPAY_CLIENT.set_app_details({'title': 'Flask', 'version': '2.2.2'})


def create_app():
    """
    Create a Flask application instance. Register blueprints and updates celery configuration.

        :return: application instance
    """
    try:
        application = Flask(__name__, instance_relative_config=True)
        application.config.from_object(config_data)
        # Global error handler for error code 429 (Too Many Requests)
        application.register_error_handler(429, ratelimit_handler)  # type: ignore  # noqa: FKA100
        app_set_configurations(application=application,
                               config_data=config_data)
        initialize_extensions(application)
        register_blueprints(application)
        register_swagger_blueprints(application)
        if environment not in ENVIRONMENT_LIST:
            CORS(application, resources={r'/api/*': {'origins': '*'}})
        # CORS(application, resources={r'/api/*': {'origins': '*'}})
        return application

    except Exception as exception_error:
        logger.error('Unable to create flask app instance : '
                     + str(exception_error))


def initialize_extensions(application):
    """
    Initialize extensions.
    :param application:
    :return:
    """
    try:
        db.init_app(application)
        migrate = Migrate(app=application, db=db, compare_type=True)
        return db, migrate

    except Exception as exception_error:
        logger.error('Unable to initialize extensions : '
                     + str(exception_error))


def register_blueprints(application):
    """
    Registers blueprints.
    :param application:
    :return: None
    """
    try:
        from app.views import v1_blueprints
        application.register_blueprint(v1_blueprints, url_prefix='/api/v1')
        # application.register_blueprint(v2_blueprints, url_prefix='/api/v2')
        # application.register_blueprint(v3_blueprints, url_prefix='/api/v3')

    except Exception as exception_error:
        logger.error('Unable to register blueprints : '
                     + str(exception_error))


def register_swagger_blueprints(application):
    """
    Registers swagger blueprints.
    :param application:
    :return: None
    """
    try:
        swagger_url = '/api-docs/'
        api_url = '/static/swagger_json/swagger.new.json'
        swagger_config = {'app_name': 'Ecomm Pulse', 'deepLinking': False, }
        swagger_blueprint = get_swaggerui_blueprint(
            base_url=swagger_url,
            api_url=api_url,
            config=swagger_config
        )
        application.register_blueprint(
            swagger_blueprint, url_prefix=swagger_url)

    except Exception as exception_error:
        logger.error('Unable to register blueprints : '
                     + str(exception_error))


def app_set_configurations(application, config_data):
    """This method is used to setting configuration data from config.yml"""
    try:
        for config in config_data:
            application.config[config] = config_data[config]

    except Exception as exception_error:
        logger.error('problem setting app configuration : '
                     + str(exception_error))


def clear_scheduler():
    """ Method to delete scheduled jobs in scheduler. """
    try:
        scheduler = Scheduler(connection=r)
        for job in scheduler.get_jobs():
            scheduler.cancel(job)
    except Exception as exception_error:
        logger.error('Unable to delete or clear scheduled jobs : '
                     + str(exception_error))


app = Flask(__name__)
app_set_configurations(application=app, config_data=config_data)
db = SQLAlchemy(app, session_options={'expire_on_commit': False})
migrate = Migrate(app=app, db=db, compare_type=True)
# CORS(app, resources={r'/api/*': {'origins': '*'}})
r = redis.Redis(host=config_data.get('REDIS').get('HOST'), port=config_data.get(
    'REDIS').get('PORT'), db=config_data.get('REDIS').get('DB'))

# Queue for Creating Amazon reports.
asp_create_report_q = Queue(QueueName.ASP_CREATE_REPORT, connection=r)
asp_create_report_scheduler = Scheduler(
    queue=asp_create_report_q, connection=r)

# Queue for Creating Amazon ADs reports.
az_ads_create_report_q = Queue(QueueName.AZ_ADS_CREATE_REPORT, connection=r)
az_ads_create_report_scheduler = Scheduler(
    queue=az_ads_create_report_q, connection=r)

# Queue for verifying Amazon pending and progress reports.
asp_verify_report_q = Queue(QueueName.ASP_VERIFY_REPORT, connection=r)
asp_verify_report_scheduler = Scheduler(
    queue=asp_verify_report_q, connection=r)

# Queue for verifying Amazon Ads pending and progress reports.
az_ads_verify_report_q = Queue(QueueName.AZ_ADS_VERIFY_REPORT, connection=r)
az_ads_verify_report_scheduler = Scheduler(
    queue=az_ads_verify_report_q, connection=r)


# Queue for Item Master reports.
item_master_q = Queue(QueueName.ITEM_MASTER_REPORT, connection=r)
item_master_scheduler = Scheduler(queue=item_master_q, connection=r)

# Queue for Order reports.
order_report_q = Queue(QueueName.ORDER_REPORT, connection=r)
order_report_scheduler = Scheduler(queue=order_report_q, connection=r)

# Queue for Sales and Traffic reports.
sales_traffic_report_q = Queue(
    QueueName.SALES_TRAFFIC_REPORT, connection=r)
sales_traffic_report_scheduler = Scheduler(
    queue=sales_traffic_report_q, connection=r)

# Queue for Settlement Report v2reports.
settlement_report_v2_q = Queue(QueueName.SETTLEMENT_REPORT_V2, connection=r)
settlement_report_v2_scheduler = Scheduler(
    queue=settlement_report_v2_q, connection=r)

# Queue for Finance event list reports.
finance_event_q = Queue(QueueName.FINANCE_EVENT_LIST, connection=r)
finance_event_scheduler = Scheduler(queue=finance_event_q, connection=r)

# Queue for Ledger Summary reports.
ledger_summary_report_q = Queue(QueueName.LEDGER_SUMMARY_REPORT, connection=r)
ledger_summary_report_scheduler = Scheduler(
    queue=ledger_summary_report_q, connection=r)

# Queue for Item master cogs import.
import_item_master_cogs_q = Queue(
    QueueName.ITEM_MASTER_COGS_IMPORT, connection=r)
import_item_master_cogs_scheduler = Scheduler(
    queue=import_item_master_cogs_q, connection=r)

# Queue for Item master catalog update.
item_master_update_catalog_q = Queue(
    QueueName.ITEM_MASTER_UPDATE_CATALOG, connection=r)
item_master_update_catalog_scheduler = Scheduler(
    queue=item_master_update_catalog_q, connection=r)

# Queue for all types of export.
export_csv_q = Queue(
    QueueName.EXPORT_CSV, connection=r)
export_csv_scheduler = Scheduler(
    queue=export_csv_q, connection=r)

# Queue for sponsored brand report
az_sponsored_brand_q = Queue(
    QueueName.AZ_SPONSORED_BRAND, connection=r)
az_sponsored_brand_scheduler = Scheduler(
    queue=az_sponsored_brand_q, connection=r)

# Queue for sponsored product report
az_sponsored_product_q = Queue(
    QueueName.AZ_SPONSORED_PRODUCT, connection=r)
az_sponsored_product_scheduler = Scheduler(
    queue=az_sponsored_product_q, connection=r)

# Queue for sponsored display report
az_sponsored_display_q = Queue(
    QueueName.AZ_SPONSORED_DISPLAY, connection=r)
az_sponsored_display_scheduler = Scheduler(
    queue=az_sponsored_display_q, connection=r)

# Queue for Fba Returns Report
fba_returns_q = Queue(
    QueueName.FBA_RETURNS_REPORT, connection=r)
fba_returns_scheduler = Scheduler(
    queue=fba_returns_q, connection=r)

# Queue for Fba Reimbursements Report
fba_reimbursements_q = Queue(
    QueueName.FBA_REIMBURSEMENTS_REPORT, connection=r)
fba_reimbursements_scheduler = Scheduler(
    queue=fba_reimbursements_q, connection=r)

# Queue for Fba Customer Shipment Sales
fba_customer_shipment_sales_q = Queue(
    QueueName.FBA_CUSTOMER_SHIPMENT_SALES_REPORT, connection=r)
fba_customer_shipment_sales_scheduler = Scheduler(
    queue=fba_customer_shipment_sales_q, connection=r)

# Queue for Fba Customer Shipment Sales
az_performance_zone_q = Queue(
    QueueName.AZ_PERFORMANCE_ZONE, connection=r)
az_performance_zone_scheduler = Scheduler(
    queue=az_performance_zone_q, connection=r)

# Queue for SES Email delivery
ses_email_delivery_q = Queue(
    QueueName.SES_EMAIL_DELIVERY, connection=r)

# Queue for subscription check worker
subscription_check_q = Queue(
    QueueName.SUBSCRIPTION_CHECK, connection=r)
subscription_check_scheduler = Scheduler(
    queue=subscription_check_q, connection=r)

# Queue for Redis worker
redis_q = Queue(
    QueueName.REDIS, connection=r)
redis_scheduler = Scheduler(
    queue=redis_q, connection=r)

sales_order_metrics_q = Queue(
    QueueName.SALES_ORDER_METRICS, connection=r)
sales_order_metrics_scheduler = Scheduler(
    queue=sales_order_metrics_q, connection=r)

fba_inventory_q = Queue(
    QueueName.FBA_INVENTORY, connection=r)
fba_inventory_scheduler = Scheduler(
    queue=fba_inventory_q, connection=r)

# Clear and Start schedulers
clear_scheduler()


def clear_idle_redis_keys(redis_q):
    """
        This scheduler runs every day at midnight to clear redis keys older than a week.
    """
    try:
        from workers.redis_worker import RedisWorker

        cron_job_scheduler = Scheduler(
            queue=redis_q,
            connection=r
        )

        cron_job_scheduler.cron(
            cron_string='30 18 * * *',  # IST 12 A.M.
            func=RedisWorker.clear_idle_redis_keys,
            repeat=None,
            ttl=200,
            result_ttl=300
        )

        logger.info('SCHEDULED JOB: Redis clear Orphan Jobs schedular')
    except Exception as exception_error:
        logger.error('\n%s' % exception_error)


def create_reports(asp_create_report_q):
    """
        This scheduler runs every day at midnight to Create Amazon reports.
    """
    try:
        from workers.asp_report_worker import AspReportWorker
        from app.helpers.constants import ASpReportType

        cron_job_scheduler = Scheduler(
            queue=asp_create_report_q,
            connection=r
        )

        # “At 00:00.”
        cron_job_scheduler.cron(
            # cron_string='0 0 * * *',
            cron_string='30 18 * * *',  # IST 12 A.M.
            func=AspReportWorker.create_reports,
            # Pass the desired values for keyword arguments
            kwargs={'report_list': [
                ASpReportType.ITEM_MASTER_LIST_ALL_DATA.value, ASpReportType.SALES_TRAFFIC_REPORT.value]},
            repeat=None,
            result_ttl=300
        )
        logger.info('SCHEDULED JOB: Amazon Create Report schedular')
    except Exception as exception_error:
        logger.error('\n%s' % exception_error)


def verify_reports(asp_verify_report_q):
    """
        This scheduler runs every minute to check the report status is verified and processed.
    """
    try:
        from workers.asp_reports_worker import AspReportsWorker

        cron_job_scheduler = Scheduler(
            queue=asp_verify_report_q,
            connection=r
        )

        # “At every minute.”
        cron_job_scheduler.cron(
            cron_string='* * * * *',
            func=AspReportsWorker.verify_reports,
            args=[],
            repeat=None,
            result_ttl=300
        )
        logger.info('SCHEDULED JOB: Amazon Verify Report Schedular')
    except Exception as exception_error:
        logger.error('\n%s' % exception_error)


def create_order_reports(asp_create_report_q):
    """
        This scheduler runs every 5th minute to create Amazon order report.
    """
    try:

        from workers.asp_report_worker import AspReportWorker
        from app.helpers.constants import ASpReportType

        cron_job_scheduler = Scheduler(
            queue=asp_create_report_q,
            connection=r
        )

        # "At every 5th minute."
        cron_job_scheduler.cron(
            cron_string='*/5 * * * *',
            func=AspReportWorker.create_reports,
            # Pass the desired values for keyword arguments
            kwargs={'report_list': [
                ASpReportType.ORDER_REPORT_ORDER_DATE_GENERAL.value]},
            repeat=None,
            result_ttl=300
        )
        logger.info('SCHEDULED JOB: Amazon Create Order Report Schedular')

    except Exception as exception_error:
        logger.error('\n%s' % exception_error)


def sync_sales_order_metrics(sales_order_metrics_q):
    """
        This scheduler runs every minute to create and verify sales order metrics.
    """
    try:

        from workers.sales_order_metrics_worker import SalesOrderMetricsWorker

        cron_job_scheduler = Scheduler(
            queue=sales_order_metrics_q,
            connection=r
        )

        # "At every minute."
        cron_job_scheduler.cron(
            cron_string='* * * * *',
            func=SalesOrderMetricsWorker.create_reports,
            repeat=None,
            ttl=200,
            result_ttl=300
        )

        # "At every minute."
        cron_job_scheduler.cron(
            cron_string='* * * * *',
            func=SalesOrderMetricsWorker.process_sales_order_queue,
            repeat=None,
            ttl=200,
            result_ttl=300
        )

        logger.info('SCHEDULED JOB: Amazon Sync Sales Order Metrics Schedular')

    except Exception as exception_error:
        logger.error('\n%s' % exception_error)


def sync_fba_inventory(fba_inventory_q):
    """
        This scheduler runs every minute to create and verify FBA Inventory API.
    """
    try:

        from workers.fba_inventory_worker import FbaInventoryWorker

        cron_job_scheduler = Scheduler(
            queue=fba_inventory_q,
            connection=r
        )

        # "At every minute."
        cron_job_scheduler.cron(
            cron_string='* * * * *',
            func=FbaInventoryWorker.create_reports,
            repeat=None,
            ttl=200,
            result_ttl=300
        )

        # "At every minute."
        cron_job_scheduler.cron(
            cron_string='* * * * *',
            func=FbaInventoryWorker.process_sales_order_queue,
            repeat=None,
            ttl=200,
            result_ttl=300
        )

        logger.info('SCHEDULED JOB: Amazon Sync FBA Inventory API Schedular')

    except Exception as exception_error:
        logger.error('\n%s' % exception_error)


# def create_sales_traffic_report(asp_create_report_q):
#     """
#         This scheduler runs every 5th minute Create Amazon Sales Traffic reports.
#     """
#     try:
#         from workers.asp_report_worker import AspReportWorker
#         from app.helpers.constants import ASpReportType
#         from app.helpers.constants import TimeInSeconds

#         cron_job_scheduler = Scheduler(
#             queue=asp_create_report_q,
#             connection=r
#         )

#         # "At every 5th minute."
#         cron_job_scheduler.cron(
#             # cron_string='*/5 * * * *',
#             cron_string='*/5 3-17 * * *',  # 8:30 am - 10:30 P.M. IST
#             func=AspReportWorker.create_reports,
#             # Pass the desired values for keyword arguments
#             kwargs={'report_list': [ASpReportType.SALES_TRAFFIC_REPORT.value],
#                     'time_interval': TimeInSeconds.FIVE_MIN.value},
#             repeat=None
#         )
#         logger.info(
#             'SCHEDULED JOB: Amazon Create Sales and Traffic Report schedular')

#     except Exception as exception_error:
#         logger.error('\n%s' % exception_error)


def create_finance_event_reports(asp_create_report_q):
    """
        This scheduler runs every 8th hour to create Amazon Finance reports.
    """
    try:
        from workers.asp_report_worker import AspReportWorker
        from app.helpers.constants import ASpReportType

        cron_job_scheduler = Scheduler(
            queue=asp_create_report_q,
            connection=r
        )

        # “At minute 0 past every 8th hour from 6 through 23.”

        cron_job_scheduler.cron(
            # cron_string='0 6/8 * * *',
            cron_string='30 1/9 * * *',  # 7 A.M. every 9 hours IST.
            func=AspReportWorker.create_reports,
            # Pass the desired values for keyword arguments
            kwargs={'report_list': [
                ASpReportType.LIST_FINANCIAL_EVENTS.value]},
            repeat=None,
            result_ttl=300
        )
        logger.info(
            'SCHEDULED JOB: Amazon Create Finance Event Report Schedular')
    except Exception as exception_error:
        logger.error('\n%s' % exception_error)


def create_ledger_summary_reports(asp_create_report_q):
    """
        This scheduler runs every 7th hour to create Amazon order report.
    """
    try:
        from workers.asp_report_worker import AspReportWorker
        from app.helpers.constants import ASpReportType

        cron_job_scheduler = Scheduler(
            queue=asp_create_report_q,
            connection=r
        )

        # “At minute 0 past every 7th hour from 9 through 23.”

        cron_job_scheduler.cron(
            cron_string='30 2/7 * * *',  # 8:00 AM, every 7th hour IST
            func=AspReportWorker.create_reports,
            # Pass the desired values for keyword arguments
            kwargs={'report_list': [
                ASpReportType.LEDGER_SUMMARY_VIEW_DATA.value]},
            repeat=None,
            result_ttl=300
        )

        logger.info(
            'SCHEDULED JOB: Amazon Create Ledger Summary Report Schedular')
    except Exception as exception_error:
        logger.error('\n%s' % exception_error)


def create_fba_reports(asp_create_report_q):
    """
        This scheduler runs 4 am every week.
    """
    try:
        from workers.asp_report_worker import AspReportWorker
        from app.helpers.constants import ASpReportType

        cron_job_scheduler = Scheduler(
            queue=asp_create_report_q,
            connection=r
        )

        # “At 04:00 on Sunday.”

        cron_job_scheduler.cron(
            cron_string='30 1 * * 0',  # 6:30 am IST
            func=AspReportWorker.create_reports,
            kwargs={'report_list': [ASpReportType.FBA_RETURNS_REPORT.value, ASpReportType.FBA_REIMBURSEMENTS_REPORT.value,
                                    ASpReportType.FBA_CUSTOMER_SHIPMENT_SALES_REPORT.value]},  # Pass the desired values for keyword arguments
            repeat=None,
            result_ttl=300
        )

        logger.info(
            'SCHEDULED JOB: Amazon Create FBA Report Schedular')
    except Exception as exception_error:
        logger.error('\n%s' % exception_error)


def sync_item_master_catalog(item_master_update_catalog_q):
    """
        This scheduler runs every 6th hour to create Amazon order report.
    """
    try:
        from workers.asp_reports_worker import AspReportsWorker

        cron_job_scheduler = Scheduler(
            queue=item_master_update_catalog_q,
            connection=r
        )

        # “At 08:00.”
        cron_job_scheduler.cron(
            cron_string='30 23 * * *',  # 5:00 AM pm IST
            func=AspReportsWorker.update_catalog_items,
            args=[],
            repeat=None,
            result_ttl=300
        )
        logger.info(
            'SCHEDULED JOB: Amazon Sync Item Master Catalog Schedular')
    except Exception as exception_error:
        logger.error('\n%s' % exception_error)


def create_ads_reports(az_ads_create_report_q):
    """
        This scheduler runs every day at midnight to Create Amazon Sponsored Ads reports.
    """
    try:
        from workers.asp_reports_worker import AspReportsWorker

        cron_job_scheduler = Scheduler(
            queue=az_ads_create_report_q,
            connection=r
        )

        # "At 02:00	AM”
        cron_job_scheduler.cron(
            cron_string='30 20 * * *',  # 2:00 am IST
            func=AspReportsWorker.create_sponsored_ads_report,
            args=[],
            repeat=None,
            result_ttl=300
        )
        logger.info(
            'SCHEDULED JOB: Amazon Create Sponsored Ads Report schedular')
    except Exception as exception_error:
        logger.error('\n%s' % exception_error)


def verify_ads_reports(az_ads_verify_report_q):
    """
        This scheduler runs every minute to check the report status is verified and processed.
    """
    try:
        from workers.asp_reports_worker import AspReportsWorker

        cron_job_scheduler = Scheduler(
            queue=az_ads_verify_report_q,
            connection=r
        )

        # “At every minute.”
        cron_job_scheduler.cron(
            cron_string='* * * * *',
            func=AspReportsWorker.verify_ads_report,
            args=[],
            repeat=None,
            result_ttl=300
        )
        logger.info('SCHEDULED JOB: Amazon Ads Verify Report Schedular')
    except Exception as exception_error:
        logger.error('\n%s' % exception_error)


def generate_performance_zone(az_performance_zone_q):
    """
        This scheduler runs every 1st minute of first day of each month.
    """
    try:
        from workers.asp_reports_worker import AspReportsWorker

        cron_job_scheduler = Scheduler(
            queue=az_performance_zone_q,
            connection=r
        )

        # “At 01:00 of 1st day of each month”
        cron_job_scheduler.cron(
            cron_string='1 * 1 * *',
            func=AspReportsWorker.create_performance_zone,
            args=[],
            repeat=None,
            result_ttl=300
        )
        logger.info(
            'SCHEDULED JOB: Performance Zone Scheduler')
    except Exception as exception_error:
        logger.error('\n%s' % exception_error)


def subscription_check(subscription_check_q):
    """
        This scheduler runs every day at midnight
    """
    try:
        from workers.subscription_check_worker import SubscriptionCheckWorker

        cron_job_scheduler = Scheduler(
            queue=subscription_check_q,
            connection=r
        )

        # “At 00:02 of midnight everyday
        cron_job_scheduler.cron(
            cron_string='2 0 * * *',
            func=SubscriptionCheckWorker.subscription_check,
            args=[],
            repeat=None,
            result_ttl=300
        )
        logger.info(
            'SCHEDULED JOB: Subscription Check Scheduler')
    except Exception as exception_error:
        logger.error('\n%s' % exception_error)


clear_idle_redis_keys(redis_q)
create_reports(asp_create_report_q)
verify_reports(asp_verify_report_q)
create_order_reports(asp_create_report_q)
create_ledger_summary_reports(asp_create_report_q)
# create_sales_traffic_report(asp_create_report_q)
create_finance_event_reports(asp_create_report_q)
create_fba_reports(asp_create_report_q)
sync_item_master_catalog(item_master_update_catalog_q)
create_ads_reports(az_ads_create_report_q)
verify_ads_reports(az_ads_verify_report_q)
generate_performance_zone(az_performance_zone_q)
subscription_check(subscription_check_q)
sync_sales_order_metrics(sales_order_metrics_q)
sync_fba_inventory(fba_inventory_q)

limiter = Limiter(app=app, key_func=None, strategy=config_data.get('STRATEGY'),  # Creating instance of Flask-Limiter for rate limiting.
                  key_prefix=config_data.get('KEY_PREFIX'), storage_uri='redis://{0}:{1}/{2}'.format(
    config_data.get('REDIS').get('HOST'), config_data.get('REDIS').get('PORT'), config_data.get('RATE_LIMIT').get('REDIS_DB')))
