"""
# This script is for generating Sales Order Metrics
# It defines a class, AspReportWorker, to handle report generation
"""
from collections import defaultdict
from datetime import datetime
from datetime import timedelta
import time
import traceback
from typing import Any


class SalesOrderMetricsWorker:
    """ A class for fetching sales order metrics """

    @classmethod
    def create_reports(cls, *args, **kwargs):  # type: ignore  # noqa: C901
        """ Queue Sales Order Metrics"""
        from app import app, logger, db, config_data
        from app.helpers.constants import ASpReportType
        from app.helpers.constants import ASpReportProcessingStatus
        from app.models.user import User
        from app.models.account import Account
        from app.models.az_report import AzReport
        from providers.mail import send_error_notification
        from app.helpers.utility import generate_uuid

        with app.app_context():

            try:

                logger.info('Inside Sales Order Metrics create reports')

                cancelled_reports = AzReport.cancel_old_reports(
                    report_type=ASpReportType.SALES_ORDER_METRICS.value, max_age_minutes=60)

                cancelled_reports_dict = {}

                if cancelled_reports:

                    for cancelled_report in cancelled_reports:
                        _report = {
                            'id': cancelled_report.id,
                            'account_id': cancelled_report.account_id,
                            'asp_id': cancelled_report.asp_id,
                            'type': cancelled_report.type,
                            'status': cancelled_report.status,
                            'queue_id': cancelled_report.queue_id,
                            'created_at': cancelled_report.created_at
                        }

                        cancelled_reports_dict[cancelled_report.id] = _report

                logger.info(
                    f'Sales order metrics cancelled reports: {cancelled_reports_dict}')

                end_date = datetime.now().date()
                start_date = end_date - timedelta(days=3)

                # Get reports for all users.
                sales_api_reports = AzReport.get_pending_sales_progress(
                    type=ASpReportType.SALES_ORDER_METRICS.value)

                if not sales_api_reports:

                    users = db.session.query(User.id, Account.uuid, Account.asp_id, Account.asp_credentials).\
                        join(Account, (User.id == Account.primary_user_id) & (Account.uuid.isnot(None)) & (Account.asp_id.isnot(None)) & (Account.asp_credentials.isnot(None))).all()  # type: ignore  # noqa: FKA100

                    for user in users:
                        user_id, account_id, asp_id, asp_credentials = user

                        ref_id = generate_uuid()

                        AzReport.add(account_id=account_id, seller_partner_id=asp_id, type=ASpReportType.SALES_ORDER_METRICS.value,
                                     reference_id=ref_id, request_start_time=start_date, request_end_time=end_date, status=ASpReportProcessingStatus.NEW.value)

            except Exception as e:
                logger.error(
                    'Error while creating Sales Order Metrics in SalesOrderMetricsWorker.create_reports: ' + str(e))
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='SalesOrderMetricsWorker Create Report Failure',
                                        template='emails/slack_email.html', data=None, error_message=str(e), traceback_info=traceback.format_exc())

    @classmethod
    def process_sales_order_queue(cls, *args, **kwargs):  # type: ignore  # noqa: C901
        """ Process reports to queues"""
        from app import app, logger, db, config_data, sales_order_metrics_q
        from app.helpers.constants import ASpReportType
        from app.helpers.constants import ASpReportProcessingStatus
        from app.models.az_report import AzReport
        from providers.mail import send_error_notification
        from app.helpers.constants import QueueName
        from app.helpers.constants import EntityType
        from app.models.queue_task import QueueTask
        from app.helpers.constants import QueueTaskStatus

        with app.app_context():

            try:

                logger.info(
                    'Inside Sales Order Metrics process sales order for queue')

                # Get reports for all users.
                reports = AzReport.get_pending_reports(
                    type=ASpReportType.SALES_ORDER_METRICS.value)

                for report in reports:

                    account_id = report.account_id
                    asp_id = report.seller_partner_id
                    report_type = report.type
                    reference_id = report.reference_id
                    report_queue_id = report.queue_id
                    report_status = report.status

                    data = {
                        # 'user_id': user_id,
                        'account_id': account_id,
                        'asp_id': asp_id
                    }

                    logger.info('Report type: {}, Ref id: {}, Queue id: {}, Report status: {}'.format(
                        report_type, reference_id, report_queue_id, report_status))

                    if report_queue_id is None and report_type == ASpReportType.SALES_ORDER_METRICS.value and report_status == ASpReportProcessingStatus.NEW.value:
                        get_report_by_ref_id = AzReport.get_by_ref_id(
                            account_id=account_id, reference_id=reference_id)

                        queue_name = None
                        entity_type = None
                        if get_report_by_ref_id.type == ASpReportType.SALES_ORDER_METRICS.value:
                            queue_name = QueueName.SALES_ORDER_METRICS
                            entity_type = EntityType.SALES_ORDER_METRICS.value

                            queue_task = QueueTask.add_queue_task(queue_name=queue_name,
                                                                  account_id=account_id,
                                                                  # owner_id=user_id,
                                                                  status=QueueTaskStatus.NEW.value,
                                                                  entity_type=entity_type,
                                                                  param=str(data), input_attachment_id=None, output_attachment_id=None)

                            # update_queue_id = AzReport.get_by_ref_id(reference_id=reference_id)
                            get_report_by_ref_id.queue_id = queue_task.id
                            get_report_by_ref_id.save()

                            if queue_task:
                                queue_task_dict = {
                                    'job_id': queue_task.id,
                                    'queue_name': queue_task.queue_name,
                                    'status': QueueTaskStatus.get_status(queue_task.status),
                                    'entity_type': EntityType.get_type(queue_task.entity_type),
                                    'reference_id': reference_id,
                                    'seller_partner_id': asp_id
                                }
                                data.update(queue_task_dict)
                                queue_task.param = str(data)
                                queue_task.save()

                            if get_report_by_ref_id.type == ASpReportType.SALES_ORDER_METRICS.value:
                                sales_order_metrics_q.enqueue(SalesOrderMetricsWorker.get_sales_order_metrics, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100

                    """Break the loop so next report doesn't goes in queue until status for this report gets updated"""
                    break

            except Exception as e:
                logger.error(
                    'Error while creating Sales Order Metrics in SalesOrderMetricsWorker.create_reports: ' + str(e))
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='SalesOrderMetricsWorker Create Report Failure',
                                        template='emails/slack_email.html', data=None, error_message=str(e), traceback_info=traceback.format_exc())

    @classmethod
    def get_sales_order_metrics(cls, data):  # type: ignore  # noqa: C901
        """ Fetch sales API by asin at interval"""
        from app import app, logger, db, config_data
        from app.helpers.constants import SalesAPIGranularity
        from app.helpers.constants import ASpReportProcessingStatus
        # from app.helpers.constants import ASpReportType
        # from app.models.user import User
        from app.models.account import Account
        from app.models.az_item_master import AzItemMaster
        from providers.amazon_sp_client import AmazonReportEU
        from app.helpers.utility import get_asp_market_place_ids
        # from app.helpers.utility import generate_date_ranges
        from app.models.az_sales_traffic_asin import AzSalesTrafficAsin
        from app.models.az_sales_traffic_summary import AzSalesTrafficSummary
        from app.models.az_report import AzReport
        # from app.helpers.utility import generate_uuid
        from providers.mail import send_error_notification
        from app.models.queue_task import QueueTask
        from app.helpers.constants import QueueTaskStatus

        with app.app_context():
            job_id = data.get('job_id')
            account_id = data.get('account_id')
            report_id = data.get('reference_id')

            if job_id is None:
                logger.error(
                    "Queue Task with job_id '{}' not found".format(job_id))
                return

            queue_task = QueueTask.get_by_id(job_id)

            if not queue_task:
                logger.error(
                    "Queue Task with job_id '{}' not found".format(job_id))
                return
            else:
                if report_id is None and account_id is None:
                    raise Exception

            account = Account.get_by_uuid(uuid=account_id)

            if not account:
                logger.error(
                    "Queue Task with job_id '{}' failed. Account : {} not found".format(job_id, account_id))
                raise Exception

            get_report_ref = AzReport.get_by_ref_id(account_id=account_id,
                                                    reference_id=report_id)

            if not get_report_ref:
                logger.error(
                    "Queue Task with job_id '{}' failed. Reference id : {} not found".format(job_id, report_id))
                raise Exception
            else:
                get_report_ref.status = ASpReportProcessingStatus.IN_PROGRESS.value
                get_report_ref.status_updated_at = int(time.time())
                db.session.commit()

            try:
                logger.info('*' * 200)
                logger.info('Get Sales Order Metric')

                asp_id = account.asp_id

                credentials = account.retrieve_asp_credentials(account)[0]
                logger.info(
                    f'Credentials for sales order metrics: {credentials}')

                marketplace_ids = get_asp_market_place_ids()

                start_date = get_report_ref.request_start_time.strftime(
                    '%Y-%m-%d')
                end_date = get_report_ref.request_end_time.strftime('%Y-%m-%d')

                # end_date = datetime.now().date()
                # start_date = end_date - timedelta(days=3)
                # start_date = start_date.strftime('%Y-%m-%d')
                # end_date = end_date.strftime('%Y-%m-%d')

                items, total_count = AzItemMaster.get_all_records(
                    account_id=account_id, asp_id=asp_id)

                logger.info(f'Total Item Master Count: {total_count}')

                params = SalesOrderMetricsWorker.prepare_params(
                    cls, start_date=start_date, end_date=end_date, market_place_ids=marketplace_ids, granularity=SalesAPIGranularity.HOUR.value)
                logger.info(f'account_id: {account_id}')
                logger.info(f'asp_id: {asp_id}')
                logger.info(params)

                asin_payload_dict = defaultdict(lambda: {'payload_list': [], 'seller_sku': None, 'brand': None, 'category': None})   # noqa: FKA100

                for _i, item in enumerate(items, 1):   # noqa: FKA100
                    start_time = time.time()
                    logger.info(
                        f"Iteration {_i} - Start Time: {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}")
                    logger.info(
                        f'Iteration {_i} - Seller Sku: ***{item.seller_sku}***')
                    logger.info(f'Iteration {_i} - Asin: ***{item.asin}***')
                    logger.info(
                        f'Iteration {_i} - Status: ***{item.status}***')

                    params['asin'] = item.asin

                    report = AmazonReportEU(
                        credentials=credentials)
                    response = report.get_sales(params=params)

                    payload = response.get('payload')

                    asin_payload_dict[item.asin]['payload_list'].extend(
                        payload)
                    asin_payload_dict[item.asin]['brand'] = item.brand
                    asin_payload_dict[item.asin]['category'] = item.category
                    asin_payload_dict[item.asin]['seller_sku'] = item.seller_sku

                    end_time = time.time()
                    logger.info(
                        f"Iteration {_i} - End Time: {datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')}")
                    time.sleep(0.5)

                _sales_summary_dict = {}

                for _asin, data_dict in asin_payload_dict.items():
                    payload_list = data_dict['payload_list']
                    seller_sku = data_dict['seller_sku']
                    brand = data_dict['brand']
                    category = data_dict['category']

                    result_dict = SalesOrderMetricsWorker.calculate_hourly_sales(
                        cls, sku=seller_sku, asin=_asin, payload=payload_list)

                    for key, value in result_dict.items():
                        if key in _sales_summary_dict:
                            _sales_summary_dict[key]['total_sales'] += value.get(
                                'objects').get('total_sales')
                            _sales_summary_dict[key]['total_unit_count'] += value.get(
                                'objects').get('unit_count')
                        else:
                            _sales_summary_dict[key] = {
                                'total_sales': value.get('objects').get('total_sales'),
                                'total_unit_count': value.get('objects').get('unit_count')
                            }

                        AzSalesTrafficAsin.insert_or_update_sales_data(account_id=account_id, asp_id=asp_id, date=key,
                                                                       total_sales=value.get('objects').get('total_sales'), unit_count=value.get('objects').get('unit_count'), hourly_sales=value, asin=_asin, category=category, brand=brand)

                for _key, _value in _sales_summary_dict.items():
                    AzSalesTrafficSummary.insert_or_update_sales_data(account_id=account_id, asp_id=asp_id, date=_key, total_sales=_value.get(
                        'total_sales'), unit_count=_value.get('total_unit_count'))

                # for loop:
                if get_report_ref:
                    get_report_ref.status = ASpReportProcessingStatus.COMPLETED.value
                    get_report_ref.status_updated_at = int(time.time())
                    db.session.commit()

                if queue_task:
                    queue_task.status = QueueTaskStatus.COMPLETED.value
                    queue_task.save()
                else:
                    raise Exception

                # check if synced before
                # get_previous_reports = AzReport.check_last_report(
                #     account_id=account_id, asp_id=asp_id, type=ASpReportType.SALES_ORDER_METRICS.value)

                # if not get_previous_reports:

                #     date_list = generate_date_ranges()
                #     logger.info(f'Date range list: {date_list}')

                #     for dates in date_list:

                #         start_date = dates.get('start_date')
                #         end_date = dates.get('end_date')

                #         ref_id = generate_uuid()

                #         sales_report = AzReport.add(account_id=account_id, seller_partner_id=asp_id, type=ASpReportType.SALES_ORDER_METRICS.value,
                #                                     reference_id=ref_id, request_start_time=start_date, request_end_time=end_date)

                #         params = SalesOrderMetricsWorker.prepare_params(
                #             cls, start_date=start_date, end_date=end_date, market_place_ids=marketplace_ids, granularity=SalesAPIGranularity.HOUR.value)
                #         logger.info(f'account_id: {account_id}')
                #         logger.info(f'asp_id: {asp_id}')
                #         logger.info(params)

                #         for item in items:

                #             start_time = time.time()

                #             params['asin'] = item.asin
                #             report = AmazonReportEU(
                #                 credentials=credentials)
                #             response = report.get_sales(params=params)
                #             payload = response.get('payload')

                #             result_dict = SalesOrderMetricsWorker.calculate_hourly_sales(
                #                 cls, sku=item.seller_sku, asin=item.asin, payload=payload)
                #             # logger.info(result_dict)

                #             for key, value in result_dict.items():
                #                 AzSalesTrafficAsin.insert_or_update_sales_data(account_id=account_id, asp_id=asp_id, date=key,
                #                                                                 total_sales=value.get('objects').get('total_sales'), unit_count=value.get('objects').get('unit_count'), hourly_sales=value, asin=params['asin'], category=item.category, brand=item.brand)

                #             elapsed_time = time.time() - start_time

                #             if elapsed_time < 30:
                #                 time.sleep(30 - elapsed_time)

                #         AzReport.update_status(
                #             reference_id=sales_report.reference_id, status=ASpReportProcessingStatus.COMPLETED.value)

            except Exception as e:
                if get_report_ref:
                    get_report_ref.status = ASpReportProcessingStatus.ERROR.value
                    get_report_ref.status_updated_at = int(time.time())
                    db.session.commit()

                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                logger.error(
                    'Error while fetching sales order metrics in SalesOrderMetricsWorker.get_sales_order_metrics: ' + str(e))
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='SalesOrderMetricsWorker Get sales order metrics Failure',
                                        template='emails/slack_email.html', data=None, error_message=str(e), traceback_info=traceback.format_exc())

    def prepare_params(cls, start_date: str, end_date: str, market_place_ids: Any, granularity: str):
        """Prepare params for sales api"""
        payload_interval = f'{start_date}T18:00:00-00:30--{end_date}T18:00:00-00:30'

        params = {
            'interval': payload_interval,
            'marketplaceIds': market_place_ids,
            'granularity': granularity
        }

        return params

    def get_sales_interval_ist(cls, interval: Any):
        """retrieve from_datetime and to_datetime from interval"""

        start_str, end_str = interval.split('--')
        start_datetime = datetime.fromisoformat(start_str)
        end_datetime = datetime.fromisoformat(end_str)

        # Adjust datetime objects to the desired time zone (e.g., UTC)
        # Adjust based on your specific offset
        time_zone_offset = timedelta(hours=6, minutes=0)
        start_datetime += time_zone_offset
        end_datetime += time_zone_offset

        # Convert the string format to '12:00AM - 01:00AM'
        formatted_time_range = f"{start_datetime.strftime('%I:%M%p')} - {end_datetime.strftime('%I:%M%p')}"

        return start_datetime.strftime('%Y-%m-%d'), formatted_time_range

    def calculate_hourly_sales(cls, sku: str, asin: str, payload: Any):
        """Calculate hourly sales with counts"""
        result_dict = {}

        for data in payload:
            interval = data.get('interval')  # noqa: FKA100
            payload_date, interval_range = SalesOrderMetricsWorker.get_sales_interval_ist(
                cls, interval=interval)

            data['payload_date'] = payload_date
            data['interval_range'] = interval_range
            data['sku'] = sku
            data['asin'] = asin

            if payload_date not in result_dict:
                result_dict[payload_date] = {
                    'result': [],
                    'objects': {
                        'payload_date': payload_date,
                        'total_sales': 0,
                        'unit_count': 0,
                        'orderCount': 0,
                    },
                }

            result_dict[payload_date]['result'].append(data)
            result_dict[payload_date]['objects']['total_sales'] += data['totalSales']['amount']
            result_dict[payload_date]['objects']['unit_count'] += data['unitCount']
            result_dict[payload_date]['objects']['orderCount'] += data['orderCount']

        return result_dict
