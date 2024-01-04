import datetime
import gzip
import json
import time
import traceback

from app import app
from app import config_data
from app import db
from app import logger
from app import sales_traffic_report_q
from app.helpers.constants import AsinGranularity
from app.helpers.constants import ASpReportProcessingStatus
from app.helpers.constants import ASpReportType
from app.helpers.constants import DateGranularity
from app.helpers.constants import EntityType
from app.helpers.constants import QueueName
from app.helpers.constants import QueueTaskStatus
from app.helpers.constants import TimePeriod
from app.helpers.utility import convert_string_to_datetime
from app.helpers.utility import flatten_json
from app.helpers.utility import get_asp_market_place_ids
from app.helpers.utility import get_date_from_string
from app.helpers.utility import get_from_to_date_by_time_period
from app.models.account import Account
from app.models.az_report import AzReport
from app.models.az_sales_traffic_asin import AzSalesTrafficAsin
from app.models.az_sales_traffic_summary import AzSalesTrafficSummary
from app.models.queue_task import QueueTask
from providers.amazon_sp_client import AmazonReportEU
from providers.mail import send_error_notification
import requests


class SalesTrafficReportWorker:
    """This worker class creates sales and traffic report requesting from amazon, queues it to verify the report and then downloads the report """

    @classmethod
    def create_sales_traffic_report(cls, data) -> None:
        with app.app_context():
            job_id = data.get('job_id')

            if job_id is None:
                logger.error(
                    "Queue Task with job_id '{}' not found".format(job_id))
                return

            queue_task = QueueTask.get_by_id(job_id)

            if not queue_task:
                logger.error(
                    "Queue Task with job_id '{}' not found".format(job_id))
                return

            queue_task.status = QueueTaskStatus.RUNNING.value
            queue_task.save()

            account_id = queue_task.account_id
            # user_id = queue_task.owner_id
            default_sync = data.get('default_sync', False)  # type: ignore  # noqa: FKA100

            account = Account.get_by_uuid(uuid=account_id)

            if not account:
                logger.error(
                    "Queue Task with job_id '{}' failed. Account : {} not found".format(job_id, account_id))
                raise Exception

            try:

                asp_id = account.asp_id
                credentials = account.retrieve_asp_credentials(account)[0]

                if default_sync:
                    default_time_period = data.get('default_time_period', TimePeriod.LAST_30_DAYS.value)  # type: ignore  # noqa: FKA100
                    start_datetime, end_datetime = get_from_to_date_by_time_period(
                        default_time_period)
                else:
                    start_datetime = convert_string_to_datetime(
                        data.get('start_datetime'))
                    end_datetime = convert_string_to_datetime(
                        data.get('end_datetime'))
                    if start_datetime is None or end_datetime is None or isinstance(start_datetime, datetime.datetime) or isinstance(end_datetime, datetime.datetime):
                        logger.error(
                            "Queue Task with job_id '{}' failed. start_datetime : {}, end_datetime : {}".format(job_id, start_datetime, end_datetime))
                        raise Exception

                logger.info("Queue Task with job_id '{}'. start_datetime : {}, end_datetime : {}".format(
                    job_id, start_datetime, end_datetime))

                """When dateGranularity is WEEK or MONTH the dataStartTime and dataEndTime values are expanded to that level of granularity. For WEEK the beginning is Sunday and the end is Saturday, and for MONTH the beginning is the first day of the month and the end is the last day of the month."""
                """start date will be pulled to beginning , end datetime will be pushed to the last depending on date granularity"""

                delta = datetime.timedelta(days=1)

                while start_datetime <= end_datetime:
                    payload = {
                        'reportOptions': {
                            'dateGranularity': DateGranularity.DAY.value,
                            'asinGranularity': AsinGranularity.CHILD.value
                        },
                        'reportType': ASpReportType.SALES_TRAFFIC_REPORT.value,
                        'dataStartTime': start_datetime.strftime('%Y-%m-%d'),
                        'dataEndTime': start_datetime.strftime('%Y-%m-%d'),
                        'marketplaceIds': get_asp_market_place_ids()
                    }

                    report = AmazonReportEU(credentials=credentials)

                    response = report.create_report(payload=payload)

                    AzReport.add(account_id=account_id, seller_partner_id=asp_id, request_start_time=start_datetime,
                                 request_end_time=start_datetime, type=ASpReportType.SALES_TRAFFIC_REPORT.value, reference_id=response['reportId'])

                    start_datetime += delta
                    # time.sleep(60)

                if queue_task:
                    queue_task.status = QueueTaskStatus.COMPLETED.value
                    queue_task.save()

                #     queue_task_report_verify = QueueTask.add_queue_task(queue_name=QueueName.SALES_TRAFFIC_REPORT,
                #                                                         owner_id=user_id,
                #                                                         status=QueueTaskStatus.NEW.value,
                #                                                         entity_type=EntityType.SALES_TRAFFIC_REPORT.value,
                #                                                         param=str(data), input_attachment_id=None, output_attachment_id=None)

                #     if queue_task_report_verify:
                #         queue_task_dict = {
                #             'job_id': queue_task_report_verify.id,
                #             'queue_name': queue_task_report_verify.queue_name,
                #             'status': QueueTaskStatus.get_status(queue_task_report_verify.status),
                #             'entity_type': EntityType.get_type(queue_task_report_verify.entity_type),
                #             'seller_partner_id': asp_id,
                #             'user_id': user_id,
                #             'account_id': account_id,
                #             'reference_id': response['reportId']
                #         }

                #         # very every minute
                #         sales_traffic_report_q.enqueue_in(timedelta(minutes=3), SalesTrafficReportWorker.verify_sales_traffic_report, data=queue_task_dict, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100

                # else:
                #     raise Exception

            except Exception as e:
                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                error_message = 'Error while creating report in SalesTrafficReportWorker.create_sales_traffic_report(): ' + \
                    str(e)
                logger.error(error_message)
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='SalesTrafficReportWorker (SALES TRAFFIC REPORT) create Report Failure',
                                        template='emails/slack_email.html', data={}, error_message=error_message, traceback_info=traceback.format_exc())

    @classmethod
    def verify_sales_traffic_report(cls, data):
        """This should queue every minute to verify report"""
        with app.app_context():
            job_id = data.get('job_id')
            user_id = data.get('user_id')
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
                if account_id is None and account_id is None:
                    raise Exception

            account = Account.get_by_uuid(uuid=account_id)

            if not account:
                logger.error(
                    "Queue Task with job_id '{}' failed. Account : {} not found".format(job_id, account_id))
                raise Exception

            try:

                asp_id = account.asp_id
                credentials = account.retrieve_asp_credentials(account)[0]

                # querying Report table to get the entry for particular report_id
                get_report = AzReport.get_by_ref_id(reference_id=report_id)

                if not get_report:
                    raise Exception

                # creating AmazonReportEU object and passing the credentials
                report = AmazonReportEU(credentials=credentials)

                # calling verify_report function of report object and passing the report_id
                report_status = report.verify_report(report_id)

                # checking the processing status of the report. if complete, we update status in the table entry for that particular report_id
                if report_status['processingStatus'] != 'DONE':
                    AzReport.update_status(
                        reference_id=report_id, status=report_status['processingStatus'], document_id=None)
                else:
                    AzReport.update_status(
                        reference_id=report_id, status=report_status['processingStatus'], document_id=report_status['reportDocumentId'])

                if queue_task:
                    queue_task.status = QueueTaskStatus.COMPLETED.value
                    queue_task.save()

                    queue_task_retrieve_verify = QueueTask.add_queue_task(queue_name=QueueName.SALES_TRAFFIC_REPORT,
                                                                          owner_id=user_id,
                                                                          status=QueueTaskStatus.NEW.value,
                                                                          entity_type=EntityType.SALES_TRAFFIC_REPORT.value,
                                                                          param=str(data), input_attachment_id=None, output_attachment_id=None)

                    if queue_task_retrieve_verify:
                        queue_task_dict = {
                            'job_id': queue_task_retrieve_verify.id,
                            'queue_name': queue_task_retrieve_verify.queue_name,
                            'status': QueueTaskStatus.get_status(queue_task_retrieve_verify.status),
                            'entity_type': EntityType.get_type(queue_task_retrieve_verify.entity_type),
                            'seller_partner_id': asp_id,
                            'account_id': account_id,
                            'reference_id': report_id
                        }

                        sales_traffic_report_q.enqueue(SalesTrafficReportWorker.get_sales_traffic_report, data=queue_task_dict, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100

                else:
                    raise Exception

            except Exception as e:
                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                error_message = 'Error while verifying report in SalesTrafficReportWorker.verify_sales_traffic_report(): ' + \
                    str(e)
                logger.error(error_message)
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='SalesTrafficReportWorker (SALES TRAFFIC REPORT) Verify Report Failure',
                                        template='emails/slack_email.html', data={}, error_message=error_message, traceback_info=traceback.format_exc())

    @classmethod
    def get_sales_traffic_report(cls, data):
        """This method gets the sales and traffic reports from amazon and inserts into databse"""
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

            get_report_document_id = AzReport.get_by_ref_id(account_id=account_id,
                                                            reference_id=report_id)

            if not get_report_document_id:
                logger.error(
                    "Queue Task with job_id '{}' failed. Reference id : {} not found".format(job_id, report_id))
                raise Exception

            try:

                asp_id = account.asp_id
                credentials = account.retrieve_asp_credentials(account)[0]

                report_document_id = get_report_document_id.document_id

                # creating AmazonReportEU object and passing the credentials
                report = AmazonReportEU(credentials=credentials)

                # using retrieve_report function of report object to get report
                get_report = report.retrieve_report(report_document_id)

                if 'url' not in get_report:
                    get_report_document_id.status = ASpReportProcessingStatus.DONE.value
                    get_report_document_id.status_updated_at = int(time.time())
                    get_report_document_id.queue_id = None
                    db.session.commit()

                    if queue_task:
                        queue_task.status = QueueTaskStatus.ERROR.value
                        queue_task.save()

                    logger.error(
                        f'Error while retrieving report in SalesTrafficReportWorker.get_sales_traffic_report(), Url not found for Ref Id: {get_report_document_id.reference_id}')
                    return None

                if 'compressionAlgorithm' in get_report and get_report['compressionAlgorithm'] == 'GZIP':
                    streamed_url_response = requests.get(get_report['url'])
                    compressed_content = streamed_url_response.content
                    decompressed_content = gzip.decompress(compressed_content)
                    content = decompressed_content.decode('utf-8')
                else:
                    streamed_url_response = requests.get(get_report['url'])
                    content = streamed_url_response.content.decode('utf-8')

                # with open(config_data.get('UPLOAD_FOLDER') + ASpReportType.SALES_TRAFFIC_REPORT.value.lower()   # type: ignore  # noqa: FKA100
                #         + '/{}.txt'.format(report_document_id), 'w') as f:
                #     f.write(content)
                # f.close()

                payload_date = get_report_document_id.request_start_time

                json_dict = json.loads(content)

                date_granularity = json_dict['reportSpecification']['reportOptions']['dateGranularity']
                asin_granularity = json_dict['reportSpecification']['reportOptions']['asinGranularity']

                # sales and traffic summary (by date)
                sales_traffic_summary = json_dict.get('salesAndTrafficByDate')
                for item in sales_traffic_summary:
                    item_flat = flatten_json(item)
                    date = get_date_from_string(
                        date_string=item_flat.get('date'))
                    ordered_product_sales_amount = item_flat.get(
                        'salesByDate_orderedProductSales_amount')
                    ordered_product_sales_currency_code = item_flat.get(
                        'salesByDate_orderedProductSales_currencyCode')
                    ordered_product_sales_amount_b2b = item_flat.get(
                        'salesByDate_orderedProductSalesB2B_amount')
                    ordered_product_sales_currency_code_b2b = item_flat.get(
                        'salesByDate_orderedProductSalesB2B_currencyCode')
                    units_ordered = item_flat.get('salesByDate_unitsOrdered')
                    units_ordered_b2b = item_flat.get(
                        'salesByDate_unitsOrderedB2B')
                    total_order_items = item_flat.get(
                        'salesByDate_totalOrderItems')
                    total_order_items_b2b = item_flat.get(
                        'salesByDate_totalOrderItemsB2B')
                    average_sales_per_order_item_amount = item_flat.get(
                        'salesByDate_averageSalesPerOrderItem_amount')
                    average_sales_per_order_item_currency_code = item_flat.get(
                        'salesByDate_averageSalesPerOrderItem_currencyCode')
                    average_sales_per_order_item_amount_b2b = item_flat.get(
                        'salesByDate_averageSalesPerOrderItemB2B_amount')
                    average_sales_per_order_item_currency_code_b2b = item_flat.get(
                        'salesByDate_averageSalesPerOrderItemB2B_currencyCode')
                    average_units_per_order_item = item_flat.get(
                        'salesByDate_averageUnitsPerOrderItem')
                    average_units_per_order_item_b2b = item_flat.get(
                        'salesByDate_averageUnitsPerOrderItemB2B')
                    average_selling_price_amount = item_flat.get(
                        'salesByDate_averageSellingPrice_amount')
                    average_selling_price_currency_code = item_flat.get(
                        'salesByDate_averageSellingPrice_currencyCode')
                    average_selling_price_amount_b2b = item_flat.get(
                        'salesByDate_averageSellingPriceB2B_amount')
                    average_selling_price_currency_code_b2b = item_flat.get(
                        'salesByDate_averageSellingPriceB2B_currencyCode')
                    units_refunded = item_flat.get('salesByDate_unitsRefunded')
                    refund_rate = item_flat.get('salesByDate_refundRate')
                    claims_granted = item_flat.get('salesByDate_claimsGranted')
                    claims_amount_amount = item_flat.get(
                        'salesByDate_claimsAmount_amount')
                    claims_amount_currency_code = item_flat.get(
                        'salesByDate_claimsAmount_currencyCode')
                    shipped_product_sales_amount = item_flat.get(
                        'salesByDate_shippedProductSales_amount')
                    shipped_product_sales_currency_code = item_flat.get(
                        'salesByDate_shippedProductSales_currencyCode')
                    units_shipped = item_flat.get('salesByDate_unitsShipped')
                    orders_shipped = item_flat.get('salesByDate_ordersShipped')
                    browser_page_views = item_flat.get(
                        'trafficByDate_browserPageViews')
                    browser_page_views_b2b = item_flat.get(
                        'trafficByDate_browserPageViewsB2B')
                    mobile_app_page_views = item_flat.get(
                        'trafficByDate_mobileAppPageViews')
                    mobile_app_page_views_b2b = item_flat.get(
                        'trafficByDate_mobileAppPageViewsB2B')
                    page_views = item_flat.get('trafficByDate_pageViews')
                    page_views_b2b = item_flat.get(
                        'trafficByDate_pageViewsB2B')
                    browser_sessions = item_flat.get(
                        'trafficByDate_browserSessions')
                    browser_sessions_b2b = item_flat.get(
                        'trafficByDate_browserSessionsB2B')
                    mobile_app_sessions = item_flat.get(
                        'trafficByDate_mobileAppSessions')
                    mobile_app_sessions_b2b = item_flat.get(
                        'trafficByDate_mobileAppSessionsB2B')
                    sessions = item_flat.get('trafficByDate_sessions')
                    sessions_b2b = item_flat.get('trafficByDate_sessionsB2B')
                    buy_box_percentage = item_flat.get(
                        'trafficByDate_buyBoxPercentage')
                    buy_box_percentage_b2b = item_flat.get(
                        'trafficByDate_buyBoxPercentageB2B')
                    order_item_session_percentage = item_flat.get(
                        'trafficByDate_orderItemSessionPercentage')
                    order_item_session_percentage_b2b = item_flat.get(
                        'trafficByDate_orderItemSessionPercentageB2B')
                    unit_session_percentage = item_flat.get(
                        'trafficByDate_unitSessionPercentage')
                    unit_session_percentage_b2b = item_flat.get(
                        'trafficByDate_unitSessionPercentageB2B')
                    average_offer_count = item_flat.get(
                        'trafficByDate_averageOfferCount')
                    average_parent_items = item_flat.get(
                        'trafficByDate_averageParentItems')
                    feedback_received = item_flat.get(
                        'trafficByDate_feedbackReceived')
                    negative_feedback_received = item_flat.get(
                        'trafficByDate_negativeFeedbackReceived')
                    received_negative_feedback_rate = item_flat.get(
                        'trafficByDate_receivedNegativeFeedbackRate')
                    date_granularity = date_granularity

                    AzSalesTrafficSummary.insert_or_update(account_id=account_id, asp_id=asp_id, date=date, ordered_product_sales_amount=ordered_product_sales_amount,
                                                           ordered_product_sales_currency_code=ordered_product_sales_currency_code, units_ordered=units_ordered,
                                                           ordered_product_sales_amount_b2b=ordered_product_sales_amount_b2b, ordered_product_sales_currency_code_b2b=ordered_product_sales_currency_code_b2b,
                                                           units_ordered_b2b=units_ordered_b2b, total_order_items=total_order_items, total_order_items_b2b=total_order_items_b2b,
                                                           average_sales_per_order_item_amount=average_sales_per_order_item_amount, average_sales_per_order_item_currency_code=average_sales_per_order_item_currency_code,
                                                           average_sales_per_order_item_amount_b2b=average_sales_per_order_item_amount_b2b, average_sales_per_order_item_currency_code_b2b=average_sales_per_order_item_currency_code_b2b,
                                                           average_units_per_order_item=average_units_per_order_item, average_units_per_order_item_b2b=average_units_per_order_item_b2b,
                                                           average_selling_price_amount=average_selling_price_amount, average_selling_price_currency_code=average_selling_price_currency_code,
                                                           average_selling_price_amount_b2b=average_selling_price_amount_b2b, average_selling_price_currency_code_b2b=average_selling_price_currency_code_b2b,
                                                           units_refunded=units_refunded, refund_rate=refund_rate, claims_granted=claims_granted, claims_amount_amount=claims_amount_amount,
                                                           claims_amount_currency_code=claims_amount_currency_code, shipped_product_sales_amount=shipped_product_sales_amount,
                                                           shipped_product_sales_currency_code=shipped_product_sales_currency_code, units_shipped=units_shipped, orders_shipped=orders_shipped,
                                                           browser_page_views=browser_page_views, browser_page_views_b2b=browser_page_views_b2b, mobile_app_page_views=mobile_app_page_views,
                                                           mobile_app_page_views_b2b=mobile_app_page_views_b2b, page_views=page_views, page_views_b2b=page_views_b2b,
                                                           browser_sessions=browser_sessions, browser_sessions_b2b=browser_sessions_b2b, mobile_app_sessions=mobile_app_sessions,
                                                           mobile_app_sessions_b2b=mobile_app_sessions_b2b, sessions=sessions, sessions_b2b=sessions_b2b,
                                                           buy_box_percentage=buy_box_percentage, buy_box_percentage_b2b=buy_box_percentage_b2b, order_item_session_percentage=order_item_session_percentage,
                                                           order_item_session_percentage_b2b=order_item_session_percentage_b2b, unit_session_percentage=unit_session_percentage,
                                                           unit_session_percentage_b2b=unit_session_percentage_b2b, average_offer_count=average_offer_count, average_parent_items=average_parent_items,
                                                           feedback_received=feedback_received, negative_feedback_received=negative_feedback_received,
                                                           received_negative_feedback_rate=received_negative_feedback_rate, date_granularity=date_granularity)
                # sales and traffic by asin
                sales_traffic_asin = json_dict.get('salesAndTrafficByAsin')
                for item in sales_traffic_asin:
                    item_flat = flatten_json(item)
                    parent_asin = item_flat.get('parentAsin')
                    child_asin = item_flat.get('childAsin')
                    units_ordered = item_flat.get('salesByAsin_unitsOrdered')
                    units_ordered_b2b = item_flat.get(
                        'salesByAsin_unitsOrderedB2B')
                    ordered_product_sales_amount = item_flat.get(
                        'salesByAsin_orderedProductSales_amount')
                    ordered_product_sales_amount_b2b = item_flat.get(
                        'salesByAsin_orderedProductSalesB2B_amount')
                    ordered_product_sales_currency_code = item_flat.get(
                        'salesByAsin_orderedProductSales_currencyCode')
                    ordered_product_sales_currency_code_b2b = item_flat.get(
                        'salesByAsin_orderedProductSalesB2B_currencyCode')
                    total_order_items = item_flat.get(
                        'salesByAsin_totalOrderItems')
                    total_order_items_b2b = item_flat.get(
                        'salesByAsin_totalOrderItemsB2B')
                    browser_sessions = item_flat.get(
                        'trafficByAsin_browserSessions')
                    browser_sessions_b2b = item_flat.get(
                        'trafficByAsin_browserSessionsB2B')
                    mobile_app_sessions = item_flat.get(
                        'trafficByAsin_mobileAppSessions')
                    mobile_app_sessions_b2b = item_flat.get(
                        'trafficByAsin_mobileAppSessionsB2B')
                    sessions = item_flat.get(
                        'trafficByAsin_sessions')
                    sessions_b2b = item_flat.get(
                        'trafficByAsin_sessionsB2B')
                    browser_session_percentage = item_flat.get(
                        'trafficByAsin_browserSessionPercentage')
                    browser_session_percentage_b2b = item_flat.get(
                        'trafficByAsin_browserSessionPercentageB2B')
                    mobile_app_session_percentage = item_flat.get(
                        'trafficByAsin_mobileAppSessionPercentage')
                    mobile_app_session_percentage_b2b = item_flat.get(
                        'trafficByAsin_mobileAppSessionPercentageB2B')
                    session_percentage = item_flat.get(
                        'trafficByAsin_sessionPercentage')
                    session_percentage_b2b = item_flat.get(
                        'trafficByAsin_sessionPercentageB2B')
                    browser_page_views = item_flat.get(
                        'trafficByAsin_browserPageViews')
                    browser_page_views_b2b = item_flat.get(
                        'trafficByAsin_browserPageViewsB2B')
                    mobile_app_page_views = item_flat.get(
                        'trafficByAsin_mobileAppPageViews')
                    mobile_app_page_views_b2b = item_flat.get(
                        'trafficByAsin_mobileAppPageViewsB2B')
                    page_views = item_flat.get('trafficByAsin_pageViews')
                    page_views_b2b = item_flat.get(
                        'trafficByAsin_pageViewsB2B')
                    browser_page_views_percentage = item_flat.get(
                        'trafficByAsin_browserPageViewsPercentage')
                    browser_page_views_percentage_b2b = item_flat.get(
                        'trafficByAsin_browserPageViewsPercentageB2B')
                    mobile_app_page_views_percentage = item_flat.get(
                        'trafficByAsin_mobileAppPageViewsPercentage')
                    mobile_app_page_views_percentage_b2b = item_flat.get(
                        'trafficByAsin_mobileAppPageViewsPercentageB2B')
                    page_views_percentage = item_flat.get(
                        'trafficByAsin_pageViewsPercentage')
                    page_views_percentage_b2b = item_flat.get(
                        'trafficByAsin_pageViewsPercentageB2B')
                    buy_box_percentage = item_flat.get(
                        'trafficByAsin_buyBoxPercentage')
                    buy_box_percentage_b2b = item_flat.get(
                        'trafficByAsin_buyBoxPercentageB2B')
                    unit_session_percentage = item_flat.get(
                        'trafficByAsin_unitSessionPercentage')
                    unit_session_percentage_b2b = item_flat.get(
                        'trafficByAsin_unitSessionPercentageB2B')
                    asin_granularity = asin_granularity

                    AzSalesTrafficAsin.insert_or_update(account_id=account_id, asp_id=asp_id, parent_asin=parent_asin, child_asin=child_asin,
                                                        payload_date=payload_date, units_ordered=units_ordered, units_ordered_b2b=units_ordered_b2b,
                                                        ordered_product_sales_amount=ordered_product_sales_amount, ordered_product_sales_amount_b2b=ordered_product_sales_amount_b2b,
                                                        ordered_product_sales_currency_code=ordered_product_sales_currency_code,
                                                        ordered_product_sales_currency_code_b2b=ordered_product_sales_currency_code_b2b,
                                                        total_order_items=total_order_items, total_order_items_b2b=total_order_items_b2b,
                                                        browser_sessions=browser_sessions, browser_sessions_b2b=browser_sessions_b2b,
                                                        mobile_app_sessions=mobile_app_sessions, mobile_app_sessions_b2b=mobile_app_sessions_b2b,
                                                        sessions=sessions, sessions_b2b=sessions_b2b,
                                                        browser_session_percentage=browser_session_percentage,
                                                        browser_session_percentage_b2b=browser_session_percentage_b2b,
                                                        mobile_app_session_percentage=mobile_app_session_percentage,
                                                        mobile_app_session_percentage_b2b=mobile_app_session_percentage_b2b,
                                                        session_percentage=session_percentage, session_percentage_b2b=session_percentage_b2b,
                                                        browser_page_views=browser_page_views, browser_page_views_b2b=browser_page_views_b2b,
                                                        mobile_app_page_views=mobile_app_page_views, mobile_app_page_views_b2b=mobile_app_page_views_b2b,
                                                        page_views=page_views, page_views_b2b=page_views_b2b,
                                                        browser_page_views_percentage=browser_page_views_percentage,
                                                        browser_page_views_percentage_b2b=browser_page_views_percentage_b2b,
                                                        mobile_app_page_views_percentage=mobile_app_page_views_percentage,
                                                        mobile_app_page_views_percentage_b2b=mobile_app_page_views_percentage_b2b,
                                                        page_views_percentage=page_views_percentage, page_views_percentage_b2b=page_views_percentage_b2b,
                                                        buy_box_percentage=buy_box_percentage, buy_box_percentage_b2b=buy_box_percentage_b2b,
                                                        unit_session_percentage=unit_session_percentage, unit_session_percentage_b2b=unit_session_percentage_b2b,
                                                        asin_granularity=asin_granularity)
                if get_report_document_id:
                    get_report_document_id.status = ASpReportProcessingStatus.COMPLETED.value
                    get_report_document_id.status_updated_at = int(time.time())
                    db.session.commit()

                if queue_task:
                    queue_task.status = QueueTaskStatus.COMPLETED.value
                    queue_task.save()
                else:
                    raise Exception

            except Exception as e:
                if get_report_document_id:
                    get_report_document_id.status = ASpReportProcessingStatus.ERROR.value
                    get_report_document_id.status_updated_at = int(time.time())
                    db.session.commit()

                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                error_message = 'Error while retrieving report in SalesTrafficReportWorker.get_sales_traffic_report(): ' + \
                    str(e)
                logger.error(error_message)
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='SalesTrafficReportWorker (Business Report) Download Report Failure',
                                        template='emails/slack_email.html', data={}, error_message=error_message, traceback_info=traceback.format_exc())
