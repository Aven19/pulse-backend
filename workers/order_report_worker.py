import csv
import gzip
import io
import time
import traceback

from app import app
from app import config_data
from app import db
from app import logger
from app import order_report_q
from app.helpers.constants import ASpReportProcessingStatus
from app.helpers.constants import ASpReportType
from app.helpers.constants import EntityType
from app.helpers.constants import QueueName
from app.helpers.constants import QueueTaskStatus
from app.helpers.constants import TimePeriod
from app.helpers.utility import get_asp_market_place_ids
from app.helpers.utility import get_from_to_date_by_timestamp
from app.models.account import Account
from app.models.az_order_report import AzOrderReport
from app.models.az_report import AzReport
from app.models.queue_task import QueueTask
import numpy as np
import pandas as pd
from providers.amazon_sp_client import AmazonReportEU
from providers.mail import send_error_notification
import requests


class OrderReportWorker:

    @classmethod
    def create_order_report(cls, data) -> None:

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

                logger.info('Logging Amazon seller partner credentials')
                logger.info(credentials)

                if default_sync:
                    default_time_period = data.get('default_time_period', TimePeriod.LAST_30_DAYS.value)  # type: ignore  # noqa: FKA100
                    start_datetime, end_datetime = get_from_to_date_by_timestamp(
                        default_time_period)
                else:
                    start_datetime = data.get('start_datetime')
                    end_datetime = data.get('end_datetime')
                    if start_datetime is None or end_datetime is None:
                        logger.error(
                            "Queue Task with job_id '{}' failed. start_datetime : {}, end_datetime : {}".format(job_id, start_datetime, end_datetime))
                        raise Exception

                payload = {
                    'reportType': ASpReportType.ORDER_REPORT_ORDER_DATE_GENERAL.value,
                    'dataStartTime': start_datetime,
                    'dataEndTime': end_datetime,
                    'marketplaceIds': get_asp_market_place_ids()
                }

                # creating AmazonReportEU object and passing the credentials
                report = AmazonReportEU(credentials=credentials)

                # calling create report function of report object and passing the payload
                response = report.create_report(payload=payload)

                # adding the report_type, report_id to the report table
                AzReport.add(account_id=account_id, seller_partner_id=asp_id, request_start_time=start_datetime,
                             request_end_time=end_datetime, type=ASpReportType.ORDER_REPORT_ORDER_DATE_GENERAL.value, reference_id=response['reportId'])

                if queue_task:
                    queue_task.status = QueueTaskStatus.COMPLETED.value
                    queue_task.save()

                #     queue_task_report_verify = QueueTask.add_queue_task(queue_name=QueueName.ORDER_REPORT,
                #                                                         owner_id=user_id,
                #                                                         status=QueueTaskStatus.NEW.value,
                #                                                         entity_type=EntityType.ORDER_REPORT.value,
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
                #         order_report_q.enqueue_in(timedelta(minutes=3), OrderReportWorker.verify_order_report, data=queue_task_dict, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100

                # else:
                #     raise Exception

            except Exception as e:
                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                error_message = 'Error while creating report in OrderReportWorker.create_order_report(): ' + \
                    str(e)
                logger.error(error_message)
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='OrderReportWorker (ORDER REPORT) Create Report Failure',
                                        template='emails/slack_email.html', data={}, error_message=error_message, traceback_info=traceback.format_exc())

    @classmethod
    def verify_order_report(cls, data):
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
                get_report = AzReport.get_by_ref_id(
                    account_id=account_id, reference_id=report_id)

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

                    queue_task_retrieve_verify = QueueTask.add_queue_task(queue_name=QueueName.ORDER_REPORT,
                                                                          owner_id=user_id,
                                                                          status=QueueTaskStatus.NEW.value,
                                                                          entity_type=EntityType.ORDER_REPORT.value,
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

                        order_report_q.enqueue(OrderReportWorker.get_order_report, data=queue_task_dict, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100

                else:
                    raise Exception

            except Exception as e:
                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                error_message = 'Error while verifying report in OrderReportWorker.verify_order_report(): ' + \
                    str(e)
                logger.error(error_message)
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='Error while verifying report in OrderReportWorker',
                                        template='emails/slack_email.html', data={}, error_message=error_message, traceback_info=traceback.format_exc())

    @classmethod
    def get_order_report(cls, data):
        """This should queue every minute to verify report"""
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

            try:
                asp_id = account.asp_id
                credentials = account.retrieve_asp_credentials(account)[0]

                # querying Report table to get the entry for particular report_id
                get_report_document_id = AzReport.get_by_ref_id(account_id=account_id,
                                                                reference_id=report_id)

                if not get_report_document_id:
                    raise Exception

                report_document_id = get_report_document_id.document_id

                # creating AmazonReportEU object and passing the credentials
                report = AmazonReportEU(credentials=credentials)

                # using retrieve_report function of report object to get report
                get_report = report.retrieve_report(report_document_id)

                if 'compressionAlgorithm' in get_report and get_report['compressionAlgorithm'] == 'GZIP':
                    streamed_url_response = requests.get(get_report['url'])
                    compressed_content = streamed_url_response.content
                    decompressed_content = gzip.decompress(compressed_content)
                    content = decompressed_content.decode('utf-8')
                else:
                    streamed_url_response = requests.get(get_report['url'])
                    content = streamed_url_response.content.decode('utf-8')

                if content:
                    # Create a streaming pandas DataFrame
                    df_stream = pd.read_csv(io.StringIO(
                        content), delimiter='\t', header=None, skiprows=1, iterator=True, chunksize=1000, quoting=csv.QUOTE_NONE)
                    order_df = pd.concat(df_stream, ignore_index=True)

                    # temp
                    # order_df.to_csv(config_data.get('UPLOAD_FOLDER') + ASpReportType.ORDER_REPORT_ORDER_DATE_GENERAL.value.lower(
                    #         ) + '/{}.csv'.format(report_document_id), index=False)

                    # Rename the columns
                    order_df.columns = ['amazon_order_id', 'merchant_order_id', 'purchase_date', 'last_updated_date', 'order_status', 'fulfillment_channel', 'sales_channel', 'order_channel', 'ship_service_level', 'product_name', 'sku', 'asin', 'item_status', 'quantity', 'currency', 'item_price', 'item_tax',
                                        'shipping_price', 'shipping_tax', 'gift_wrap_price', 'gift_wrap_tax', 'item_promotion_discount', 'ship_promotion_discount', 'ship_city', 'ship_state', 'ship_postal_code', 'ship_country', 'promotion_ids', 'is_business_order', 'purchase_order_number', 'price_designation', 'fulfilled_by', 'is_iba']

                    desired_columns = ['amazon_order_id', 'merchant_order_id', 'purchase_date', 'last_updated_date', 'order_status', 'fulfillment_channel', 'sales_channel',
                                       'ship_service_level', 'product_name', 'sku', 'asin', 'item_status', 'quantity', 'currency', 'item_price', 'item_tax',
                                       'shipping_price', 'shipping_tax', 'gift_wrap_price', 'gift_wrap_tax', 'item_promotion_discount', 'ship_promotion_discount', 'ship_city', 'ship_state', 'ship_postal_code', 'ship_country']

                    # Drop columns not in the desired columns list
                    order_df_selected_columns = order_df[desired_columns]

                    # transformation
                    order_df_selected_columns = order_df_selected_columns.fillna(np.nan).replace([np.nan], [None])   # type: ignore  # noqa: FKA100

                    count = 0
                    for row in order_df_selected_columns.iloc[1:].itertuples(index=False):
                        count += 1
                        order = AzOrderReport.get_by_az_order_id_and_sku(
                            selling_partner_id=asp_id, amazon_order_id=row.amazon_order_id, sku=row.sku)

                        if order:

                            # if order.order_status == row.order_status:
                            #     quantity = order.quantity + row.quantity
                            #     order.quantity = quantity
                            #     db.session.commit()

                            AzOrderReport.update_orders(
                                account_id=account_id,
                                selling_partner_id=asp_id,
                                amazon_order_id=row.amazon_order_id,
                                merchant_order_id=row.merchant_order_id, purchase_date=row.purchase_date,
                                last_updated_date=row.last_updated_date, order_status=row.order_status, fulfillment_channel=row.fulfillment_channel, sales_channel=row.sales_channel, ship_service_level=row.ship_service_level,
                                product_name=row.product_name, sku=row.sku, asin=row.asin, item_status=row.item_status, quantity=row.quantity, currency=row.currency,
                                item_price=row.item_price, item_tax=row.item_tax, shipping_price=row.shipping_price, shipping_tax=row.shipping_tax,
                                gift_wrap_price=row.gift_wrap_price, gift_wrap_tax=row.gift_wrap_tax, item_promotion_discount=row.item_promotion_discount,
                                ship_promotion_discount=row.ship_promotion_discount, ship_city=row.ship_city, ship_state=row.ship_state, ship_postal_code=row.ship_postal_code,
                                ship_country=row.ship_country)
                        else:

                            AzOrderReport.add(
                                account_id=account_id, selling_partner_id=asp_id, amazon_order_id=row.amazon_order_id, merchant_order_id=row.merchant_order_id, purchase_date=row.purchase_date, last_updated_date=row.last_updated_date,
                                order_status=row.order_status, fulfillment_channel=row.fulfillment_channel, sales_channel=row.sales_channel, ship_service_level=row.ship_service_level,
                                product_name=row.product_name, sku=row.sku, asin=row.asin, item_status=row.item_status, quantity=row.quantity, currency=row.currency,
                                item_price=row.item_price, item_tax=row.item_tax, shipping_price=row.shipping_price, shipping_tax=row.shipping_tax,
                                gift_wrap_price=row.gift_wrap_price, gift_wrap_tax=row.gift_wrap_tax, item_promotion_discount=row.item_promotion_discount,
                                ship_promotion_discount=row.ship_promotion_discount, ship_city=row.ship_city, ship_state=row.ship_state, ship_postal_code=row.ship_postal_code,
                                ship_country=row.ship_country)
                else:
                    logger.info('Empty content received from order report')

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
                    AzReport.update_status(
                        reference_id=get_report_document_id.reference_id, status=ASpReportProcessingStatus.ERROR.value)
                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                error_message = 'Error while retrieving report in OrderReportWorker.get_order_report(): ' + \
                    str(e)
                logger.error(error_message)
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='OrderReportWorker Download Report Failure',
                                        template='emails/slack_email.html', data={}, error_message=error_message, traceback_info=traceback.format_exc())

    @classmethod
    def add_item_master(cls, data):
        with app.app_context():
            job_id = data.get('job_id')
            reference_id = data.get('reference_id')
            account_id = data.get('account_id')

            if job_id is None or reference_id is None:
                logger.error(
                    "Queue Task with job_id or reference_id, Job Id: '{}' Reference Id: '{}' not found".format(job_id, reference_id))
                return

            if reference_id is None:
                logger.error(
                    "Queue Task with job_id or reference_id, Job Id: '{}' Reference Id: '{}' not found".format(job_id, reference_id))
                return

            queue_task = QueueTask.get_by_id(job_id)

            if not queue_task:
                logger.error(
                    "Queue Task with job_id '{}' not found".format(job_id))
                return

            report = AzReport.get_by_ref_id(
                account_id=account_id, reference_id=reference_id)

            if not report:
                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()
                logger.error(
                    "Report not found with reference id '{}' not found".format(reference_id))
                return

            try:

                directory_path = f"{config_data.get('UPLOAD_FOLDER')}{data['seller_partner_id']}/{data['report_type']}"
                file = f'{directory_path}/{data["file_name"]}'
                with open(file=file, mode='r') as csvfile:
                    reader = csv.DictReader(csvfile)

                    for row in reader:
                        item_price = float(
                            row['item_price']) if row['item_price'] else None
                        item_tax = float(
                            row['item_tax']) if row['item_tax'] else None
                        shipping_price = float(
                            row['shipping_price']) if row['shipping_price'] else None
                        shipping_tax = float(
                            row['shipping_tax']) if row['shipping_tax'] else None
                        gift_wrap_price = float(
                            row['gift_wrap_price']) if row['gift_wrap_price'] else None
                        gift_wrap_tax = float(
                            row['gift_wrap_tax']) if row['gift_wrap_tax'] else None
                        item_promotion_discount = float(
                            row['item_promotion_discount']) if row['item_promotion_discount'] else None
                        ship_promotion_discount = float(
                            row['ship_promotion_discount']) if row['ship_promotion_discount'] else None

                        # update and insert amazon_order_id
                        AzOrderReport.add(
                            selling_partner_id=data['seller_partner_id'],
                            amazon_order_id=row['amazon_order_id'],
                            merchant_order_id=row['merchant_order_id'],
                            purchase_date=row['purchase_date'],
                            last_updated_date=row['last_updated_date'],
                            order_status=row['order_status'],
                            fulfillment_channel=row['fulfillment_channel'],
                            sales_channel=row['sales_channel'],
                            ship_service_level=row['ship_service_level'],
                            product_name=row['product_name'],
                            sku=row['sku'],
                            asin=row['asin'],
                            item_status=row['item_status'],
                            quantity=int(row['quantity']),
                            currency=row['currency'],
                            item_price=item_price,
                            item_tax=item_tax,
                            shipping_price=shipping_price,
                            shipping_tax=shipping_tax,
                            gift_wrap_price=gift_wrap_price,
                            gift_wrap_tax=gift_wrap_tax,
                            item_promotion_discount=item_promotion_discount,
                            ship_promotion_discount=ship_promotion_discount,
                            ship_city=row['ship_city'],
                            ship_state=row['ship_state'],
                            ship_postal_code=row['ship_postal_code'],
                            ship_country=row['ship_country']
                        )

                AzReport.update_status(
                    reference_id=reference_id, status=ASpReportProcessingStatus.COMPLETED.value)

                if queue_task:
                    queue_task.status = QueueTaskStatus.COMPLETED.value
                    queue_task.save()

            except Exception as e:
                if report:
                    AzReport.update_status(
                        reference_id=report.reference_id, status=ASpReportProcessingStatus.ERROR.value)
                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()
                logger.error(
                    'Exception occured while inserting Item Master: ' + str(e))
                logger.error(traceback.format_exc())
