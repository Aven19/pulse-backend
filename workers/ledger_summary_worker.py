import gzip
import io
import time
import traceback

from app import app
from app import config_data
from app import db
from app import logger
from app.helpers.constants import AggregateByLocation
from app.helpers.constants import AggregateByTimePeriod
from app.helpers.constants import ASpReportProcessingStatus
from app.helpers.constants import ASpReportType
from app.helpers.constants import QueueTaskStatus
from app.helpers.constants import TimePeriod
from app.helpers.utility import get_asp_market_place_ids
from app.helpers.utility import get_from_to_date_by_timestamp
from app.models.account import Account
from app.models.az_ledger_summary import AzLedgerSummary
from app.models.az_report import AzReport
from app.models.queue_task import QueueTask
import numpy as np
import pandas as pd
from providers.amazon_sp_client import AmazonReportEU
from providers.mail import send_error_notification
import requests


class LedgerSummaryWorker:
    """Class method to create Ledger Summary Report from amazon API's"""

    @classmethod
    def create_report(cls, data) -> None:

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
                    'reportOptions': {
                        'aggregateByLocation': AggregateByLocation.FC.value,
                        'aggregatedByTimePeriod': AggregateByTimePeriod.DAILY.value
                    },
                    'reportType': ASpReportType.LEDGER_SUMMARY_VIEW_DATA.value,
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
                             request_end_time=end_datetime, type=ASpReportType.LEDGER_SUMMARY_VIEW_DATA.value, reference_id=response['reportId'])

                if queue_task:
                    queue_task.status = QueueTaskStatus.COMPLETED.value
                    queue_task.save()

                #     queue_task_report_verify = QueueTask.add_queue_task(queue_name=QueueName.LEDGER_SUMMARY_REPORT,
                #                                                         owner_id=user_id,
                #                                                         status=QueueTaskStatus.NEW.value,
                #                                                         entity_type=EntityType.LEDGER_SUMMARY_REPORT.value,
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

                error_message = 'Error while creating report in LedgerSummaryWorker.create_report(): ' + \
                    str(e)
                logger.error(error_message)
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='LedgerSummaryWorker (LEDGER SUMMARY REPORT) Create Report Failure',
                                        template='emails/slack_email.html', data={}, error_message=error_message, traceback_info=traceback.format_exc())

    @classmethod
    def get_ledger_summary_report(cls, data):
        """This method gets the ledger summary reports from amazon and inserts into databse"""
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

                # get url
                if 'url' in get_report:
                    file_url = get_report['url']
                else:
                    logger.error(
                        'file url not found for ledger summary report')
                    raise Exception

                # get file data by accessing the url
                file_data = requests.get(file_url)

                if file_data.status_code != 200:
                    logger.error(
                        'Exception occured while retrieving ledger summary report')
                    raise Exception

                if 'compressionAlgorithm' in get_report and get_report['compressionAlgorithm'] == 'GZIP':
                    streamed_url_response = requests.get(get_report['url'])
                    compressed_content = streamed_url_response.content
                    decompressed_content = gzip.decompress(compressed_content)
                    content = decompressed_content.decode('utf-8')
                else:
                    streamed_url_response = requests.get(get_report['url'])
                    content = streamed_url_response.content.decode('utf-8')

                df_stream = pd.read_csv(io.StringIO(
                    content), delimiter='\t', header=0, iterator=True, chunksize=1000)

                data_frame = pd.concat(df_stream, ignore_index=True)

                # transforming data before db insertion
                data_frame.columns = ['date', 'fnsku', 'asin', 'msku', 'title', 'disposition', 'starting_warehouse_balance',
                                      'in_transit_btw_warehouse', 'receipts', 'customer_shipments', 'customer_returns', 'vendor_returns',
                                      'warehouse_transfer', 'found', 'lost', 'damaged', 'disposed', 'other_events',
                                      'ending_warehouse_balance', 'unknown_events', 'location']

                data_frame['date'] = pd.to_datetime(
                    data_frame['date'], format='%m/%d/%Y').dt.date

                # transformation
                data_frame = data_frame.fillna(np.nan).replace([np.nan], [None])   # type: ignore  # noqa: FKA100

                # inserting data into db
                for row in data_frame.itertuples(index=False):
                    AzLedgerSummary.add_update(account_id=account_id, asp_id=asp_id, date=row[0], fnsku=row[1], asin=row[2],
                                               msku=row[3], title=row[4], disposition=row[5], starting_warehouse_balance=row[6],
                                               in_transit_btw_warehouse=row[7], receipts=row[8], customer_shipments=row[
                        9], customer_returns=row[10], vendor_returns=row[11], warehouse_transfer=row[12],
                        found=row[13], lost=row[14], damaged=row[15], disposed=row[16], other_events=row[17], ending_warehouse_balance=row[18], unknown_events=row[19], location=row[20])

                get_report_document_id.status = ASpReportProcessingStatus.COMPLETED.value
                get_report_document_id.status_updated_at = int(time.time())
                db.session.commit()

                if queue_task:
                    queue_task.status = QueueTaskStatus.COMPLETED.value
                    queue_task.save()
                else:
                    raise Exception

            except Exception as e:
                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                error_message = 'Error while retrieving report in LedgerSummaryWorker.get_ledger_summary_report(): ' + \
                    str(e)
                logger.error(error_message)
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='LedgerSummaryWorker Download Report Failure',
                                        template='emails/slack_email.html', data={}, error_message=error_message, traceback_info=traceback.format_exc())
