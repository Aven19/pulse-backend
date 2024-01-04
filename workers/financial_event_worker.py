import datetime
import time
import traceback

from app import app
from app import config_data
from app import logger
from app.helpers.constants import AspFinanceEventList
from app.helpers.constants import ASpReportProcessingStatus
from app.helpers.constants import ASpReportType
from app.helpers.constants import FINANCIAL_EVENTS_MAX_RESULTS_PER_PAGE
from app.helpers.constants import QueueTaskStatus
from app.helpers.constants import TimePeriod
from app.helpers.utility import convert_string_to_datetime
from app.helpers.utility import generate_uuid
from app.helpers.utility import get_from_to_date_by_time_period
from app.models.account import Account
from app.models.az_report import AzReport
from app.models.queue_task import QueueTask
from providers.amazon_sp_client import AmazonReportEU
from providers.mail import send_error_notification


class FinanceEventWorker:
    """Class to store finance event list data from amazon API's"""

    @classmethod
    def create_finance_event_report(cls, data):  # type: ignore  # noqa: C901
        with app.app_context():
            from app.views.asp_finance_view import AspFinanceView
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

                max_results_per_page = int(data.get('max_results_per_page')) if data.get(
                    'max_results_per_page') is not None else FINANCIAL_EVENTS_MAX_RESULTS_PER_PAGE

                # start_datetime = datetime.datetime(2023, 7, 3)  # type: ignore  # noqa: FKA100
                # end_datetime = datetime.datetime(2023, 7, 5)  # type: ignore  # noqa: FKA100

                # set the number of days to fetch reports
                delta = datetime.timedelta(days=1)

                while start_datetime < end_datetime:
                    params = {
                        'PostedAfter': start_datetime.strftime('%Y-%m-%d'),
                        'PostedBefore': (start_datetime + delta).strftime('%Y-%m-%d'),
                        'MaxResultsPerPage': max_results_per_page
                    }

                    count = 1

                    lower_max_results_per_page = 5

                    while True:

                        ref_id = generate_uuid()

                        try:

                            report = AmazonReportEU(credentials=credentials)

                            get_report = AzReport.add(account_id=account_id, seller_partner_id=asp_id, request_start_time=start_datetime,
                                                      request_end_time=start_datetime + delta, type=ASpReportType.LIST_FINANCIAL_EVENTS.value, reference_id=ref_id)

                            response, response_headers, rate_limit, response_status_code = report.get_financial_events(
                                params=params, _response_headers=True, _rate_limit=True, _response_status=True)

                            if response_status_code == 400 and 'x-amzn-ErrorType' in response_headers:
                                x_amzn_error_type = response_headers.get('x-amzn-ErrorType', None)  # type: ignore  # noqa: FKA100
                                if x_amzn_error_type == 'InvalidInputException':
                                    logger.info('max_results_per_page: {}'.format(
                                        max_results_per_page))
                                    max_results_per_page -= lower_max_results_per_page

                                    if max_results_per_page >= 5:
                                        params = {
                                            'PostedAfter': start_datetime.strftime('%Y-%m-%d'),
                                            'PostedBefore': (start_datetime + delta).strftime('%Y-%m-%d'),
                                            'MaxResultsPerPage': max_results_per_page
                                        }
                                        continue

                            if 'payload' not in response:
                                raise Exception(
                                    f'Error Fetching Report for {get_report.reference_id}')

                            if get_report:
                                # pytype: disable=not-writable
                                get_report.status = ASpReportProcessingStatus.FETCHED.value
                                get_report.status_updated_at = int(time.time())
                                get_report.save()

                            finance_data = response['payload'].get(
                                'FinancialEvents')

                            # import json
                            # from app import config_data
                            # with open(config_data.get('UPLOAD_FOLDER') + ASpReportType.LIST_FINANCIAL_EVENTS.value.lower() + '/finance_event_list_page_{}_{}_{}_{}.json'.format(count, ref_id, start_datetime, start_datetime+delta), 'w') as f:                           # type: ignore  # noqa: FKA100
                            #     json.dump(finance_data, f)

                            shipment_event_list = finance_data.get(
                                AspFinanceEventList.SHIPMENT.value)
                            refund_event_list = finance_data.get(
                                AspFinanceEventList.REFUND.value)
                            service_event_list = finance_data.get(
                                AspFinanceEventList.SERVICE_FEE.value)
                            product_ads_payment_event_list = finance_data.get(
                                AspFinanceEventList.PRODUCT_ADS_PAYMENT.value)
                            adjustment_event_list = finance_data.get(
                                AspFinanceEventList.ADJUSTMENT.value)

                            # For Shipment event list
                            AspFinanceView.add_finance_events(
                                account_id=account_id, asp_id=asp_id, event_type=AspFinanceEventList.SHIPMENT.value, event_list=shipment_event_list)

                            # For Refund and Service Fee event list
                            AspFinanceView.add_finance_events(
                                account_id=account_id, asp_id=asp_id, event_type=AspFinanceEventList.REFUND.value, event_list=refund_event_list, service_event_list=service_event_list)

                            if product_ads_payment_event_list:
                                AspFinanceView.add_finance_events(
                                    account_id=account_id, asp_id=asp_id, event_type=AspFinanceEventList.PRODUCT_ADS_PAYMENT.value, event_list=product_ads_payment_event_list)

                            if adjustment_event_list:
                                AspFinanceView.add_finance_events(
                                    account_id=account_id, asp_id=asp_id, event_type=AspFinanceEventList.ADJUSTMENT.value, event_list=adjustment_event_list)

                            if get_report:
                                # pytype: disable=not-writable
                                get_report.status = ASpReportProcessingStatus.COMPLETED.value
                                get_report.status_updated_at = int(time.time())
                                get_report.save()

                            if 'NextToken' not in response['payload']:
                                break

                            next_token = response['payload']['NextToken']
                            params = {'NextToken': next_token}
                            count += 1
                            time.sleep(3)

                        except Exception as e:
                            error_message = 'Error processing report with reference ID {}: {}'.format(
                                ref_id, str(e))
                            logger.error(error_message)
                            logger.error(traceback.format_exc())
                            send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='FinanceEventWorker (Financial Event List), Error processing report',
                                                    template='emails/slack_email.html', data={}, error_message=error_message, traceback_info=traceback.format_exc())
                            if get_report:
                                # pytype: disable=not-writable
                                get_report.status = ASpReportProcessingStatus.ERROR.value
                                get_report.status_updated_at = int(time.time())
                                get_report.save()
                            break

                    max_results_per_page = int(data.get('max_results_per_page')) if data.get(
                        'max_results_per_page') is not None else FINANCIAL_EVENTS_MAX_RESULTS_PER_PAGE
                    start_datetime += delta

                if queue_task:
                    queue_task.status = QueueTaskStatus.COMPLETED.value
                    queue_task.save()

            except Exception as e:
                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                error_message = 'Error while creating report in FinanceEventWorker.create_finance_event_report(): ' + \
                    str(e)
                logger.error(error_message)
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='FinanceEventWorker (Financial Event List) Download Report Failure',
                                        template='emails/slack_email.html', data={}, error_message=error_message, traceback_info=traceback.format_exc())
