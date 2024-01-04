"""
# This script is for generating Amazon Reports
# It defines a class, AspReportWorker, to handle report generation
"""
from datetime import datetime
from datetime import timedelta
import time
import traceback


class AspReportWorker:
    """ A class for generating Amazon Reports """

    @classmethod
    def create_reports(cls, *args, **kwargs):  # type: ignore  # noqa: C901
        """ Create Amazon Reports"""
        from app import app, logger, db, config_data
        from app.helpers.constants import ASpReportType
        from app.helpers.constants import QueueName
        from app.helpers.constants import EntityType
        from app.helpers.constants import TimeInSeconds
        from app.models.user import User
        from app.models.account import Account
        from app.models.az_report import AzReport
        from app.helpers.utility import convert_date_string
        from app.helpers.utility import get_current_datetime
        from app.helpers.queue_helper import add_queue_task_and_enqueue
        from providers.mail import send_error_notification

        with app.app_context():

            try:
                report_list = kwargs.get('report_list')
                time_interval = kwargs.get('time_interval', None)   # type: ignore  # noqa: FKA100

                logger.info(f'Report List: {report_list}')
                logger.info(f'Time Interval: {time_interval}')

                users = db.session.query(User.id, Account.uuid, Account.asp_id, Account.asp_credentials).join(Account, User.id == Account.primary_user_id).all()  # type: ignore  # noqa: FKA100

                for row in users:
                    user_id, account_id, asp_id, asp_credentials = row

                    if account_id is not None and asp_id is not None and asp_credentials is not None:

                        logged_in_user = user_id

                        data = {
                            'user_id': user_id,
                            'account_id': account_id,
                        }

                        for asp_report_type in report_list:

                            logger.info('*' * 200)

                            logger.info(
                                f'Report Type Creation: {asp_report_type}')

                            """Get the last report created by report type"""
                            last_report = AzReport.get_last_report(
                                account_id=account_id, type=asp_report_type)

                            start_datetime, end_datetime = None, None

                            if last_report:
                                logger.info(
                                    f'last_report: {last_report.request_end_time}')

                                convert_date_string_flag = True
                                if asp_report_type == ASpReportType.LIST_FINANCIAL_EVENTS.value:
                                    """Fetch for last 3 Days"""
                                    get_end_date = last_report.request_end_time - \
                                        timedelta(days=3)
                                elif asp_report_type == ASpReportType.LEDGER_SUMMARY_VIEW_DATA.value:
                                    """Fetch for last 3 Days"""
                                    get_end_date = last_report.request_end_time - \
                                        timedelta(days=3)
                                elif asp_report_type == ASpReportType.SALES_TRAFFIC_REPORT.value:
                                    """Fetch for last day till current date"""
                                    if time_interval == TimeInSeconds.FIVE_MIN.value:
                                        convert_date_string_flag = False
                                        start_datetime = last_report.request_end_time
                                        end_datetime = datetime.now()
                                    else:
                                        convert_date_string_flag = False
                                        start_datetime = last_report.request_end_time - \
                                            timedelta(days=14)
                                        end_datetime = datetime.now()
                                elif asp_report_type == ASpReportType.FBA_RETURNS_REPORT.value:
                                    """Fetch for last 14 Days"""
                                    get_end_date = last_report.request_end_time - \
                                        timedelta(days=14)
                                elif asp_report_type == ASpReportType.FBA_REIMBURSEMENTS_REPORT.value:
                                    """Fetch for last 14 Days"""
                                    get_end_date = last_report.request_end_time - \
                                        timedelta(days=14)
                                elif asp_report_type == ASpReportType.FBA_CUSTOMER_SHIPMENT_SALES_REPORT.value:
                                    """Fetch for last 14 Days"""
                                    get_end_date = last_report.request_end_time - \
                                        timedelta(days=14)
                                else:
                                    get_end_date = last_report.request_end_time + \
                                        timedelta(seconds=1)

                                if convert_date_string_flag:
                                    start_datetime = convert_date_string(
                                        get_end_date)
                                    end_datetime = convert_date_string(
                                        get_current_datetime())

                                payload = {
                                    'start_datetime': start_datetime,
                                    'end_datetime': end_datetime
                                }

                                data.update(payload)

                                logger.info(f'Log data -> {data}')

                                if asp_report_type == ASpReportType.ITEM_MASTER_LIST_ALL_DATA.value:
                                    add_queue_task_and_enqueue(queue_name=QueueName.ITEM_MASTER_REPORT, account_id=account_id,
                                                               logged_in_user=logged_in_user, entity_type=EntityType.ITEM_MASTER.value, data=data)
                                elif asp_report_type == ASpReportType.ORDER_REPORT_ORDER_DATE_GENERAL.value:
                                    add_queue_task_and_enqueue(queue_name=QueueName.ORDER_REPORT, account_id=account_id,
                                                               logged_in_user=logged_in_user, entity_type=EntityType.ORDER_REPORT.value, data=data)
                                elif asp_report_type == ASpReportType.LIST_FINANCIAL_EVENTS.value:
                                    add_queue_task_and_enqueue(queue_name=QueueName.FINANCE_EVENT_LIST, account_id=account_id,
                                                               logged_in_user=logged_in_user, entity_type=EntityType.FINANCE_EVENT_LIST.value, data=data)
                                elif asp_report_type == ASpReportType.LEDGER_SUMMARY_VIEW_DATA.value:
                                    add_queue_task_and_enqueue(queue_name=QueueName.LEDGER_SUMMARY_REPORT, account_id=account_id,
                                                               logged_in_user=logged_in_user, entity_type=EntityType.LEDGER_SUMMARY_REPORT.value, data=data)
                                elif asp_report_type == ASpReportType.SALES_TRAFFIC_REPORT.value:
                                    add_queue_task_and_enqueue(queue_name=QueueName.SALES_TRAFFIC_REPORT, account_id=account_id,
                                                               logged_in_user=logged_in_user, entity_type=EntityType.SALES_TRAFFIC_REPORT.value, data=data)
                                elif asp_report_type == ASpReportType.FBA_RETURNS_REPORT.value:
                                    add_queue_task_and_enqueue(queue_name=QueueName.FBA_RETURNS_REPORT, account_id=account_id,
                                                               logged_in_user=logged_in_user, entity_type=EntityType.FBA_RETURNS_REPORT.value, data=data)
                                elif asp_report_type == ASpReportType.FBA_REIMBURSEMENTS_REPORT.value:
                                    add_queue_task_and_enqueue(queue_name=QueueName.FBA_REIMBURSEMENTS_REPORT, account_id=account_id,
                                                               logged_in_user=logged_in_user, entity_type=EntityType.FBA_REIMBURSEMENTS_REPORT.value, data=data)
                                elif asp_report_type == ASpReportType.FBA_CUSTOMER_SHIPMENT_SALES_REPORT.value:
                                    add_queue_task_and_enqueue(queue_name=QueueName.FBA_CUSTOMER_SHIPMENT_SALES_REPORT, account_id=account_id,
                                                               logged_in_user=logged_in_user, entity_type=EntityType.FBA_CUSTOMER_SHIPMENT_SALES_REPORT.value, data=data)

                            time.sleep(1 / 15)

            except Exception as e:
                logger.error(
                    f'Error while creating {report_list} report in AspReportWorker.create_reports: ' + str(e))
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='AspReportWorker Create Report Failure',
                                        template='emails/slack_email.html', data={'report_list': report_list}, error_message=str(e), traceback_info=traceback.format_exc())
