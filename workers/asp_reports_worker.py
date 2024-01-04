

class AspReportsWorker:

    @classmethod
    def create_reports(cls):
        from app import app

        """This method creates all the reports from the report type list"""
        with app.app_context():
            from app import db
            from app import logger
            from app.models.account import Account
            from app.models.az_report import AzReport
            from app.models.user import User
            from app.helpers.utility import convert_date_string
            from app.helpers.utility import get_current_datetime
            from datetime import timedelta
            from app.helpers.constants import ASpReportType
            import time
            import traceback
            from app.helpers.constants import EntityType
            from app.helpers.constants import QueueName
            from app.helpers.queue_helper import add_queue_task_and_enqueue

            try:

                asp_report_type_list = [
                    ASpReportType.ITEM_MASTER_LIST_ALL_DATA.value]

                users = db.session.query(User.id, Account.uuid, Account.asp_id, Account.asp_credentials).join(Account, User.id == Account.primary_user_id).all()  # type: ignore  # noqa: FKA100

                for row in users:
                    user_id, account_id, asp_id, asp_credentials = row
                    if account_id is not None and asp_id is not None and asp_credentials is not None:
                        logger.info(
                            f'user_id: {user_id}, account_id: {account_id}, asp_id: {asp_id}, asp_credentials:{asp_credentials}')

                        logged_in_user = user_id

                        data = {
                            'user_id': user_id,
                            'account_id': account_id,
                        }

                        for asp_report_type in asp_report_type_list:
                            """Get the last report created by report type"""
                            last_report = AzReport.get_last_report(
                                account_id=account_id, type=asp_report_type)

                            start_datetime, end_datetime = None, None

                            if last_report:
                                get_end_date = last_report.request_end_time + \
                                    timedelta(seconds=1)

                                if asp_report_type == ASpReportType.LEDGER_SUMMARY_VIEW_DATA.value:
                                    get_end_date = last_report.request_end_time - \
                                        timedelta(days=3)
                                elif asp_report_type == ASpReportType.LIST_FINANCIAL_EVENTS.value:
                                    get_end_date = last_report.request_end_time - \
                                        timedelta(days=3)
                                elif asp_report_type == ASpReportType.SALES_TRAFFIC_REPORT.value:
                                    get_end_date = last_report.request_end_time - \
                                        timedelta(days=3)

                                start_datetime = convert_date_string(
                                    get_end_date)
                                end_datetime = convert_date_string(
                                    get_current_datetime())

                                payload = {
                                    'start_datetime': start_datetime,
                                    'end_datetime': end_datetime,
                                }

                                data.update(payload)

                                if asp_report_type == ASpReportType.ITEM_MASTER_LIST_ALL_DATA.value:
                                    add_queue_task_and_enqueue(queue_name=QueueName.ITEM_MASTER_REPORT, account_id=account_id,
                                                               logged_in_user=logged_in_user, entity_type=EntityType.ITEM_MASTER.value, data=data)
                                elif asp_report_type == ASpReportType.SALES_TRAFFIC_REPORT.value:
                                    add_queue_task_and_enqueue(queue_name=QueueName.SALES_TRAFFIC_REPORT, account_id=account_id,
                                                               logged_in_user=logged_in_user, entity_type=EntityType.SALES_TRAFFIC_REPORT.value, data=data)
                                # elif asp_report_type == ASpReportType.SETTLEMENT_REPORT_FLAT_FILE_V2.value:
                                #     add_queue_task_and_enqueue(queue_name=QueueName.SETTLEMENT_REPORT_V2, account_id=account_id,
                                #                                logged_in_user=logged_in_user, entity_type=EntityType.SETTLEMENT_V2_REPORT.value, data=data)
                                elif asp_report_type == ASpReportType.LEDGER_SUMMARY_VIEW_DATA.value:
                                    add_queue_task_and_enqueue(queue_name=QueueName.LEDGER_SUMMARY_REPORT, account_id=account_id,
                                                               logged_in_user=logged_in_user, entity_type=EntityType.LEDGER_SUMMARY_REPORT.value, data=data)
                                elif asp_report_type == ASpReportType.LIST_FINANCIAL_EVENTS.value:
                                    add_queue_task_and_enqueue(queue_name=QueueName.FINANCE_EVENT_LIST, account_id=account_id,
                                                               logged_in_user=logged_in_user, entity_type=EntityType.FINANCE_EVENT_LIST.value, data=data)

                        # Add a delay after each request
                        # Delay for 1/15th of a second (assuming 15 requests per second)
                        time.sleep(1 / 15)

            except Exception as e:
                logger.error(
                    'Error while creating report in AspReportsWorker.create_reports(): ' + str(e))
                logger.error(traceback.format_exc())

    @classmethod
    def verify_reports(cls):  # type: ignore  # noqa: C901
        from app import app

        """This method verifies all the reports from amazon api's which are in Done or In Progress status"""
        with app.app_context():
            from app import config_data
            from app import logger
            from app import item_master_q
            from app import order_report_q
            from app import sales_traffic_report_q
            from app import settlement_report_v2_q
            from app import ledger_summary_report_q
            from app import db
            from app.models.account import Account
            from app.models.az_report import AzReport
            from app.models.user import User
            import traceback
            from providers.amazon_sp_client import AmazonReportEU
            from app.helpers.constants import ASpReportType
            import time
            from app.models.queue_task import QueueTask
            from app.helpers.constants import QueueTaskStatus
            from app.helpers.constants import QueueName
            from app.helpers.constants import EntityType
            from app.helpers.constants import ASpReportProcessingStatus
            from app import fba_customer_shipment_sales_q
            from app import fba_reimbursements_q
            from app import fba_returns_q
            from providers.mail import send_error_notification
            from datetime import timedelta

            try:

                users = db.session.query(User.id, Account.uuid, Account.asp_id, Account.asp_credentials).join(Account, User.id == Account.primary_user_id).all()  # type: ignore  # noqa: FKA100

                for row in users:
                    user_id, account_id, asp_id, asp_credentials = row
                    if account_id is not None and asp_id is not None and asp_credentials is not None:

                        data = {
                            'user_id': user_id,
                            'account_id': account_id,
                        }

                        reports = AzReport.get_pending_reports(
                            account_id=account_id, seller_partner_id=asp_id)

                        if reports:

                            credentials = {
                                'seller_partner_id': asp_credentials.get('seller_partner_id'),
                                'refresh_token': asp_credentials.get('refresh_token'),
                                'client_id': config_data.get('SP_LWA_APP_ID'),
                                'client_secret': config_data.get('SP_LWA_CLIENT_SECRET')
                            }

                            delay = 0
                            sales_and_traffic_delay = 0

                            for report in reports:

                                report_type = report.type
                                reference_id = report.reference_id
                                report_queue_id = report.queue_id

                                if report_queue_id is None and report_type != ASpReportType.LIST_FINANCIAL_EVENTS.value:

                                    # Verify processing status from amazon alawys

                                    az_report = AmazonReportEU(
                                        credentials=credentials)

                                    az_report_status = az_report.verify_report(
                                        reference_id)

                                    if az_report_status['processingStatus'] != 'DONE':
                                        _update_report = AzReport.update_by_id(
                                            id=report.id, status=az_report_status['processingStatus'], document_id=None)
                                    else:
                                        _update_report = AzReport.update_by_id(
                                            id=report.id, status=az_report_status['processingStatus'], document_id=az_report_status['reportDocumentId'])

                                    if _update_report.status == ASpReportProcessingStatus.DONE.value and _update_report.queue_id is None:
                                        queue_name = None
                                        entity_type = None
                                        if _update_report.type == ASpReportType.ITEM_MASTER_LIST_ALL_DATA.value:
                                            queue_name = QueueName.ITEM_MASTER_REPORT
                                            entity_type = EntityType.ITEM_MASTER.value
                                        elif _update_report.type == ASpReportType.ORDER_REPORT_ORDER_DATE_GENERAL.value:
                                            queue_name = QueueName.ORDER_REPORT
                                            entity_type = EntityType.ORDER_REPORT.value
                                        elif _update_report.type == ASpReportType.SALES_TRAFFIC_REPORT.value:
                                            queue_name = QueueName.SALES_TRAFFIC_REPORT
                                            entity_type = EntityType.SALES_TRAFFIC_REPORT.value
                                        # elif _update_report.type == ASpReportType.SETTLEMENT_REPORT_FLAT_FILE_V2.value:
                                        #     queue_name = QueueName.SETTLEMENT_REPORT_V2
                                        #     entity_type = EntityType.SETTLEMENT_V2_REPORT.value
                                        elif _update_report.type == ASpReportType.LEDGER_SUMMARY_VIEW_DATA.value:
                                            queue_name = QueueName.LEDGER_SUMMARY_REPORT
                                            entity_type = EntityType.LEDGER_SUMMARY_REPORT.value
                                        elif _update_report.type == ASpReportType.FBA_RETURNS_REPORT.value:
                                            queue_name = QueueName.FBA_RETURNS_REPORT
                                            entity_type = EntityType.FBA_RETURNS_REPORT.value
                                        elif _update_report.type == ASpReportType.FBA_REIMBURSEMENTS_REPORT.value:
                                            queue_name = QueueName.FBA_REIMBURSEMENTS_REPORT
                                            entity_type = EntityType.FBA_REIMBURSEMENTS_REPORT.value
                                        elif _update_report.type == ASpReportType.FBA_CUSTOMER_SHIPMENT_SALES_REPORT.value:
                                            queue_name = QueueName.FBA_CUSTOMER_SHIPMENT_SALES_REPORT
                                            entity_type = EntityType.FBA_CUSTOMER_SHIPMENT_SALES_REPORT.value

                                        queue_task = QueueTask.add_queue_task(queue_name=queue_name,
                                                                              account_id=account_id,
                                                                              owner_id=user_id,
                                                                              status=QueueTaskStatus.NEW.value,
                                                                              entity_type=entity_type,
                                                                              param=str(data), input_attachment_id=None, output_attachment_id=None)

                                        # update_queue_id = AzReport.get_by_ref_id(reference_id=reference_id)
                                        _update_report.queue_id = queue_task.id
                                        _update_report.save()

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

                                        if _update_report.type == ASpReportType.ITEM_MASTER_LIST_ALL_DATA.value:
                                            from workers.item_master_worker import ItemMasterWorker
                                            item_master_q.enqueue(ItemMasterWorker.get_item_master_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100
                                        elif _update_report.type == ASpReportType.ORDER_REPORT_ORDER_DATE_GENERAL.value:
                                            from workers.order_report_worker import OrderReportWorker
                                            order_report_q.enqueue(OrderReportWorker.get_order_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100
                                        elif _update_report.type == ASpReportType.SALES_TRAFFIC_REPORT.value:
                                            from workers.sales_traffic_report_worker import SalesTrafficReportWorker
                                            sales_traffic_report_q.enqueue_in(time_delta=timedelta(seconds=sales_and_traffic_delay), func=SalesTrafficReportWorker.get_sales_traffic_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100
                                            # sales_and_traffic_delay += 60
                                        # elif _update_report.type == ASpReportType.SETTLEMENT_REPORT_FLAT_FILE_V2.value:
                                        #     from workers.settlement_report_v2_worker import SettlementReportV2Worker
                                        #     settlement_report_v2_q.enqueue_in(timedelta(seconds=delay), SettlementReportV2Worker.process_csv_reports, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100
                                        elif _update_report.type == ASpReportType.LEDGER_SUMMARY_VIEW_DATA.value:
                                            from workers.ledger_summary_worker import LedgerSummaryWorker
                                            ledger_summary_report_q.enqueue(LedgerSummaryWorker.get_ledger_summary_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100

                                        elif _update_report.type == ASpReportType.FBA_RETURNS_REPORT.value:
                                            from workers.fba_concessions_report_worker import FbaConcessionsReportWorker
                                            fba_returns_q.enqueue(FbaConcessionsReportWorker.get_fba_returns_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100
                                        elif _update_report.type == ASpReportType.FBA_REIMBURSEMENTS_REPORT.value:
                                            from workers.fba_payments_report_worker import FbaPaymentsReportWorker
                                            fba_reimbursements_q.enqueue(FbaPaymentsReportWorker.get_fba_reimbursements_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100
                                        elif _update_report.type == ASpReportType.FBA_CUSTOMER_SHIPMENT_SALES_REPORT.value:
                                            from workers.fba_sales_report_worker import FbaSalesReportWorker
                                            fba_customer_shipment_sales_q.enqueue(FbaSalesReportWorker.get_customer_shipment_sales_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100

                            delay += 3
                            # Add a delay after each request
                            # Delay, (assuming 2 requests per second)
                            time.sleep(1 / 2)

            except Exception as e:
                logger.error(
                    'Error while creating report in AspReportsWorker.verify_reports(): ' + str(e))
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='Error while verifying Amazon report in AspReportsWorker.verify_reports',
                                        template='emails/slack_email.html', data={})
                logger.error(traceback.format_exc())

    @classmethod
    def create_order_report(cls):
        from app import app

        """This method verifies all the reports from amazon api's which are in cancelled or done status"""
        with app.app_context():
            from app import db
            from app import logger
            from app.models.account import Account
            from app.models.az_report import AzReport
            from app.models.user import User
            from app.helpers.utility import convert_date_string
            from app.helpers.utility import get_current_datetime
            from datetime import timedelta
            from app.helpers.constants import ASpReportType
            import time
            import traceback
            from app.helpers.constants import EntityType
            from app.helpers.constants import QueueName
            from app.helpers.queue_helper import add_queue_task_and_enqueue

            try:

                users = db.session.query(User.id, Account.uuid, Account.asp_id, Account.asp_credentials).join(Account, User.id == Account.primary_user_id).all()  # type: ignore  # noqa: FKA100

                for row in users:
                    user_id, account_id, asp_id, asp_credentials = row
                    if account_id is not None and asp_id is not None and asp_credentials is not None:

                        logged_in_user = user_id

                        data = {
                            'user_id': user_id,
                            'account_id': account_id,
                        }

                        """Get the last report created by report type"""
                        last_report = AzReport.get_last_report(
                            account_id=account_id, type=ASpReportType.ORDER_REPORT_ORDER_DATE_GENERAL.value)

                        start_datetime, end_datetime = None, None

                        if last_report:
                            get_end_date = last_report.request_end_time + \
                                timedelta(seconds=1)
                            start_datetime = convert_date_string(get_end_date)
                            end_datetime = convert_date_string(
                                get_current_datetime())

                            payload = {
                                'start_datetime': start_datetime,
                                'end_datetime': end_datetime,
                            }

                            data.update(payload)

                            add_queue_task_and_enqueue(queue_name=QueueName.ORDER_REPORT, account_id=account_id,
                                                       logged_in_user=logged_in_user, entity_type=EntityType.ORDER_REPORT.value, data=data)

                        # Add a delay after each request
                        # Delay for 1/15th of a second (assuming 15 requests per second)
                        time.sleep(1 / 15)

            except Exception as e:
                logger.error(
                    'Error while creating report in AspReportsWorker.create_order_report(): ' + str(e))
                logger.error(traceback.format_exc())

    @classmethod
    def create_finance_event_report(cls):
        from app import app

        """This method creates finance event list"""
        with app.app_context():
            from app.helpers.queue_helper import add_queue_task_and_enqueue
            from app.helpers.constants import QueueName
            from app.helpers.constants import EntityType
            from app.models.az_report import AzReport
            import datetime
            from app import logger
            import traceback
            from app import db
            from app.models.account import Account
            from app.models.user import User
            from app.helpers.constants import ASpReportType
            from app.helpers.utility import convert_date_string
            from app.helpers.utility import get_current_datetime

            try:

                users = db.session.query(User.id, Account.uuid, Account.asp_id, Account.asp_credentials).join(Account, User.id == Account.primary_user_id).all()  # type: ignore  # noqa: FKA100

                for row in users:
                    user_id, account_id, asp_id, asp_credentials = row
                    if account_id is not None and asp_id is not None and asp_credentials is not None:

                        logged_in_user = user_id

                        data = {
                            'user_id': user_id,
                            'account_id': account_id,
                        }

                        """Get the last report created by report type"""
                        last_report = AzReport.get_last_report(
                            account_id=account_id, type=ASpReportType.LIST_FINANCIAL_EVENTS.value)

                        start_datetime, end_datetime = None, None

                        if last_report:

                            # Calculate the end date by subtracting 3 days from last_report_end_time
                            get_end_date = last_report.request_end_time - \
                                datetime.timedelta(days=3)
                            start_datetime = convert_date_string(get_end_date)
                            end_datetime = convert_date_string(
                                get_current_datetime())

                            payload = {
                                'start_datetime': start_datetime,
                                'end_datetime': end_datetime,
                            }

                            data.update(payload)

                            add_queue_task_and_enqueue(queue_name=QueueName.FINANCE_EVENT_LIST, account_id=account_id,
                                                       logged_in_user=logged_in_user, entity_type=EntityType.FINANCE_EVENT_LIST.value, data=data)

            except Exception as e:
                logger.error(
                    'Error while creating finance event report in AspReportsWorker.create_finance_event_report(): ' + str(e))
                logger.error(traceback.format_exc())

    @classmethod
    def create_ledger_summary_report(cls):
        from app import app

        """This method creates finance event list"""
        with app.app_context():
            from app.helpers.queue_helper import add_queue_task_and_enqueue
            from app.helpers.constants import QueueName
            from app.helpers.constants import EntityType
            from app.models.az_report import AzReport
            import datetime
            from app import logger
            import traceback
            from app import db
            from app.models.account import Account
            from app.models.user import User
            from app.helpers.constants import ASpReportType
            from app.helpers.utility import convert_date_string
            from app.helpers.utility import get_current_datetime

            try:

                users = db.session.query(User.id, Account.uuid, Account.asp_id, Account.asp_credentials).join(Account, User.id == Account.primary_user_id).all()  # type: ignore  # noqa: FKA100

                for row in users:
                    user_id, account_id, asp_id, asp_credentials = row
                    if account_id is not None and asp_id is not None and asp_credentials is not None:

                        logged_in_user = user_id

                        data = {
                            'user_id': user_id,
                            'account_id': account_id,
                        }

                        """Get the last report created by report type"""
                        last_report = AzReport.get_last_report(
                            account_id=account_id, type=ASpReportType.LEDGER_SUMMARY_VIEW_DATA.value)

                        start_datetime, end_datetime = None, None

                        if last_report:

                            # Calculate the end date by subtracting 3 days from last_report_end_time
                            get_end_date = last_report.request_end_time - \
                                datetime.timedelta(days=3)
                            start_datetime = convert_date_string(get_end_date)
                            end_datetime = convert_date_string(
                                get_current_datetime())

                            payload = {
                                'start_datetime': start_datetime,
                                'end_datetime': end_datetime,
                            }

                            data.update(payload)

                            add_queue_task_and_enqueue(queue_name=QueueName.LEDGER_SUMMARY_REPORT, account_id=account_id,
                                                       logged_in_user=logged_in_user, entity_type=EntityType.LEDGER_SUMMARY_REPORT.value, data=data)

            except Exception as e:
                logger.error(
                    'Error while creating ledger summary report in AspReportsWorker.create_ledger_summary_report(): ' + str(e))
                logger.error(traceback.format_exc())

    @classmethod
    def update_catalog_items(cls):

        from app import app

        """Queue to update Item Master catalog"""
        with app.app_context():
            from app.helpers.constants import QueueName
            from app.helpers.constants import EntityType
            from app import logger
            import traceback
            from app import db
            from app.models.account import Account
            from app.models.user import User
            from app.models.queue_task import QueueTask
            from app.helpers.constants import QueueTaskStatus
            from app import item_master_update_catalog_q
            from workers.item_master_worker import ItemMasterWorker
            from app.models.az_item_master import AzItemMaster
            from app import config_data
            from providers.mail import send_error_notification

            try:

                users = db.session.query(User.id, Account.uuid, Account.asp_id, Account.asp_credentials).join(Account, User.id == Account.primary_user_id).all()  # type: ignore  # noqa: FKA100

                for row in users:
                    user_id, account_id, asp_id, asp_credentials = row
                    if account_id is not None and asp_id is not None and asp_credentials is not None:

                        logged_in_user = user_id

                        data = {
                            'user_id': user_id,
                            'account_id': account_id,
                            'asp_id': asp_id
                        }

                        item_total_count = AzItemMaster.get_total_records(
                            account_id=account_id, asp_id=asp_id)

                        if item_total_count > 0:

                            queue_task = QueueTask.add_queue_task(queue_name=QueueName.ITEM_MASTER_UPDATE_CATALOG,
                                                                  owner_id=logged_in_user,
                                                                  account_id=account_id,
                                                                  status=QueueTaskStatus.NEW.value,
                                                                  entity_type=EntityType.ITEM_MASTER.value,
                                                                  param=str(
                                                                      data),
                                                                  input_attachment_id=None,
                                                                  output_attachment_id=None)

                            if queue_task:
                                queue_task_dict = {
                                    'job_id': queue_task.id,
                                    'queue_name': queue_task.queue_name,
                                    'status': QueueTaskStatus.get_status(queue_task.status),
                                    'entity_type': EntityType.get_type(queue_task.entity_type)
                                }
                                data.update(queue_task_dict)
                                queue_task.param = str(data)
                                queue_task.save()
                                item_master_update_catalog_q.enqueue(ItemMasterWorker.update_catalog_items, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100

            except Exception as e:
                logger.error(
                    'Error while Updating catalog items in AspReportsWorker.update_catalog_items(): ' + str(e))
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='Error while Updating catalog items (ITEM MASTER REPORT)',
                                        template='emails/slack_email.html', data={})
                logger.error(traceback.format_exc())

    @classmethod
    def create_sponsored_ads_report(cls):
        from app import app

        """This method creates sponsored ads reports"""
        with app.app_context():
            from app.helpers.queue_helper import add_queue_task_and_enqueue
            from app.helpers.constants import QueueName
            from app.helpers.constants import EntityType
            from app.models.az_report import AzReport
            from app import logger
            import traceback
            from app import db
            from app.models.account import Account
            from app.models.user import User
            from app.helpers.constants import ASpReportType
            from app.helpers.utility import convert_date_string
            from app.helpers.utility import get_current_datetime
            from app import config_data
            from providers.mail import send_error_notification

            try:

                ad_report_type_list = [
                    ASpReportType.AZ_SPONSORED_BRAND_BANNER.value,
                    ASpReportType.AZ_SPONSORED_BRAND_VIDEO.value,
                    ASpReportType.AZ_SPONSORED_DISPLAY.value,
                    ASpReportType.AZ_SPONSORED_PRODUCT.value
                ]

                users = db.session.query(User.id, Account.uuid, Account.asp_id, Account.az_ads_profile_ids).join(Account, User.id == Account.primary_user_id).all()  # type: ignore  # noqa: FKA100

                for row in users:
                    user_id, account_id, asp_id, az_ads_profile_ids = row

                    if account_id is not None and asp_id is not None and az_ads_profile_ids is not None:

                        logged_in_user = user_id

                        data = {
                            'user_id': user_id,
                            'account_id': account_id,
                        }

                        for az_ads_profile_id in az_ads_profile_ids:

                            for ad_report_type in ad_report_type_list:
                                """Get the last report created by report type"""

                                last_ad_report = AzReport.get_last_ads_report(
                                    account_id=account_id, type=ad_report_type, az_ads_profile_id=az_ads_profile_id)

                                start_datetime, end_datetime = None, None

                                if last_ad_report:

                                    # Calculate the end date from last_ad_report end_time
                                    get_end_date = last_ad_report.request_end_time

                                    start_datetime = convert_date_string(
                                        get_end_date)
                                    end_datetime = convert_date_string(
                                        get_current_datetime())

                                    payload = {
                                        'start_datetime': start_datetime,
                                        'end_datetime': end_datetime,
                                        'az_ads_profile_id': str(az_ads_profile_id)
                                    }

                                    data.update(payload)
                                    if ad_report_type == ASpReportType.AZ_SPONSORED_BRAND_BANNER.value:
                                        add_queue_task_and_enqueue(queue_name=QueueName.AZ_SPONSORED_BRAND, account_id=account_id,
                                                                   logged_in_user=logged_in_user, entity_type=EntityType.AZ_SPONSORED_BRAND_BANNER.value, data=data)
                                    elif ad_report_type == ASpReportType.AZ_SPONSORED_BRAND_VIDEO.value:
                                        add_queue_task_and_enqueue(queue_name=QueueName.AZ_SPONSORED_BRAND, account_id=account_id,
                                                                   logged_in_user=logged_in_user, entity_type=EntityType.AZ_SPONSORED_BRAND_VIDEO.value, data=data)
                                    elif ad_report_type == ASpReportType.AZ_SPONSORED_DISPLAY.value:
                                        add_queue_task_and_enqueue(queue_name=QueueName.AZ_SPONSORED_DISPLAY, account_id=account_id,
                                                                   logged_in_user=logged_in_user, entity_type=EntityType.AZ_SPONSORED_DISPLAY.value, data=data)
                                    elif ad_report_type == ASpReportType.AZ_SPONSORED_PRODUCT.value:
                                        add_queue_task_and_enqueue(queue_name=QueueName.AZ_SPONSORED_PRODUCT, account_id=account_id,
                                                                   logged_in_user=logged_in_user, entity_type=EntityType.AZ_SPONSORED_PRODUCT.value, data=data)

            except Exception as e:
                logger.error(
                    'Error while creating sponsored ads report in AspReportsWorker.create_sponsored_ads_report(): ' + str(e))
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='Error while creating sponsored ads report',
                                        template='emails/slack_email.html', data={})
                logger.error(traceback.format_exc())

    @classmethod
    def verify_ads_report(cls):                                                 # type: ignore  # noqa: C901
        from app import app

        """This method verify ads report"""
        with app.app_context():
            from app import config_data
            from app import logger
            from app import az_sponsored_brand_q
            from app import az_sponsored_display_q
            from app import az_sponsored_product_q
            from app import db
            from app.models.account import Account
            from app.models.az_report import AzReport
            from app.models.user import User
            import traceback
            from providers.amazon_ads_api import AmazonAdsReportEU
            from app.helpers.constants import ASpReportType
            import time
            from app.models.queue_task import QueueTask
            from app.helpers.constants import QueueTaskStatus
            from app.helpers.constants import QueueName
            from app.helpers.constants import EntityType
            from app.helpers.constants import ASpReportProcessingStatus
            from providers.mail import send_mail

            try:

                users = db.session.query(User.id, Account.uuid, Account.asp_id, Account.az_ads_credentials, Account.az_ads_profile_ids).join(Account, User.id == Account.primary_user_id).all()  # type: ignore  # noqa: FKA100

                for row in users:
                    user_id, account_id, asp_id, az_ads_credentials, az_ads_profile_ids = row
                    logger.info(
                        f'user_id: {user_id}, account_id: {account_id}, asp_id: {asp_id}, az_ads_credentials:{az_ads_credentials}, az_ads_profile_ids: {az_ads_profile_ids}')

                    if account_id and asp_id and az_ads_credentials and az_ads_profile_ids is not None:

                        data = {
                            'user_id': user_id,
                            'account_id': account_id,
                        }

                        for az_ads_profile_id in az_ads_profile_ids:
                            logger.info(f'Ad Profile ID: {az_ads_profile_id}')

                            reports = AzReport.get_pending_ads_reports(
                                account_id=account_id, seller_partner_id=asp_id, az_ads_profile_id=az_ads_profile_id)

                            if reports:
                                ads_credentials = None
                                for cred_item in az_ads_credentials:
                                    if str(cred_item.get('az_ads_profile_id')) == az_ads_profile_id:
                                        ads_credentials = {
                                            'refresh_token': cred_item.get('refresh_token'),
                                            'client_id': config_data.get('AZ_AD_CLIENT_ID'),
                                            'client_secret': config_data.get('AZ_AD_CLIENT_SECRET'),
                                            'az_ads_profile_id': cred_item.get('az_ads_profile_id'),
                                        }
                                        break

                                delay = 0

                                for report in reports:

                                    report_type = report.type
                                    reference_id = report.reference_id
                                    report_queue_id = report.queue_id

                                    if report_queue_id is None and report_type in [ASpReportType.AZ_SPONSORED_BRAND_BANNER.value,
                                                                                   ASpReportType.AZ_SPONSORED_BRAND_VIDEO.value, ASpReportType.AZ_SPONSORED_DISPLAY.value, ASpReportType.AZ_SPONSORED_PRODUCT.value]:

                                        az_report = AmazonAdsReportEU(
                                            credentials=ads_credentials)

                                        # Verify processing status from amazon alawys
                                        if report_type in [ASpReportType.AZ_SPONSORED_BRAND_BANNER.value, ASpReportType.AZ_SPONSORED_BRAND_VIDEO.value, ASpReportType.AZ_SPONSORED_DISPLAY.value]:

                                            az_report_status = az_report.verify_report_v2(
                                                document_id=reference_id)

                                            if az_report_status['status'] != ASpReportProcessingStatus.SUCCESS.value:
                                                _update_report = AzReport.update_by_id(
                                                    id=report.id, status=az_report_status['status'], document_id=None)
                                            else:
                                                _update_report = AzReport.update_by_id(
                                                    id=report.id, status=ASpReportProcessingStatus.SUCCESS.value, document_id=az_report_status['reportId'])

                                        elif report_type == ASpReportType.AZ_SPONSORED_PRODUCT.value:

                                            az_report_status = az_report.verify_report(
                                                report_id=reference_id)

                                            if az_report_status['status'] != ASpReportProcessingStatus.COMPLETED.value:
                                                _update_report = AzReport.update_by_id(
                                                    id=report.id, status=az_report_status['status'], document_id=None)
                                            else:
                                                _update_report = AzReport.update_by_id(
                                                    id=report.id, status=ASpReportProcessingStatus.SUCCESS.value, document_id=az_report_status['reportId'])

                                        if _update_report.status == ASpReportProcessingStatus.SUCCESS.value and _update_report.queue_id is None:

                                            queue_name = None
                                            entity_type = None
                                            if _update_report.type == ASpReportType.AZ_SPONSORED_BRAND_BANNER.value:
                                                queue_name = QueueName.AZ_SPONSORED_BRAND
                                                entity_type = EntityType.AZ_SPONSORED_BRAND_BANNER.value
                                            elif _update_report.type == ASpReportType.AZ_SPONSORED_BRAND_VIDEO.value:
                                                queue_name = QueueName.AZ_SPONSORED_BRAND
                                                entity_type = EntityType.AZ_SPONSORED_BRAND_VIDEO.value
                                            elif _update_report.type == ASpReportType.AZ_SPONSORED_DISPLAY.value:
                                                queue_name = QueueName.AZ_SPONSORED_DISPLAY
                                                entity_type = EntityType.AZ_SPONSORED_DISPLAY.value
                                                time.sleep(5)
                                            elif _update_report.type == ASpReportType.AZ_SPONSORED_PRODUCT.value:
                                                queue_name = QueueName.AZ_SPONSORED_PRODUCT
                                                entity_type = EntityType.AZ_SPONSORED_PRODUCT.value

                                            queue_task = QueueTask.add_queue_task(queue_name=queue_name,
                                                                                  account_id=account_id,
                                                                                  owner_id=user_id,
                                                                                  status=QueueTaskStatus.NEW.value,
                                                                                  entity_type=entity_type,
                                                                                  param=str(data), input_attachment_id=None, output_attachment_id=None)

                                            # update_queue_id = AzReport.get_by_ref_id(reference_id=reference_id)
                                            _update_report.queue_id = queue_task.id
                                            _update_report.save()

                                            if queue_task:
                                                queue_task_dict = {
                                                    'job_id': queue_task.id,
                                                    'queue_name': queue_task.queue_name,
                                                    'status': QueueTaskStatus.get_status(queue_task.status),
                                                    'entity_type': EntityType.get_type(queue_task.entity_type),
                                                    'reference_id': reference_id,
                                                    'seller_partner_id': asp_id,
                                                    'az_ads_profile_id': str(az_ads_profile_id)
                                                }
                                                data.update(queue_task_dict)
                                                queue_task.param = str(data)
                                                queue_task.save()

                                            if _update_report.type == ASpReportType.AZ_SPONSORED_BRAND_BANNER.value or _update_report.type == ASpReportType.AZ_SPONSORED_BRAND_VIDEO.value:
                                                from workers.sponsored_brand_worker import SponsoredBrandWorker
                                                az_sponsored_brand_q.enqueue(SponsoredBrandWorker.get_sponsored_brand_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100
                                            elif _update_report.type == ASpReportType.AZ_SPONSORED_DISPLAY.value:
                                                from workers.sponsored_display_worker import SponsoredDisplayWorker
                                                az_sponsored_display_q.enqueue(SponsoredDisplayWorker.get_sponsored_display_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100
                                            elif _update_report.type == ASpReportType.AZ_SPONSORED_PRODUCT.value:
                                                from workers.sponsored_product_worker import SponsoredProductWorker
                                                az_sponsored_product_q.enqueue(SponsoredProductWorker.get_sponsored_product_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100

                                delay += 3
                                # Add a delay after each request
                                # Delay, (assuming 2 requests per second)
                                time.sleep(1 / 2)

            except Exception as e:
                logger.error(
                    'Error while verifying Ads report in AspReportsWorker.verify_ads_report(): ' + str(e))
                send_mail(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='Error while verifying Ads report in AspReportsWorker.verify_ads_report',
                          template='emails/slack_email.html', data={})
                logger.error(traceback.format_exc())

    @classmethod
    def create_sales_traffic_report(cls):

        from app import app

        """This method verifies all the reports from amazon api's which are in cancelled or done status"""
        with app.app_context():
            from app import db
            from app import logger
            from app.models.account import Account
            from app.models.az_report import AzReport
            from app.models.user import User
            from datetime import datetime
            from app.helpers.constants import ASpReportType
            import traceback
            from app.helpers.constants import EntityType
            from app.helpers.constants import QueueName
            from app.helpers.queue_helper import add_queue_task_and_enqueue

            try:

                users = db.session.query(User.id, Account.uuid, Account.asp_id, Account.asp_credentials).join(Account, User.id == Account.primary_user_id).all()  # type: ignore  # noqa: FKA100

                for row in users:
                    user_id, account_id, asp_id, asp_credentials = row
                    if account_id is not None and asp_id is not None and asp_credentials is not None:

                        logged_in_user = user_id

                        data = {
                            'user_id': user_id,
                            'account_id': account_id,
                        }

                        """Get the last report created by report type"""
                        last_report = AzReport.get_last_report(
                            account_id=account_id, type=ASpReportType.SALES_TRAFFIC_REPORT.value)

                        if last_report:
                            start_datetime = last_report.request_end_time
                            end_datetime = datetime.now()

                            payload = {
                                'start_datetime': start_datetime,
                                'end_datetime': end_datetime
                            }

                            data.update(payload)

                            add_queue_task_and_enqueue(queue_name=QueueName.SALES_TRAFFIC_REPORT, account_id=account_id,
                                                       logged_in_user=logged_in_user, entity_type=EntityType.SALES_TRAFFIC_REPORT.value, data=data)

            except Exception as e:
                logger.error(
                    'Error while creating report in AspReportsWorker.create_sales_traffic_report(): ' + str(e))
                logger.error(traceback.format_exc())

    @classmethod
    def create_performance_zone(cls):
        from app import app
        from app import db
        from app import logger

        """This method creates finance event list"""
        with app.app_context():
            import traceback
            from app.helpers.constants import QueueName, EntityType
            from app.models.account import Account
            from app.models.user import User
            from app.helpers.queue_helper import add_queue_task_and_enqueue
            from app.helpers.utility import get_previous_month_dates

            try:

                users = db.session.query(User.id, Account.uuid, Account.asp_id, Account.asp_credentials).join(Account, User.id == Account.primary_user_id).all()  # type: ignore  # noqa: FKA100

                for row in users:
                    user_id, account_id, asp_id, asp_credentials = row
                    if account_id and asp_id and asp_credentials is not None:

                        logged_in_user = user_id

                        data = {
                            'user_id': user_id,
                            'account_id': account_id,
                        }

                        start_datetime, end_datetime = get_previous_month_dates()
                        logger.error(
                            f'start_datetime {start_datetime}, end_datetime {end_datetime}')
                        payload = {
                            'start_datetime': start_datetime,
                            'end_datetime': end_datetime,
                        }

                        data.update(payload)

                        # queuing performance zone
                        add_queue_task_and_enqueue(queue_name=QueueName.AZ_PERFORMANCE_ZONE, account_id=account_id,
                                                   logged_in_user=logged_in_user, entity_type=EntityType.MR_PERFORMANCE_ZONE.value, data=data)

            except Exception as e:
                logger.error(
                    'Error while creating performance zone schedule worker ' + str(e))
                logger.error(traceback.format_exc())
