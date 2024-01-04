"""
# This script is for generating FBA Inventory API details
# It defines a class, FbaInventoryWorker, to handle report generation
"""

from datetime import datetime
from datetime import timedelta
import time
import traceback


class FbaInventoryWorker:
    """Class to create, verify, store item master from amazon API's"""

    @classmethod
    def create_reports(cls, *args, **kwargs):  # type: ignore  # noqa: C901
        """ Queue FBA Inventory API"""
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

                AzReport.cancel_old_reports(
                    report_type=ASpReportType.FBA_INVENTORY.value)

                end_date = datetime.now().date()
                start_date = end_date - timedelta(days=1)

                # Get reports for all users.
                fba_api_reports = AzReport.get_pending_sales_progress(
                    type=ASpReportType.FBA_INVENTORY.value)

                if not fba_api_reports:

                    users = db.session.query(User.id, Account.uuid, Account.asp_id, Account.asp_credentials).\
                        join(Account, (User.id == Account.primary_user_id) & (Account.uuid.isnot(None)) & (Account.asp_id.isnot(None)) & (Account.asp_credentials.isnot(None))).all()  # type: ignore  # noqa: FKA100

                    for user in users:
                        user_id, account_id, asp_id, asp_credentials = user

                        ref_id = generate_uuid()

                        AzReport.add(account_id=account_id, seller_partner_id=asp_id, type=ASpReportType.FBA_INVENTORY.value,
                                     reference_id=ref_id, request_start_time=start_date, request_end_time=end_date, status=ASpReportProcessingStatus.NEW.value)

            except Exception as e:
                logger.error(
                    'Error while Creating FBA inventory API in FbaInventoryWorker.create_reports: ' + str(e))
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='FbaInventoryWorker Create Report Failure',
                                        template='emails/slack_email.html', data=None, error_message=str(e), traceback_info=traceback.format_exc())

    @classmethod
    def process_sales_order_queue(cls, *args, **kwargs):  # type: ignore  # noqa: C901
        """ Process reports to queues"""
        from app import app, logger, db, config_data, fba_inventory_q
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

                # Get reports for all users.
                reports = AzReport.get_pending_reports(
                    type=ASpReportType.FBA_INVENTORY.value)

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

                    if report_queue_id is None and report_type == ASpReportType.FBA_INVENTORY.value and report_status == ASpReportProcessingStatus.NEW.value:
                        get_report_by_ref_id = AzReport.get_by_ref_id(
                            account_id=account_id, reference_id=reference_id)

                        queue_name = None
                        entity_type = None
                        if get_report_by_ref_id.type == ASpReportType.FBA_INVENTORY.value:
                            queue_name = QueueName.FBA_INVENTORY
                            entity_type = EntityType.FBA_INVENTORY.value

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

                            if get_report_by_ref_id.type == ASpReportType.FBA_INVENTORY.value:
                                fba_inventory_q.enqueue(FbaInventoryWorker.get_fba_api_details, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100

                    """Break the loop so next report doesn't goes in queue until status for this report gets updated"""
                    break

            except Exception as e:
                logger.error(
                    'Error while creating FBA Inventory API in FbaInventoryWorker.create_reports: ' + str(e))
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='FbaInventoryWorker Create Report Failure',
                                        template='emails/slack_email.html', data=None, error_message=str(e), traceback_info=traceback.format_exc())

    @classmethod
    def get_fba_api_details(cls, data):  # type: ignore  # noqa: C901
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
        from app.helpers.utility import get_asp_data_start_time
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
                logger.info('Get FBA Inventory API')

                asp_id = account.asp_id

                credentials = account.retrieve_asp_credentials(account)[0]
                logger.info(
                    f'Credentials for FBA Inventory API: {credentials}')

                params = {
                    'details': True,
                    'startDateTime': '2023-12-17T00:00:00Z',
                    'granularityType': 'Marketplace',
                    'granularityId': get_asp_market_place_ids(),
                    'marketplaceIds': get_asp_market_place_ids()
                }

                all_inventory_summaries = []

                while True:

                    report = AmazonReportEU(credentials=credentials)

                    response = report.get_fba_inventory(params=params)

                    # Process the current page of results
                    inventory_summaries = response.get('payload', {}).get('inventorySummaries', [])   # type: ignore  # noqa: FKA100
                    all_inventory_summaries.extend(inventory_summaries)

                    params['nextToken'] = response.get('pagination', {}).get('nextToken')   # type: ignore  # noqa: FKA100

                    # Break the loop if there are no more pages
                    if not params['nextToken']:
                        break

                    # Add sleep time to ensure 2 requests per second
                    time.sleep(0.5)

                prepare_fba_json = []
                for inventory in all_inventory_summaries:
                    # fba_inventory_json = json.dumps(inventory)
                    prepare_fba_json.append({
                        'fba_inventory_json': inventory,
                        'account_id': account_id,
                        'selling_partner_id': asp_id,
                        'seller_sku': inventory.get('sellerSku'),
                        'asin': inventory.get('asin'),
                        'item_name': inventory.get('productName'),
                        'created_at': int(time.time())
                    })

                AzItemMaster.upsert_fba_inventory(prepare_fba_json)

                if get_report_ref:
                    get_report_ref.status = ASpReportProcessingStatus.COMPLETED.value
                    get_report_ref.status_updated_at = int(time.time())
                    db.session.commit()

                if queue_task:
                    queue_task.status = QueueTaskStatus.COMPLETED.value
                    queue_task.save()
                else:
                    raise Exception

            except Exception as e:
                if get_report_ref:
                    get_report_ref.status = ASpReportProcessingStatus.ERROR.value
                    get_report_ref.status_updated_at = int(time.time())
                    db.session.commit()

                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                logger.error(
                    'Error while fetching FBA Inventory API in FbaInventoryWorker.get_fba_api_details: ' + str(e))
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='FbaInventoryWorker Get FBA API details Failure',
                                        template='emails/slack_email.html', data=None, error_message=str(e), traceback_info=traceback.format_exc())
