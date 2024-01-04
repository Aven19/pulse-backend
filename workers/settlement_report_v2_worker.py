import io
import os
import time
import traceback

from app import app
from app import config_data
from app import logger
from app.helpers.constants import ASpReportProcessingStatus
from app.helpers.constants import ASpReportType
from app.helpers.constants import AttachmentType
from app.helpers.constants import EntityType
from app.helpers.constants import QueueTaskStatus
from app.helpers.constants import SubEntityType
from app.helpers.constants import TimePeriod
from app.helpers.utility import amount_details_to_json
from app.helpers.utility import get_asp_market_place_ids
from app.helpers.utility import get_from_to_date_by_timestamp
from app.models.account import Account
from app.models.attachment import Attachment
from app.models.az_report import AzReport
from app.models.az_settlement_v2 import AzSettlementV2
from app.models.queue_task import QueueTask
import numpy as np
import pandas as pd
from providers.amazon_sp_client import AmazonReportEU
import requests
from werkzeug.datastructures import FileStorage
from workers.s3_worker import upload_file_and_get_object_details


class SettlementReportV2Worker:

    @classmethod
    def create_settlement_report_v2(cls, data):
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
                    start_datetime, end_datetime = get_from_to_date_by_timestamp(
                        TimePeriod.LAST_7_DAYS.value)
                else:
                    start_datetime = data.get('start_datetime')
                    end_datetime = data.get('end_datetime')
                    if start_datetime is None or end_datetime is None:
                        logger.error(
                            "Queue Task with job_id '{}' failed. start_datetime : {}, end_datetime : {}".format(job_id, start_datetime, end_datetime))
                        raise Exception

                params = {
                    'reportTypes': ASpReportType.SETTLEMENT_REPORT_FLAT_FILE_V2.value,
                    'marketplaceIds': get_asp_market_place_ids(),
                    'createdSince': start_datetime,
                    'createdUntil': end_datetime,
                }

                get_settlement_report = AmazonReportEU(credentials=credentials)

                response = get_settlement_report.get_scheduled_report(
                    params=params)

                data.update(response)

                _data = {
                    'asp_id': asp_id
                }

                data.update(_data)

                SettlementReportV2Worker.process_reports(data=data)

                if queue_task:
                    queue_task.status = QueueTaskStatus.COMPLETED.value
                    queue_task.save()

            except Exception as e:
                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                logger.error(
                    'Error while creating report in SettlementReportV2Worker.create_item_master_report(): ' + str(e))
                logger.error(traceback.format_exc())

    @classmethod
    def process_csv_reports(cls, data):
        with app.app_context():
            job_id = data.get('job_id')
            account_id = data.get('account_id')
            reference_id = data.get('reference_id')

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
                if reference_id is None and account_id is None:
                    raise Exception

            account = Account.get_by_uuid(uuid=account_id)

            if not account:
                logger.error(
                    "Queue Task with job_id '{}' failed. Account : {} not found".format(job_id, account_id))
                raise Exception

            try:
                asp_cred = account.asp_credentials
                asp_id = account.asp_id
                credentials = {}

                credentials['seller_partner_id'] = asp_cred.get(
                    'seller_partner_id')
                credentials['refresh_token'] = asp_cred.get('refresh_token')
                credentials['client_id'] = config_data.get('SP_LWA_APP_ID')
                credentials['client_secret'] = config_data.get(
                    'SP_LWA_CLIENT_SECRET')

                # querying Report table to get the entry for particular report_id
                get_report_document_id = AzReport.get_by_ref_id(account_id=account_id,
                                                                reference_id=reference_id)

                if not get_report_document_id:
                    raise Exception

                report_document_id = get_report_document_id.document_id

                get_settlement_report = AmazonReportEU(credentials=credentials)

                get_report = get_settlement_report.retrieve_report(
                    report_document_id)

                if 'url' in get_report:
                    file_url = get_report['url']
                else:
                    logger.error(
                        'File url not found for settlement report')
                    raise Exception

                file_url = get_report['url']
                # Making request to the report document URL to get the file data
                streamed_url_response = requests.get(file_url)

                if streamed_url_response.status_code != 200:
                    logger.error('Error occurred while retrieving Settlement Report: Report ID: {}, Reference ID: {}'.format(
                        get_report_document_id.id, get_report_document_id.reference_id))

                content = streamed_url_response.content.decode('utf-8')

                # directory = config_data.get(
                #     'UPLOAD_FOLDER') + ASpReportType.SETTLEMENT_REPORT_FLAT_FILE_V2.value.lower()
                # os.makedirs(directory, exist_ok=True)

                # Read the content into a streaming pandas DataFrame
                df_stream = pd.read_csv(io.StringIO(
                    content), delimiter='\t', header=None, skiprows=1, iterator=True, chunksize=1000)
                settlement_df = pd.concat(df_stream, ignore_index=True)

                settlement_df.columns = [
                    'settlement_id', 'settlement_start_date', 'settlement_end_date', 'deposit_date', 'total_amount', 'currency',
                    'transaction_type', 'order_id', 'merchant_order_id', 'adjustment_id', 'shipment_id', 'marketplace_name',
                    'amount_type', 'amount_description', 'amount', 'fulfillment_id', 'posted_date', 'posted_date_time',
                    'order_item_code', 'merchant_order_item_id', 'merchant_adjustment_item_id', 'sku', 'quantity_purchased',
                    'promotion_id'
                ]

                settlement_df['posted_date'] = pd.to_datetime(
                    settlement_df['posted_date'], format='%d.%m.%Y').dt.date
                settlement_df['posted_date_time'] = pd.to_datetime(
                    settlement_df['posted_date_time'], format='%d.%m.%Y %H:%M:%S UTC')

                # Save the DataFrame to a CSV file
                # start_date = re.search(r'\d{4}-\d{2}-\d{2}', get_report_document_id.request_start_time).group(0)  # type: ignore  # noqa: FKA100
                # end_date = re.search(r'\d{4}-\d{2}-\d{2}', get_report_document_id.request_end_time).group(0)  # type: ignore  # noqa: FKA100
                # file_path = os.path.join(directory, '{}-{}-{}.csv'.format(reference_id, start_date, end_date))  # type: ignore  # noqa: FKA100
                # settlement_df.to_csv(file_path, index=False, header=True)

                # Perform additional transformations and save to the database
                settlement_df = settlement_df.fillna(np.nan).replace([np.nan], [None])  # type: ignore  # noqa: FKA100

                # store amount details as json
                previous_order_id = None
                first_row = True
                amount_details_dict = {'amount_details': []}
                for row in settlement_df.iloc[1:].itertuples(index=False):
                    current_order_id = row.order_id

                    if current_order_id == previous_order_id or first_row is True:
                        amount_details_dict = amount_details_to_json(
                            amount_details_dict=amount_details_dict, row=row)

                        selling_partner_id = asp_id
                        settlement_id = row.settlement_id
                        settlement_start_date = row.settlement_start_date
                        settlement_end_date = row.settlement_end_date
                        deposit_date = row.deposit_date
                        total_amount = row.total_amount
                        currency = row.currency
                        transaction_type = row.transaction_type
                        order_id = current_order_id
                        merchant_order_id = row.merchant_order_id
                        adjustment_id = row.adjustment_id
                        shipment_id = row.shipment_id
                        marketplace_name = row.marketplace_name
                        fulfillment_id = row.fulfillment_id
                        posted_date = row.posted_date
                        posted_date_time = row.posted_date_time
                        order_item_code = row.order_item_code
                        merchant_order_item_id = row.merchant_order_item_id
                        merchant_adjustment_item_id = row.merchant_adjustment_item_id
                        sku = row.sku
                        quantity_purchased = row.quantity_purchased
                        promotion_id = row.promotion_id

                        first_row = False
                    else:
                        if not first_row:
                            cls.store_settlement_data(
                                selling_partner_id=selling_partner_id,
                                settlement_id=settlement_id,
                                settlement_start_date=settlement_start_date,
                                settlement_end_date=settlement_end_date,
                                deposit_date=deposit_date,
                                total_amount=total_amount,
                                currency=currency,
                                transaction_type=transaction_type,
                                order_id=order_id,
                                merchant_order_id=merchant_order_id,
                                adjustment_id=adjustment_id,
                                shipment_id=shipment_id,
                                marketplace_name=marketplace_name,
                                fulfillment_id=fulfillment_id,
                                posted_date=posted_date,
                                posted_date_time=posted_date_time,
                                order_item_code=order_item_code,
                                merchant_order_item_id=merchant_order_item_id,
                                merchant_adjustment_item_id=merchant_adjustment_item_id,
                                sku=sku,
                                quantity_purchased=quantity_purchased,
                                promotion_id=promotion_id,
                                amount_details=amount_details_dict
                            )

                            first_row = True

                            amount_details_dict = {'amount_details': []}

                            amount_details_dict = amount_details_to_json(
                                amount_details_dict=amount_details_dict, row=row)

                            selling_partner_id = asp_id
                            settlement_id = row.settlement_id
                            settlement_start_date = row.settlement_start_date
                            settlement_end_date = row.settlement_end_date
                            deposit_date = row.deposit_date
                            total_amount = row.total_amount
                            currency = row.currency
                            transaction_type = row.transaction_type
                            order_id = current_order_id
                            merchant_order_id = row.merchant_order_id
                            adjustment_id = row.adjustment_id
                            shipment_id = row.shipment_id
                            marketplace_name = row.marketplace_name
                            fulfillment_id = row.fulfillment_id
                            posted_date = row.posted_date
                            posted_date_time = row.posted_date_time
                            order_item_code = row.order_item_code
                            merchant_order_item_id = row.merchant_order_item_id
                            merchant_adjustment_item_id = row.merchant_adjustment_item_id
                            sku = row.sku
                            quantity_purchased = row.quantity_purchased
                            promotion_id = row.promotion_id

                    previous_order_id = current_order_id

                cls.store_settlement_data(
                    selling_partner_id=selling_partner_id,
                    settlement_id=settlement_id,
                    settlement_start_date=settlement_start_date,
                    settlement_end_date=settlement_end_date,
                    deposit_date=deposit_date,
                    total_amount=total_amount,
                    currency=currency,
                    transaction_type=transaction_type,
                    order_id=order_id,
                    merchant_order_id=merchant_order_id,
                    adjustment_id=adjustment_id,
                    shipment_id=shipment_id,
                    marketplace_name=marketplace_name,
                    fulfillment_id=fulfillment_id,
                    posted_date=posted_date,
                    posted_date_time=posted_date_time,
                    order_item_code=order_item_code,
                    merchant_order_item_id=merchant_order_item_id,
                    merchant_adjustment_item_id=merchant_adjustment_item_id,
                    sku=sku,
                    quantity_purchased=quantity_purchased,
                    promotion_id=promotion_id,
                    amount_details=amount_details_dict
                )

                get_report_document_id.status = ASpReportProcessingStatus.COMPLETED.value
                get_report_document_id.status_updated_at = int(time.time())
                get_report_document_id.save()

                if queue_task:
                    queue_task.status = QueueTaskStatus.COMPLETED.value
                    queue_task.save()
                else:
                    raise Exception

            except Exception as e:
                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                logger.error(
                    'Error while retrieving report in SettlementReportV2Worker.process_csv_reports(): ' + str(e))
                logger.error(traceback.format_exc())

    # @classmethod
    # def get_settlement_report_v2(cls) -> None:
    #     with app.app_context():
    #         """This method is used for creating order reports."""

    #         current_time = datetime.now()
    #         request_start_time = current_time - timedelta(days=1)
    #         request_end_time = current_time

    #         sellers = User.get_all()

    #         for seller in sellers:

    #             last_report = Report.get_last_report_by_seller(
    #                 seller.seller_partner_id, type=ASpReportType.SETTLEMENT_REPORT_FLAT_FILE_V2.value)

    #             if last_report is not None and last_report.request_end_time is not None:
    #                 request_start_time = datetime.fromtimestamp(
    #                     last_report.request_end_time)

    #             logger.warning(request_start_time)
    #             logger.warning(request_end_time)
    #             logger.warning(request_start_time.strftime(
    #                 '%Y-%m-%dT%H:%M:00.000Z'))
    #             logger.warning(request_end_time.strftime(
    #                 '%Y-%m-%dT%H:%M:59.999Z'))

    #             credentials = {
    #                 'seller_partner_id': seller.seller_partner_id,
    #                 'refresh_token': seller.refresh_token,
    #                 'client_id': config_data.get('SP_LWA_APP_ID'),
    #                 'client_secret': config_data.get('SP_LWA_CLIENT_SECRET')
    #             }

    #             params = {
    #                 'reportType': ASpReportType.SETTLEMENT_REPORT_FLAT_FILE_V2.value,
    #                 'dataStartTime': request_start_time.strftime('%Y-%m-%dT%H:%M:00.000Z'),
    #                 'dataEndTime': request_end_time.strftime('%Y-%m-%dT%H:%M:59.999Z'),
    #                 'marketplaceIds': get_asp_market_place_ids()
    #             }

    #             get_settlement_report_v2 = AmazonReportEU(
    #                 credentials=credentials)
    #             get_response = get_settlement_report_v2.get_scheduled_report(
    #                 params=params)

    #             SettlementReportV2Worker.process_reports(
    #                 response=get_response, get_settlement_report=get_settlement_report_v2)

    @classmethod
    def process_reports(cls, data):

        account_id = data.get('account_id')
        user_id = data.get('user_id')
        next_token = data.get('nextToken')
        response = data.get('reports')

        if account_id is None and user_id is None:
            raise Exception

        if not response:
            raise Exception

        account = Account.get_by_uuid(uuid=account_id)

        asp_cred = account.asp_credentials
        asp_id = account.asp_id
        credentials = {}

        credentials['seller_partner_id'] = asp_cred.get(
            'seller_partner_id')
        credentials['refresh_token'] = asp_cred.get('refresh_token')
        credentials['client_id'] = config_data.get('SP_LWA_APP_ID')
        credentials['client_secret'] = config_data.get(
            'LWA_CLIENT_SECRET')

        # settlement_report_data = {
        #     'user_id': user_id,
        #     'account_id': account_id,
        #     'reference_id' : report_id,
        #     'seller_partner_id': asp_id
        # }

        get_settlement_report = AmazonReportEU(credentials=credentials)
        """Function to save paginated response from amazon api"""
        if next_token:

            # delay = 0

            for report in response:
                report_type = report['reportType']
                report_id = report['reportId']
                report_status = report['processingStatus']
                report_document_id = report['reportDocumentId']
                start_time = report['dataStartTime']
                end_time = report['dataEndTime']

                AzReport.add(account_id=account_id, seller_partner_id=asp_id, type=report_type,
                             request_start_time=start_time, request_end_time=end_time,
                             reference_id=report_id, status=report_status, document_id=report_document_id)

                # qt_settlement_report_v2 = QueueTask.add_queue_task(queue_name=QueueName.SETTLEMENT_REPORT_V2,
                #                             account_id=account_id,
                #                             owner_id=user_id,
                #                             status=QueueTaskStatus.NEW.value,
                #                             entity_type=EntityType.SETTLEMENT_V2_REPORT.value,
                #                             param=str(settlement_report_data), input_attachment_id=None, output_attachment_id=None)

                # if qt_settlement_report_v2:
                #     qt_settlement_report_v2_dict = {
                #         'job_id': qt_settlement_report_v2.id,
                #         'queue_name': qt_settlement_report_v2.queue_name,
                #         'status': QueueTaskStatus.get_status(qt_settlement_report_v2.status),
                #         'entity_type': EntityType.get_type(qt_settlement_report_v2.entity_type),
                #     }
                #     settlement_report_data.update(qt_settlement_report_v2_dict)
                #     qt_settlement_report_v2.param = str(settlement_report_data)
                #     qt_settlement_report_v2.save()

                #     settlement_report_v2_q.enqueue_in(timedelta(seconds=delay), SettlementReportV2Worker.process_csv_reports, data=settlement_report_data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100

                # delay += 2

            params = {
                'nextToken': next_token
            }

            next_response = get_settlement_report.get_scheduled_report(
                params=params)

            del data['nextToken']
            del data['reports']
            data.update(next_response)

            SettlementReportV2Worker.process_reports(data=data)

        else:

            # delay = 0

            for report in response:
                report_type = report['reportType']
                report_id = report['reportId']
                report_status = report['processingStatus']
                report_document_id = report['reportDocumentId']
                start_time = report['dataStartTime']
                end_time = report['dataEndTime']

                AzReport.add(account_id=data.get('account_id'), seller_partner_id=asp_id, type=report_type,
                             request_start_time=start_time, request_end_time=end_time,
                             reference_id=report_id, status=report_status, document_id=report_document_id)

                # qt_settlement_report_v2 = QueueTask.add_queue_task(queue_name=QueueName.SETTLEMENT_REPORT_V2,
                #                             account_id=account_id,
                #                             owner_id=user_id,
                #                             status=QueueTaskStatus.NEW.value,
                #                             entity_type=EntityType.SETTLEMENT_V2_REPORT.value,
                #                             param=str(settlement_report_data), input_attachment_id=None, output_attachment_id=None)

                # if qt_settlement_report_v2:
                #     qt_settlement_report_v2_dict = {
                #         'job_id': qt_settlement_report_v2.id,
                #         'queue_name': qt_settlement_report_v2.queue_name,
                #         'status': QueueTaskStatus.get_status(qt_settlement_report_v2.status),
                #         'entity_type': EntityType.get_type(qt_settlement_report_v2.entity_type),
                #     }
                #     settlement_report_data.update(qt_settlement_report_v2_dict)
                #     qt_settlement_report_v2.param = str(settlement_report_data)
                #     qt_settlement_report_v2.save()

                #     settlement_report_v2_q.enqueue_in(timedelta(seconds=delay), SettlementReportV2Worker.process_csv_reports, data=settlement_report_data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100

                # delay += 2

    @classmethod
    def settlement_report_v2_csv_export(cls, data):   # type: ignore  # noqa: C901
        with app.app_context():
            """export csv acccording to date range"""

            logger.warning('*' * 50)
            logger.warning('EXPORT CSV Queue data %s', str(data))

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

            try:
                start_date = data.get('from_date')
                end_date = data.get('to_date')

                settlements = AzSettlementV2.get_settlement_by_date(
                    selling_partner_id=config_data['SELLER_ID'], start_date=start_date, end_date=end_date)

                # creating all possible columns as per amount description fields
                columns = []

                for settlement in settlements:
                    amount_details = settlement.amount_details
                    if amount_details and isinstance(amount_details, list):
                        for amount_detail in amount_details:
                            amount_details_list = amount_detail.get('amount_details', [])       # type: ignore  # noqa: FKA100
                    elif amount_details and isinstance(amount_details, dict):
                        amount_details_list = amount_details.get(
                            'amount_details')

                    for detail in amount_details_list:
                        column = detail.get('amount_description')
                        if column and column not in columns:
                            columns.append(column)

                settlement_df = pd.DataFrame(columns=columns)

                # iterating settlement entries and inserting data in dataframe
                for settlement in settlements:
                    amount_details = settlement.amount_details
                    if amount_details and isinstance(amount_details, list):
                        for amount_detail in amount_details:
                            amount_details_list = amount_detail.get('amount_details', [])       # type: ignore  # noqa: FKA100
                    elif amount_details and isinstance(amount_details, dict):
                        amount_details_list = amount_details.get(
                            'amount_details')

                    new_settlement = {'settlement_id': settlement.settlement_id, 'settlement_start_date': settlement.settlement_start_date, 'settlement_end_date': settlement.settlement_end_date, 'deposit_date': settlement.deposit_date, 'total_amount': settlement.total_amount, 'currency': settlement.currency,
                                      'transaction_type': settlement.transaction_type, 'order_id': settlement.order_id, 'merchant_order_id': settlement.merchant_order_id, 'adjustment_id': '', 'shipment_id': settlement.shipment_id,
                                      'marketplace_name': settlement.marketplace_name, 'fulfillment_id': settlement.fulfillment_id, 'posted_date': settlement.posted_date, 'posted_date_time': settlement.posted_date_time,
                                      'order_item_code': settlement.order_item_code, 'merchant_order_item_id': settlement.merchant_order_item_id, 'merchant_adjustment_item_id': settlement.merchant_adjustment_item_id, 'sku': settlement.sku, 'quantity_purchased': settlement.quantity_purchased,
                                      'promotion_id': settlement.promotion_id}

                    for detail in amount_details_list:
                        amount_description = detail.get('amount_description')
                        amount = detail.get('amount')
                        if amount_description:
                            new_settlement[amount_description] = amount

                    data_frame = pd.DataFrame([new_settlement])

                    settlement_df = pd.concat(
                        [data_frame, settlement_df], ignore_index=True)

                seller_partner_id = config_data.get('SP_ID')
                directory_path = f"{config_data.get('UPLOAD_FOLDER')}{'tmp/csv_exports/'}{seller_partner_id.lower()}"
                os.makedirs(directory_path, exist_ok=True)

                file_name = 'settlement_v2_export_{}.csv'.format(
                    str(int(time.time())))
                export_file_path = f'{directory_path}/{file_name}.csv'

                settlement_df.to_csv(
                    export_file_path, index=False, header=True)

                file_storage = FileStorage(stream=open(export_file_path, 'rb'), filename=file_name)  # type: ignore  # noqa: FKA100

                attachment_name, attachment_path, attachment_size = upload_file_and_get_object_details(
                    file_obj=file_storage,
                    obj_name=file_name,
                    entity_type=EntityType.SETTLEMENT_V2_REPORT.value,
                    attachment_type=AttachmentType.SETTLEMENT_V2_CSV_EXPORT.value,
                )

                output_attachment = Attachment.add(
                    entity_type=EntityType.SETTLEMENT_V2_REPORT.value,
                    entity_id=None,
                    name=attachment_name,
                    path=attachment_path,
                    size=attachment_size,
                    sub_entity_type=SubEntityType.EXPORT_FILE.value,
                    sub_entity_id=None,
                    description='Settlement V2 Report CSV export from {} to {}'.format(
                        start_date, end_date)
                )

                if output_attachment:
                    if os.path.exists(export_file_path):
                        os.remove(path=export_file_path)
                        if queue_task:
                            queue_task.status = QueueTaskStatus.COMPLETED.value
                            queue_task.output_attachment_id = output_attachment.id
                            queue_task.save()
                    else:
                        raise Exception
                else:
                    if queue_task:
                        queue_task.status = QueueTaskStatus.ERROR.value
                        queue_task.save(queue_task)

            except Exception as e:
                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()
                logger.error("Queue Task with job_id '{}' failed. Exception: {}".format(
                    data.get('job_id'), str(e)))
                logger.error(traceback.format_exc())

    @classmethod
    def store_settlement_data(cls, selling_partner_id, settlement_id, settlement_start_date, settlement_end_date, deposit_date, total_amount,
                              currency, transaction_type, order_id, merchant_order_id, adjustment_id, shipment_id, marketplace_name,
                              fulfillment_id, posted_date, posted_date_time, order_item_code, merchant_order_item_id, merchant_adjustment_item_id,
                              sku, quantity_purchased, promotion_id, amount_details):

        selling_partner_id = selling_partner_id
        settlement_id = settlement_id
        settlement_start_date = settlement_start_date
        settlement_end_date = settlement_end_date
        deposit_date = deposit_date
        total_amount = total_amount
        currency = currency
        transaction_type = transaction_type
        order_id = order_id
        merchant_order_id = merchant_order_id
        adjustment_id = adjustment_id
        shipment_id = shipment_id
        marketplace_name = marketplace_name
        fulfillment_id = fulfillment_id
        posted_date = posted_date
        posted_date_time = posted_date_time
        order_item_code = order_item_code
        merchant_order_item_id = merchant_order_item_id
        merchant_adjustment_item_id = merchant_adjustment_item_id
        sku = sku
        quantity_purchased = quantity_purchased
        promotion_id = promotion_id
        amount_details = amount_details

        settlement_data_exists = AzSettlementV2.get_by_order_id(
            selling_partner_id=selling_partner_id, order_id=order_id)

        # if order id already exists
        if settlement_data_exists:

            # same transaction type - same sku
            if settlement_data_exists.transaction_type == transaction_type and settlement_data_exists.sku == sku:

                # summing up quantity and amount details acccording to quantity
                # quantity_purchased = sum(
                #     filter(None, [int(settlement_data_exists.quantity_purchased), int(quantity_purchased)]))
                values = [int(value) for value in [
                    settlement_data_exists.quantity_purchased, quantity_purchased] if value is not None]
                quantity_purchased = sum(values)

                previous_amount_details = settlement_data_exists.amount_details

                # fetching amount details from json column
                if previous_amount_details and isinstance(previous_amount_details, list):
                    for amount_detail in previous_amount_details:
                        previous_amount_details_list = amount_detail.get('amount_details', [])       # type: ignore  # noqa: FKA100
                elif previous_amount_details and isinstance(previous_amount_details, dict):
                    previous_amount_details_list = previous_amount_details.get(
                        'amount_details')

                new_amount_details = {'amount_details': []}
                already_traversed = {}

                # summing up amount details on update
                for detail in previous_amount_details_list + amount_details.get('amount_details'):
                    amount_description = detail.get('amount_description')
                    if amount_description not in already_traversed:
                        already_traversed[amount_description] = detail.get(
                            'amount')
                    else:
                        already_traversed[amount_description] += detail.get(
                            'amount')
                        detail['amount'] = already_traversed[amount_description]

                        new_amount_details['amount_details'].append(detail)

                AzSettlementV2.update_by_order_id(selling_partner_id=selling_partner_id,
                                                  settlement_id=settlement_id,
                                                  settlement_start_date=settlement_start_date,
                                                  settlement_end_date=settlement_end_date,
                                                  deposit_date=deposit_date,
                                                  total_amount=total_amount,
                                                  currency=currency,
                                                  transaction_type=transaction_type,
                                                  order_id=order_id,
                                                  merchant_order_id=merchant_order_id,
                                                  adjustment_id=adjustment_id,
                                                  shipment_id=shipment_id,
                                                  marketplace_name=marketplace_name,
                                                  fulfillment_id=fulfillment_id,
                                                  posted_date=posted_date,
                                                  posted_date_time=posted_date_time,
                                                  order_item_code=order_item_code,
                                                  merchant_order_item_id=merchant_order_item_id,
                                                  merchant_adjustment_item_id=merchant_adjustment_item_id,
                                                  sku=sku,
                                                  quantity_purchased=quantity_purchased,
                                                  promotion_id=promotion_id,
                                                  amount_details=new_amount_details)

            else:
                AzSettlementV2.add(selling_partner_id=selling_partner_id,
                                   settlement_id=settlement_id,
                                   settlement_start_date=settlement_start_date,
                                   settlement_end_date=settlement_end_date,
                                   deposit_date=deposit_date,
                                   total_amount=total_amount,
                                   currency=currency,
                                   transaction_type=transaction_type,
                                   order_id=order_id,
                                   merchant_order_id=merchant_order_id,
                                   adjustment_id=adjustment_id,
                                   shipment_id=shipment_id,
                                   marketplace_name=marketplace_name,
                                   fulfillment_id=fulfillment_id,
                                   posted_date=posted_date,
                                   posted_date_time=posted_date_time,
                                   order_item_code=order_item_code,
                                   merchant_order_item_id=merchant_order_item_id,
                                   merchant_adjustment_item_id=merchant_adjustment_item_id,
                                   sku=sku,
                                   quantity_purchased=quantity_purchased,
                                   promotion_id=promotion_id,
                                   amount_details=amount_details)

            # #same transaction type - different skus
            # elif settlement_data_exists.transaction_type == data.get('transaction_type') and settlement_data_exists.sku != data.get('sku'):
            #     AzSettlementV2.add(data) #

            # #different transaction type
            # elif settlement_data_exists.transaction_type != data.get('transaction_type'):
            #     AzSettlementV2.add(data) #

            # else:
            #     AzSettlementV2.add(data) #

        else:
            AzSettlementV2.add(selling_partner_id=selling_partner_id,
                               settlement_id=settlement_id,
                               settlement_start_date=settlement_start_date,
                               settlement_end_date=settlement_end_date,
                               deposit_date=deposit_date,
                               total_amount=total_amount,
                               currency=currency,
                               transaction_type=transaction_type,
                               order_id=order_id,
                               merchant_order_id=merchant_order_id,
                               adjustment_id=adjustment_id,
                               shipment_id=shipment_id,
                               marketplace_name=marketplace_name,
                               fulfillment_id=fulfillment_id,
                               posted_date=posted_date,
                               posted_date_time=posted_date_time,
                               order_item_code=order_item_code,
                               merchant_order_item_id=merchant_order_item_id,
                               merchant_adjustment_item_id=merchant_adjustment_item_id,
                               sku=sku,
                               quantity_purchased=quantity_purchased,
                               promotion_id=promotion_id,
                               amount_details=amount_details)
