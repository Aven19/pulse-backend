"""Contains Amazon Seller Settlement Report v2 related API definitions."""
from datetime import timedelta
import io

from app import config_data
from app import export_csv_q
from app import logger
from app import settlement_report_v2_q
from app.helpers.constants import ASpReportType
from app.helpers.constants import EntityType
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import QueueName
from app.helpers.constants import QueueTaskStatus
from app.helpers.constants import ResponseMessageKeys
from app.helpers.decorators import api_time_logger
from app.helpers.utility import amount_details_to_json
from app.helpers.utility import field_type_validator
from app.helpers.utility import get_asp_market_place_ids
from app.helpers.utility import required_validator
from app.helpers.utility import send_json_response
from app.models.az_report import AzReport
from app.models.az_settlement_v2 import AzSettlementV2
from app.models.queue_task import QueueTask
from flask import request
import numpy as np
import pandas as pd
from providers.amazon_sp_client import AmazonReportEU
import requests
from workers.settlement_report_v2_worker import SettlementReportV2Worker


@api_time_logger
def request_settlement_report_v2():
    """Create Settlement Report v2 Report"""

    try:
        credentials = {
            'seller_partner_id': config_data.get('SP_ID'),
            'refresh_token': config_data.get('SP_REFRESH_TOKEN'),
            'client_id': config_data.get('SP_LWA_APP_ID'),
            'client_secret': config_data.get('SP_LWA_CLIENT_SECRET')
        }

        params = {
            'reportTypes': ASpReportType.SETTLEMENT_REPORT_FLAT_FILE_V2.value,
            'marketplaceIds': get_asp_market_place_ids(),
            'createdSince': '2023-06-10T00:00:00.000Z',
            'createdUntil': '2023-06-16T00:23:59.999Z',
        }

        get_settlement_report = AmazonReportEU(credentials=credentials)

        response = get_settlement_report.get_scheduled_report(params=params)

        process_reports(response=response,
                        get_settlement_report=get_settlement_report)

        return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response, error=None)

    except Exception as exception_error:
        """ Exception in creating Settlement Report v2 """
        logger.error(
            f'GET -> Settlement Report v2 creation Failed: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value,
        )


@api_time_logger
def get_settlement_report_v2(reference_id: int):
    """Retrive Settlement Report v2"""
    try:

        credentials = {
            'seller_partner_id': config_data.get('SP_ID'),
            'refresh_token': config_data.get('SP_REFRESH_TOKEN'),
            'client_id': config_data.get('SP_LWA_APP_ID'),
            'client_secret': config_data.get('SP_LWA_CLIENT_SECRET')
        }

        get_report_document_id = AzReport.get_by_ref_id(
            reference_id=reference_id)

        if not get_report_document_id:
            return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=reference_id)

        document_id = get_report_document_id.document_id

        # return get_report
        report = AmazonReportEU(credentials=credentials)

        retrive_report = report.retrieve_report(document_id)

        streamed_url_response = requests.get(retrive_report['url'])

        # Decode the content as UTF-8
        content = streamed_url_response.content.decode('utf-8')

        # Create a streaming pandas DataFrame
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

        # transformation
        settlement_df = settlement_df.fillna(np.nan).replace([np.nan], [None])   # type: ignore  # noqa: FKA100

        # store amount details as json
        previous_order_id = None
        first_row = True
        amount_details_dict = {'amount_details': []}
        for row in settlement_df.iloc[1:].itertuples(index=False):
            current_order_id = row.order_id

            if current_order_id == previous_order_id or first_row is True:
                amount_details_dict = amount_details_to_json(
                    amount_details_dict=amount_details_dict, row=row)

                selling_partner_id = config_data['SELLER_ID']
                settlement_id = row.settlement_id
                settlement_start_date = row.settlement_start_date
                settlement_end_date = row.settlement_end_date
                deposit_date = row.deposit_date
                total_amount = row.total_amount
                currency = row.currency
                transaction_type = row.transaction_type
                order_id = row.order_id
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
                    AzSettlementV2.add(
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

                    amount_details_dict = {'amount_details': []}

                    amount_details_dict = amount_details_to_json(
                        amount_details_dict=amount_details_dict, row=row)

            previous_order_id = current_order_id

        return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=retrive_report, error=None)

    except Exception as exception_error:
        """ Exception in retriving Settlement Report v2 """
        logger.error(
            f'GET -> Failed retriving Settlement Report v2 : {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value,
        )


def process_reports(response, get_settlement_report):
    """Function to save paginated response from amazon api"""
    if 'nextToken' in response:

        delay = 0

        for report in response['reports']:
            report_type = report['reportType']
            report_id = report['reportId']
            report_status = report['processingStatus']
            report_document_id = report['reportDocumentId']

            AzReport.add(seller_partner_id=None, type=report_type, reference_id=report_id,
                         status=report_status, document_id=report_document_id)

            data = {
                'reference_id': report_id,
                'dataStartTime': report['dataStartTime'],
                'dataEndTime': report['dataEndTime']
            }

            settlement_report_v2_q.enqueue_in(timedelta(seconds=delay), SettlementReportV2Worker.process_csv_reports, data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100

            delay += 5

        params = {
            'nextToken': response['nextToken']
        }

        next_response = get_settlement_report.get_scheduled_report(
            params=params)

        process_reports(response=next_response,
                        get_settlement_report=get_settlement_report)

    else:

        delay = 0

        for report in response['reports']:
            report_type = report['reportType']
            report_id = report['reportId']
            report_status = report['processingStatus']
            report_document_id = report['reportDocumentId']

            AzReport.add(seller_partner_id=None, type=report_type, reference_id=report_id,
                         status=report_status, document_id=report_document_id)

            data = {
                'reference_id': report_id,
                'dataStartTime': report['dataStartTime'],
                'dataEndTime': report['dataEndTime']
            }

            settlement_report_v2_q.enqueue_in(timedelta(seconds=delay), SettlementReportV2Worker.process_csv_reports, data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100

            delay += 5


@api_time_logger
def export_settlement_v2_csv():
    """ view for exporting settlement v2 data"""

    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')

    # validation
    data = {}

    if from_date:
        data['from_date'] = from_date
    if to_date:
        data['to_date'] = to_date

    field_types = {'from_date': str, 'to_date': str}

    required_fields = ['from_date', 'to_date']

    validate_data = field_type_validator(
        request_data=data, field_types=field_types)

    if validate_data['is_error']:
        return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                  message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                  error=validate_data['data'])

    is_valid = required_validator(
        request_data=data, required_fields=required_fields)

    if is_valid['is_error']:
        return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                  message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                  error=is_valid['data'])

    data['seller_partner_id'] = config_data.get('SP_ID')

    queue_task = QueueTask.add_queue_task(queue_name=QueueName.EXPORT_CSV,
                                          owner_id=1,
                                          status=QueueTaskStatus.NEW.value,
                                          entity_type=EntityType.SETTLEMENT_V2_REPORT.value,
                                          param=str(data), input_attachment_id=None, output_attachment_id=None)

    if queue_task:
        queue_task_dict = {
            'job_id': queue_task.id,
            'queue_name': queue_task.queue_name,
            'status': QueueTaskStatus.get_status(queue_task.status),
            'entity_type': EntityType.get_type(queue_task.entity_type)
        }

        data.update(queue_task_dict)

        export_csv_q.enqueue_in(timedelta(seconds=2), SettlementReportV2Worker.settlement_report_v2_csv_export, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100

        return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.EXPORT_QUEUED.value, data=queue_task_dict, error=None)
