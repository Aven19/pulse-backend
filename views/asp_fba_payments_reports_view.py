"""Contains Amazon Seller FBA Reimbursements Report related API definitions."""
import gzip
import io

from app import logger
from app.helpers.constants import ASpReportType
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import ResponseMessageKeys
from app.helpers.decorators import api_time_logger
from app.helpers.decorators import token_required
from app.helpers.utility import get_asp_market_place_ids
from app.helpers.utility import send_json_response
from app.models.az_fba_reimbursements import AzFbaReimbursements
from app.models.az_report import AzReport
from flask import request
import numpy as np
import pandas as pd
from providers.amazon_sp_client import AmazonReportEU
import requests


class FbaPaymentsReports:
    """class for getting fba reimbursements report from sp-api"""

    # FBA Concessions Reports
    @staticmethod
    @api_time_logger
    @token_required
    def create_reimbursements_report(user_object, account_object):
        """Create FBA Reimbursements Report"""

        try:

            asp_id = account_object.asp_id
            account_id = account_object.uuid

            credentials = account_object.retrieve_asp_credentials(account_object)[
                0]

            payload = {
                'reportType': ASpReportType.FBA_REIMBURSEMENTS_REPORT.value,
                'dataStartTime': '2023-08-01T00:00:00.000Z',
                'marketplaceIds': get_asp_market_place_ids()
            }

            report = AmazonReportEU(credentials=credentials)

            response = report.create_report(payload=payload)
            AzReport.add(account_id=account_id, seller_partner_id=asp_id,
                         type=ASpReportType.FBA_REIMBURSEMENTS_REPORT.value, reference_id=response['reportId'])

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response['reportId'], error=None)

        except Exception as exception_error:
            """ Exception in creating FBA Reimbursements Report """
            logger.error(
                f'GET -> FBA Reimbursements Report creation Failed: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    def verify_reimbursements_report(user_object, account_object):
        """Verify FBA Reimbursements Report Id"""
        try:

            reference_id = request.args.get('report_id')
            account_id = account_object.uuid

            credentials = account_object.retrieve_asp_credentials(account_object)[
                0]

            get_report = AzReport.get_by_ref_id(
                account_id=account_id, reference_id=reference_id)

            if not get_report:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=reference_id)

            report = AmazonReportEU(credentials=credentials)
            report_status = report.verify_report(reference_id)
            if report_status['processingStatus'] != 'DONE':
                AzReport.update_status(
                    reference_id=reference_id, status=report_status['processingStatus'], document_id=None)
            else:
                AzReport.update_status(
                    reference_id=reference_id, status=report_status['processingStatus'], document_id=report_status['reportDocumentId'])

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=report_status, error=None)

        except Exception as exception_error:
            """ Exception in verifying FBA Reimbursements Report Id """
            logger.error(
                f'GET -> FBA Reimbursements Report Id verification failed: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    def get_reimbursements_report(user_object, account_object):
        """Retrive FBA Reimbursements Report"""
        try:

            report_id = request.args.get('report_id')
            asp_id = account_object.asp_id
            account_id = account_object.uuid
            credentials = account_object.retrieve_asp_credentials(account_object)[
                0]

            get_report_document_id = AzReport.get_by_ref_id(
                account_id=account_id, reference_id=report_id)

            if not get_report_document_id:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=report_id)

            document_id = get_report_document_id.document_id

            # return get_report
            report = AmazonReportEU(credentials=credentials)

            retrive_report = report.retrieve_report(document_id)

            if 'compressionAlgorithm' in retrive_report and retrive_report['compressionAlgorithm'] == 'GZIP':
                streamed_url_response = requests.get(retrive_report['url'])
                compressed_content = streamed_url_response.content
                decompressed_content = gzip.decompress(compressed_content)
                content = decompressed_content.decode('utf-8')
            else:
                streamed_url_response = requests.get(retrive_report['url'])
                content = streamed_url_response.content.decode('utf-8')

            if streamed_url_response.status_code != 200:
                logger.error(
                    'Exception occured while retrieving FBA Reimbursements Report')
                return send_json_response(
                    http_status=500,
                    response_status=False,
                    message_key=ResponseMessageKeys.FAILED.value
                )

            # Create a streaming pandas DataFrame
            df_stream = pd.read_csv(io.StringIO(
                content), delimiter='\t', header=None, skiprows=1, iterator=True, chunksize=1000)
            data_frame = pd.concat(df_stream, ignore_index=True)

            # data_frame.to_csv(config_data.get('UPLOAD_FOLDER') + ASpReportType.FBA_REIMBURSEMENTS_REPORT.value.lower()
            #                     + '/{}.csv'.format(document_id), index=False)

            data_frame.columns = [
                'approval_date',
                'reimbursement_id',
                'case_id',
                'az_order_id',
                'reason',
                'sku',
                'fnsku',
                'asin',
                'product_name',
                'condition',
                'currency_unit',
                'amount_per_unit',
                'amount_total',
                'quantity_reimbursed_cash',
                'quantity_reimbursed_inventory',
                'quantity_reimbursed_total',
                'original_reimbursement_id',
                'original_reimbursement_type'
            ]

            fba_reimbursements_df = data_frame.fillna(np.nan).replace([np.nan], [None])   # type: ignore  # noqa: FKA100
            for row in fba_reimbursements_df.itertuples(index=False):
                AzFbaReimbursements.add_or_update(
                    account_id=account_id,
                    asp_id=asp_id,
                    approval_date=row.approval_date,
                    reimbursement_id=row.reimbursement_id,
                    case_id=row.case_id,
                    az_order_id=row.az_order_id,
                    reason=row.reason,
                    sku=row.sku,
                    fnsku=row.fnsku,
                    asin=row.asin,
                    product_name=row.product_name,
                    condition=row.condition,
                    currency_unit=row.currency_unit,
                    amount_per_unit=row.amount_per_unit,
                    amount_total=row.amount_total,
                    quantity_reimbursed_cash=row.quantity_reimbursed_cash,
                    quantity_reimbursed_inventory=row.quantity_reimbursed_inventory,
                    quantity_reimbursed_total=row.quantity_reimbursed_total,
                    original_reimbursement_id=row.original_reimbursement_id,
                    original_reimbursement_type=row.original_reimbursement_type
                )

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=retrive_report, error=None)

        except Exception as exception_error:
            """ Exception in retriving FBA Reimbursements Report """
            logger.error(
                f'GET -> Failed retriving FBA Reimbursements Report: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )
