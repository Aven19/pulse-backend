"""Contains Amazon Seller FBA Returns Report Report related API definitions."""
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
from app.models.az_fba_returns import AzFbaReturns
from app.models.az_report import AzReport
from flask import request
import numpy as np
import pandas as pd
from providers.amazon_sp_client import AmazonReportEU
import requests


class FbaConcessionsReports:
    """class for getting fba returns report from sp-api"""

    # FBA Concessions Reports
    @staticmethod
    @api_time_logger
    @token_required
    def create_returns_report(user_object, account_object):
        """Create FBA Returns Report"""

        try:

            asp_id = account_object.asp_id
            account_id = account_object.uuid

            credentials = account_object.retrieve_asp_credentials(account_object)[
                0]

            payload = {
                'reportType': ASpReportType.FBA_RETURNS_REPORT.value,
                'dataStartTime': '2023-08-01T00:00:00.000Z',
                'marketplaceIds': get_asp_market_place_ids()
            }

            report = AmazonReportEU(credentials=credentials)

            response = report.create_report(payload=payload)
            AzReport.add(account_id=account_id, seller_partner_id=asp_id,
                         type=ASpReportType.FBA_RETURNS_REPORT.value, reference_id=response['reportId'])

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response['reportId'], error=None)

        except Exception as exception_error:
            """ Exception in creating FBA Returns Report """
            logger.error(
                f'GET -> FBA Returns Report creation Failed: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    def verify_returns_report(user_object, account_object):
        """Verify FBA Returns Report Id"""
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
            """ Exception in verifying FBA Returns Report Id """
            logger.error(
                f'GET -> FBA Returns Report Id verification failed: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    def get_returns_report(user_object, account_object):
        """Retrive FBA Returns Report"""
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
                    'Exception occured while retrieving FBA Returns Report')
                return send_json_response(
                    http_status=500,
                    response_status=False,
                    message_key=ResponseMessageKeys.FAILED.value
                )

            # Create a streaming pandas DataFrame
            df_stream = pd.read_csv(io.StringIO(
                content), delimiter='\t', header=None, skiprows=1, iterator=True, chunksize=1000)
            data_frame = pd.concat(df_stream, ignore_index=True)

            # data_frame.to_csv(config_data.get('UPLOAD_FOLDER') + ASpReportType.FBA_RETURNS_REPORT.value.lower()
            #                   + '/{}.csv'.format(document_id), index=False)

            data_frame.columns = [
                'return_date', 'order_id', 'sku', 'asin', 'fnsku', 'product_name',
                'quantity', 'fulfillment_center_id', 'detailed_disposition', 'reason', 'license_plate_number', 'customer_comments'
            ]

            fba_returns_df = data_frame.fillna(np.nan).replace([np.nan], [None])   # type: ignore  # noqa: FKA100
            for row in fba_returns_df.itertuples(index=False):
                AzFbaReturns.add_or_update(
                    account_id=account_id,
                    asp_id=asp_id,
                    return_date=row.return_date,
                    order_id=row.order_id,
                    sku=row.sku,
                    asin=row.asin,
                    fnsku=row.fnsku,
                    product_name=row.product_name,
                    quantity=row.quantity,
                    fulfillment_center_id=row.fulfillment_center_id,
                    detailed_disposition=row.detailed_disposition,
                    reason=row.reason,
                    license_plate_number=row.license_plate_number,
                    customer_comments=row.customer_comments
                )

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=retrive_report, error=None)

        except Exception as exception_error:
            """ Exception in retriving FBA Returns Report """
            logger.error(
                f'GET -> Failed retriving FBA Returns Report: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )
