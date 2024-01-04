"""Contains Amazon Seller FBA Replacements Report Report related API definitions."""
import io

from app import config_data
from app import logger
from app.helpers.constants import ASpReportType
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import ResponseMessageKeys
from app.helpers.decorators import api_time_logger
from app.helpers.decorators import asp_credentials_required
from app.helpers.utility import get_asp_market_place_ids
from app.helpers.utility import send_json_response
from app.models.az_fba_replacement import AzFbaReplacement
from app.models.az_report import AzReport
import numpy as np
import pandas as pd
from providers.amazon_sp_client import AmazonReportEU
import requests


@api_time_logger
@asp_credentials_required
def create_fba_replacements_report(credentials):
    """Create FBA Replacements Report"""

    try:
        credentials = {
            'seller_partner_id': credentials['seller_partner_id'],
            'refresh_token': credentials['refresh_token'],
            'client_id': credentials['client_id'],
            'client_secret': credentials['client_secret']
        }

        payload = {
            'reportType': ASpReportType.FBA_REPLACEMENTS_REPORT.value,
            'dataStartTime': '2023-06-01T00:00:00+00:00',
            'marketplaceIds': get_asp_market_place_ids()
        }

        report = AmazonReportEU(credentials=credentials)

        response = report.create_report(payload=payload)
        AzReport.add(seller_partner_id=credentials['seller_partner_id'],
                     type=ASpReportType.FBA_REPLACEMENTS_REPORT.value, reference_id=response['reportId'])

        return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response['reportId'], error=None)

    except Exception as exception_error:
        """ Exception in creating FBA Replacements Report """
        logger.error(
            f'GET -> FBA Replacements Report creation Failed: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value,
        )


@api_time_logger
def verify_fba_replacements_report(reference_id: int):
    """Verify FBA Replacements Report Id"""
    try:

        get_report = AzReport.get_by_ref_id(reference_id=reference_id)

        if not get_report:
            return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=reference_id)

        credentials = {
            'seller_partner_id': config_data.get('SP_ID'),
            'refresh_token': config_data.get('SP_REFRESH_TOKEN'),
            'client_id': config_data.get('SP_LWA_APP_ID'),
            'client_secret': config_data.get('SP_LWA_CLIENT_SECRET')
        }

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
        """ Exception in verifying FBA Replacements Report Id """
        logger.error(
            f'GET -> FBA Replacements Report Id verification failed: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value,
        )


@api_time_logger
def get_fba_replacements_report(reference_id: int):
    """Retrive FBA Replacements Report"""
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

        if streamed_url_response.status_code != 200:
            logger.error(
                'Exception occured while retrieving stock transfer report')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

        # Decode the content as UTF-8
        content = streamed_url_response.content.decode('utf-8')

        # Create a streaming pandas DataFrame
        df_stream = pd.read_csv(io.StringIO(
            content), delimiter='\t', header=None, skiprows=1, iterator=True, chunksize=1000)
        fba_replacements_df = pd.concat(df_stream, ignore_index=True)

        fba_replacements_df.columns = [
            'shipment_date', 'sku', 'asin', 'fulfillment_center_id', 'original_fulfillment_center_id', 'quantity',
            'replacement_reason_code', 'replacement_amazon_order_id', 'original_amazon_order_id']

        fba_replacements_df['shipment_date'] = pd.to_datetime(
            fba_replacements_df['shipment_date']).apply(lambda x: int(x.timestamp()))

        fba_replacements_df = fba_replacements_df.fillna(np.nan).replace([np.nan], [None])   # type: ignore  # noqa: FKA100

        for row in fba_replacements_df.itertuples(index=False):
            AzFbaReplacement.add(
                selling_partner_id=config_data['SELLER_ID'],
                shipment_date=row.shipment_date,
                sku=row.sku,
                asin=row.asin,
                fulfillment_center_id=row.fulfillment_center_id,
                original_fulfillment_center_id=row.original_fulfillment_center_id,
                quantity=row.quantity,
                replacement_reason_code=row.replacement_reason_code,
                replacement_amazon_order_id=row.replacement_amazon_order_id,
                original_amazon_order_id=row.original_amazon_order_id
            )

        return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=retrive_report, error=None)

    except Exception as exception_error:
        """ Exception in retriving FBA Replacements Report """
        logger.error(
            f'GET -> Failed retriving FBA Replacements Report: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value,
        )
