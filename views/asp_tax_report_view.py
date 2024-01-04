"""Contains Amazon Seller Tax Report related API definitions."""
import gzip
import io

from app import config_data
from app import logger
from app.helpers.constants import ASpReportType
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import ResponseMessageKeys
from app.helpers.decorators import api_time_logger
from app.helpers.decorators import asp_credentials_required
from app.helpers.utility import get_asp_data_start_time
from app.helpers.utility import get_asp_market_place_ids
from app.helpers.utility import send_json_response
from app.models.az_report import AzReport
import pandas as pd
from providers.amazon_sp_client import AmazonReportEU
import requests

# On Demand GST Merchant Tax Report B2B


@api_time_logger
@asp_credentials_required
def create_gst_b2b_report(credentials):
    """Create GST MTR B2B Custom Report"""

    try:

        payload = {
            'reportType': ASpReportType.TAX_REPORT_GST_MTR_B2B.value,
            'dataStartTime': '2023-05-21T00:00:00.000Z',
            'dataEndTime': '2023-05-30T23:59:59.000Z',
            'marketplaceIds': get_asp_market_place_ids()
        }

        report = AmazonReportEU(credentials=credentials)

        response = report.create_report(payload=payload)

        AzReport.add(seller_partner_id=config_data.get('SP_ID'),
                     type=ASpReportType.TAX_REPORT_GST_MTR_B2B.value, reference_id=response['reportId'])

        return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response['reportId'], error=None)

    except Exception as exception_error:
        """ Exception in creating Tax Report GST MTR B2B Custom"""
        logger.error(
            f'GET -> Tax Report GST MTR B2B Custom report creation Failed: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value,
        )


@api_time_logger
@asp_credentials_required
def verify_gst_b2b_report(credentials, report_id: int):
    """Verify GST MTR B2B Report"""
    try:

        get_report = AzReport.get_by_ref_id(reference_id=report_id)

        if not get_report:
            return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=report_id)

        report = AmazonReportEU(credentials=credentials)

        report_status = report.verify_report(report_id)

        if report_status['processingStatus'] != 'DONE':
            AzReport.update_status(
                reference_id=report_id, status=report_status['processingStatus'], document_id=None)
        else:
            AzReport.update_status(
                reference_id=report_id, status=report_status['processingStatus'], document_id=report_status['reportDocumentId'])

        return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=report_status, error=None)

    except Exception as exception_error:
        """ Exception in verifying GST MTR B2B report """
        logger.error(
            f'GET -> Tax Report GST MTR B2B Custom report verification Failed: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value,
        )


@api_time_logger
@asp_credentials_required
def get_gst_b2b_report(credentials, report_id: int):
    """Retrieve GST MTR B2B Report"""
    try:

        get_report_document_id = AzReport.get_by_ref_id(reference_id=report_id)

        if not get_report_document_id:
            return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=report_id)

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
        data_frame = pd.concat(df_stream, ignore_index=True)

        data_frame.to_csv(config_data.get('UPLOAD_FOLDER') + ASpReportType.TAX_REPORT_GST_MTR_B2B.value.lower()
                          + '/{}.csv'.format(document_id), index=False)

        return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=retrive_report, error=None)

    except Exception as exception_error:
        """ Exception in getting GST MTR B2B report"""
        logger.error(
            f'GET -> Tax Report GST MTR B2B Custom report retrieval Failed: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value,
        )


# On Demand GST Merchant Tax Report B2C
@api_time_logger
@asp_credentials_required
def create_gst_b2c_report(credentials):
    """Create GST MTR B2C Custom Report"""

    try:

        payload = {
            'reportType': ASpReportType.TAX_REPORT_GST_MTR_B2C.value,
            'dataStartTime': get_asp_data_start_time(),
            'marketplaceIds': get_asp_market_place_ids()
        }

        report = AmazonReportEU(credentials=credentials)

        response = report.create_report(payload=payload)

        AzReport.add(
            type=ASpReportType.TAX_REPORT_GST_MTR_B2C.value, reference_id=str(response['reportId']))

        return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response['reportId'], error=None)

    except Exception as exception_error:
        """ Exception in creating Tax Report GST MTR B2C Custom"""
        logger.error(
            f'GET -> Tax Report GST MTR B2C Custom report creation Failed: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value,
        )


@api_time_logger
@asp_credentials_required
def verify_gst_b2c_report(credentials, report_id: int):
    """Verify GST MTR B2C Report"""
    try:

        get_report = AzReport.get_by_ref_id(reference_id=report_id)

        if not get_report:
            return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=report_id)

        credentials = {
            'refresh_token': config_data.get('SP_REFRESH_TOKEN'),
            'client_id': config_data.get('SP_LWA_APP_ID'),
            'client_secret': config_data.get('SP_LWA_CLIENT_SECRET')
        }

        report = AmazonReportEU(credentials=credentials)

        report_status = report.verify_report(report_id)

        if report_status['processingStatus'] != 'DONE':
            AzReport.update_status(
                reference_id=report_id, status=report_status['processingStatus'], document_id=None)
        else:
            AzReport.update_status(
                reference_id=report_id, status=report_status['processingStatus'], document_id=report_status['reportDocumentId'])

        return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=report_status, error=None)

    except Exception as exception_error:
        """ Exception in GST MTR B2C report """
        logger.error(
            f'GET -> Tax Report GST MTR B2C Custom report verification Failed: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value,
        )


@api_time_logger
@asp_credentials_required
def get_gst_b2c_report(credentials, report_id: int):
    """Retrive GST B2C Report"""
    try:

        get_report_document_id = AzReport.get_by_ref_id(reference_id=report_id)

        if not get_report_document_id:
            return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=report_id)

        document_id = get_report_document_id.document_id

        # return get_report
        report = AmazonReportEU(credentials=credentials)

        retrive_report = report.retrieve_report(document_id)
        streamed_url_response = requests.get(retrive_report['url'])

        # Decode the content as UTF-8
        content = streamed_url_response.content.decode('utf-8')

        # Create a streaming pandas DataFrame
        df_stream = pd.read_csv(io.StringIO(
            content), delimiter='\t', header=None, skiprows=0, iterator=True, chunksize=1000)
        data_frame = pd.concat(df_stream, ignore_index=True)

        data_frame.to_csv(config_data.get('UPLOAD_FOLDER') + ASpReportType.TAX_REPORT_GST_MTR_B2C.value.lower()
                          + '/{}.csv'.format(document_id), index=False)

        return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=retrive_report, error=None)

    except Exception as exception_error:
        """ Exception in getting GST MTR B2C report """
        logger.error(
            f'GET -> Tax Report GST MTR B2C Custom report retrieval Failed: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value,
        )


# On Demand Stock Transfer Report


@api_time_logger
@asp_credentials_required
def create_stock_transfer_report(credentials):
    """Create Stock Transfer Report"""

    try:

        payload = {
            'reportType': ASpReportType.TAX_REPORT_B2B_STR_ADHOC.value,
            # 'dataStartTime': get_created_since(start_date),
            'dataStartTime': '2023-05-21T00:00:00.000Z',
            'dataEndTime': '2023-05-31T23:59:59.000Z',
            'marketplaceIds': get_asp_market_place_ids()
        }

        report = AmazonReportEU(credentials=credentials)

        response = report.create_report(payload=payload)

        AzReport.add(seller_partner_id=credentials.get('seller_partner_id'),
                     type=ASpReportType.TAX_REPORT_B2B_STR_ADHOC.value, reference_id=response['reportId'])

        return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response['reportId'], error=None)

    except Exception as exception_error:
        """ Exception in creating stock transfer"""
        logger.error(
            f'GET -> Stock transfer report creation Failed: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value,
        )


@api_time_logger
@asp_credentials_required
def verify_stock_transfer_report(credentials, report_id: int):
    """Verify Stock Transfer Report Id"""
    try:

        get_report = AzReport.get_by_ref_id(reference_id=report_id)

        if not get_report:
            return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=report_id)

        report = AmazonReportEU(credentials=credentials)

        report_status = report.verify_report(report_id)

        if report_status['processingStatus'] != 'DONE':
            AzReport.update_status(
                reference_id=report_id, status=report_status['processingStatus'], document_id=None)
        else:
            AzReport.update_status(
                reference_id=report_id, status=report_status['processingStatus'], document_id=report_status['reportDocumentId'])

        return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=report_status, error=None)

    except Exception as exception_error:
        """ Exception in verifying Stock Transfer Report Id """
        logger.error(
            f'GET -> Stock Transfer Report Id verification failed: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value,
        )


@api_time_logger
@asp_credentials_required
def get_stock_transfer_report(credentials, report_id: int):
    """Retrieve Stock transfer Report"""
    try:

        get_report_document_id = AzReport.get_by_ref_id(reference_id=report_id)

        if not get_report_document_id:
            return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=report_id)

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

        # decompressing the data
        content = streamed_url_response.content

        if retrive_report['compressionAlgorithm'] == 'GZIP':
            content = gzip.decompress(content)

        content = content.decode('utf-8')

        # Create a streaming pandas DataFrame
        df_stream = pd.read_csv(io.StringIO(
            content), delimiter='\t', header=None, skiprows=0, iterator=True, chunksize=1000)
        data_frame = pd.concat(df_stream, ignore_index=True)

        data_frame.to_csv(config_data.get('UPLOAD_FOLDER') + ASpReportType.TAX_REPORT_B2B_STR_ADHOC.value.lower()
                          + '/{}.csv'.format(document_id), index=False)

        return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=retrive_report, error=None)

    except Exception as exception_error:
        """ Exception in retrieving stock transfer Report """
        logger.error(
            f'GET -> Failed retrieving stock transfer Report: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value,
        )
