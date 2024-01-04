"""Contains Amazon Seller Returns Report related API definitions."""
import gzip

from app import logger
from app.helpers.constants import ASpReportType
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import ResponseMessageKeys
from app.helpers.decorators import api_time_logger
from app.helpers.decorators import asp_credentials_required
from app.helpers.utility import get_asp_market_place_ids
from app.helpers.utility import send_json_response
from app.models.az_report import AzReport
import pandas as pd
from providers.amazon_sp_client import AmazonReportEU
import requests


@api_time_logger
@asp_credentials_required
def create_returns_report(credentials):
    """Create Returns Report"""

    try:

        payload = {
            'reportType': ASpReportType.RETURN_REPORT_RETURN_DATE.value,
            'dataStartTime': '2023-05-01T00:00:00.000Z',
            'dataEndTime': '2023-05-05T23:59:59.000Z',
            'marketplaceIds': get_asp_market_place_ids()
        }

        request_return_report = AmazonReportEU(credentials=credentials)

        response = request_return_report.create_report(payload=payload)

        AzReport.add(seller_partner_id=credentials.get('seller_partner_id'),
                     type=ASpReportType.RETURN_REPORT_RETURN_DATE.value, reference_id=response['reportId'])

        return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response['reportId'], error=None)

    except Exception as exception_error:
        """ Exception in getting returns report"""
        logger.error(
            f'GET -> Exception occured during creating returns report: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value,
        )


@api_time_logger
@asp_credentials_required
def verify_returns_report(credentials, report_id: int):
    """Verify Returns Report"""
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
        """ Exception in returns report """
        logger.error(
            f'GET -> Exception occured during verifying returns report: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value,
        )


@api_time_logger
@asp_credentials_required
def get_returns_report(credentials, report_id: int):
    """retrieve returns report using sp-apis"""

    try:

        # querying Report table to get the entry for particular report_id
        get_report_document_id = AzReport.get_by_ref_id(reference_id=report_id)

        if not get_report_document_id:
            return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=report_id)

        # getting the document_id
        report_document_id = get_report_document_id.document_id
        # creating AmazonReportEU object and passing the credentials
        report = AmazonReportEU(credentials=credentials)

        # using retrieve_report function of report object to get report
        get_report = report.retrieve_report(report_document_id)

        # get url
        file_url = get_report['url']

        # get file data by accessing the url
        file_data = requests.get(file_url)

        if file_data.status_code != 200:
            logger.error(
                'Exception occured while retrieving returns report')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

        # decompressing the data
        content = file_data.content

        if get_report['compressionAlgorithm'] == 'GZIP':
            content = gzip.decompress(content)

        content = content.decode('utf-8')

        # df_stream = pd.read_csv(io.StringIO(
        #     content), delimiter='\t', header=0, iterator=True, chunksize=1000)

        # data_frame = pd.concat(df_stream, ignore_index=True)

        # data_frame.to_csv(config_data.get('UPLOAD_FOLDER') + ASpReportType.RETURN_REPORT_RETURN_DATE.value.lower()
        #                   + '/{}.csv'.format(report_document_id), index=False)

        return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=None, error=None)

    except Exception as exception_error:
        logger.error(
            f'Exception occured during retrieving returns report: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value
        )


@api_time_logger
@asp_credentials_required
def create_returns_csv_prime_report(credentials):
    """Create returns CSV Prime Report"""
    try:

        payload = {
            'reportType': ASpReportType.RETURN_REPORT_CSV_MFN_PRIME.value,
            'dataStartTime': '2023-05-01T00:00:00.000Z',
            'dataEndTime': '2023-05-05T23:59:59.000Z',
            'marketplaceIds': get_asp_market_place_ids()
        }

        report = AmazonReportEU(credentials=credentials)

        response = report.create_report(payload=payload)

        AzReport.add(seller_partner_id=credentials['seller_partner_id'],
                     type=ASpReportType.RETURN_REPORT_CSV_MFN_PRIME.value, reference_id=response['reportId'])

        return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response['reportId'], error=None)

    except Exception as exception_error:
        """ Exception in creating CSV Prime Returns Report """
        logger.error(
            f'GET -> CSV Prime Returns Report creation Failed: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value,
        )


@api_time_logger
@asp_credentials_required
def verify_returns_csv_prime_report(credentials, report_id: int):
    """Verify Returns Report"""
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
        """ Exception in returns report """
        logger.error(
            f'GET -> Exception occured during verifying returns report: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value,
        )


@api_time_logger
@asp_credentials_required
def create_returns_flat_file_attributes(credentials):
    """Create returns flat file attributes report"""
    try:

        payload = {
            'reportType': ASpReportType.RETURN_REPORT_FLAT_FILE_ATTRIBUTES.value,
            'dataStartTime': '2023-04-01T00:00:00.000Z',
            'dateEndTime': '2023-05-31T23::59.999Z',
            'marketplaceIds': get_asp_market_place_ids()
        }

        report = AmazonReportEU(credentials=credentials)

        response = report.create_report(payload=payload)

        AzReport.add(seller_partner_id=credentials['seller_partner_id'],
                     type=ASpReportType.RETURN_REPORT_FLAT_FILE_ATTRIBUTES.value, reference_id=response['reportId'])

        return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response['reportId'], error=None)

    except Exception as exception_error:
        """ Exception in creating returns flat file attributes Report """
        logger.error(
            f'GET -> Returns flat file attributes Report creation Failed: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value,
        )


@api_time_logger
@asp_credentials_required
def get_returns_report_flat_file_attributes(credentials, report_id: int):
    """retrieve returns report using sp-apis"""

    try:

        # querying Report table to get the entry for particular report_id
        get_report_document_id = AzReport.get_by_ref_id(reference_id=report_id)

        if not get_report_document_id:
            return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=report_id)

        # getting the document_id
        report_document_id = get_report_document_id.document_id
        # creating AmazonReportEU object and passing the credentials
        report = AmazonReportEU(credentials=credentials)

        # using retrieve_report function of report object to get report
        get_report = report.retrieve_report(report_document_id)
        # get url
        file_url = get_report['url']

        # get file data by accessing the url
        file_data = requests.get(file_url)

        if file_data.status_code != 200:
            logger.error(
                'Exception occured while retrieving returns report')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

        content = file_data.content

        return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=content, error=None)

    except Exception as exception_error:
        logger.error(
            f'Exception occured during retrieving returns report: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value
        )
