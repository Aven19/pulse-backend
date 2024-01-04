'''ledger summaray report view'''
import gzip
import io

from app import logger
from app.helpers.constants import AggregateByLocation
from app.helpers.constants import AggregateByTimePeriod
from app.helpers.constants import ASpReportType
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import ResponseMessageKeys
from app.helpers.decorators import api_time_logger
from app.helpers.decorators import brand_filter
from app.helpers.decorators import token_required
from app.helpers.utility import field_type_validator
from app.helpers.utility import get_asp_market_place_ids
from app.helpers.utility import get_created_since
from app.helpers.utility import get_created_until
from app.helpers.utility import required_validator
from app.helpers.utility import send_json_response
from app.models.az_ledger_summary import AzLedgerSummary
from app.models.az_report import AzReport
from flask import request
import numpy as np
import pandas as pd
from providers.amazon_sp_client import AmazonReportEU
import requests


@api_time_logger
@token_required
@brand_filter
def create_ledger_summary_report(user_object, account_object, allowed_brands):
    """create ledger summary view using sp-apis"""
    try:

        field_types = {'marketplace': str,
                       'from_date': str, 'to_date': str}

        required_fields = ['from_date', 'to_date']

        data = field_type_validator(
            request_data=request.args, field_types=field_types)

        if data['is_error']:
            return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                      message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                      error=data['data'])

        is_valid = required_validator(
            request_data=request.args, required_fields=required_fields)

        if is_valid['is_error']:
            return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                      message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                      error=is_valid['data'])

        account_id = account_object.uuid
        asp_id = account_object.asp_id
        credentials = account_object.retrieve_asp_credentials(account_object)[
            0]

        data_start_time = get_created_since(request.args.get('from_date'))
        data_end_time = get_created_until(request.args.get('to_date'))

        payload = {
            'reportOptions': {
                'aggregateByLocation': AggregateByLocation.FC.value,
                'aggregatedByTimePeriod': AggregateByTimePeriod.DAILY.value
            },
            'reportType': ASpReportType.LEDGER_SUMMARY_VIEW_DATA.value,
            'dataStartTime': data_start_time,
            'dataEndTime': data_end_time,
            'marketplaceIds': get_asp_market_place_ids()
        }

        # creating AmazonReportEU object and passing the credentials
        report = AmazonReportEU(credentials=credentials)

        # calling create report function of report object and passing the payload
        response = report.create_report(payload=payload)

        # adding the report_type, report_id to the report table
        AzReport.add(account_id=account_id,
                     seller_partner_id=asp_id, type=ASpReportType.LEDGER_SUMMARY_VIEW_DATA.value, reference_id=response['reportId'])

        return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response['reportId'], error=None)

    except Exception as exception_error:
        logger.error(
            f'Exception occured while creating ledger summary report: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value
        )


@api_time_logger
@token_required
@brand_filter
def verify_ledger_summary_report(user_object, account_object, allowed_brands):
    """verify processing status of document based on reportID"""

    try:

        report_id = request.args.get('report_id')
        account_id = account_object.uuid
        credentials = account_object.retrieve_asp_credentials(account_object)[
            0]

        # querying Report table to get the entry for particular report_id
        get_report = AzReport.get_by_ref_id(
            account_id=account_id, reference_id=report_id)

        if not get_report:
            return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=report_id)

        # creating AmazonReportEU object and passing the credentials
        report = AmazonReportEU(credentials=credentials)

        # calling verify_report function of report object and passing the report_id
        report_status = report.verify_report(report_id)

        # checking the processing status of the report. if complete, we update status in the table entry for that particular report_id
        if report_status['processingStatus'] != 'DONE':
            AzReport.update_status(
                reference_id=report_id, status=report_status['processingStatus'], document_id=None)
        else:
            AzReport.update_status(
                reference_id=report_id, status=report_status['processingStatus'], document_id=report_status['reportDocumentId'])

        return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=report_status, error=None)

    except Exception as exception_error:
        logger.error(
            f'Exception occured while verifying ledger summary report: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value
        )


@api_time_logger
@token_required
@brand_filter
def get_ledger_summary_report(user_object, account_object, allowed_brands):
    """retrieve ledger summary report using sp-apis"""

    try:
        report_id = request.args.get('report_id')
        account_id = account_object.uuid
        asp_id = account_object.asp_id
        credentials = account_object.retrieve_asp_credentials(account_object)[
            0]

        # querying Report table to get the entry for particular report_id
        get_report_document_id = AzReport.get_by_ref_id(
            account_id=account_id, reference_id=report_id)

        if not get_report_document_id:
            return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=report_id)

        # getting the document_id
        report_document_id = get_report_document_id.document_id
        # creating AmazonReportEU object and passing the credentials
        report = AmazonReportEU(credentials=credentials)

        # using retrieve_report function of report object to get report
        get_report = report.retrieve_report(report_document_id)

        # get url
        if 'url' in get_report:
            file_url = get_report['url']
        else:
            logger.error(
                'file url not found for ledger summary report')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

        # get file data by accessing the url
        file_data = requests.get(file_url)

        if file_data.status_code != 200:
            logger.error(
                'Exception occured while retrieving ledger summary report')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

        if 'compressionAlgorithm' in get_report and get_report['compressionAlgorithm'] == 'GZIP':
            streamed_url_response = requests.get(get_report['url'])
            compressed_content = streamed_url_response.content
            decompressed_content = gzip.decompress(compressed_content)
            content = decompressed_content.decode('utf-8')
        else:
            streamed_url_response = requests.get(get_report['url'])
            content = streamed_url_response.content.decode('utf-8')

        df_stream = pd.read_csv(io.StringIO(
            content), delimiter='\t', header=0, iterator=True, chunksize=1000)

        data_frame = pd.concat(df_stream, ignore_index=True)

        # data_frame.to_csv(config_data.get('UPLOAD_FOLDER') + ASpReportType.LEDGER_SUMMARY_VIEW_DATA.value.lower()
        #                   + '/{}.csv'.format(report_document_id), index=False)

        # transforming data before db insertion
        data_frame.columns = ['date', 'fnsku', 'asin', 'msku', 'title', 'disposition', 'starting_warehouse_balance',
                              'in_transit_btw_warehouse', 'receipts', 'customer_shipments', 'customer_returns', 'vendor_returns',
                              'warehouse_transfer', 'found', 'lost', 'damaged', 'disposed', 'other_events',
                              'ending_warehouse_balance', 'unknown_events', 'location']

        data_frame['date'] = pd.to_datetime(
            data_frame['date'], format='%m/%d/%Y').dt.date

        # transformation
        data_frame = data_frame.fillna(np.nan).replace([np.nan], [None])   # type: ignore  # noqa: FKA100

        # inserting data into db
        for row in data_frame.itertuples(index=False):
            AzLedgerSummary.add_update(account_id=account_id, asp_id=asp_id, date=row[0], fnsku=row[1], asin=row[2],
                                       msku=row[3], title=row[4], disposition=row[5], starting_warehouse_balance=row[6],
                                       in_transit_btw_warehouse=row[7], receipts=row[8], customer_shipments=row[
                9], customer_returns=row[10], vendor_returns=row[11], warehouse_transfer=row[12],
                found=row[13], lost=row[14], damaged=row[15], disposed=row[16], other_events=row[17], ending_warehouse_balance=row[18], unknown_events=row[19], location=row[20])

        return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=None, error=None)

    except Exception as exception_error:
        logger.error(
            f'Exception occured during retrieving ledger summary report: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value
        )
