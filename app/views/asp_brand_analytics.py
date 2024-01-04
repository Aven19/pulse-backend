'''brand analytics report view'''
import gzip

from app import config_data
from app import logger
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import ResponseMessageKeys
from app.helpers.decorators import api_time_logger
from app.helpers.decorators import token_required
from app.helpers.utility import get_asp_market_place_ids
from app.helpers.utility import send_json_response
from app.models.az_report import AzReport
from flask import request
import numpy as np
import pandas as pd
from providers.amazon_sp_client import AmazonReportEU
import requests


class BrandAnalyticsReports:
    '''class for getting brand analytics report from sp-apis'''

    @staticmethod
    @api_time_logger
    @token_required
    def create_brand_analytics_report(user_object, account_object):
        """create brand analytics report for item master using sp-apis"""

        try:

            asp_id = account_object.asp_id
            account_id = account_object.uuid

            credentials = account_object.retrieve_asp_credentials(account_object)[
                0]

            payload = {
                'reportType': 'GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT',
                'reportOptions': {'reportPeriod': 'MONTH'},
                'dataStartTime': '2023-07-01',
                'dataEndTime': '2023-07-31',
                'marketplaceIds': get_asp_market_place_ids()
            }

            # creating AmazonReportEU object and passing the credentials
            report = AmazonReportEU(credentials=credentials)

            # calling create report function of report object and passing the payload
            response = report.create_report(payload=payload)

            AzReport.add(account_id=account_id, seller_partner_id=asp_id,
                         type='GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT', reference_id=response['reportId'])

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response['reportId'], error=None)

        except Exception as exception_error:
            logger.error(
                f'Exception occured while creating item master report: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

    @staticmethod
    @api_time_logger
    @token_required
    def verify_brand_analytics_report(user_object, account_object):
        """verify processing status of document based on reportID"""

        try:

            credentials = account_object.retrieve_asp_credentials(account_object)[
                0]

            report_id = request.args.get('report_id')

            # creating AmazonReportEU object and passing the credentials
            report = AmazonReportEU(credentials=credentials)

            # calling verify_report function of report object and passing the report_id
            report_status = report.verify_report(report_id)

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=report_status, error=None)

        except Exception as exception_error:
            logger.error(
                f'Exception occured while verifying item master report: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

    @staticmethod
    @api_time_logger
    @token_required
    def get_ba_report(user_object, account_object):
        """retrieve sales and traffic report using sp-apis"""

        try:

            account_id = account_object.uuid

            credentials = account_object.retrieve_asp_credentials(account_object)[
                0]

            reference_id = request.args.get('report_id')

            report = AmazonReportEU(credentials=credentials)

            get_report_document_id = AzReport.get_by_ref_id(account_id=account_id,
                                                            reference_id=reference_id)

            document_id = get_report_document_id.document_id

            # using retrieve_report function of report object to get report
            get_report = report.retrieve_report(document_id)
            # get url
            if 'url' in get_report:
                file_url = get_report['url']
            else:
                logger.error(
                    'file url not found for sales and traffic report')
                return send_json_response(
                    http_status=500,
                    response_status=False,
                    message_key=ResponseMessageKeys.FAILED.value
                )

            # get file data by accessing the url
            file_data = requests.get(file_url)

            if file_data.status_code != 200:
                logger.error(
                    'Exception occured while retrieving sales and traffic report')
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

            # reading text in file_data using pandas and converting into dataframe
            # data_frame = pd.read_csv(io.StringIO(file_data.text),
            #                          sep='\t', index_col=False, header=None, skiprows=1)

            with open(config_data.get('UPLOAD_FOLDER') + 'brand_analytics'   # type: ignore  # noqa: FKA100
                      + '/repeat_purchase_report_july_2023.json', 'w') as f:
                f.write(content)

            # json_dict = json.loads(content)

            # data_frame = pd.DataFrame(json_dict['dataByDepartmentAndSearchTerm'])

            # data_frame.to_csv('media/brand_analytics/sample.csv')

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=None, error=None)

        except Exception as exception_error:
            logger.error(
                f'Exception occured while verifying item master report: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )
