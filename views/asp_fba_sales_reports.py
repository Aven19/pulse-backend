"""Contains Amazon Seller FBA Sales Reports related API definitions."""
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
from app.models.az_fba_customer_shipment_sales import AzFbaCustomerShipmentSales
from app.models.az_report import AzReport
from flask import request
import numpy as np
import pandas as pd
from providers.amazon_sp_client import AmazonReportEU
import requests


class FbaSalesReports:
    """API to fetch types of FBA Sales Reports Data"""

    @staticmethod
    @api_time_logger
    @token_required
    def create_customer_shipment_sales(user_object, account_object):
        """To get Customer shipment sales data"""
        try:

            asp_id = account_object.asp_id
            account_id = account_object.uuid

            credentials = account_object.retrieve_asp_credentials(account_object)[
                0]

            payload = {
                'reportType': ASpReportType.FBA_CUSTOMER_SHIPMENT_SALES_REPORT.value,
                'dataStartTime': '2023-09-20T00:00:00.000Z',
                'dataEndTime': '2023-09-21T00:00:00.000Z',
                'marketplaceIds': get_asp_market_place_ids()
            }

            report = AmazonReportEU(credentials=credentials)

            response = report.create_report(payload=payload)
            AzReport.add(account_id=account_id, seller_partner_id=asp_id,
                         type=ASpReportType.FBA_CUSTOMER_SHIPMENT_SALES_REPORT.value, reference_id=response['reportId'])

            return response

        except Exception as exception_error:
            """Exception while getting Customer shipment sales data"""
            logger.error(
                f'GET -> Customer shipment sales data creation Failed: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    def verify_customer_shipment_sales(user_object, account_object):
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
                f'Exception occured while verifying Customer shipment sales data: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

    @staticmethod
    @api_time_logger
    @token_required
    def get_customer_shipment_sales(user_object, account_object):
        """Retrieve Customer shipment sales data"""
        try:

            report_id = request.args.get('report_id')
            account_id = account_object.uuid
            asp_id = account_object.asp_id
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
                    'Exception occured while retrieving Customer shipment sales data')
                return send_json_response(
                    http_status=500,
                    response_status=False,
                    message_key=ResponseMessageKeys.FAILED.value
                )

            # Create a streaming pandas DataFrame
            df_stream = pd.read_csv(io.StringIO(
                content), delimiter='\t', header=None, skiprows=1, iterator=True, chunksize=1000)
            data_frame = pd.concat(df_stream, ignore_index=True)

            # data_frame.to_csv(config_data.get('UPLOAD_FOLDER') + ASpReportType.FBA_CUSTOMER_SHIPMENT_SALES_REPORT.value.lower()
            #                     + '/{}.csv'.format(document_id), index=False)

            data_frame.columns = [
                'shipment_date',
                'sku',
                'fnsku',
                'asin',
                'fulfillment_center_id',
                'quantity',
                'amazon_order_id',
                'currency',
                'item_price_per_unit',
                'shipping_price',
                'gift_wrap_price',
                'ship_city',
                'ship_state',
                'ship_postal_code'
            ]

            fba_customer_shipment_df = data_frame.fillna(np.nan).replace([np.nan], [None])   # type: ignore  # noqa: FKA100
            for row in fba_customer_shipment_df.itertuples(index=False):
                AzFbaCustomerShipmentSales.add_or_update(
                    account_id=account_id,
                    asp_id=asp_id,
                    shipment_date=row.shipment_date,
                    sku=row.sku,
                    fnsku=row.fnsku,
                    asin=row.asin,
                    fulfillment_center_id=row.fulfillment_center_id,
                    quantity=row.quantity,
                    amazon_order_id=row.amazon_order_id,
                    currency=row.currency,
                    item_price_per_unit=row.item_price_per_unit,
                    shipping_price=row.shipping_price,
                    gift_wrap_price=row.gift_wrap_price,
                    ship_city=row.ship_city,
                    ship_state=row.ship_state,
                    ship_postal_code=row.ship_postal_code
                )

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=retrive_report, error=None)

        except Exception as exception_error:
            """ Exception in retrieving Customer shipment sales data """
            logger.error(
                f'GET -> Failed retrieving Customer shipment sales data: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )
