"""Contains Amazon Seller Orders Report related API definitions."""
import gzip
import io

from app import config_data
from app import db
from app import logger
from app.helpers.constants import ASpReportType
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import ResponseMessageKeys
from app.helpers.decorators import api_time_logger
from app.helpers.decorators import asp_credentials_required
from app.helpers.decorators import token_required
from app.helpers.utility import field_type_validator
from app.helpers.utility import get_asp_data_start_time
from app.helpers.utility import get_asp_market_place_ids
from app.helpers.utility import get_created_since
from app.helpers.utility import get_created_until
from app.helpers.utility import required_validator
from app.helpers.utility import send_json_response
from app.models.az_order_report import AzOrderReport
from app.models.az_report import AzReport
from flask import request
import numpy as np
import pandas as pd
from providers.amazon_sp_client import AmazonReportEU
import requests


class AspOrderReportView:
    """Class which contains methods related to Order reports"""
    @staticmethod
    @api_time_logger
    @token_required
    def create_order_report(user_object, account_object):
        """Create Order Report"""

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

            credentials = account_object.retrieve_asp_credentials(account_object)[
                0]

            data_start_time = get_created_since(request.args.get('from_date'))
            data_end_time = get_created_until(request.args.get('to_date'))

            payload = {
                'reportType': ASpReportType.ORDER_REPORT_ORDER_DATE_GENERAL.value,
                'dataStartTime': data_start_time,
                'dataEndTime': data_end_time,
                'marketplaceIds': get_asp_market_place_ids()
            }

            report = AmazonReportEU(credentials=credentials)

            response = report.create_report(payload=payload)
            AzReport.add(account_id=account_object.uuid, seller_partner_id=account_object.asp_id,
                         type=ASpReportType.ORDER_REPORT_ORDER_DATE_GENERAL.value,
                         request_start_time=data_start_time,
                         request_end_time=data_end_time,
                         reference_id=response['reportId'])

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response['reportId'], error=None)

        except Exception as exception_error:
            """ Exception in creating Order Report """
            logger.error(
                f'GET -> Order Report creation Failed: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    # @asp_credentials_required
    def verify_order_report(user_object, account_object):
        """Verify Order Report Id"""
        try:

            account_id = account_object.uuid

            reference_id = request.args.get('report_id')
            get_report = AzReport.get_by_ref_id(
                account_id=account_id, reference_id=reference_id)

            if not get_report:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=reference_id)

            asp_cred = account_object.asp_credentials
            credentials = {
                'seller_partner_id': asp_cred.get('seller_partner_id'),
                'refresh_token': asp_cred.get('refresh_token'),
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
            """ Exception in verifying Order Report Id """
            logger.error(
                f'GET -> Order Report Id verification failed: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    def get_order_report(user_object, account_object):
        """Retrive Order Report"""
        try:

            asp_id = account_object.asp_id
            account_id = account_object.uuid

            reference_id = request.args.get('report_id')
            credentials = account_object.retrieve_asp_credentials(account_object)[
                0]

            get_report_document_id = AzReport.get_by_ref_id(account_id=account_id,
                                                            reference_id=reference_id)

            if not get_report_document_id:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=reference_id)

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

            # Create a streaming pandas DataFrame
            df_stream = pd.read_csv(io.StringIO(
                content), delimiter='\t', header=None, skiprows=1, iterator=True, chunksize=1000)
            order_df = pd.concat(df_stream, ignore_index=True)

            # Rename the columns
            order_df.columns = ['amazon_order_id', 'merchant_order_id', 'purchase_date', 'last_updated_date', 'order_status', 'fulfillment_channel', 'sales_channel', 'order_channel', 'ship_service_level', 'product_name', 'sku', 'asin', 'item_status', 'quantity', 'currency', 'item_price', 'item_tax',
                                'shipping_price', 'shipping_tax', 'gift_wrap_price', 'gift_wrap_tax', 'item_promotion_discount', 'ship_promotion_discount', 'ship_city', 'ship_state', 'ship_postal_code', 'ship_country', 'promotion_ids', 'is_business_order', 'purchase_order_number', 'price_designation', 'fulfilled_by', 'is_iba']

            desired_columns = ['amazon_order_id', 'merchant_order_id', 'purchase_date', 'last_updated_date', 'order_status', 'fulfillment_channel', 'sales_channel',
                               'ship_service_level', 'product_name', 'sku', 'asin', 'item_status', 'quantity', 'currency', 'item_price', 'item_tax',
                               'shipping_price', 'shipping_tax', 'gift_wrap_price', 'gift_wrap_tax', 'item_promotion_discount', 'ship_promotion_discount', 'ship_city', 'ship_state', 'ship_postal_code', 'ship_country']

            # Drop columns not in the desired columns list
            order_df_selected_columns = order_df[desired_columns]

            # transformation
            order_df_selected_columns = order_df_selected_columns.fillna(np.nan).replace([np.nan], [None])   # type: ignore  # noqa: FKA100

            for row in order_df_selected_columns.iloc[1:].itertuples(index=False):
                order = AzOrderReport.get_by_az_order_id_and_sku(
                    selling_partner_id=asp_id, amazon_order_id=row.amazon_order_id, sku=row.sku)

                if order:

                    if order.order_status == row.order_status:
                        quantity = order.quantity + row.quantity
                        order.quantity = quantity
                        db.session.commit()
                    else:
                        AzOrderReport.update_orders(
                            account_id=account_id,
                            selling_partner_id=asp_id,
                            amazon_order_id=row.amazon_order_id,
                            merchant_order_id=row.merchant_order_id, purchase_date=row.purchase_date,
                            last_updated_date=row.last_updated_date, order_status=row.order_status, fulfillment_channel=row.fulfillment_channel, sales_channel=row.sales_channel, ship_service_level=row.ship_service_level,
                            product_name=row.product_name, sku=row.sku, asin=row.asin, item_status=row.item_status, quantity=row.quantity, currency=row.currency,
                            item_price=row.item_price, item_tax=row.item_tax, shipping_price=row.shipping_price, shipping_tax=row.shipping_tax,
                            gift_wrap_price=row.gift_wrap_price, gift_wrap_tax=row.gift_wrap_tax, item_promotion_discount=row.item_promotion_discount,
                            ship_promotion_discount=row.ship_promotion_discount, ship_city=row.ship_city, ship_state=row.ship_state, ship_postal_code=row.ship_postal_code,
                            ship_country=row.ship_country)
                else:
                    AzOrderReport.add(account_id=account_id,
                                      selling_partner_id=asp_id, amazon_order_id=row.amazon_order_id, merchant_order_id=row.merchant_order_id, purchase_date=row.purchase_date, last_updated_date=row.last_updated_date,
                                      order_status=row.order_status, fulfillment_channel=row.fulfillment_channel, sales_channel=row.sales_channel, ship_service_level=row.ship_service_level,
                                      product_name=row.product_name, sku=row.sku, asin=row.asin, item_status=row.item_status, quantity=row.quantity, currency=row.currency,
                                      item_price=row.item_price, item_tax=row.item_tax, shipping_price=row.shipping_price, shipping_tax=row.shipping_tax,
                                      gift_wrap_price=row.gift_wrap_price, gift_wrap_tax=row.gift_wrap_tax, item_promotion_discount=row.item_promotion_discount,
                                      ship_promotion_discount=row.ship_promotion_discount, ship_city=row.ship_city, ship_state=row.ship_state, ship_postal_code=row.ship_postal_code,
                                      ship_country=row.ship_country)

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=retrive_report, error=None)

        except Exception as exception_error:
            """ Exception in retriving Order Report """
            logger.error(
                f'GET -> Failed retriving Order Report: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @asp_credentials_required
    def create_order_report_data_invoicing(credentials):
        """Create Order Report"""

        try:

            payload = {
                'reportType': ASpReportType.ORDER_REPORT_DATA_INVOICING.value,
                'dataStartTime': get_asp_data_start_time(),
                'marketplaceIds': get_asp_market_place_ids()
            }

            report = AmazonReportEU(credentials=credentials)

            response = report.create_report(payload=payload)

            AzReport.add(seller_partner_id=credentials.get('seller_partner_id'),
                         type=ASpReportType.ORDER_REPORT_DATA_INVOICING.value, reference_id=response['reportId'])

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response['reportId'], error=None)

        except Exception as exception_error:
            """ Exception in creating Order Report """
            logger.error(
                f'GET -> Order Report creation Failed: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @asp_credentials_required
    def create_order_report_data_invoicing_flat_file(credentials):
        """Create Order Report"""

        try:

            payload = {
                'reportType': ASpReportType.FLAT_FILE_ORDER_REPORT_DATA_INVOICING.value,
                'dataStartTime': get_asp_data_start_time(),
                'marketplaceIds': get_asp_market_place_ids()
            }

            report = AmazonReportEU(credentials=credentials)

            response = report.create_report(payload=payload)

            AzReport.add(seller_partner_id=credentials.get('seller_partner_id'),
                         type=ASpReportType.FLAT_FILE_ORDER_REPORT_DATA_INVOICING.value, reference_id=response['reportId'])

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response['reportId'], error=None)

        except Exception as exception_error:
            """ Exception in creating Order Report """
            logger.error(
                f'GET -> Order Report creation Failed: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )
