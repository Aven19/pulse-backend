'''sales and traffic report view'''
import gzip
import json

from app import logger
from app.helpers.constants import EntityType
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import QueueName
from app.helpers.constants import ResponseMessageKeys
from app.helpers.decorators import api_time_logger
from app.helpers.decorators import brand_filter
from app.helpers.decorators import token_required
from app.helpers.queue_helper import add_queue_task_and_enqueue
from app.helpers.utility import enum_validator
from app.helpers.utility import field_type_validator
from app.helpers.utility import flatten_json
from app.helpers.utility import get_date_from_string
from app.helpers.utility import required_validator
from app.helpers.utility import send_json_response
from app.models.az_report import AzReport
from app.models.az_sales_traffic_asin import AzSalesTrafficAsin
from app.models.az_sales_traffic_summary import AzSalesTrafficSummary
from flask import request
import pandas as pd
from providers.amazon_sp_client import AmazonReportEU
import requests


class SalesTrafficReport:
    """views for asp sales and traffic report apis"""

    @staticmethod
    @api_time_logger
    @token_required
    def create_sales_traffic_report(user_object, account_object):
        """create sales and traffic view using sp-apis"""

        try:

            from_date = request.args.get('from_date')
            to_date = request.args.get('to_date')

            field_types = {'from_date': str,
                           'to_date': str}

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

            data = {}

            data.update({'start_datetime': from_date, 'end_datetime': to_date})

            # queuing ads report
            add_queue_task_and_enqueue(queue_name=QueueName.SALES_TRAFFIC_REPORT, account_id=account_object.uuid,
                                       logged_in_user=user_object.id, entity_type=EntityType.SALES_TRAFFIC_REPORT.value, data=data)

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=None, error=None)

        except Exception as exception_error:
            logger.error(
                f'Exception occured while creating sales and traffic report: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

    @staticmethod
    @api_time_logger
    @token_required
    def verify_sales_traffic_report(user_object, account_object):
        """verify processing status of document based on reportId"""

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
                f'Exception occured while verifying sales and traffic report: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

    @staticmethod
    @api_time_logger
    @token_required
    def get_sales_traffic_report(user_object, account_object):
        """retrieve sales and traffic report using sp-apis"""

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
            payload_date = get_report_document_id.request_start_time
            # creating AmazonReportEU object and passing the credentials
            report = AmazonReportEU(credentials=credentials)
            # using retrieve_report function of report object to get report
            get_report = report.retrieve_report(report_document_id)

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
            # with open(config_data.get('UPLOAD_FOLDER') + ASpReportType.SALES_TRAFFIC_REPORT.value.lower()   # type: ignore  # noqa: FKA100
            #           + '/{}.txt'.format(report_document_id), 'w') as f:
            #     f.write(content)
            # f.close()

            json_dict = json.loads(content)

            # # transforming data before db insertion

            date_granularity = json_dict['reportSpecification']['reportOptions']['dateGranularity']
            asin_granularity = json_dict['reportSpecification']['reportOptions']['asinGranularity']

            # sales and traffic summary (by date)
            sales_traffic_summary = json_dict.get('salesAndTrafficByDate')
            for item in sales_traffic_summary:
                item_flat = flatten_json(item)
                date = get_date_from_string(
                    date_string=item_flat.get('date'))
                ordered_product_sales_amount = item_flat.get(
                    'salesByDate_orderedProductSales_amount')
                ordered_product_sales_currency_code = item_flat.get(
                    'salesByDate_orderedProductSales_currencyCode')
                ordered_product_sales_amount_b2b = item_flat.get(
                    'salesByDate_orderedProductSalesB2B_amount')
                ordered_product_sales_currency_code_b2b = item_flat.get(
                    'salesByDate_orderedProductSalesB2B_currencyCode')
                units_ordered = item_flat.get('salesByDate_unitsOrdered')
                units_ordered_b2b = item_flat.get(
                    'salesByDate_unitsOrderedB2B')
                total_order_items = item_flat.get(
                    'salesByDate_totalOrderItems')
                total_order_items_b2b = item_flat.get(
                    'salesByDate_totalOrderItemsB2B')
                average_sales_per_order_item_amount = item_flat.get(
                    'salesByDate_averageSalesPerOrderItem_amount')
                average_sales_per_order_item_currency_code = item_flat.get(
                    'salesByDate_averageSalesPerOrderItem_currencyCode')
                average_sales_per_order_item_amount_b2b = item_flat.get(
                    'salesByDate_averageSalesPerOrderItemB2B_amount')
                average_sales_per_order_item_currency_code_b2b = item_flat.get(
                    'salesByDate_averageSalesPerOrderItemB2B_currencyCode')
                average_units_per_order_item = item_flat.get(
                    'salesByDate_averageUnitsPerOrderItem')
                average_units_per_order_item_b2b = item_flat.get(
                    'salesByDate_averageUnitsPerOrderItemB2B')
                average_selling_price_amount = item_flat.get(
                    'salesByDate_averageSellingPrice_amount')
                average_selling_price_currency_code = item_flat.get(
                    'salesByDate_averageSellingPrice_currencyCode')
                average_selling_price_amount_b2b = item_flat.get(
                    'salesByDate_averageSellingPriceB2B_amount')
                average_selling_price_currency_code_b2b = item_flat.get(
                    'salesByDate_averageSellingPriceB2B_currencyCode')
                units_refunded = item_flat.get('salesByDate_unitsRefunded')
                refund_rate = item_flat.get('salesByDate_refundRate')
                claims_granted = item_flat.get('salesByDate_claimsGranted')
                claims_amount_amount = item_flat.get(
                    'salesByDate_claimsAmount_amount')
                claims_amount_currency_code = item_flat.get(
                    'salesByDate_claimsAmount_currencyCode')
                shipped_product_sales_amount = item_flat.get(
                    'salesByDate_shippedProductSales_amount')
                shipped_product_sales_currency_code = item_flat.get(
                    'salesByDate_shippedProductSales_currencyCode')
                units_shipped = item_flat.get('salesByDate_unitsShipped')
                orders_shipped = item_flat.get('salesByDate_ordersShipped')
                browser_page_views = item_flat.get(
                    'trafficByDate_browserPageViews')
                browser_page_views_b2b = item_flat.get(
                    'trafficByDate_browserPageViewsB2B')
                mobile_app_page_views = item_flat.get(
                    'trafficByDate_mobileAppPageViews')
                mobile_app_page_views_b2b = item_flat.get(
                    'trafficByDate_mobileAppPageViewsB2B')
                page_views = item_flat.get('trafficByDate_pageViews')
                page_views_b2b = item_flat.get(
                    'trafficByDate_pageViewsB2B')
                browser_sessions = item_flat.get(
                    'trafficByDate_browserSessions')
                browser_sessions_b2b = item_flat.get(
                    'trafficByDate_browserSessionsB2B')
                mobile_app_sessions = item_flat.get(
                    'trafficByDate_mobileAppSessions')
                mobile_app_sessions_b2b = item_flat.get(
                    'trafficByDate_mobileAppSessionsB2B')
                sessions = item_flat.get('trafficByDate_sessions')
                sessions_b2b = item_flat.get('trafficByDate_sessionsB2B')
                buy_box_percentage = item_flat.get(
                    'trafficByDate_buyBoxPercentage')
                buy_box_percentage_b2b = item_flat.get(
                    'trafficByDate_buyBoxPercentageB2B')
                order_item_session_percentage = item_flat.get(
                    'trafficByDate_orderItemSessionPercentage')
                order_item_session_percentage_b2b = item_flat.get(
                    'trafficByDate_orderItemSessionPercentageB2B')
                unit_session_percentage = item_flat.get(
                    'trafficByDate_unitSessionPercentage')
                unit_session_percentage_b2b = item_flat.get(
                    'trafficByDate_unitSessionPercentageB2B')
                average_offer_count = item_flat.get(
                    'trafficByDate_averageOfferCount')
                average_parent_items = item_flat.get(
                    'trafficByDate_averageParentItems')
                feedback_received = item_flat.get(
                    'trafficByDate_feedbackReceived')
                negative_feedback_received = item_flat.get(
                    'trafficByDate_negativeFeedbackReceived')
                received_negative_feedback_rate = item_flat.get(
                    'trafficByDate_receivedNegativeFeedbackRate')
                date_granularity = date_granularity

                AzSalesTrafficSummary.insert_or_update(account_id=account_id, asp_id=asp_id, date=date, ordered_product_sales_amount=ordered_product_sales_amount,
                                                       ordered_product_sales_currency_code=ordered_product_sales_currency_code, units_ordered=units_ordered,
                                                       ordered_product_sales_amount_b2b=ordered_product_sales_amount_b2b, ordered_product_sales_currency_code_b2b=ordered_product_sales_currency_code_b2b,
                                                       units_ordered_b2b=units_ordered_b2b, total_order_items=total_order_items, total_order_items_b2b=total_order_items_b2b,
                                                       average_sales_per_order_item_amount=average_sales_per_order_item_amount, average_sales_per_order_item_currency_code=average_sales_per_order_item_currency_code,
                                                       average_sales_per_order_item_amount_b2b=average_sales_per_order_item_amount_b2b, average_sales_per_order_item_currency_code_b2b=average_sales_per_order_item_currency_code_b2b,
                                                       average_units_per_order_item=average_units_per_order_item, average_units_per_order_item_b2b=average_units_per_order_item_b2b,
                                                       average_selling_price_amount=average_selling_price_amount, average_selling_price_currency_code=average_selling_price_currency_code,
                                                       average_selling_price_amount_b2b=average_selling_price_amount_b2b, average_selling_price_currency_code_b2b=average_selling_price_currency_code_b2b,
                                                       units_refunded=units_refunded, refund_rate=refund_rate, claims_granted=claims_granted, claims_amount_amount=claims_amount_amount,
                                                       claims_amount_currency_code=claims_amount_currency_code, shipped_product_sales_amount=shipped_product_sales_amount,
                                                       shipped_product_sales_currency_code=shipped_product_sales_currency_code, units_shipped=units_shipped, orders_shipped=orders_shipped,
                                                       browser_page_views=browser_page_views, browser_page_views_b2b=browser_page_views_b2b, mobile_app_page_views=mobile_app_page_views,
                                                       mobile_app_page_views_b2b=mobile_app_page_views_b2b, page_views=page_views, page_views_b2b=page_views_b2b,
                                                       browser_sessions=browser_sessions, browser_sessions_b2b=browser_sessions_b2b, mobile_app_sessions=mobile_app_sessions,
                                                       mobile_app_sessions_b2b=mobile_app_sessions_b2b, sessions=sessions, sessions_b2b=sessions_b2b,
                                                       buy_box_percentage=buy_box_percentage, buy_box_percentage_b2b=buy_box_percentage_b2b, order_item_session_percentage=order_item_session_percentage,
                                                       order_item_session_percentage_b2b=order_item_session_percentage_b2b, unit_session_percentage=unit_session_percentage,
                                                       unit_session_percentage_b2b=unit_session_percentage_b2b, average_offer_count=average_offer_count, average_parent_items=average_parent_items,
                                                       feedback_received=feedback_received, negative_feedback_received=negative_feedback_received,
                                                       received_negative_feedback_rate=received_negative_feedback_rate, date_granularity=date_granularity)
            # sales and traffic by asin
            sales_traffic_asin = json_dict.get('salesAndTrafficByAsin')
            for item in sales_traffic_asin:
                item_flat = flatten_json(item)
                parent_asin = item_flat.get('parentAsin')
                child_asin = item_flat.get('childAsin')
                units_ordered = item_flat.get('salesByAsin_unitsOrdered')
                units_ordered_b2b = item_flat.get(
                    'salesByAsin_unitsOrderedB2B')
                ordered_product_sales_amount = item_flat.get(
                    'salesByAsin_orderedProductSales_amount')
                ordered_product_sales_amount_b2b = item_flat.get(
                    'salesByAsin_orderedProductSalesB2B_amount')
                ordered_product_sales_currency_code = item_flat.get(
                    'salesByAsin_orderedProductSales_currencyCode')
                ordered_product_sales_currency_code_b2b = item_flat.get(
                    'salesByAsin_orderedProductSalesB2B_currencyCode')
                total_order_items = item_flat.get(
                    'salesByAsin_totalOrderItems')
                total_order_items_b2b = item_flat.get(
                    'salesByAsin_totalOrderItemsB2B')
                browser_sessions = item_flat.get(
                    'trafficByAsin_browserSessions')
                browser_sessions_b2b = item_flat.get(
                    'trafficByAsin_browserSessionsB2B')
                mobile_app_sessions = item_flat.get(
                    'trafficByAsin_mobileAppSessions')
                mobile_app_sessions_b2b = item_flat.get(
                    'trafficByAsin_mobileAppSessionsB2B')
                sessions = item_flat.get(
                    'trafficByAsin_sessions')
                sessions_b2b = item_flat.get(
                    'trafficByAsin_sessionsB2B')
                browser_session_percentage = item_flat.get(
                    'trafficByAsin_browserSessionPercentage')
                browser_session_percentage_b2b = item_flat.get(
                    'trafficByAsin_browserSessionPercentageB2B')
                mobile_app_session_percentage = item_flat.get(
                    'trafficByAsin_mobileAppSessionPercentage')
                mobile_app_session_percentage_b2b = item_flat.get(
                    'trafficByAsin_mobileAppSessionPercentageB2B')
                session_percentage = item_flat.get(
                    'trafficByAsin_sessionPercentage')
                session_percentage_b2b = item_flat.get(
                    'trafficByAsin_sessionPercentageB2B')
                browser_page_views = item_flat.get(
                    'trafficByAsin_browserPageViews')
                browser_page_views_b2b = item_flat.get(
                    'trafficByAsin_browserPageViewsB2B')
                mobile_app_page_views = item_flat.get(
                    'trafficByAsin_mobileAppPageViews')
                mobile_app_page_views_b2b = item_flat.get(
                    'trafficByAsin_mobileAppPageViewsB2B')
                page_views = item_flat.get('trafficByAsin_pageViews')
                page_views_b2b = item_flat.get(
                    'trafficByAsin_pageViewsB2B')
                browser_page_views_percentage = item_flat.get(
                    'trafficByAsin_browserPageViewsPercentage')
                browser_page_views_percentage_b2b = item_flat.get(
                    'trafficByAsin_browserPageViewsPercentageB2B')
                mobile_app_page_views_percentage = item_flat.get(
                    'trafficByAsin_mobileAppPageViewsPercentage')
                mobile_app_page_views_percentage_b2b = item_flat.get(
                    'trafficByAsin_mobileAppPageViewsPercentageB2B')
                page_views_percentage = item_flat.get(
                    'trafficByAsin_pageViewsPercentage')
                page_views_percentage_b2b = item_flat.get(
                    'trafficByAsin_pageViewsPercentageB2B')
                buy_box_percentage = item_flat.get(
                    'trafficByAsin_buyBoxPercentage')
                buy_box_percentage_b2b = item_flat.get(
                    'trafficByAsin_buyBoxPercentageB2B')
                unit_session_percentage = item_flat.get(
                    'trafficByAsin_unitSessionPercentage')
                unit_session_percentage_b2b = item_flat.get(
                    'trafficByAsin_unitSessionPercentageB2B')
                asin_granularity = asin_granularity

                AzSalesTrafficAsin.insert_or_update(account_id=account_id, asp_id=asp_id, parent_asin=parent_asin, child_asin=child_asin,
                                                    payload_date=payload_date, units_ordered=units_ordered, units_ordered_b2b=units_ordered_b2b,
                                                    ordered_product_sales_amount=ordered_product_sales_amount, ordered_product_sales_amount_b2b=ordered_product_sales_amount_b2b,
                                                    ordered_product_sales_currency_code=ordered_product_sales_currency_code,
                                                    ordered_product_sales_currency_code_b2b=ordered_product_sales_currency_code_b2b,
                                                    total_order_items=total_order_items, total_order_items_b2b=total_order_items_b2b,
                                                    browser_sessions=browser_sessions, browser_sessions_b2b=browser_sessions_b2b,
                                                    mobile_app_sessions=mobile_app_sessions, mobile_app_sessions_b2b=mobile_app_sessions_b2b,
                                                    sessions=sessions, sessions_b2b=sessions_b2b,
                                                    browser_session_percentage=browser_session_percentage,
                                                    browser_session_percentage_b2b=browser_session_percentage_b2b,
                                                    mobile_app_session_percentage=mobile_app_session_percentage,
                                                    mobile_app_session_percentage_b2b=mobile_app_session_percentage_b2b,
                                                    session_percentage=session_percentage, session_percentage_b2b=session_percentage_b2b,
                                                    browser_page_views=browser_page_views, browser_page_views_b2b=browser_page_views_b2b,
                                                    mobile_app_page_views=mobile_app_page_views, mobile_app_page_views_b2b=mobile_app_page_views_b2b,
                                                    page_views=page_views, page_views_b2b=page_views_b2b,
                                                    browser_page_views_percentage=browser_page_views_percentage,
                                                    browser_page_views_percentage_b2b=browser_page_views_percentage_b2b,
                                                    mobile_app_page_views_percentage=mobile_app_page_views_percentage,
                                                    mobile_app_page_views_percentage_b2b=mobile_app_page_views_percentage_b2b,
                                                    page_views_percentage=page_views_percentage, page_views_percentage_b2b=page_views_percentage_b2b,
                                                    buy_box_percentage=buy_box_percentage, buy_box_percentage_b2b=buy_box_percentage_b2b,
                                                    unit_session_percentage=unit_session_percentage, unit_session_percentage_b2b=unit_session_percentage_b2b,
                                                    asin_granularity=asin_granularity)

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=None, error=None)

        except Exception as exception_error:
            logger.error(
                f'Exception occured during retrieving sales and traffic report: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

    @api_time_logger
    @token_required
    def get_sales_and_traffic_worker(user_object, account_object):
        """create sales and traffic report """
        try:
            data = {}

            data.update({'default_sync': True})

            # queuing ads report
            add_queue_task_and_enqueue(queue_name=QueueName.SALES_TRAFFIC_REPORT, account_id=account_object.uuid,
                                       logged_in_user=user_object.id, entity_type=EntityType.SALES_TRAFFIC_REPORT.value, data=data)

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=None, error=None)

        except Exception as exception_error:
            logger.error(
                f'Exception occured while creating sponsored ads report: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def get_glance_summary(user_object, account_object, allowed_brands):
        """Get Glance summary details by date filter"""

        try:

            account_id = account_object.uuid
            asp_id = account_object.asp_id

            from_date = request.args.get('from_date')
            to_date = request.args.get('to_date')
            marketplace = request.args.get('marketplace')
            sort_order = request.args.get(key='sort_order', default=None)
            sort_by = request.args.get(key='sort_by', default=None)
            category = request.args.getlist('category')   # type: ignore  # noqa: FKA100
            brand = request.args.getlist('brand') if 'brand' in request.args and request.args.getlist(
                'brand') else allowed_brands
            product = request.args.getlist('product')  # type: ignore  # noqa: FKA100

            # validation
            params = {}
            if marketplace:
                params['marketplace'] = marketplace
            if from_date:
                params['from_date'] = from_date
            if to_date:
                params['to_date'] = to_date
            if category:
                params['category'] = category
            if not brand:
                params['brand'] = brand
            else:
                if account_object.primary_user_id != user_object.id:
                    valid_brands = [b for b in brand if b in allowed_brands]
                    if len(valid_brands) != len(brand):
                        return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                                  message_key=ResponseMessageKeys.INVALID_BRAND.value, data=None,
                                                  error=ResponseMessageKeys.ENTER_CORRECT_INPUT.value)
                    params['brand'] = valid_brands
            if product:
                params['product'] = product
            if sort_order:
                params['sort_order'] = sort_order
            if sort_by:
                params['sort_by'] = sort_by

            field_types = {'marketplace': str, 'from_date': str, 'to_date': str,
                           'category': list, 'brand': list, 'product': list,
                           'sort_order': str, 'sort_by': str}

            required_fields = ['marketplace']

            enum_fields = {
                'marketplace': (marketplace, 'ProductMarketplace')
            }

            valid_enum = enum_validator(enum_fields)

            if valid_enum['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=valid_enum['data'])

            data = field_type_validator(
                request_data=params, field_types=field_types)

            if data['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=data['data'])

            is_valid = required_validator(
                request_data=params, required_fields=required_fields)

            if is_valid['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=is_valid['data'])

            glance_summary, glance_summarytotal_count, glance_summarytotal_request_count = AzSalesTrafficAsin.get_glance_summary(account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date,
                                                                                                                                 category=tuple(category), brand=tuple(brand), product=tuple(product))

            result = []

            if glance_summary:
                for glance_view in glance_summary:
                    item_data = {
                        'month': glance_view.month,
                        'total_sales': float(glance_view.total_sales) if glance_view.total_sales is not None else 0.0,
                        'asp': float(glance_view.asp) if glance_view.asp is not None else 0.0,
                        'units': int(glance_view.units_ordered) if glance_view.units_ordered is not None else 0,
                        'conversion': float(glance_view.conversion) if glance_view.conversion is not None else 0.0,
                        'total_gvs': int(glance_view.total_gv) if glance_view.total_gv is not None else 0
                    }
                    result.append(item_data)

                data = {
                    'result': result
                }

                return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=data, error=None)

            return send_json_response(
                http_status=404,
                response_status=False,
                message_key=ResponseMessageKeys.NO_DATA_FOUND.value,
                error=None
            )

        except Exception as exception_error:
            logger.error(
                f'Exception occured while Getting Glance View Detail page: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )
