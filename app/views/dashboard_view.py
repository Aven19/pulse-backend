"""endpoint for dashboard"""

from datetime import date
from datetime import datetime
from datetime import timedelta
import json
from typing import Optional

from app import db
from app import logger
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import PAGE_DEFAULT
from app.helpers.constants import PAGE_LIMIT
from app.helpers.constants import PRODUCT_RANK_LIMIT
from app.helpers.constants import ResponseMessageKeys
from app.helpers.constants import SortingOrder
from app.helpers.decorators import api_time_logger
from app.helpers.decorators import brand_filter
from app.helpers.decorators import token_required
from app.helpers.utility import calculate_percentage_growth
from app.helpers.utility import convert_to_numeric
from app.helpers.utility import enum_validator
from app.helpers.utility import field_type_validator
from app.helpers.utility import format_iso_to_12_hour_format
from app.helpers.utility import get_pagination_meta
from app.helpers.utility import get_previous_year_to_from_date
from app.helpers.utility import get_prior_to_from_date
from app.helpers.utility import required_validator
from app.helpers.utility import send_json_response
from app.models.az_item_master import AzItemMaster
from app.models.az_ledger_summary import AzLedgerSummary
from app.models.az_order_report import AzOrderReport
from app.models.az_sales_traffic_summary import AzSalesTrafficSummary
from app.views.sales_api_view import AZSalesView
from flask import request
from sqlalchemy import text
from werkzeug.exceptions import BadRequest


class DashboardView:
    """class for dashboard views"""

    # function to update cogs in item master
    @api_time_logger
    @token_required
    def update_item_master_cogs(user_object, account_object):
        """endpoint for updating cog of one item"""
        try:

            data = request.get_json(force=True)
            field_types = {
                'sku': str, 'cogs': float}
            required_fields = ['sku', 'cogs']

            post_data = field_type_validator(
                request_data=data, field_types=field_types)
            if post_data['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=post_data['data'])

            is_valid = required_validator(
                request_data=data, required_fields=required_fields)
            if is_valid['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=is_valid['data'])

            selling_partner_id = account_object.asp_id
            account_id = account_object.uuid
            sku = data['sku']
            cogs = data['cogs']

            item = AzItemMaster.get_item_by_sku(account_id=account_id,
                                                sku=sku, selling_partner_id=selling_partner_id)

            if item:
                AzItemMaster.update_cogs(account_id=account_id,
                                         sku=sku, selling_partner_id=selling_partner_id, cogs=cogs)
            else:
                return send_json_response(
                    http_status=404,
                    response_status=False,
                    message_key=ResponseMessageKeys.SKU_DOES_NOT_EXIST.value,
                    error=None
                )

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.UPDATED.value, data=None, error=None)

        except BadRequest as exception_error:
            logger.error(f'POST -> Update Item Cogs Failed: {exception_error}')
            return send_json_response(
                http_status=HttpStatusCode.BAD_REQUEST.value,
                response_status=False,
                message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
            )

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed updating cogs for an item: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    # function to update brand in item master
    @api_time_logger
    @token_required
    def update_item_master_brand(user_object, account_object):
        """endpoint for updating brand of one item"""
        try:

            asp_cred = account_object.asp_credentials

            data = request.get_json(force=True)
            field_types = {'sku': str, 'brand': str}
            required_fields = ['sku', 'brand']

            post_data = field_type_validator(
                request_data=data, field_types=field_types)
            if post_data['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=post_data['data'])

            is_valid = required_validator(
                request_data=data, required_fields=required_fields)
            if is_valid['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=is_valid['data'])

            selling_partner_id = asp_cred.get('seller_partner_id')
            account_id = account_object.uuid
            sku = data['sku']
            brand = data['brand']

            item = AzItemMaster.get_item_by_sku(account_id=account_id,
                                                sku=sku, selling_partner_id=selling_partner_id)

            if item:
                AzItemMaster.update_brand(account_id=account_id,
                                          sku=sku, selling_partner_id=selling_partner_id, brand=brand)
            else:
                return send_json_response(
                    http_status=404,
                    response_status=False,
                    message_key=ResponseMessageKeys.SKU_DOES_NOT_EXIST.value,
                    error=None
                )

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.UPDATED.value, data=None, error=None)

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed updating brand for an item: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    # function to get inventory by location
    @api_time_logger
    @token_required
    def get_inventory_by_location(user_object, account_object):
        """endpoint for getting ledger inventory by location"""
        try:

            asp_id = account_object.asp_id
            account_id = account_object.uuid

            inventory_by_location = AzLedgerSummary.get_inventory_by_location(account_id=account_id,
                                                                              asp_id=asp_id)

            response = {}

            if inventory_by_location:
                for inventory in inventory_by_location:
                    response[inventory[0]] = inventory[1]

                return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response, error=None)

            else:
                return send_json_response(
                    http_status=200,
                    response_status=True,
                    message_key=ResponseMessageKeys.NO_DATA_FOUND.value,
                    error=None
                )

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed getting inventory by location: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    # Dasboard endpoint - sales statistics

    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def get_sales_statistics(user_object, account_object, allowed_brands):                  # type: ignore  # noqa: C901
        """endpoint for getting various metrics for sales statistics dashboard section"""
        try:

            selling_partner_id = account_object.asp_id
            account_id = account_object.uuid

            from_date = request.args.get(
                'from_date', default=date.today().strftime('%Y-%m-%d'))
            to_date = request.args.get(
                'to_date', default=date.today().strftime('%Y-%m-%d'))

            category = request.args.getlist('category')
            brand = request.args.getlist('brand') if 'brand' in request.args and request.args.getlist(
                'brand') else allowed_brands
            product = request.args.getlist('product')

            """Get live data from order reports"""

            # max_date = None
            # if category or brand or product:
            #     max_date = db.session.query(
            #         func.max(AzSalesTrafficAsin.payload_date)).scalar()
            #     if max_date is not None:
            #         from_date = max_date.strftime('%Y-%m-%d')
            #         to_date = max_date.strftime('%Y-%m-%d')

            if brand:
                if account_object.primary_user_id != user_object.id:
                    valid_brands = [b for b in brand if b in allowed_brands]
                    if len(valid_brands) != len(brand):
                        return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                                  message_key=ResponseMessageKeys.INVALID_BRAND.value, data=None,
                                                  error=ResponseMessageKeys.ENTER_CORRECT_INPUT.value)
                    brand = valid_brands

            # current sales stats
            current_sales_stats = DashboardView.__get_sales_stats_metrics(
                account_id=account_id, selling_partner_id=selling_partner_id, from_date=from_date, to_date=to_date, category=tuple(category), brand=tuple(brand), product=tuple(product)).fetchone()

            prior_from_date, prior_to_date = get_prior_to_from_date(
                from_date=from_date, to_date=to_date)

            # prior sales stats
            prior_sales_stats = DashboardView.__get_sales_stats_metrics(
                account_id=account_id, selling_partner_id=selling_partner_id, from_date=prior_from_date, to_date=prior_to_date, category=tuple(category), brand=tuple(brand), product=tuple(product)).fetchone()

            if current_sales_stats:
                # gross sales
                current_gross_sales = convert_to_numeric(
                    current_sales_stats.gross_sales)
                prior_gross_sales = convert_to_numeric(
                    prior_sales_stats.gross_sales)

                # total orders
                current_total_orders = convert_to_numeric(
                    current_sales_stats.total_orders)
                prior_total_orders = convert_to_numeric(
                    prior_sales_stats.total_orders)

                # AOV
                current_aov = 0 if current_total_orders == 0 else current_gross_sales / \
                    current_total_orders
                prior_aov = 0 if prior_total_orders == 0 else prior_gross_sales / prior_total_orders

                # units sold
                current_units_sold = convert_to_numeric(
                    current_sales_stats.units_sold)
                prior_units_sold = convert_to_numeric(
                    prior_sales_stats.units_sold)

                result_dict = {
                    # 'objects': {
                    #     'from_date': max_date.strftime('%Y-%m-%d') if max_date is not None else from_date
                    # },
                    'result': {
                        'gross_sales': {
                            'current_gross_sales': current_gross_sales,
                            'prior_gross_sales': prior_gross_sales,
                            'gross_sales_percentage_growth': calculate_percentage_growth(current_value=current_gross_sales, prior_value=prior_gross_sales),
                            'gross_sales_difference': current_gross_sales - prior_gross_sales
                        },
                        'total_orders': {
                            'current_total_orders': current_total_orders,
                            'prior_total_orders': prior_total_orders,
                            'total_orders_percentage_growth': calculate_percentage_growth(current_value=current_total_orders, prior_value=prior_total_orders),
                            'total_orders_difference': current_total_orders - prior_total_orders
                        },
                        'aov': {
                            'current_aov': current_aov,
                            'prior_aov': prior_aov,
                            'aov_percentage_growth': calculate_percentage_growth(current_value=current_aov, prior_value=prior_aov),
                            'aov_difference': current_aov - prior_aov
                        },
                        'units_sold': {
                            'current_units_sold': current_units_sold,
                            'prior_units_sold': prior_units_sold,
                            'units_sold_percentage_growth': calculate_percentage_growth(current_value=current_units_sold, prior_value=prior_units_sold),
                            'units_sold_difference': current_units_sold - prior_units_sold
                        }
                    }
                }

                return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=result_dict, error=None)

            return send_json_response(
                http_status=404,
                response_status=True,
                data=None,
                message_key=ResponseMessageKeys.NO_DATA_FOUND.value,
                error=None
            )

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed getting sales statistics for dashboard: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    # Dashboard endpoint - sales stats bar graph

    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def get_sales_statistics_bar_graph(user_object, account_object, allowed_brands):                  # type: ignore  # noqa: C901
        """endpoint for getting various metrics for sales statistics graph for sales and units section"""
        try:

            from_date = request.args.get('from_date', default=None)
            to_date = request.args.get('to_date', default=None)
            marketplace = request.args.get('marketplace')
            selling_partner_id = account_object.asp_id
            account_id = account_object.uuid
            category = request.args.getlist('category')
            brand = request.args.getlist('brand') if 'brand' in request.args and request.args.getlist(
                'brand') else allowed_brands
            product = request.args.getlist('product')

            # validation
            params = {}
            if from_date:
                params['from_date'] = from_date
            if to_date:
                params['to_date'] = to_date
            if marketplace:
                params['marketplace'] = marketplace
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

            field_types = {'from_date': str,
                           'to_date': str, 'marketplace': str}

            required_fields = ['from_date', 'to_date', 'marketplace']

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

            graph_list = []
            sales_statistics_graph = []

            sales_statistics_graph = DashboardView.__get_sales_statistics_bar_graph_metrics(
                account_id=account_id, selling_partner_id=selling_partner_id, from_date=from_date, to_date=to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))

            if sales_statistics_graph:

                for graph in sales_statistics_graph:
                    graph_data = {
                        'date': graph.date.strftime('%Y-%m-%d'),
                        'gross_sales': convert_to_numeric(graph.gross_sales),
                        'units_sold': convert_to_numeric(graph.units_sold)
                    }

                    graph_list.append(graph_data)

                # gross sales and units sold comp
                current_metrics = DashboardView.__get_gross_sales_units_sold_bar_graph(
                    account_id=account_id, selling_partner_id=selling_partner_id, from_date=from_date, to_date=to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))

                prior_from_date, prior_to_date = get_prior_to_from_date(
                    from_date=from_date, to_date=to_date)

                prior_metrics = DashboardView.__get_gross_sales_units_sold_bar_graph(
                    account_id=account_id, selling_partner_id=selling_partner_id, from_date=prior_from_date, to_date=prior_to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))

                previous_year_from_date, previous_year_to_date = get_previous_year_to_from_date(
                    from_date=from_date, to_date=to_date)

                previous_year_metrics = DashboardView.__get_gross_sales_units_sold_bar_graph(
                    account_id=account_id, selling_partner_id=selling_partner_id, from_date=previous_year_from_date, to_date=previous_year_to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))

                # current metrics
                current_gross_sales = convert_to_numeric(
                    current_metrics.gross_sales)
                current_units_sold = convert_to_numeric(
                    current_metrics.units_sold)

                # prior metrics
                prior_gross_sales = convert_to_numeric(
                    prior_metrics.gross_sales)
                prior_units_sold = convert_to_numeric(prior_metrics.units_sold)

                # previous year metrics
                previous_year_gross_sales = convert_to_numeric(
                    previous_year_metrics.gross_sales)
                previous_year_units_sold = convert_to_numeric(
                    previous_year_metrics.units_sold)

                result_dict = {
                    'result': {
                        'graph': graph_list,
                        'gross_sales': {
                            'current': current_gross_sales,
                            'percentage_growth': calculate_percentage_growth(current_value=current_gross_sales, prior_value=prior_gross_sales),
                            'previous_year_percentage_growth': None if previous_year_gross_sales <= 0 else calculate_percentage_growth(current_value=current_gross_sales, prior_value=previous_year_gross_sales),
                        },
                        'units_sold': {
                            'current': current_units_sold,
                            'percentage_growth': calculate_percentage_growth(current_value=current_units_sold, prior_value=prior_units_sold),
                            'previous_year_percentage_growth': None if previous_year_units_sold <= 0 else calculate_percentage_growth(current_value=current_units_sold, prior_value=previous_year_units_sold)
                        }
                    }
                }

                return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=result_dict, error=None)

            else:
                return send_json_response(
                    http_status=404,
                    response_status=True,
                    data=None,
                    message_key=ResponseMessageKeys.NO_DATA_FOUND.value,
                    error=None
                )

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed getting sales statistics bar graph for dashboard: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    # Dashboard endpoint - sales stats hourly bar graph
    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def get_hourly_sales_stats(user_object, account_object, allowed_brands):
        """endpoint for getting various metrics for sales statistics hourly"""
        try:

            field_types = {'from_date': str,
                           'to_date': str, 'marketplace': str}

            required_fields = ['from_date', 'to_date', 'marketplace']

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

            from_date = request.args.get('from_date')
            to_date = request.args.get('to_date')
            page = request.args.get('page', default=PAGE_DEFAULT)
            size = request.args.get('size', default=PAGE_LIMIT)
            category = request.args.getlist('category')
            brand = request.args.getlist('brand') if 'brand' in request.args and request.args.getlist(
                'brand') else allowed_brands
            product = request.args.getlist('product')
            if brand:
                if account_object.primary_user_id != user_object.id:
                    valid_brands = [b for b in brand if b in allowed_brands]
                    if len(valid_brands) != len(brand):
                        return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                                  message_key=ResponseMessageKeys.INVALID_BRAND.value, data=None,
                                                  error=ResponseMessageKeys.ENTER_CORRECT_INPUT.value)

                    brand = valid_brands

            sales_data = DashboardView.__get_sales_stats_by_hour(
                account_id=account_object.uuid, asp_id=account_object.asp_id, from_date=from_date, to_date=to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))

            orders_data, total_order_count = AzOrderReport.get_purchased_date_orders(
                account_id=account_object.uuid, asp_id=account_object.asp_id, from_date=from_date, to_date=to_date, page=page, size=size, category=tuple(category), brand=tuple(brand), product=tuple(product))

            top_selling_products = AzOrderReport.get_top_least_selling_orders(
                account_id=account_object.uuid, asp_id=account_object.asp_id, from_date=from_date, to_date=to_date, sort_order=SortingOrder.DESC.value, size=PRODUCT_RANK_LIMIT, category=tuple(category), brand=tuple(brand), product=tuple(product))

            result = []
            sales_graph_data = []
            top_selling_product_data = []

            if sales_data:
                for sales_entry in sales_data:
                    if sales_entry is not None:
                        parsed_data = json.loads(sales_entry)
                        logger.info(f'Sales Data: {parsed_data}')
                        for _sales in parsed_data:
                            interval_range = _sales.get('interval_range', '')  # type: ignore  # noqa: FKA100
                            total_sales = _sales.get('totalSales', {}).get('amount', 0.0)   # type: ignore  # noqa: FKA100
                            units_ordered = _sales.get('unitCount', 0)   # type: ignore  # noqa: FKA100

                            found_entry = next(
                                (entry for entry in sales_graph_data if entry['time_interval'] == interval_range), None)

                            if found_entry:
                                found_entry['gross_sales'] += total_sales
                                found_entry['units_ordered'] += units_ordered
                            else:
                                sales_graph_data.append({
                                    'gross_sales': total_sales,
                                    'time_interval': interval_range,
                                    'units_ordered': units_ordered
                                })

            if orders_data:
                for get_order in orders_data:
                    order_object = {
                        'timing': format_iso_to_12_hour_format(get_order.purchase_date),
                        'price': get_order.item_price if get_order.item_price is not None else 0.0,
                        'quantity': get_order.quantity if get_order.quantity is not None else 0,
                        'sku': get_order.sku if get_order.sku is not None else '',
                        'product_name': get_order.product_name if get_order.product_name is not None else '',
                        'item_image': get_order.face_image if get_order.face_image is not None else '',
                        'asin': get_order.asin if get_order.asin is not None else '',
                    }
                    result.append(order_object)

            if top_selling_products:
                for product in top_selling_products:
                    product_object = {
                        'product_name': product.product_name,
                        'item_image': product.product_image,
                        'sku': product.sku,
                        'asin': product.asin,
                        'gross_sales': product.gross_sales,
                        'units_sold': product.units_sold,
                    }
                    top_selling_product_data.append(product_object)

            if result or sales_graph_data:

                data = {
                    'result': result,
                    'objects': {
                        'date': from_date,
                        'hourly_sales': sales_graph_data,
                        'top_selling_products': top_selling_product_data,
                        'pagination_metadata': get_pagination_meta(current_page=1 if page is None else int(page), page_size=int(size), total_items=total_order_count)
                    }
                }

                return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=data, error=None)

            return send_json_response(
                http_status=404,
                response_status=True,
                data=None,
                message_key=ResponseMessageKeys.NO_DATA_FOUND.value,
                error=None
            )

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed getting sales statistics bar graph for dashboard: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    # Dasboard endpoint - sales statistics

    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def get_sales_statistics_v2(user_object, account_object, allowed_brands):                  # type: ignore  # noqa: C901
        """endpoint for getting various metrics for sales statistics dashboard section"""
        try:

            selling_partner_id = account_object.asp_id
            account_id = account_object.uuid

            credentials = account_object.retrieve_asp_credentials(account_object)[
                0]

            from_date = request.args.get(
                'from_date', default=date.today().strftime('%Y-%m-%d'))
            to_date = request.args.get(
                'to_date', default=date.today().strftime('%Y-%m-%d'))

            category = request.args.getlist('category')
            brand = request.args.getlist('brand') if 'brand' in request.args and request.args.getlist(
                'brand') else allowed_brands
            product = request.args.getlist('product')

            """Get live data from order reports"""

            # max_date = None
            # if category or brand or product:
            #     max_date = db.session.query(
            #         func.max(AzSalesTrafficAsin.payload_date)).scalar()
            #     if max_date is not None:
            #         from_date = max_date.strftime('%Y-%m-%d')
            #         to_date = max_date.strftime('%Y-%m-%d')

            if brand:
                if account_object.primary_user_id != user_object.id:
                    valid_brands = [b for b in brand if b in allowed_brands]
                    if len(valid_brands) != len(brand):
                        return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                                  message_key=ResponseMessageKeys.INVALID_BRAND.value, data=None,
                                                  error=ResponseMessageKeys.ENTER_CORRECT_INPUT.value)
                    brand = valid_brands

            try:
                AZSalesView.call_sales_api(
                    account_id=account_id, asp_id=selling_partner_id, to_date=to_date, credentials=credentials)

            except Exception as exception_error:
                logger.error(
                    f'Exception occured while getting sales api data : {exception_error}')

            finally:
                """ querying and doing operations regardless of sales api working or not."""

                # current sales stats
                current_sales_stats = DashboardView.__get_sales_stats_metrics(
                    account_id=account_id, selling_partner_id=selling_partner_id, from_date=from_date, to_date=to_date, category=tuple(category), brand=tuple(brand), product=tuple(product)).fetchone()

                prior_from_date, prior_to_date = get_prior_to_from_date(
                    from_date=from_date, to_date=to_date)

                # prior sales stats
                prior_sales_stats = DashboardView.__get_sales_stats_metrics(
                    account_id=account_id, selling_partner_id=selling_partner_id, from_date=prior_from_date, to_date=prior_to_date, category=tuple(category), brand=tuple(brand), product=tuple(product)).fetchone()

                if current_sales_stats:
                    # gross sales
                    current_gross_sales = convert_to_numeric(
                        current_sales_stats.gross_sales)
                    prior_gross_sales = convert_to_numeric(
                        prior_sales_stats.gross_sales)

                    # total orders
                    current_hourly_sale = current_sales_stats.hourly_sales_json
                    prior_hourly_sale = prior_sales_stats.hourly_sales_json
                    current_total_order = current_hourly_sale.get(
                        'objects').get('order_count')
                    prior_total_order = prior_hourly_sale.get(
                        'objects').get('order_count')

                    current_total_orders = convert_to_numeric(
                        current_total_order)
                    prior_total_orders = convert_to_numeric(
                        prior_total_order)

                    # AOV
                    current_aov = 0 if current_total_orders == 0 else current_gross_sales / \
                        current_total_orders
                    prior_aov = 0 if prior_total_orders == 0 else prior_gross_sales / prior_total_orders

                    # units sold
                    current_units_sold = convert_to_numeric(
                        current_sales_stats.units_sold)
                    prior_units_sold = convert_to_numeric(
                        prior_sales_stats.units_sold)

                    result_dict = {
                        # 'objects': {
                        #     'from_date': max_date.strftime('%Y-%m-%d') if max_date is not None else from_date
                        # },
                        'result': {
                            'gross_sales': {
                                'current_gross_sales': current_gross_sales,
                                'prior_gross_sales': prior_gross_sales,
                                'gross_sales_percentage_growth': calculate_percentage_growth(current_value=current_gross_sales, prior_value=prior_gross_sales),
                                'gross_sales_difference': current_gross_sales - prior_gross_sales
                            },
                            'total_orders': {
                                'current_total_orders': current_total_orders,
                                'prior_total_orders': prior_total_orders,
                                'total_orders_percentage_growth': calculate_percentage_growth(current_value=current_total_orders, prior_value=prior_total_orders),
                                'total_orders_difference': current_total_orders - prior_total_orders
                            },
                            'aov': {
                                'current_aov': current_aov,
                                'prior_aov': prior_aov,
                                'aov_percentage_growth': calculate_percentage_growth(current_value=current_aov, prior_value=prior_aov),
                                'aov_difference': current_aov - prior_aov
                            },
                            'units_sold': {
                                'current_units_sold': current_units_sold,
                                'prior_units_sold': prior_units_sold,
                                'units_sold_percentage_growth': calculate_percentage_growth(current_value=current_units_sold, prior_value=prior_units_sold),
                                'units_sold_difference': current_units_sold - prior_units_sold
                            }
                        }
                    }

                    return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=result_dict, error=None)

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed getting sales statistics for dashboard: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    # Dashboard endpoint - sales stats bar graph
    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def get_sales_statistics_bar_graph_v2(user_object, account_object, allowed_brands):                  # type: ignore  # noqa: C901
        """endpoint for getting various metrics for sales statistics graph for sales and units section"""
        try:

            from_date = request.args.get('from_date', default=None)
            to_date = request.args.get('to_date', default=None)
            marketplace = request.args.get('marketplace')
            selling_partner_id = account_object.asp_id
            account_id = account_object.uuid
            category = request.args.getlist('category')
            brand = request.args.getlist('brand') if 'brand' in request.args and request.args.getlist(
                'brand') else allowed_brands
            product = request.args.getlist('product')

            # validation
            params = {}
            if from_date:
                params['from_date'] = from_date
            if to_date:
                params['to_date'] = to_date
            if marketplace:
                params['marketplace'] = marketplace
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

            field_types = {'from_date': str,
                           'to_date': str, 'marketplace': str}

            required_fields = ['from_date', 'to_date', 'marketplace']

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

            credentials = account_object.retrieve_asp_credentials(account_object)[
                0]

            try:
                AZSalesView.call_sales_api(
                    account_id=account_id, asp_id=selling_partner_id, to_date=to_date, credentials=credentials)

            except Exception as exception_error:
                logger.error(
                    f'Exception occured while getting sales api data : {exception_error}')

            finally:

                graph_list = []
                # Get today's date
                today = datetime.today()

                # Calculate the date two days ago
                two_days_ago = today - timedelta(days=2)
                t2_from_date = two_days_ago.strftime('%Y-%m-%d')
                t2_to_date = to_date

                sales_summary_sales_statistics_graph = []
                """Get sales summary data for last 3 days including today"""
                if to_date >= t2_from_date:
                    if from_date >= t2_from_date:
                        t2_from_date = from_date
                    three_days_ago = today - timedelta(days=3)
                    to_date = three_days_ago.strftime('%Y-%m-%d')

                    logger.info(
                        f't2_from_date: {t2_from_date}, t2_to_date: {t2_to_date}')
                    sales_summary_sales_statistics_graph = AzSalesTrafficSummary.get_by_date(
                        account_id=account_id, asp_id=selling_partner_id, from_date=t2_from_date, to_date=t2_to_date)

                    # _order_report_sales_statistics_graph = DashboardView.__get_sales_statistics_bar_graph_metrics_order_report(
                    #     account_id=account_id, selling_partner_id=selling_partner_id, from_date=t2_from_date, to_date=t2_to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))

                """Get Sales traffic report till T-2 days."""
                sales_statistics_graph = DashboardView.__get_sales_statistics_bar_graph_metrics(
                    account_id=account_id, selling_partner_id=selling_partner_id, from_date=from_date, to_date=to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))

                if sales_statistics_graph:

                    for graph in sales_statistics_graph:
                        graph_data = {
                            'date': graph.date.strftime('%Y-%m-%d'),
                            'gross_sales': convert_to_numeric(graph.gross_sales),
                            'units_sold': convert_to_numeric(graph.units_sold)
                        }
                        graph_list.append(graph_data)

                sales_summary_gross_sales = 0
                sales_summary_units_sold = 0

                if sales_summary_sales_statistics_graph:
                    for _graph in sales_summary_sales_statistics_graph:
                        _graph_data = {
                            'date': _graph.date.strftime('%Y-%m-%d'),
                            'gross_sales': convert_to_numeric(_graph.ordered_product_sales_amount),
                            'units_sold': convert_to_numeric(_graph.units_ordered)
                        }

                        sales_summary_gross_sales += convert_to_numeric(
                            _graph.ordered_product_sales_amount)
                        sales_summary_units_sold += convert_to_numeric(
                            _graph.units_ordered)

                        graph_list.append(_graph_data)

                    # if _order_report_sales_statistics_graph:
                    #     for _graph in _order_report_sales_statistics_graph:
                    #         _graph_data = {
                    #             'date': _graph.date.strftime('%Y-%m-%d'),
                    #             'gross_sales': convert_to_numeric(_graph.gross_sales),
                    #             'units_sold': convert_to_numeric(_graph.units_sold)
                    #         }
                    #         _order_report_gross_sales += convert_to_numeric(
                    #             _graph.gross_sales)
                    #         _order_report_units_sold += convert_to_numeric(
                    #             _graph.units_sold)
                    #         logger.info(f'graph_data : {graph_data}')
                    #         graph_list.append(_graph_data)

                if graph_list:
                    # gross sales and units sold comp
                    current_metrics = DashboardView.__get_gross_sales_units_sold_bar_graph(
                        account_id=account_id, selling_partner_id=selling_partner_id, from_date=from_date, to_date=to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))

                    prior_from_date, prior_to_date = get_prior_to_from_date(
                        from_date=from_date, to_date=t2_to_date)

                    prior_metrics = DashboardView.__get_gross_sales_units_sold_bar_graph(
                        account_id=account_id, selling_partner_id=selling_partner_id, from_date=prior_from_date, to_date=prior_to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))

                    previous_year_from_date, previous_year_to_date = get_previous_year_to_from_date(
                        from_date=from_date, to_date=t2_to_date)

                    previous_year_metrics = DashboardView.__get_gross_sales_units_sold_bar_graph(
                        account_id=account_id, selling_partner_id=selling_partner_id, from_date=previous_year_from_date, to_date=previous_year_to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))

                    # current metrics
                    current_gross_sales = convert_to_numeric(
                        current_metrics.gross_sales) + sales_summary_gross_sales
                    current_units_sold = convert_to_numeric(
                        current_metrics.units_sold) + sales_summary_units_sold

                    # prior metrics
                    prior_gross_sales = convert_to_numeric(
                        prior_metrics.gross_sales)
                    prior_units_sold = convert_to_numeric(
                        prior_metrics.units_sold)

                    # previous year metrics
                    previous_year_gross_sales = convert_to_numeric(
                        previous_year_metrics.gross_sales)
                    previous_year_units_sold = convert_to_numeric(
                        previous_year_metrics.units_sold)

                    result_dict = {
                        'result': {
                            'graph': sorted(graph_list, key=lambda x: x['date']),
                            'gross_sales': {
                                'current': current_gross_sales,
                                'percentage_growth': calculate_percentage_growth(current_value=current_gross_sales, prior_value=prior_gross_sales),
                                'previous_year_percentage_growth': None if previous_year_gross_sales <= 0 else calculate_percentage_growth(current_value=current_gross_sales, prior_value=previous_year_gross_sales),
                            },
                            'units_sold': {
                                'current': current_units_sold,
                                'percentage_growth': calculate_percentage_growth(current_value=current_units_sold, prior_value=prior_units_sold),
                                'previous_year_percentage_growth': None if previous_year_units_sold <= 0 else calculate_percentage_growth(current_value=current_units_sold, prior_value=previous_year_units_sold)
                            }
                        }
                    }

                    return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=result_dict, error=None)

                else:
                    return send_json_response(
                        http_status=404,
                        response_status=True,
                        data=None,
                        message_key=ResponseMessageKeys.NO_DATA_FOUND.value,
                        error=None
                    )

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed getting sales statistics bar graph for dashboard: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    # Dashboard endpoint - sales stats hourly bar graph
    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def get_hourly_sales_stats_v2(user_object, account_object, allowed_brands):
        """endpoint for getting various metrics for sales statistics hourly"""
        try:

            field_types = {'from_date': str,
                           'to_date': str, 'marketplace': str}

            required_fields = ['from_date', 'to_date', 'marketplace']

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

            from_date = request.args.get('from_date')
            to_date = request.args.get('to_date')
            page = request.args.get('page', default=PAGE_DEFAULT)
            size = request.args.get('size', default=PAGE_LIMIT)
            category = request.args.getlist('category')
            brand = request.args.getlist('brand') if 'brand' in request.args and request.args.getlist(
                'brand') else allowed_brands
            product = request.args.getlist('product')

            account_id = account_object.uuid
            asp_id = account_object.asp_id
            credentials = account_object.retrieve_asp_credentials(account_object)[
                0]

            if brand:
                if account_object.primary_user_id != user_object.id:
                    valid_brands = [b for b in brand if b in allowed_brands]
                    if len(valid_brands) != len(brand):
                        return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                                  message_key=ResponseMessageKeys.INVALID_BRAND.value, data=None,
                                                  error=ResponseMessageKeys.ENTER_CORRECT_INPUT.value)

                    brand = valid_brands

            try:
                AZSalesView.call_sales_api(
                    account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date, credentials=credentials, last_three_days=False)

            except Exception as exception_error:
                logger.error(
                    f'Exception occured while getting sales api data : {exception_error}')

            finally:

                result = []
                sales_graph_data = []
                top_selling_product_data = []

                sales_data = AzSalesTrafficSummary.get_hourly_sales_by_single_date(
                    account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date)

                if sales_data:
                    logger.info('Hourly Sales Endpoint using sales data')
                    hourly_sales_dict = sales_data.hourly_sales.get(
                        'result').get('payload')
                    if hourly_sales_dict:
                        for sales_by_hour in hourly_sales_dict:
                            sales_object = {
                                'time_interval': sales_by_hour.get('formatted_interval_range'),
                                'gross_sales': float(sales_by_hour.get('totalSales').get('amount')) if sales_by_hour.get('totalSales').get('amount') is not None else 0.0,
                                'units_ordered': int(sales_by_hour.get('unitCount')) if sales_by_hour.get('unitCount') is not None else 0,
                            }
                            sales_graph_data.append(sales_object)

                else:
                    sales_data = DashboardView.__get_sales_stats_by_hour(
                        account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))

                    if sales_data:
                        logger.info(
                            'Hourly Sales Endpoint using order report data')
                        for sales_by_hour in sales_data:
                            sales_object = {
                                'time_interval': sales_by_hour.hour_range,
                                'gross_sales': float(sales_by_hour.total_sales_amount) if sales_by_hour.total_sales_amount is not None else 0.0,
                                'units_ordered': int(sales_by_hour.total_units_ordered) if sales_by_hour.total_units_ordered is not None else 0,
                            }
                            sales_graph_data.append(sales_object)

                orders_data, total_order_count = AzOrderReport.get_purchased_date_orders(
                    account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date, page=page, size=size, category=tuple(category), brand=tuple(brand), product=tuple(product))

                if orders_data:
                    for get_order in orders_data:
                        order_object = {
                            'timing': format_iso_to_12_hour_format(get_order.purchase_date),
                            'price': get_order.item_price if get_order.item_price is not None else 0.0,
                            'quantity': get_order.quantity if get_order.quantity is not None else 0,
                            'sku': get_order.sku if get_order.sku is not None else '',
                            'product_name': get_order.product_name if get_order.product_name is not None else '',
                            'item_image': get_order.face_image if get_order.face_image is not None else '',
                            'asin': get_order.asin if get_order.asin is not None else '',
                        }
                        result.append(order_object)

                top_selling_products = AzOrderReport.get_top_least_selling_orders(
                    account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date, sort_order=SortingOrder.DESC.value, size=PRODUCT_RANK_LIMIT, category=tuple(category), brand=tuple(brand), product=tuple(product))

                if top_selling_products:
                    for product in top_selling_products:
                        product_object = {
                            'product_name': product.product_name,
                            'item_image': product.product_image,
                            'sku': product.sku,
                            'asin': product.asin,
                            'gross_sales': product.gross_sales,
                            'units_sold': product.units_sold,
                        }
                        top_selling_product_data.append(product_object)

                if result or sales_graph_data:

                    data = {
                        'result': result,
                        'objects': {
                            'date': from_date,
                            'hourly_sales': sales_graph_data,
                            'top_selling_products': top_selling_product_data,
                            'pagination_metadata': get_pagination_meta(current_page=1 if page is None else int(page), page_size=int(size), total_items=total_order_count)
                        }
                    }

                    return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=data, error=None)

                return send_json_response(
                    http_status=404,
                    response_status=True,
                    data=None,
                    message_key=ResponseMessageKeys.NO_DATA_FOUND.value,
                    error=None
                )

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed getting sales statistics bar graph for dashboard: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    # Dasboard endpoint - marketplace breakdown
    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def get_marketplace_breakdown(user_object, account_object, allowed_brands):                  # type: ignore  # noqa: C901
        """endpoint for getting various metrics for marketplace breakdown and graph dashboard section"""
        try:

            from_date = request.args.get('from_date', default=None)
            to_date = request.args.get('to_date', default=None)
            category = request.args.getlist('category')
            brand = request.args.getlist('brand') if 'brand' in request.args and request.args.getlist(
                'brand') else allowed_brands
            product = request.args.getlist('product')

            selling_partner_id = account_object.asp_id
            account_id = account_object.uuid

            # validation
            params = {}
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

            field_types = {'from_date': str,
                           'to_date': str, 'product': list, 'brand': list, 'category': list}

            required_fields = ['from_date', 'to_date']

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

            result_dict = {'result': {}}

            marketplace = DashboardView.__get_marketplace_breakdown_metrics(
                account_id=account_id, selling_partner_id=selling_partner_id, from_date=from_date, to_date=to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))

            if marketplace:

                # gross sales
                current_gross_sales = convert_to_numeric(
                    marketplace.current_gross_sales)
                prior_gross_sales = convert_to_numeric(
                    marketplace.prior_gross_sales)
                previous_year_gross_sales = convert_to_numeric(
                    marketplace.previous_year_gross_sales)

                # units sold
                current_units_sold = convert_to_numeric(
                    marketplace.current_units_sold)
                prior_units_sold = convert_to_numeric(
                    marketplace.prior_units_sold)
                previous_year_units_sold = convert_to_numeric(
                    marketplace.previous_year_units_sold)

                # refund
                current_returns = convert_to_numeric(
                    marketplace.current_refund)
                prior_returns = convert_to_numeric(marketplace.prior_refund)

                # units returned
                current_units_returned = convert_to_numeric(
                    marketplace.current_units_returned)
                prior_units_returned = convert_to_numeric(
                    marketplace.prior_units_returned)

                # return rate
                current_return_rate = convert_to_numeric(
                    marketplace.current_return_rate)
                prior_return_rate = convert_to_numeric(
                    marketplace.prior_return_rate)

                # marketplace fee
                current_marketplace_fee = convert_to_numeric(
                    marketplace.current_marketplace_fee)
                prior_marketplace_fee = convert_to_numeric(
                    marketplace.prior_marketplace_fee)

                # total cogs
                current_total_cogs = convert_to_numeric(
                    marketplace.current_total_cogs)
                prior_total_cogs = convert_to_numeric(
                    marketplace.prior_total_cogs)

                # profit
                current_profit = convert_to_numeric(
                    current_gross_sales - (current_marketplace_fee + current_total_cogs + current_returns))
                prior_profit = convert_to_numeric(
                    prior_gross_sales - (prior_marketplace_fee + prior_total_cogs + prior_returns))

                # aov
                current_aov = 0 if current_units_sold == 0 else current_gross_sales / current_units_sold
                prior_aov = 0 if prior_units_sold == 0 else prior_gross_sales / prior_units_sold

                # margin
                current_margin = 0 if current_gross_sales == 0 else (
                    current_profit * 100) / current_gross_sales
                prior_margin = 0 if prior_gross_sales == 0 else (
                    prior_profit * 100) / prior_gross_sales

                # roi
                current_roi = 0 if current_total_cogs == 0 else (
                    current_profit * 100) / current_total_cogs
                prior_roi = 0 if prior_total_cogs == 0 else (
                    prior_profit * 100) / prior_total_cogs

                result_dict['result'] = {
                    'amazon': {
                        'gross_sales': {
                            'current_gross_sales': current_gross_sales,
                            'prior_gross_sales': prior_gross_sales,
                            'gross_sales_percentage_growth': calculate_percentage_growth(current_value=current_gross_sales, prior_value=prior_gross_sales),
                            'previous_year_gross_sales': previous_year_gross_sales,
                            'gross_sales_percentage_growth_previous_year': calculate_percentage_growth(current_value=current_gross_sales, prior_value=previous_year_gross_sales)
                        },
                        'units_sold': {
                            'current_units_sold': current_units_sold,
                            'prior_units_sold': prior_units_sold,
                            'units_sold_percentage_growth': calculate_percentage_growth(current_value=current_units_sold, prior_value=prior_units_sold),
                            'previous_year_units_sold': previous_year_units_sold,
                            'units_sold_percentage_growth_previous_year': calculate_percentage_growth(current_value=current_units_sold, prior_value=previous_year_units_sold)
                        },
                        'returns': {
                            'current_returns': -abs(current_returns),
                            'prior_returns': -abs(prior_returns),
                            'returns_percentage_growth': calculate_percentage_growth(current_value=current_returns, prior_value=prior_returns)
                        },
                        'units_returned': {
                            'current_units_returned': current_units_returned,
                            'prior_units_returned': prior_units_returned,
                            'units_returned_percentage_growth': calculate_percentage_growth(current_value=current_units_returned, prior_value=prior_units_returned)
                        },
                        'return_rate': {
                            'current_return_rate': current_return_rate,
                            'prior_return_rate': prior_return_rate,
                            'return_rate_percentage_growth': calculate_percentage_growth(current_value=current_return_rate, prior_value=prior_return_rate)
                        },
                        'marketplace_fee': {
                            'current_marketplace_fee': -abs(current_marketplace_fee),
                            'prior_marketplace_fee': -abs(prior_marketplace_fee),
                            'marketplace_fee_percentage_growth': calculate_percentage_growth(current_value=current_marketplace_fee, prior_value=prior_marketplace_fee)
                        },
                        'total_cogs': {
                            'current_total_cogs': current_total_cogs,
                            'prior_total_cogs': prior_total_cogs,
                            'total_cogs_percentage_growth': calculate_percentage_growth(current_value=current_total_cogs, prior_value=prior_total_cogs)
                        },
                        'profit': {
                            'current_profit': current_profit,
                            'prior_profit': prior_profit,
                            'profit_percentage_growth': calculate_percentage_growth(current_value=current_profit, prior_value=prior_profit)
                        },
                        'aov': {
                            'current_aov': convert_to_numeric(current_aov),
                            'prior_aov': convert_to_numeric(prior_aov),
                            'aov_percentage_growth': calculate_percentage_growth(current_value=convert_to_numeric(current_aov), prior_value=convert_to_numeric(prior_aov))
                        },
                        'margin': {
                            'current_margin': convert_to_numeric(current_margin),
                            'prior_margin': convert_to_numeric(prior_margin),
                            'margin_percentage_growth': calculate_percentage_growth(current_value=convert_to_numeric(current_margin), prior_value=convert_to_numeric(prior_margin))
                        },
                        'roi': {
                            'current_roi': convert_to_numeric(current_roi),
                            'prior_roi': convert_to_numeric(prior_roi),
                            'roi_percentage_growth': calculate_percentage_growth(current_value=convert_to_numeric(current_roi), prior_value=convert_to_numeric(prior_roi))
                        }
                    }
                }

                result_dict['result']['all_marketplaces'] = result_dict['result']['amazon']

                graph_metrics = DashboardView.__get_sales_statistics_graph_metrics(
                    account_id=account_id, selling_partner_id=selling_partner_id, from_date=from_date, to_date=to_date).fetchall()

                graph_metrics_list = []
                if graph_metrics:
                    for graph in graph_metrics:
                        shp_date = graph.shp_date
                        graph_metrics_list.append({'gross_sales': convert_to_numeric(
                            graph.gross_sales), 'units_sold': convert_to_numeric(graph.units_sold), 'date': shp_date.strftime('%Y-%m-%d')})

                result_dict['result']['graph'] = graph_metrics_list

                return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=result_dict, error=None)

            else:
                return send_json_response(
                    http_status=404,
                    response_status=True,
                    data=None,
                    message_key=ResponseMessageKeys.NO_DATA_FOUND.value,
                    error=None
                )

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed getting marketplace breakdown endpoint for dashboard: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    # Dasboard endpoint - sales and trends

    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def get_sales_and_trends(user_object, account_object, allowed_brands):                                  # type: ignore  # noqa: C901
        """endpoint for getting top/least 20 asins according to their gross sales"""
        try:

            size = request.args.get('size')
            from_date = request.args.get('from_date')
            to_date = request.args.get('to_date')
            marketplace = request.args.get('marketplace')
            category = request.args.getlist('category')
            brand = request.args.getlist('brand') if 'brand' in request.args and request.args.getlist(
                'brand') else allowed_brands
            product = request.args.getlist('product')

            selling_partner_id = account_object.asp_id
            account_id = account_object.uuid

            # validation

            data = {}

            if size:
                data['size'] = size
            if from_date:
                data['from_date'] = from_date
            if to_date:
                data['to_date'] = to_date
            if marketplace:
                data['marketplace'] = marketplace
            if category:
                data['category'] = category
            if not brand:
                data['brand'] = brand
            else:
                if account_object.primary_user_id != user_object.id:
                    valid_brands = [b for b in brand if b in allowed_brands]
                    if len(valid_brands) != len(brand):
                        return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                                  message_key=ResponseMessageKeys.INVALID_BRAND.value, data=None,
                                                  error=ResponseMessageKeys.ENTER_CORRECT_INPUT.value)

                    data['brand'] = valid_brands
            if product:
                data['product'] = product

            field_types = {'size': int, 'from_date': str,
                           'to_date': str, 'marketplace': str, 'product': list, 'brand': list, 'category': list, }

            required_fields = ['size', 'from_date', 'to_date', 'marketplace']

            enum_fields = {
                'marketplace': (marketplace, 'ProductMarketplace')
            }

            valid_enum = enum_validator(enum_fields)

            if valid_enum['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=valid_enum['data'])

            validate_data = field_type_validator(
                request_data=data, field_types=field_types)

            if validate_data['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=validate_data['data'])

            is_valid = required_validator(
                request_data=data, required_fields=required_fields)

            if is_valid['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=is_valid['data'])

            # top selling items
            top_selling_items_list = DashboardView.__calculate_sales_ranks(account_id=account_id, selling_partner_id=selling_partner_id, size=data[
                                                                           'size'], sort_order=SortingOrder.DESC.value, from_date=data['from_date'], to_date=data['to_date'], category=tuple(category), brand=tuple(brand), product=tuple(product))

            if top_selling_items_list:

                least_selling_items_list = DashboardView.__calculate_sales_ranks(account_id=account_id, selling_partner_id=selling_partner_id, size=data[
                    'size'], sort_order=SortingOrder.ASC.value, from_date=data['from_date'], to_date=data['to_date'], category=tuple(category), brand=tuple(brand), product=tuple(product))
                # sales trend increasing/decreasing
                sales_trend_list = DashboardView.__calculate_sales_trends(
                    account_id=account_id, selling_partner_id=selling_partner_id, from_date=from_date, to_date=to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))

                sales_trend_increasing = sorted(
                    sales_trend_list, key=lambda x: x['stats']['gross_sales']['gross_sales_percentage_growth'], reverse=True)

                sales_trend_decreasing = sorted(
                    sales_trend_list, key=lambda x: x['stats']['gross_sales']['gross_sales_percentage_growth'])

                result_dict = {
                    'result': {
                        'top_selling_items': top_selling_items_list,
                        'least_selling_items': least_selling_items_list,
                        'sales_trend_increasing': sales_trend_increasing[:5],
                        'sales_trend_decreasing': sales_trend_decreasing[:5]
                    }
                }

                return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=result_dict, error=None)

            else:
                return send_json_response(
                    http_status=404,
                    response_status=True,
                    data=None,
                    message_key=ResponseMessageKeys.NO_DATA_FOUND.value,
                    error=None
                )

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed getting sales and trends data for dashboard: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def get_gross_sales_comp(user_object, account_object, allowed_brands):                  # type: ignore  # noqa: C901
        """endpoint for getting various metrics for gross sales comp section"""
        try:

            from_date = request.args.get('from_date', default=None)
            to_date = request.args.get('to_date', default=None)
            prior_from_date = request.args.get('prior_from_date', default=None)
            prior_to_date = request.args.get('prior_to_date', default=None)
            marketplace = request.args.get('marketplace')
            category = request.args.getlist('category')
            brand = request.args.getlist('brand') if 'brand' in request.args and request.args.getlist(
                'brand') else allowed_brands
            product = request.args.getlist('product')

            account_id = account_object.uuid
            selling_partner_id = account_object.asp_id

            # validation
            params = {}
            if from_date:
                params['from_date'] = from_date
            if to_date:
                params['to_date'] = to_date
            if prior_from_date:
                params['prior_from_date'] = prior_from_date
            if prior_to_date:
                params['prior_to_date'] = prior_to_date
            if marketplace:
                params['marketplace'] = marketplace
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

            field_types = {'from_date': str, 'to_date': str, 'product': list,
                           'brand': list, 'category': list, 'marketplace': str, 'prior_from_date': str, 'prior_to_date': str}
            required_fields = ['from_date', 'to_date', 'marketplace']

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

            gross_sales_comp = DashboardView.__calculate_gross_sales_comp(
                account_id=account_id, selling_partner_id=selling_partner_id, from_date=from_date, to_date=to_date, prior_from_date=prior_from_date, prior_to_date=prior_to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))

            if gross_sales_comp:
                result_dict = {'result': gross_sales_comp}

                return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=result_dict, error=None)

            else:
                return send_json_response(
                    http_status=404,
                    response_status=False,
                    message_key=ResponseMessageKeys.NO_DATA_FOUND.value,
                    error=None
                )

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed getting gross sales comp: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def get_profit_and_loss(user_object, account_object, allowed_brands):                  # type: ignore  # noqa: C901
        """endpoint for getting various metrics for profit and loss section"""
        try:

            from_date = request.args.get('from_date', default=None)
            to_date = request.args.get('to_date', default=None)
            marketplace = request.args.get('marketplace')
            category = request.args.getlist('category')
            brand = request.args.getlist('brand') if 'brand' in request.args and request.args.getlist(
                'brand') else allowed_brands
            product = request.args.getlist('product')

            account_id = account_object.uuid
            selling_partner_id = account_object.asp_id

            # validation
            params = {}
            if from_date:
                params['from_date'] = from_date
            if to_date:
                params['to_date'] = to_date
            if marketplace:
                params['marketplace'] = marketplace
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

            field_types = {'from_date': str, 'to_date': str, 'product': list,
                           'brand': list, 'category': list, 'marketplace': str}

            required_fields = ['from_date', 'to_date', 'marketplace']

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

            profit_and_loss = DashboardView.__calculate_profit_and_loss(
                account_id=account_id, selling_partner_id=selling_partner_id, from_date=from_date, to_date=to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))

            if profit_and_loss:
                # graph data
                profit_loss_graph = DashboardView.__get_profit_loss_graph_metrics(account_id=account_id, selling_partner_id=selling_partner_id,
                                                                                  from_date=from_date, to_date=to_date, product=tuple(product), brand=tuple(brand), category=tuple(category))

                graph_metrics_list = []

                if profit_loss_graph:
                    for graph_metrics in profit_loss_graph:
                        gross_sales = graph_metrics.gross_sales
                        expense = float(
                            graph_metrics.market_place_fee) + float(graph_metrics.total_cogs)
                        net_profit = float(
                            graph_metrics.gross_sales) - float(graph_metrics.refund) - float(expense)
                        units_sold = graph_metrics.units_sold

                        graph_metrics_list.append({'gross_sales': convert_to_numeric(gross_sales),
                                                   'units_sold': convert_to_numeric(units_sold),
                                                   'date': graph_metrics.shp_date.strftime('%Y-%m-%d'),
                                                   'expense': -abs(expense),
                                                   'net_profit': net_profit
                                                   })

                result_dict = {'result':
                               {
                                   'profit_and_loss': profit_and_loss,
                                   'graph': graph_metrics_list
                               }
                               }

                return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=result_dict, error=None)

            else:

                return send_json_response(
                    http_status=404,
                    response_status=True,
                    data=None,
                    message_key=ResponseMessageKeys.NO_DATA_FOUND.value,
                    error=None
                )
        except Exception as exception_error:
            logger.error(
                f'GET -> Failed getting profit and loss: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    def __calculate_sales_ranks(account_id: str, selling_partner_id: str, size: int, sort_order: str, from_date: str, to_date: str, product: Optional[tuple], brand: Optional[tuple],
                                category: Optional[tuple]):
        """calculate current and previous metrics for sales rank section"""

        prior_from_date, prior_to_date = get_prior_to_from_date(
            from_date=from_date, to_date=to_date)

        current_metrics_list = DashboardView.__get_product_sales_rank_metrics(
            account_id=account_id, selling_partner_id=selling_partner_id, sort_order=sort_order, from_date=from_date, to_date=to_date, product=tuple(product), brand=tuple(brand), category=tuple(category), size=size).fetchall()

        prior_metrics_list = DashboardView.__get_product_sales_rank_metrics(
            account_id=account_id, selling_partner_id=selling_partner_id, sort_order=sort_order, from_date=prior_from_date, to_date=prior_to_date, product=tuple(product), brand=tuple(brand), category=tuple(category), size=None).fetchall()

        if current_metrics_list:
            result_list = []

            current_asin_metrics_dict = {}
            prior_asin_metrics_dict = {}

            for current_asin_metrics in current_metrics_list:
                current_asin_metrics_dict[current_asin_metrics.asin] = current_asin_metrics

            for prior_asin_metrics in prior_metrics_list:
                prior_asin_metrics_dict[prior_asin_metrics.asin] = prior_asin_metrics
            for current_asin in current_asin_metrics_dict:

                current_metrics = current_asin_metrics_dict.get(current_asin)

                prior_metrics = prior_asin_metrics_dict.get(current_asin)

                # gross sales
                current_gross_sales = float(current_metrics.gross_sales)
                prior_gross_sales = float(
                    prior_metrics.gross_sales) if prior_metrics else 0

                gross_sales_percentage_growth = calculate_percentage_growth(
                    current_value=current_gross_sales, prior_value=prior_gross_sales)

                # units sold
                current_units_sold = int(current_metrics.units_sold)
                prior_units_sold = int(
                    prior_metrics.units_sold) if prior_metrics else 0

                units_sold_percentage_growth = calculate_percentage_growth(
                    current_value=current_units_sold, prior_value=prior_units_sold)

                # page views
                current_page_views = int(current_metrics.page_views)
                prior_page_views = int(
                    prior_metrics.page_views) if prior_metrics else 0

                page_views_percentage_growth = calculate_percentage_growth(
                    current_value=current_page_views, prior_value=prior_page_views)

                # conversin rate
                try:
                    current_conversion_rate = (
                        current_units_sold / current_page_views) * 100
                except ZeroDivisionError:
                    current_conversion_rate = 0

                try:
                    prior_conversion_rate = (
                        prior_units_sold / prior_page_views) * 100
                except ZeroDivisionError:
                    prior_conversion_rate = 0

                conversion_rate_percentage_growth = calculate_percentage_growth(
                    current_value=current_conversion_rate, prior_value=prior_conversion_rate)

                result = {
                    'product_name': current_metrics.product_name,
                    'product_image': current_metrics.product_image,
                    'asin': current_metrics.asin,
                    'sku': current_metrics.sku,
                    'stats':
                    {
                        'gross_sales': {
                            'current_gross_sales': current_gross_sales,
                            'prior_gross_sales': prior_gross_sales,
                            'gross_sales_percentage_growth': gross_sales_percentage_growth
                        },
                        'units_sold': {
                            'current_units_sold': current_units_sold,
                            'prior_units_sold': prior_units_sold,
                            'units_sold_percentage_growth': units_sold_percentage_growth
                        },
                        'conversion_rate': {
                            'current_conversion_rate': current_conversion_rate,
                            'prior_conversion_rate': prior_conversion_rate,
                            'conversion_rate_percentage_growth': conversion_rate_percentage_growth
                        },
                        'page_views': {
                            'current_page_views': current_page_views,
                            'prior_page_views': prior_page_views,
                            'page_views_percentage_growth': page_views_percentage_growth
                        }
                    }
                }

                result_list.append(result)

            return result_list

        return None

    @staticmethod
    def __calculate_sales_trends(account_id: str, selling_partner_id: str, from_date: str, to_date: str, product: Optional[tuple], brand: Optional[tuple],
                                 category: Optional[tuple]):
        """calculate current and previous metrics for sales trend section"""

        prior_from_date, prior_to_date = get_prior_to_from_date(
            from_date=from_date, to_date=to_date)

        current_metrics_list = DashboardView.__get_sales_trend_metrics(
            account_id=account_id, selling_partner_id=selling_partner_id, from_date=from_date, to_date=to_date, product=tuple(product), brand=tuple(brand), category=tuple(category)).fetchall()

        prior_metrics_list = DashboardView.__get_sales_trend_metrics(
            account_id=account_id, selling_partner_id=selling_partner_id, from_date=prior_from_date, to_date=prior_to_date, product=tuple(product), brand=tuple(brand), category=tuple(category)).fetchall()

        if current_metrics_list:
            result_list = []

            current_asin_metrics_dict = {}
            prior_asin_metrics_dict = {}

            for current_asin_metrics in current_metrics_list:
                current_asin_metrics_dict[current_asin_metrics.asin] = current_asin_metrics

            for prior_asin_metrics in prior_metrics_list:
                prior_asin_metrics_dict[prior_asin_metrics.asin] = prior_asin_metrics

            for current_asin in current_asin_metrics_dict:

                current_metrics = current_asin_metrics_dict.get(current_asin)

                prior_metrics = prior_asin_metrics_dict.get(current_asin)

                # gross sales
                current_gross_sales = float(current_metrics.gross_sales)
                prior_gross_sales = float(
                    prior_metrics.gross_sales) if prior_metrics else 0

                gross_sales_difference = current_gross_sales - prior_gross_sales
                gross_sales_percentage_growth = calculate_percentage_growth(
                    current_value=current_gross_sales, prior_value=prior_gross_sales)

                # expense
                # current_expense = float(
                #     current_metrics.market_place_fee) + float(current_metrics.total_cogs)
                # prior_expense = float(prior_metrics.market_place_fee if prior_metrics else 0) + \
                #     float(prior_metrics.total_cogs if prior_metrics else 0)

                # expense_difference = current_expense - prior_expense
                # expense_percentage_growth = calculate_percentage_growth(
                #     current_value=current_expense, prior_value=prior_expense)

                # refund
                # current_refund = float(current_metrics.refund)
                # prior_refund = float(
                #     prior_metrics.refund if prior_metrics else 0)

                # refund_difference = current_refund - prior_refund
                # refund_percentage_growth = calculate_percentage_growth(
                #     current_value=current_refund, prior_value=prior_refund)

                # net profit
                # current_net_profit = current_gross_sales - current_refund - current_expense
                # prior_net_profit = prior_gross_sales - prior_refund - prior_expense

                # net_profit_difference = current_net_profit - prior_net_profit
                # net_profit_percentage_growth = calculate_percentage_growth(
                #     current_value=current_net_profit, prior_value=prior_net_profit)

                # units sold
                current_units_sold = int(current_metrics.units_sold)
                prior_units_sold = int(
                    prior_metrics.units_sold if prior_metrics else 0)

                units_sold_difference = current_units_sold - prior_units_sold
                units_sold_percentage_growth = calculate_percentage_growth(
                    current_value=current_units_sold, prior_value=prior_units_sold)

                # page views
                current_page_views = int(current_metrics.page_views)
                prior_page_views = int(
                    prior_metrics.page_views if prior_metrics else 0)

                page_views_difference = current_page_views - prior_page_views
                page_views_percentage_growth = calculate_percentage_growth(
                    current_value=current_page_views, prior_value=prior_page_views)

                try:
                    current_conversion_rate = (
                        current_units_sold / current_page_views) * 100
                except ZeroDivisionError:
                    current_conversion_rate = 0

                try:
                    prior_conversion_rate = (
                        prior_units_sold / prior_page_views) * 100
                except ZeroDivisionError:
                    prior_conversion_rate = 0

                conversion_rate_difference = current_conversion_rate - prior_conversion_rate
                conversion_rate_percentage_growth = calculate_percentage_growth(
                    current_value=current_conversion_rate, prior_value=prior_conversion_rate)

                result = {
                    'product_name': current_metrics.product_name,
                    'product_image': current_metrics.product_image,
                    'asin': current_metrics.asin,
                    'sku': current_metrics.sku,
                    'stats':
                    {
                        'gross_sales': {
                            'current_gross_sales': current_gross_sales,
                            'prior_gross_sales': prior_gross_sales,
                            'gross_sales_difference': gross_sales_difference,
                            'gross_sales_percentage_growth': gross_sales_percentage_growth
                        },
                        'units_sold': {
                            'current_units_sold': current_units_sold,
                            'prior_units_sold': prior_units_sold,
                            'units_sold_difference': units_sold_difference,
                            'units_sold_percentage_growth': units_sold_percentage_growth
                        },
                        # 'net_profit': {
                        #     'current_net_profit': current_net_profit,
                        #     'prior_net_profit': prior_net_profit,
                        #     'net_profit_difference': net_profit_difference,
                        #     'net_profit_percentage_growth': net_profit_percentage_growth
                        # },
                        'page_views': {
                            'current_page_views': current_page_views,
                            'prior_page_views': prior_page_views,
                            'page_views_difference': page_views_difference,
                            'page_views_percentage_growth': page_views_percentage_growth
                        },
                        'conversion_rate': {
                            'current_conversion_rate': convert_to_numeric(current_conversion_rate),
                            'prior_conversion_rate': convert_to_numeric(prior_conversion_rate),
                            'conversion_rate_difference': convert_to_numeric(conversion_rate_difference),
                            'conversion_rate_percentage_growth': convert_to_numeric(conversion_rate_percentage_growth)
                        }
                    }
                }

                result_list.append(result)

            return result_list

        return None

    @staticmethod
    def __calculate_profit_and_loss(account_id: str, selling_partner_id: str, from_date: str, to_date: str, product: Optional[tuple], brand: Optional[tuple],               # type: ignore  # noqa: C901
                                    category: Optional[tuple]):
        """calculate current and previous metrics for profit and loss section"""

        prior_from_date, prior_to_date = get_prior_to_from_date(
            from_date=from_date, to_date=to_date)

        current_metrics_list = DashboardView.__get_profit_and_loss_metrics(
            account_id=account_id, selling_partner_id=selling_partner_id, from_date=from_date, to_date=to_date, product=tuple(product), brand=tuple(brand), category=tuple(category)).fetchall()
        prior_metrics_list = DashboardView.__get_profit_and_loss_metrics(
            account_id=account_id, selling_partner_id=selling_partner_id, from_date=prior_from_date, to_date=prior_to_date, product=tuple(product), brand=tuple(brand), category=tuple(category)).fetchall()

        current_metrics_dict = {'gross_sales': 0, 'returns': 0, 'net_sales': 0, 'marketplace_fee_breakdown': {
        }, 'total_cogs': 0, 'ads_spend': {}, 'net_profit': 0, 'market_place_fee': 0, 'gross_sales_breakdown': {}, 'other_fee_breakdown': {}, 'other_fee': 0}
        prior_metrics_dict = {'gross_sales': 0, 'returns': 0, 'net_sales': 0, 'marketplace_fee_breakdown': {
        }, 'total_cogs': 0, 'ads_spend': {}, 'net_profit': 0, 'market_place_fee': 0, 'gross_sales_breakdown': {}, 'other_fee_breakdown': {}, 'other_fee': 0}
        if current_metrics_list:
            for current_metrics in current_metrics_list:

                # current_expense = float(
                #     current_metrics.market_place_fee) + float(current_metrics.total_cogs)

                # Net Profit = Net Sales - Marketplace Fee - Other fee - Cogs - Ad Spends + Reimbursements
                current_net_profit = float(current_metrics.gross_sales) - float(
                    current_metrics.refund) - float(abs(current_metrics.total_cogs))
                # - float(abs(current_metrics.market_place_fee))
                # - float(abs(current_metrics.other_fee)) - float(abs(current_metrics.total_cogs))

                current_metrics_dict['gross_sales'] += current_metrics.gross_sales
                current_metrics_dict['returns'] += current_metrics.refund
                current_metrics_dict['net_sales'] += current_metrics.gross_sales - \
                    current_metrics.refund
                current_metrics_dict['net_profit'] += current_net_profit
                current_metrics_dict['total_cogs'] += current_metrics.total_cogs
                current_metrics_dict['market_place_fee'] += current_metrics.market_place_fee

                # marketplace fee breakdown
                if current_metrics.summary_analysis:
                    json_dict = current_metrics.summary_analysis

                    current_other_fee = json_dict.get('_other_fee')
                    if current_other_fee:
                        current_metrics_dict['other_fee'] += current_other_fee

                    marketplace_fee_breakdown_dict = json_dict.get(
                        '_market_place_fee_breakdown')
                    if marketplace_fee_breakdown_dict:
                        for current_marketplace_fee_key, current_marketplace_fee_value in marketplace_fee_breakdown_dict.items():
                            current_metrics_dict['marketplace_fee_breakdown'][current_marketplace_fee_key] = current_metrics_dict['marketplace_fee_breakdown'].get(                 # type: ignore  # noqa: FKA100
                                current_marketplace_fee_key, 0) + current_marketplace_fee_value

                    gross_sales_breakdown_dict = json_dict.get(
                        '_gross_sales_breakdown')
                    if gross_sales_breakdown_dict:
                        for current_gross_sales_key, current_gross_sales_value in gross_sales_breakdown_dict.items():
                            current_metrics_dict['gross_sales_breakdown'][current_gross_sales_key] = current_metrics_dict['gross_sales_breakdown'].get(                 # type: ignore  # noqa: FKA100
                                current_gross_sales_key, 0) + current_gross_sales_value

                    other_fee_breakdown_dict = json_dict.get(
                        '_other_fee_breakdown')
                    if other_fee_breakdown_dict:
                        for current_other_fee_key, current_other_fee_value in other_fee_breakdown_dict.items():
                            current_metrics_dict['other_fee_breakdown'][current_other_fee_key] = current_metrics_dict['other_fee_breakdown'].get(                 # type: ignore  # noqa: FKA100
                                current_other_fee_key, 0) + current_other_fee_value

            for prior_metrics in prior_metrics_list:

                # prior_expense = float(
                #     prior_metrics.market_place_fee) + float(prior_metrics.total_cogs)
                # prior_net_profit = float(
                #     prior_metrics.gross_sales) - float(prior_metrics.refund) - float(prior_expense)
                prior_net_profit = float(prior_metrics.gross_sales) - float(
                    prior_metrics.refund) - float(abs(prior_metrics.total_cogs))

                prior_metrics_dict['gross_sales'] += prior_metrics.gross_sales
                prior_metrics_dict['returns'] += prior_metrics.refund
                prior_metrics_dict['net_sales'] += prior_metrics.gross_sales - \
                    prior_metrics.refund
                prior_metrics_dict['net_profit'] += prior_net_profit
                prior_metrics_dict['total_cogs'] += prior_metrics.total_cogs
                prior_metrics_dict['market_place_fee'] += prior_metrics.market_place_fee

                # marketplace fee breakdown
                if prior_metrics.summary_analysis:
                    json_dict = prior_metrics.summary_analysis

                    prior_other_fee = json_dict.get('_other_fee')
                    if prior_other_fee:
                        prior_metrics_dict['other_fee'] += prior_other_fee

                    marketplace_fee_breakdown_dict = json_dict.get(
                        '_market_place_fee_breakdown')
                    if marketplace_fee_breakdown_dict:
                        for prior_marketplace_fee_key, prior_marketplace_fee_value in marketplace_fee_breakdown_dict.items():
                            prior_metrics_dict['marketplace_fee_breakdown'][prior_marketplace_fee_key] = prior_metrics_dict['marketplace_fee_breakdown'].get(                 # type: ignore  # noqa: FKA100
                                prior_marketplace_fee_key, 0) + prior_marketplace_fee_value

                    gross_sales_breakdown_dict = json_dict.get(
                        '_gross_sales_breakdown')

                    if gross_sales_breakdown_dict:
                        for prior_gross_sales_key, prior_gross_sales_value in gross_sales_breakdown_dict.items():
                            prior_metrics_dict['gross_sales_breakdown'][prior_gross_sales_key] = prior_metrics_dict['gross_sales_breakdown'].get(                 # type: ignore  # noqa: FKA100
                                prior_gross_sales_key, 0) + prior_gross_sales_value

                    other_fee_breakdown_dict = json_dict.get(
                        '_other_fee_breakdown')
                    if other_fee_breakdown_dict:
                        for prior_other_fee_key, prior_other_fee_value in other_fee_breakdown_dict.items():
                            prior_metrics_dict['other_fee_breakdown'][prior_other_fee_key] = prior_metrics_dict['other_fee_breakdown'].get(                 # type: ignore  # noqa: FKA100
                                prior_other_fee_key, 0) + prior_other_fee_value

            # calculating gross sales
            current_gross_sales = convert_to_numeric(
                current_metrics_dict.get('gross_sales'))
            prior_gross_sales = convert_to_numeric(
                prior_metrics_dict.get('gross_sales'))

            # gross_sales_difference = current_gross_sales - prior_gross_sales
            gross_sales_percentage_growth = calculate_percentage_growth(
                current_value=current_gross_sales, prior_value=prior_gross_sales)

            # calculating returns
            current_refund = convert_to_numeric(
                current_metrics_dict.get('returns'))
            prior_refund = convert_to_numeric(
                prior_metrics_dict.get('returns'))

            # refund_difference = current_refund - prior_refund
            refund_percentage_growth = calculate_percentage_growth(
                current_value=current_refund, prior_value=prior_refund)

            #  calculating net sales
            current_net_sales = convert_to_numeric(
                current_metrics_dict.get('net_sales'))
            prior_net_sales = convert_to_numeric(
                prior_metrics_dict.get('net_sales'))

            # net_sales_difference = current_net_sales - prior_net_sales
            net_sales_percentage_growth = calculate_percentage_growth(
                current_value=current_net_sales, prior_value=prior_net_sales)

            # calculating total_cogs
            current_total_cogs = convert_to_numeric(
                value=current_metrics_dict.get('total_cogs'))
            prior_total_cogs = convert_to_numeric(
                value=prior_metrics_dict.get('total_cogs'))

            # total_cogs_difference = current_total_cogs - prior_total_cogs
            total_cogs_percentage_growth = calculate_percentage_growth(
                current_value=current_total_cogs, prior_value=prior_total_cogs)

            # calculating other fee
            current_other_fee_breakdown, current_other_fee = DashboardView.get_other_fee_metrics(account_id=account_id, selling_partner_id=selling_partner_id, from_date=from_date,
                                                                                                 to_date=to_date, product=tuple(product), brand=tuple(brand), category=tuple(category))
            prior_other_fee_breakdown, prior_other_fee = DashboardView.get_other_fee_metrics(account_id=account_id, selling_partner_id=selling_partner_id, from_date=prior_from_date,
                                                                                             to_date=prior_to_date, product=tuple(product), brand=tuple(brand), category=tuple(category))

            # uncomment the two lines below >> if fba forward/reverse fee required to be added
            # current_other_fee += current_metrics_dict.get('other_fee')
            # prior_other_fee += prior_metrics_dict.get('other_fee')

            other_fee_percentage_growth = calculate_percentage_growth(
                current_value=current_other_fee, prior_value=prior_other_fee)

            # calculating marketplace fee
            current_market_place_fee = convert_to_numeric(
                current_metrics_dict.get('market_place_fee'))
            prior_market_place_fee = convert_to_numeric(
                prior_metrics_dict.get('market_place_fee'))

            market_place_fee_percentage_growth = calculate_percentage_growth(
                current_value=current_market_place_fee, prior_value=prior_market_place_fee)

            # calculating marketplace fee breakdown
            marketplace_fee_breakdown_comp = {}

            for key in current_metrics_dict['marketplace_fee_breakdown']:
                marketplace_fee_breakdown_comp[key] = {}
                current_metric = current_metrics_dict['marketplace_fee_breakdown'][key]
                prior_metric = prior_metrics_dict['marketplace_fee_breakdown'].get(                         # type: ignore  # noqa: FKA100
                    key, 0)
                # marketplace_fee_breakdown_comp[key] = current_metric + prior_metric
                marketplace_fee_breakdown_comp[key][f'current_{key}'] = current_metric
                marketplace_fee_breakdown_comp[key][f'{key}_growth_percentage'] = calculate_percentage_growth(
                    current_value=abs(current_metric), prior_value=abs(prior_metric))

            # calculating gross sales breakdown
            gross_sales_breakdown_comp = {}

            for current_gross_sales_key in current_metrics_dict['gross_sales_breakdown']:
                gross_sales_breakdown_comp[current_gross_sales_key] = {}
                current_metric = current_metrics_dict['gross_sales_breakdown'][current_gross_sales_key]
                prior_metric = prior_metrics_dict['gross_sales_breakdown'].get(                         # type: ignore  # noqa: FKA100
                    current_gross_sales_key, 0)
                gross_sales_breakdown_comp[current_gross_sales_key][
                    f'current_{current_gross_sales_key}'] = current_metric
                gross_sales_breakdown_comp[current_gross_sales_key][f'{current_gross_sales_key}_growth_percentage'] = calculate_percentage_growth(
                    current_value=abs(current_metric), prior_value=abs(prior_metric))

            # calculating reimbursement
            current_reimbursement_breakdown, current_reimbursement_value = DashboardView.get_reimbursement_fee_metrics(account_id=account_id, selling_partner_id=selling_partner_id,
                                                                                                                       from_date=from_date, to_date=to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))
            prior_reimbursement_breakdown, prior_reimbursement_value = DashboardView.get_reimbursement_fee_metrics(account_id=account_id, selling_partner_id=selling_partner_id,
                                                                                                                   from_date=prior_from_date, to_date=prior_to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))

            reimbursement_percentage_growth = calculate_percentage_growth(
                current_value=current_reimbursement_value, prior_value=prior_reimbursement_value)

            # calculating ads data
            current_sponsored_brand_cost, current_sponsored_display_cost, current_sponsored_product_cost = DashboardView.__get_ads_spend_metrics(
                account_id=account_id, selling_partner_id=selling_partner_id, from_date=from_date, to_date=to_date, product=tuple(product), brand=tuple(brand), category=tuple(category))
            prior_sponsored_brand_cost, prior_sponsored_display_cost, prior_sponsored_product_cost = DashboardView.__get_ads_spend_metrics(
                account_id=account_id, selling_partner_id=selling_partner_id, from_date=prior_from_date, to_date=prior_to_date, product=tuple(product), brand=tuple(brand), category=tuple(category))

            sponsored_brand_percentage_growth = calculate_percentage_growth(
                current_value=current_sponsored_brand_cost, prior_value=prior_sponsored_brand_cost)
            sponsored_display_percentage_growth = calculate_percentage_growth(
                current_value=current_sponsored_display_cost, prior_value=prior_sponsored_display_cost)
            sponsored_product_percentage_growth = calculate_percentage_growth(
                current_value=current_sponsored_product_cost, prior_value=prior_sponsored_product_cost)

            current_total_ads_spend = current_sponsored_brand_cost + \
                current_sponsored_display_cost + current_sponsored_product_cost
            prior_total_ads_spend = prior_sponsored_brand_cost + \
                prior_sponsored_display_cost + prior_sponsored_product_cost

            total_ads_spend_percentage_growth = calculate_percentage_growth(
                current_value=current_total_ads_spend, prior_value=prior_total_ads_spend)

            # calculating net profit
            current_net_profit = convert_to_numeric(
                current_metrics_dict.get('net_sales')) - abs(current_market_place_fee) - abs(convert_to_numeric(current_other_fee)) - abs(convert_to_numeric(current_metrics_dict.get('total_cogs'))) - abs(convert_to_numeric(current_total_ads_spend)) + convert_to_numeric(current_reimbursement_value)
            prior_net_profit = convert_to_numeric(
                prior_metrics_dict.get('net_sales')) - abs(prior_market_place_fee) - abs(convert_to_numeric(prior_other_fee)) - abs(convert_to_numeric(prior_metrics_dict.get('total_cogs'))) - abs(convert_to_numeric(prior_total_ads_spend)) + convert_to_numeric(prior_reimbursement_value)

            # net_profit_difference = current_net_profit - prior_net_profit
            net_profit_percentage_growth = calculate_percentage_growth(
                current_value=current_net_profit, prior_value=prior_net_profit)

            # creating result dict
            result_dict = {
                'gross_sales': {
                    'current_gross_sales': current_gross_sales,
                    'gross_sales_percentage_growth': gross_sales_percentage_growth
                },
                'gross_sales_breakdown': gross_sales_breakdown_comp,
                'other_fee': {
                    'current_other_fee': -abs(current_other_fee),
                    'other_fee_percentage_growth': other_fee_percentage_growth
                },
                'other_fee_breakdown': current_other_fee_breakdown,
                'refund': {
                    'current_refund': -abs(current_refund),
                    'refund_percentage_growth': refund_percentage_growth
                },
                'net_sales': {
                    'current_net_sales': current_net_sales,
                    'net_sales_percentage_growth': net_sales_percentage_growth
                },
                'marketplace_fee_breakdown': marketplace_fee_breakdown_comp,
                'total_cogs': {
                    'current_total_cogs': -abs(current_total_cogs),
                    'total_cogs_percentage_growth': total_cogs_percentage_growth
                },
                'net_profit': {
                    'current_net_profit': current_net_profit,
                    'net_profit_percentage_growth': net_profit_percentage_growth
                },
                'market_place_fee': {
                    'current_market_place_fee': -abs(current_market_place_fee),
                    'market_place_fee_percentage_growth': market_place_fee_percentage_growth
                },
                'reimbursement': {
                    'current_reimbursement': current_reimbursement_value,
                    'reimbursement_percentage_growth': reimbursement_percentage_growth
                },
                'reimbursement_breakdown': current_reimbursement_breakdown,
                'ads_spend': {
                    'total_ads_spend': -abs(convert_to_numeric(current_total_ads_spend)),
                    'total_ads_spend_percentage_growth': convert_to_numeric(total_ads_spend_percentage_growth)
                },
                'ads_spend_breakdown': {
                    'sponsored_brand': {
                        'current_sponsored_brand_cost': convert_to_numeric(current_sponsored_brand_cost),
                        'sponsored_brand_percentage_growth': convert_to_numeric(sponsored_brand_percentage_growth)
                    },
                    'sponsored_display': {
                        'current_sponsored_display_cost': convert_to_numeric(current_sponsored_display_cost),
                        'sponsored_display_percentage_growth': convert_to_numeric(sponsored_display_percentage_growth)
                    },
                    'sponsored_product': {
                        'sponsored_product_percentage_growth': convert_to_numeric(sponsored_product_percentage_growth),
                        'current_sponsored_product_cost': convert_to_numeric(current_sponsored_product_cost)
                    }
                }
            }

            return result_dict

        return None

    @staticmethod
    def __calculate_gross_sales_comp(account_id: str, selling_partner_id: str, from_date: str, to_date: str, prior_from_date: str, prior_to_date: str, product: Optional[tuple], brand: Optional[tuple],
                                     category: Optional[tuple]):
        """calculate current and previous metrics for gross sales comp section"""

        if not prior_from_date:
            prior_from_date, prior_to_date = get_prior_to_from_date(
                from_date=from_date, to_date=to_date)

        current_metrics = DashboardView.__get_gross_sales_comp_metrics(
            account_id=account_id, selling_partner_id=selling_partner_id, from_date=from_date, to_date=to_date, product=tuple(product), brand=tuple(brand), category=tuple(category)).fetchone()
        prior_metrics = DashboardView.__get_gross_sales_comp_metrics(
            account_id=account_id, selling_partner_id=selling_partner_id, from_date=prior_from_date, to_date=prior_to_date, product=tuple(product), brand=tuple(brand), category=tuple(category)).fetchone()
        if current_metrics:
            # gross sales
            current_gross_sales = float(current_metrics.gross_sales)
            prior_gross_sales = float(prior_metrics.gross_sales)

            gross_sales_difference = current_gross_sales - prior_gross_sales
            gross_sales_percentage_growth = calculate_percentage_growth(
                current_value=current_gross_sales, prior_value=prior_gross_sales)

            # orders
            current_unique_orders = int(current_metrics.unique_orders)
            prior_unique_orders = int(prior_metrics.unique_orders)

            unique_orders_difference = current_unique_orders - prior_unique_orders
            unique_orders_percentage_growth = calculate_percentage_growth(
                current_value=current_unique_orders, prior_value=prior_unique_orders)

            # expense
            current_expense = float(
                current_metrics.market_place_fee) + float(current_metrics.total_cogs)
            prior_expense = float(
                prior_metrics.market_place_fee) + float(prior_metrics.total_cogs)

            expense_difference = current_expense - prior_expense
            expense_percentage_growth = calculate_percentage_growth(
                current_value=current_expense, prior_value=prior_expense)

            # units sold
            current_units_sold = int(current_metrics.units_sold)
            prior_units_sold = int(prior_metrics.units_sold)

            units_sold_difference = current_units_sold - prior_units_sold
            units_sold_percentage_growth = calculate_percentage_growth(
                current_value=current_units_sold, prior_value=prior_units_sold)

            # refund
            current_refund = float(current_metrics.refund)
            prior_refund = float(prior_metrics.refund)

            refund_difference = current_refund - prior_refund
            refund_percentage_growth = calculate_percentage_growth(
                current_value=current_refund, prior_value=prior_refund)

            # net profit
            current_net_profit = current_gross_sales - current_refund - current_expense
            prior_net_profit = prior_gross_sales - prior_refund - prior_expense

            net_profit_difference = current_net_profit - prior_net_profit
            net_profit_percentage_growth = calculate_percentage_growth(
                current_value=current_net_profit, prior_value=prior_net_profit)

            # margin
            try:
                current_margin = (current_net_profit * 100) / \
                    current_gross_sales
            except ZeroDivisionError:
                current_margin = 0

            try:
                prior_margin = (prior_net_profit * 100) / prior_gross_sales
            except ZeroDivisionError:
                prior_margin = 0

            margin_difference = current_margin - prior_margin
            margin_percentage_growth = calculate_percentage_growth(
                current_value=current_margin, prior_value=prior_margin)

            # unique sku sold
            current_unique_sku_sold = int(current_metrics.unique_sku_sold)
            prior_unique_sku_sold = int(prior_metrics.unique_sku_sold)

            unique_sku_sold_difference = current_unique_sku_sold - prior_unique_sku_sold
            unique_sku_sold_percentage_growth = calculate_percentage_growth(
                current_value=current_unique_sku_sold, prior_value=prior_unique_sku_sold)

            # page views
            current_page_views = int(current_metrics.page_views)
            prior_page_views = int(prior_metrics.page_views)

            page_views_difference = current_page_views - prior_page_views
            page_views_percentage_growth = calculate_percentage_growth(
                current_value=current_page_views, prior_value=prior_page_views)

            # conversion rate
            try:
                current_conversion_rate = (
                    current_units_sold / current_page_views) * 100
            except ZeroDivisionError:
                current_conversion_rate = 0

            try:
                prior_conversion_rate = (
                    prior_units_sold / prior_page_views) * 100
            except ZeroDivisionError:
                prior_conversion_rate = 0

            conversion_rate_difference = current_conversion_rate - prior_conversion_rate
            conversion_rate_percentage_growth = calculate_percentage_growth(
                current_value=current_conversion_rate, prior_value=prior_conversion_rate)

            result = {
                'gross_sales': {
                    'current_gross_sales': convert_to_numeric(current_gross_sales),
                    'prior_gross_sales': convert_to_numeric(prior_gross_sales),
                    'gross_sales_difference': convert_to_numeric(gross_sales_difference),
                    'gross_sales_percentage_growth': convert_to_numeric(gross_sales_percentage_growth)
                },
                'unique_orders': {
                    'current_unique_orders': convert_to_numeric(current_unique_orders),
                    'prior_unique_orders': convert_to_numeric(prior_unique_orders),
                    'unique_orders_difference': convert_to_numeric(unique_orders_difference),
                    'unique_orders_percentage_growth': convert_to_numeric(unique_orders_percentage_growth)
                },
                'expense': {
                    'current_expense': -abs(convert_to_numeric(current_expense)),
                    'prior_expense': -abs(convert_to_numeric(prior_expense)),
                    'expense_difference': convert_to_numeric(expense_difference),
                    'expense_percentage_growth': convert_to_numeric(expense_percentage_growth)
                },
                'units_sold': {
                    'current_units_sold': convert_to_numeric(current_units_sold),
                    'prior_units_sold': convert_to_numeric(prior_units_sold),
                    'units_sold_difference': convert_to_numeric(units_sold_difference),
                    'units_sold_percentage_growth': convert_to_numeric(units_sold_percentage_growth)
                },
                'refund': {
                    'current_refund': -abs(convert_to_numeric(current_refund)),
                    'prior_refund': -abs(convert_to_numeric(prior_refund)),
                    'refund_difference': convert_to_numeric(refund_difference),
                    'refund_percentage_growth': convert_to_numeric(refund_percentage_growth)
                },
                'net_profit': {
                    'current_net_profit': convert_to_numeric(current_net_profit),
                    'prior_net_profit': convert_to_numeric(prior_net_profit),
                    'net_profit_difference': convert_to_numeric(net_profit_difference),
                    'net_profit_percentage_growth': convert_to_numeric(net_profit_percentage_growth)
                },
                'margin': {
                    'current_margin': convert_to_numeric(current_margin),
                    'prior_margin': convert_to_numeric(prior_margin),
                    'margin_difference': convert_to_numeric(margin_difference),
                    'margin_percentage_growth': convert_to_numeric(margin_percentage_growth)
                },
                'unique_sku_sold': {
                    'current_unique_sku_sold': convert_to_numeric(current_unique_sku_sold),
                    'prior_unique_sku_sold': convert_to_numeric(prior_unique_sku_sold),
                    'unique_sku_sold_difference': convert_to_numeric(unique_sku_sold_difference),
                    'unique_sku_sold_percentage_growth': convert_to_numeric(unique_sku_sold_percentage_growth)
                },
                'page_views': {
                    'current_page_views': convert_to_numeric(current_page_views),
                    'prior_page_views': convert_to_numeric(prior_page_views),
                    'page_views_difference': convert_to_numeric(page_views_difference),
                    'page_views_percentage_growth': convert_to_numeric(page_views_percentage_growth)
                },
                'conversion_rate': {
                    'current_conversion_rate': convert_to_numeric(current_conversion_rate),
                    'prior_conversion_rate': convert_to_numeric(prior_conversion_rate),
                    'conversion_rate_difference': convert_to_numeric(conversion_rate_difference),
                    'conversion_rate_percentage_growth': convert_to_numeric(conversion_rate_percentage_growth)
                },
            }

            return result

        return None

    @staticmethod
    def __get_marketplace_breakdown_metrics(account_id: str, selling_partner_id: str, from_date: str, to_date: str, category: Optional[tuple] = None, brand: Optional[tuple] = None, product: Optional[tuple] = None):
        """get metrics for marketplace breakdown and sales-statistics graph section"""

        prior_from_date, prior_to_date = get_prior_to_from_date(
            from_date=from_date, to_date=to_date)

        previous_year_from_date, previous_year_to_date = get_previous_year_to_from_date(
            from_date=from_date, to_date=to_date)

        from_date = datetime.strptime(from_date, '%Y-%m-%d').date()     # type: ignore  # noqa: FKA100
        to_date = datetime.strptime(to_date, '%Y-%m-%d').date()         # type: ignore  # noqa: FKA100

        raw_query = f'''
            select
            coalesce(sum(sub_query.current_gross_sales),0) as current_gross_sales,
            coalesce(sum(sub_query.prior_gross_sales),0) as prior_gross_sales,
            coalesce(sum(previous_year_gross_sales),0) as previous_year_gross_sales,
            coalesce(sum(sub_query.current_market_place_fee),0) as current_marketplace_fee,
            coalesce(sum(sub_query.prior_market_place_fee),0) as prior_marketplace_fee,
            coalesce(sum(sub_query.current_refund),0) as current_refund,
            coalesce(sum(sub_query.prior_refund),0) as prior_refund,
            coalesce(sum(sub_query.current_units_sold),0) as current_units_sold,
            coalesce(sum(sub_query.prior_units_sold),0) as prior_units_sold,
            coalesce(sum(previous_year_units_sold),0) as previous_year_units_sold,
            coalesce(sum(sub_query.current_units_returned),0) as current_units_returned,
            coalesce(sum(sub_query.prior_units_returned),0) as prior_units_returned,
            coalesce(sum(sub_query.current_total_cogs),0) as current_total_cogs,
            coalesce(sum(sub_query.prior_total_cogs),0) as prior_total_cogs,
            coalesce((sum(sub_query.current_units_returned) * 100)/nullif(sum(sub_query.current_units_sold),0)) as current_return_rate,
            coalesce((sum(sub_query.prior_units_returned) * 100)/nullif(sum(sub_query.prior_units_sold),0)) as prior_return_rate,
            coalesce(sum(sub_query.current_gross_sales)/nullif(sum(sub_query.current_units_sold),0)) as curent_aov,
            coalesce(sum(sub_query.prior_gross_sales)/nullif(sum(sub_query.prior_units_sold),0)) as prior_aov
            from
            (
                with finance_summary as (
                            select az_order_id ,
                            az_product_performance.seller_sku,
                            shipment_date,
                            refund_date,
                            case
                                when TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between :from_date and :to_date then coalesce(gross_sales,0)
                                else 0 end current_gross_sales,
                            case
                                when TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between :prior_from_date and :prior_to_date then coalesce(gross_sales,0)
                                else 0 end prior_gross_sales,
                            case
                                when TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between :previous_year_from_date and :previous_year_to_date then coalesce(gross_sales,0)
                                else 0 end previous_year_gross_sales,
                            case
                                when TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between :from_date and :to_date and units_sold>0 then coalesce(units_sold,0)
                                else 0 end current_units_sold,
                            case
                                when TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between :prior_from_date and :prior_to_date and units_sold>0 then coalesce(units_sold,0)
                                else 0 end prior_units_sold,
                            case
                                when TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between :previous_year_from_date and :previous_year_to_date and units_sold>0 then coalesce(units_sold,0)
                                else 0 end previous_year_units_sold,
                            case
                                when TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between :from_date and :to_date and units_sold > 0 then coalesce(-market_place_fee,0)
                                else 0
                                end current_market_place_fee,
                            case
                                when TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between :prior_from_date and :prior_to_date and units_sold > 0 then coalesce(-market_place_fee,0)
                                else 0
                                end prior_market_place_fee,
                            case
                                when TO_DATE(SUBSTRING(refund_date, 1, 10), :date_format) between :from_date and :to_date and
                                    TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between :from_date and :to_date and
                                    units_returned > 0 then coalesce(-returns,0)
                                    else 0
                                    end current_refund,
                            case
                                when TO_DATE(SUBSTRING(refund_date, 1, 10), :date_format) between :prior_from_date and :prior_to_date and
                                    TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between :prior_from_date and :prior_to_date and
                                    units_returned > 0 then coalesce(-returns,0)
                                    else 0
                                    end prior_refund,
                            case
                                when TO_DATE(SUBSTRING(refund_date, 1, 10), :date_format) between :from_date and :to_date and
                                    TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between :from_date and :to_date and
                                    units_returned > 0 then coalesce(units_returned,0)
                                    else 0 end current_units_returned,
                            case
                                when TO_DATE(SUBSTRING(refund_date, 1, 10), :date_format) between :prior_from_date and :prior_to_date and
                                TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between :prior_from_date and :prior_to_date and
                                units_returned > 0 then coalesce(units_returned,0)
                                else 0 end prior_units_returned

                            from az_product_performance
                            LEFT JOIN az_item_master as im on az_product_performance.seller_sku=im.seller_sku
                            where az_product_performance.account_id = :account_id and az_product_performance.asp_id = :asp_id
                            {f" AND az_product_performance.category IN :category" if category else ""}
                            {f" AND az_product_performance.brand IN :brand" if brand else ""}
                            {f" AND im.asin IN :product" if product else ""}
                            AND shipment_date is not null and
                            TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between :prior_from_date and :to_date
                        )
                        select i.asin as asin,
                                TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) as shp_date,
                                coalesce(sum(current_gross_sales),0) as current_gross_sales,
                                coalesce(sum(prior_gross_sales),0) as prior_gross_sales,
                                coalesce(sum(previous_year_gross_sales),0) as previous_year_gross_sales,
                                coalesce(sum(current_market_place_fee),0) as current_market_place_fee,
                                coalesce(sum(prior_market_place_fee),0) as prior_market_place_fee,
                                coalesce(sum(current_refund),0) as current_refund,
                                coalesce(sum(prior_refund),0) as prior_refund,
                                coalesce(sum(current_units_sold),0) as current_units_sold,
                                coalesce(sum(prior_units_sold),0) as prior_units_sold,
                                coalesce(sum(previous_year_units_sold),0) as previous_year_units_sold,
                                coalesce(sum(current_units_returned),0) as current_units_returned,
                                coalesce(sum(prior_units_returned),0) as prior_units_returned,
                                coalesce(sum((current_units_sold - current_units_returned) * cogs),0) as current_total_cogs,
                                coalesce(sum((prior_units_sold - prior_units_returned) * cogs),0) as prior_total_cogs,
                                coalesce(count(distinct az_order_id),0) as unique_orders, coalesce(count(distinct f.seller_sku),0) as unique_sku_sold
                                from finance_summary f
                                left join az_item_master i
                                on f.seller_sku = i.seller_sku and i.account_id = :account_id and i.selling_partner_id = :asp_id
                                {f" AND i.category IN :category" if category else ""}
                                {f" AND i.brand IN :brand" if brand else ""}
                                {f" AND i.asin IN :product" if product else ""}
                                group by asin,shp_date
                                ) as sub_query
                        '''

        marketplace = db.session.execute(text(raw_query), {                 # type: ignore  # noqa: FKA100
            'from_date': from_date,
            'to_date': to_date,
            'prior_from_date': prior_from_date,
            'prior_to_date': prior_to_date,
            'previous_year_from_date': previous_year_from_date,
            'previous_year_to_date': previous_year_to_date,
            'account_id': account_id,
            'asp_id': selling_partner_id,
            'date_format': 'YYYY-MM-DD',
            'category': category,
            'brand': brand,
            'product': product
        }).fetchone()

        return marketplace

    @staticmethod
    def __get_sales_stats_metrics(account_id: str, selling_partner_id: str, from_date: str, to_date: str, category: Optional[tuple] = None, brand: Optional[tuple] = None,
                                  product: Optional[tuple] = None):
        """get metrics for sales-statistics cards metrics"""

        raw_query = f'''
                select
                COALESCE(SUM(ordered_product_sales_amount),0) as gross_sales,
                COALESCE(SUM(CAST(hourly_sales->'objects'->>'orderCount' AS INTEGER)), 0) AS total_orders,
                COALESCE(SUM(units_ordered),0) as units_sold
                from az_sales_traffic_asin
                where account_id = :account_id and asp_id = :asp_id
                {f" AND az_sales_traffic_asin.category IN :category" if category else ""}
                {f" AND az_sales_traffic_asin.brand IN :brand" if brand else ""}
                {f" AND az_sales_traffic_asin.child_asin IN :product" if product else ""}
                AND payload_date between :from_date and :to_date
                AND hourly_sales IS NOT NULL
                AND hourly_sales->'objects'->>'orderCount' IS NOT NULL
                '''

        # raw_query = f'''
        #         select
        #         COALESCE(SUM(COALESCE(item_price, 0) + COALESCE(item_tax, 0)), 0) AS gross_sales,
        #         COUNT(DISTINCT amazon_order_id) AS total_orders,
        #         COALESCE(SUM(quantity),0) as units_sold
        #         from az_order_report
        #         where account_id = :account_id and selling_partner_id = :asp_id
        #         {f" AND az_order_report.category IN :category" if category else ""}
        #         {f" AND az_order_report.brand IN :brand" if brand else ""}
        #         {f" AND az_order_report.asin IN :product" if product else ""}
        #         and cast(purchase_date as DATE) between :from_date and :to_date
        #         '''

        # raw_query_v2 = '''
        #             select
        #             coalesce(ordered_product_sales_amount,0) as gross_sales,
        #             hourly_sales as hourly_sales_json,
        #             coalesce(units_ordered,0) as units_sold
        #             from az_sales_traffic_summary
        #             where account_id = :account_id and asp_id = :asp_id
        #             and cast(date as DATE) between :from_date and :to_date
        # '''

        sales_stats = db.session.execute(text(raw_query), {                 # type: ignore  # noqa: FKA100
            'from_date': from_date,
            'to_date': to_date,
            'account_id': account_id,
            'asp_id': selling_partner_id,
            'category': category,
            'brand': brand,
            'product': product
        })

        return sales_stats

    @staticmethod
    def __get_gross_sales_comp_metrics(account_id: str, selling_partner_id: str, from_date: str, to_date: str, product: Optional[tuple], brand: Optional[tuple],
                                       category: Optional[tuple]):
        """get metrics for gross sales comp section of dashboard"""

        condition_list = []
        condition_a = ''

        if category:
            condition_list.append('i.category IN :category')

        if brand:
            condition_list.append('i.brand IN :brand')

        if product:
            condition_list.append('i.asin IN :product')

        if condition_list:
            condition_a = 'where ' + ' and '.join(condition_list)

        raw_query = f'''
            select
            coalesce(sum(sub_query.page_views),0) as page_views,
            coalesce(sum(sub_query.gross_sales),0) as gross_sales,
            coalesce(sum(sub_query.market_place_fee),0) as market_place_fee,
            coalesce(sum(sub_query.refund),0) as refund,
            coalesce(sum(sub_query.units_sold),0) as units_sold,
            coalesce(sum(sub_query.units_returned),0) as units_returned,
            coalesce(sum(sub_query.total_cogs),0) as total_cogs,
            coalesce(sum(sub_query.unique_orders),0) as unique_orders,
            coalesce(count(distinct sub_query.seller_sku),0) as unique_sku_sold
            from
            (
                with finance_summary as (
                            select az_order_id ,
                            seller_sku,
                            shipment_date,
                            refund_date,
                            case
                                when TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between :from_date and :to_date then coalesce(gross_sales,0)
                                else 0 end gross_sales,
                            case
                                when TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between :from_date and :to_date and units_sold>0 then coalesce(units_sold,0)
                                else 0 end units_sold,
                            case
                                when units_sold > 0 then coalesce(-market_place_fee,0)
                                else 0
                                end market_place_fee,
                            case
                                when TO_DATE(SUBSTRING(refund_date, 1, 10), :date_format) between :from_date and :to_date and units_returned > 0 then coalesce(-returns,0)
                                else 0
                                end refund,
                            case
                                when TO_DATE(SUBSTRING(refund_date, 1, 10), :date_format) between :from_date and :to_date and units_returned > 0 then coalesce(units_returned,0)
                                else 0 end units_returned
                            from az_product_performance
                            where account_id = :account_id and asp_id = :asp_id and
                            shipment_date is not null and
                            TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between :from_date and :to_date
                        )
                        select coalesce(sum(gross_sales),0) as gross_sales, coalesce(sum(market_place_fee),0) as market_place_fee, coalesce(sum(refund),0) as refund, coalesce(sum(units_sold),0) as units_sold,
                        coalesce(sum(units_returned),0) as units_returned, coalesce(sum((units_sold - units_returned) * cogs),0) as total_cogs, max(page_views) as page_views ,
                        coalesce(count(distinct az_order_id),0) as unique_orders, max(f.seller_sku) as seller_sku                        from finance_summary f
                        left join az_item_master i
                        on f.seller_sku = i.seller_sku and i.account_id = :account_id and i.selling_partner_id = :asp_id
                        left join (
                            select
                            child_asin as asin,
                            sum(page_views) as page_views
                            FROM az_sales_traffic_asin
                            WHERE
                            account_id = :account_id
                            AND asp_id = :asp_id
                            AND payload_date BETWEEN :from_date AND :to_date
                            GROUP BY child_asin
                            ) as sub_query_az_sales_traffic_asin
                            on i.asin=sub_query_az_sales_traffic_asin.asin
                            {condition_a}
                            group by i.asin
                            ) as sub_query
                    '''

        gross_sales_comp = db.session.execute(text(raw_query), {                 # type: ignore  # noqa: FKA100
            'from_date': from_date,
            'to_date': to_date,
            'account_id': account_id,
            'asp_id': selling_partner_id,
            'date_format': 'YYYY-MM-DD',
            'category': category,
            'brand': brand,
            'product': product
        })

        return gross_sales_comp

    @staticmethod
    def __get_product_sales_rank_metrics(account_id: str, selling_partner_id: str, sort_order: str, from_date: str, to_date: str, product: Optional[tuple], brand: Optional[tuple],
                                         category: Optional[tuple], size: Optional[int] = None):
        """get metrics for sales and trends section of dashboard"""

        condition_a = ''

        if category:
            condition_a += ' AND im.category IN :category'

        if brand:
            condition_a += ' AND im.brand IN :brand'

        if product:
            condition_a += ' AND im.asin IN :product'

        condition_b = ''

        if size:
            condition_b = 'limit :limit'

        # raw_query = f'''
        #     select
        #     product_name,
        #     sku,
        #     asin,
        #     product_image,
        #     coalesce(current_gross_sales,0) as gross_sales,
        #     coalesce(current_units_sold,0) as units_sold,
        #     coalesce(current_page_views,0) as page_views
        #         from(
        #             select i.asin as asin,
        #             max(i.item_name) as product_name,
        #             max(pp.seller_sku) as sku,
        #             max(i.face_image) as product_image,
        #             sum(
        #             case
        #                 when TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between :from_date and :to_date then coalesce(pp.gross_sales,0)
        #                 else 0 end) as current_gross_sales,
        #             sum(
        #             case
        #                 when TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between  :from_date and :to_date then coalesce(pp.units_sold,0)
        #                 else 0 end) as current_units_sold,
        #             max(
        #             case
        #             when sub_query_az_sales_traffic_asin.payload_date between :from_date and :to_date then coalesce(page_views,0)
        #             else 0 end ) as current_page_views
        #             from az_product_performance pp
        #             left join az_item_master i
        #             on pp.seller_sku = i.seller_sku and i.account_id = :account_id and i.selling_partner_id = :asp_id
        #             left join (
        #                         select
        #                         child_asin as asin,
        #                         sum(page_views) as page_views,
        #                         max(payload_date) as payload_date
        #                         FROM az_sales_traffic_asin
        #                         WHERE
        #                         account_id = :account_id
        #                         AND asp_id = :asp_id
        #                         AND payload_date BETWEEN :from_date AND :to_date
        #                         GROUP BY child_asin
        #                         ) as sub_query_az_sales_traffic_asin
        #             on i.asin=sub_query_az_sales_traffic_asin.asin
        #             where pp.account_id = :account_id and pp.asp_id = :asp_id and
        #             shipment_date is not null and
        #             TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between :from_date and :to_date
        #             {condition_a}
        #             group by i.asin
        #             ) as subquery
        #             where current_gross_sales > 0
        #             order by current_gross_sales {sort_order}
        #             {condition_b}
        #                 '''

        raw_query = f'''
            SELECT
            MAX(im.item_name) as product_name,
            MAX(im.face_image) as product_image,
            MAX(im.seller_sku) as sku,
            asta.child_asin as asin,
            SUM(ordered_product_sales_amount)::numeric(12,2) as gross_sales,
            COALESCE(SUM(units_ordered), 0) AS units_sold,
            COALESCE(SUM(page_views), 0) AS page_views
            FROM az_sales_traffic_asin as asta
            LEFT join az_item_master as im
            on asta.child_asin=im.asin AND  asta.account_id=im.account_id AND asta.asp_id=im.selling_partner_id
            where asta.payload_date between :from_date AND :to_date
            AND asta.ordered_product_sales_amount > 0
            AND asta.account_id = :account_id
            AND asta.asp_id = :asp_id
            {condition_a}
            GROUP BY asta.account_id, asta.asp_id, asta.child_asin
            ORDER BY gross_sales {sort_order}
            {condition_b}
        '''

        product_sales_rank = db.session.execute(text(raw_query), {                 # type: ignore  # noqa: FKA100
            'from_date': from_date,
            'to_date': to_date,
            'account_id': account_id,
            'asp_id': selling_partner_id,
            'date_format': 'YYYY-MM-DD',
            'category': category,
            'brand': brand,
            'product': product,
            'limit': size
        })

        return product_sales_rank

    @staticmethod
    def __get_sales_trend_metrics(account_id: str, selling_partner_id: str, from_date: str, to_date: str, product: Optional[tuple], brand: Optional[tuple],
                                  category: Optional[tuple]):
        """get metrics for sales trends section of dashboard"""

        condition_list = []
        condition_a = ''

        if category:
            condition_list.append('im.category IN :category')

        if brand:
            condition_list.append('im.brand IN :brand')

        if product:
            condition_list.append('im.asin IN :product')

        if condition_list:
            condition_a = ' and ' + ' and '.join(condition_list)

        # raw_query = f'''
        #     select
        #     sub_query.asin as asin,
        #     max(sub_query.sku) as sku,
        #     max(sub_query.product_name) as product_name,
        #     max(sub_query.product_image) as product_image,
        #     coalesce(sum(sub_query.gross_sales),0) as gross_sales,
        #     coalesce(sum(sub_query.market_place_fee),0) as market_place_fee,
        #     coalesce(sum(sub_query.refund),0) as refund,
        #     coalesce(sum(sub_query.units_sold),0) as units_sold,
        #     coalesce(sum(sub_query.units_returned),0) as units_returned,
        #     coalesce(sum(sub_query.total_cogs),0) as total_cogs,
        #     coalesce(max(page_views),0) as page_views,
        #     coalesce(sum(sub_query.unique_orders),0) as unique_orders,
        #     coalesce(sum(sub_query.unique_sku_sold),0) as unique_sku_sold
        #     from
        #     (
        #     with finance_summary as (
        #                     select az_order_id ,
        #                     seller_sku,
        #                     shipment_date,
        #                     refund_date,
        #                     case
        #                         when TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between :from_date and :to_date then coalesce(gross_sales,0)
        #                         else 0 end gross_sales,
        #                     case
        #                         when TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between :from_date and :to_date and units_sold>0 then coalesce(units_sold,0)
        #                         else 0 end units_sold,
        #                     case
        #                         when units_sold > 0 then coalesce(-market_place_fee,0)
        #                         else 0
        #                         end market_place_fee,
        #                     case
        #                         when TO_DATE(SUBSTRING(refund_date, 1, 10), :date_format) between :from_date and :to_date and units_returned > 0 then coalesce(-returns,0)
        #                         else 0
        #                         end refund,
        #                     case
        #                         when TO_DATE(SUBSTRING(refund_date, 1, 10), :date_format) between :from_date and :to_date and units_returned > 0 then coalesce(units_returned,0)
        #                         else 0 end units_returned
        #                     from az_product_performance
        #                     where account_id =  :account_id and asp_id = :asp_id and
        #                     shipment_date is not null and
        #                     TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between :from_date and :to_date
        #                 )
        #                 select i.asin as asin, max(f.seller_sku) as sku, max(i.item_name) as product_name, max(i.face_image) as product_image, coalesce(sum(gross_sales),0) as gross_sales, coalesce(sum(market_place_fee),0) as market_place_fee, coalesce(sum(refund),0) as refund, coalesce(sum(units_sold),0) as units_sold,
        #                 coalesce(sum(units_returned),0) as units_returned, coalesce(sum((units_sold - units_returned) * cogs),0) as total_cogs, coalesce(max(page_views),0) as page_views ,
        #                 coalesce(count(distinct az_order_id),0) as unique_orders, coalesce(count(distinct f.seller_sku),0) as unique_sku_sold
        #                 from finance_summary f
        #                 left join az_item_master i
        #                 on f.seller_sku = i.seller_sku and i.account_id =  :account_id and i.selling_partner_id = :asp_id
        #                 left join (
        #                     select
        #                     child_asin as asin,
        #                     sum(page_views) as page_views
        #                     FROM az_sales_traffic_asin
        #                     WHERE
        #                     account_id = :account_id
        #                     AND asp_id = :asp_id
        #                     AND payload_date BETWEEN :from_date AND :to_date
        #                     GROUP BY child_asin
        #                     ) as sub_query_az_sales_traffic_asin
        #                 on i.asin=sub_query_az_sales_traffic_asin.asin
        #                 {condition_a}
        #                 group by i.asin
        #                 ) as sub_query
        #     group by asin
        # '''
        raw_query = f'''
                    SELECT
                    MAX(im.item_name) as product_name,
                    MAX(im.face_image) as product_image,
                    MAX(im.seller_sku) as sku,
                    asta.child_asin as asin,
                    SUM(ordered_product_sales_amount)::numeric(12,2) as gross_sales,
                    COALESCE(SUM(units_ordered), 0) AS units_sold,
                    COALESCE(SUM(page_views), 0) AS page_views
                    FROM az_sales_traffic_asin as asta
                    LEFT join az_item_master as im
                    on asta.child_asin=im.asin AND  asta.account_id=im.account_id AND asta.asp_id=im.selling_partner_id
                    where asta.payload_date between :from_date AND :to_date
                    AND asta.ordered_product_sales_amount > 0
                    AND asta.account_id = :account_id
                    AND asta.asp_id = :asp_id
                    {condition_a}
                    GROUP BY asta.account_id, asta.asp_id, asta.child_asin
        '''

        sales_trend = db.session.execute(text(raw_query), {                 # type: ignore  # noqa: FKA100
            'from_date': from_date,
            'to_date': to_date,
            'account_id': account_id,
            'asp_id': selling_partner_id,
            'date_format': 'YYYY-MM-DD',
            'category': category,
            'brand': brand,
            'product': product
        })

        return sales_trend

    @staticmethod
    def __get_profit_and_loss_metrics(account_id: str, selling_partner_id: str, from_date: str, to_date: str, product: Optional[tuple], brand: Optional[tuple],
                                      category: Optional[tuple]):
        """get metrics for profit and loss section of dashboard"""

        condition_list = []
        condition_a = ''

        if category:
            condition_list.append('i.category IN :category')

        if brand:
            condition_list.append('i.brand IN :brand')

        if product:
            condition_list.append('i.asin IN :product')

        if condition_list:
            condition_a = 'where ' + ' and '.join(condition_list)

        raw_query = f'''
            with finance_summary as (
                select az_order_id ,
                seller_sku,
                shipment_date,
                refund_date,
                forward_fba_fee,
                reverse_fba_fee,
                summary_analysis,
                case
                    when TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between :from_date and :to_date then coalesce(gross_sales,0)
                    else 0 end gross_sales,
                case
                    when TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between :from_date and :to_date and units_sold>0 then coalesce(units_sold,0)
                    else 0 end units_sold,
                case
                    when units_sold > 0 then coalesce(-market_place_fee,0)
                    else 0
                    end market_place_fee,
                case
                    when TO_DATE(SUBSTRING(refund_date, 1, 10), :date_format) between :from_date and :to_date and units_returned > 0 then coalesce(-returns,0)
                    else 0
                    end refund,
                case
                    when TO_DATE(SUBSTRING(refund_date, 1, 10), :date_format) between :from_date and :to_date and units_returned > 0 then coalesce(units_returned,0)
                    else 0 end units_returned
                from az_product_performance
                where account_id = :account_id and asp_id = :asp_id and
                shipment_date is not null and
                TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between :from_date and :to_date

            )
            select i.asin as asin, i.seller_sku as sku, coalesce((forward_fba_fee + reverse_fba_fee),0) as other_fee, coalesce(gross_sales,0) as gross_sales, coalesce(market_place_fee,0) as market_place_fee, coalesce(refund,0) as refund, coalesce(units_sold,0) as units_sold,
            coalesce(units_returned,0) as units_returned, coalesce((units_sold - units_returned) * cogs,0) as total_cogs, summary_analysis
            from finance_summary f
            left join az_item_master i
            on f.seller_sku = i.seller_sku and i.account_id = :account_id and i.selling_partner_id = :asp_id
            {condition_a}
        '''

        profit_and_loss = db.session.execute(text(raw_query), {                 # type: ignore  # noqa: FKA100
            'from_date': from_date,
            'to_date': to_date,
            'account_id': account_id,
            'asp_id': selling_partner_id,
            'date_format': 'YYYY-MM-DD',
            'category': category,
            'brand': brand,
            'product': product
        })

        return profit_and_loss

    @staticmethod
    def __get_sales_statistics_graph_metrics(account_id: str, selling_partner_id: str, from_date: str, to_date: str):
        """function to get metrics for bar graph in sales statistics section"""

        raw_query = '''
                with finance_summary as (
                        select
                        shipment_date,
                        gross_sales,
                        case
                            when units_sold>0 then coalesce(units_sold,0)
                            else 0 end units_sold
                        from az_product_performance
                        where account_id = :account_id and asp_id = :asp_id and
                        shipment_date is not null and
                        TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between :from_date and :to_date
                    )
                select coalesce(sum(gross_sales),0) as gross_sales, coalesce(sum(units_sold),0) as units_sold, TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) as shp_date
                from finance_summary f
                group by shp_date
        '''

        sales_statistics_graph = db.session.execute(text(raw_query), {                 # type: ignore  # noqa: FKA100
            'from_date': from_date,
            'to_date': to_date,
            'account_id': account_id,
            'asp_id': selling_partner_id,
            'date_format': 'YYYY-MM-DD'
        })

        return sales_statistics_graph

    @staticmethod
    def get_reimbursement_fee_metrics(account_id: str, selling_partner_id: str, from_date: str, to_date: str, product: Optional[tuple], brand: Optional[tuple],
                                      category: Optional[tuple]):
        """function to get reimbursement metrics for profit and loss section"""

        condition_a = ''

        if category:
            condition_a += ' AND im.category IN :category'

        if brand:
            condition_a += ' AND im.brand IN :brand'

        if product:
            condition_a += ' AND im.asin IN :product'

        raw_query = f'''
                SELECT coalesce(sum(cast(fe.finance_value as numeric)),0) as reimbursement_value, fe.finance_type as finance_type
                    FROM az_financial_event fe
                    left join az_item_master im
                    on fe.seller_sku=im.seller_sku and im.account_id=:account_id and im.selling_partner_id=:asp_id
                    where fe.account_id=:account_id and fe.asp_id =:asp_id and
                    TO_DATE(SUBSTRING(fe.posted_date, 1, 10), :date_format) between :from_date and :to_date and
                    fe.event_type in ('AdjustmentEventList') and
                    fe.finance_type in ('REVERSAL_REIMBURSEMENT', 'CS_ERROR_ITEMS', 'WAREHOUSE_DAMAGE', 'FREE_REPLACEMENT_REFUND_ITEMS', 'INCORRECT_FEES_NON_ITEMIZED',
                     'INCORRECT_FEES_ITEMS', 'PAYMENT_RETRACTION_ITEMS') and
                    fe.az_order_id is null and
                    fe.seller_sku is not null and
                    fe.finance_type is not null
                    {condition_a}
                    group by fe.finance_type
        '''

        reimbursement = db.session.execute(text(raw_query), {                 # type: ignore  # noqa: FKA100
            'from_date': from_date,
            'to_date': to_date,
            'account_id': account_id,
            'asp_id': selling_partner_id,
            'date_format': 'YYYY-MM-DD',
            'category': category,
            'brand': brand,
            'product': product
        }).fetchall()

        reimbursement_value = 0

        reimbursement_breakdown = {}

        if reimbursement:

            for finance_type in reimbursement:
                reimbursement_breakdown[finance_type.finance_type] = convert_to_numeric(
                    finance_type.reimbursement_value)
                reimbursement_value += convert_to_numeric(
                    finance_type.reimbursement_value)

        return reimbursement_breakdown, reimbursement_value

    @staticmethod
    def get_other_fee_metrics(account_id: str, selling_partner_id: str, from_date: str, to_date: str, product: Optional[tuple], brand: Optional[tuple],
                              category: Optional[tuple]):
        """function to get other fee metrics for profit and loss section"""

        condition_a = ''

        if category:
            condition_a += ' AND im.category IN :category'

        if brand:
            condition_a += ' AND im.brand IN :brand'

        if product:
            condition_a += ' AND im.asin IN :product'

        raw_query = f'''
                SELECT event_json as other_fee_json
                    FROM az_financial_event fe
                    left join az_item_master im
                    on fe.seller_sku=im.seller_sku and im.account_id=:account_id and im.selling_partner_id=:asp_id
                    where fe.account_id=:account_id and fe.asp_id =:asp_id and
                    TO_DATE(SUBSTRING(fe.posted_date, 1, 10), :date_format) between :from_date and :to_date and
                    fe.event_type in ('ServiceFeeEventList') and
                    fe.az_order_id is null and
                    fe.seller_sku is not null and
                    fe.finance_type is not null
                    {condition_a}
        '''

        service_fee_list = db.session.execute(text(raw_query), {                 # type: ignore  # noqa: FKA100
            'from_date': from_date,
            'to_date': to_date,
            'account_id': account_id,
            'asp_id': selling_partner_id,
            'date_format': 'YYYY-MM-DD',
            'category': category,
            'brand': brand,
            'product': product
        }).fetchall()

        other_fee_dict = {'FBARemovalFee': 0, 'FBAInboundTransportationFee': 0, 'MFNPostageFee': 0,
                          'Storage Fee': 0, 'StorageRenewalBilling': 0, 'Order Cancellation Charge': 0}

        for service_fee_dict in service_fee_list:

            fee_list = service_fee_dict.get('FeeList')

            if fee_list:

                for fee_type_dict in fee_list:
                    fee_type = fee_type_dict.get('FeeType')
                    amount = fee_type_dict.get(
                        'FeeAmount').get('CurrencyAmount')

                    if fee_type in other_fee_dict:
                        other_fee_dict[fee_type] += abs(amount)

        other_fee = sum(other_fee_dict.values())

        return other_fee_dict, other_fee

    @staticmethod
    def __get_ads_spend_metrics(account_id: str, selling_partner_id: str, from_date: str, to_date: str, product: Optional[tuple], brand: Optional[tuple],
                                category: Optional[tuple]):
        """function to get ads spend metrics for profit and loss section"""

        condition_a = ''
        brand_query = ''

        calculate_brand = True
        if category or product:
            calculate_brand = False

        if category:
            condition_a += ' AND im.category IN :category'
        if brand:
            condition_a += ' AND im.brand IN :brand'
            brand_list = [f'%{brand_data}%' for brand_data in brand]
            brand_query = f'and campaign_name ILIKE any(array{brand_list})'
        if product:
            condition_a += ' AND im.asin IN :product'

        # display data
        sponsored_display_raw_query = f'''
                select coalesce(sum(ds.cost * ds.clicks),0) as cost
                from az_sponsored_display ds
                left join az_item_master im
                on ds.sku = im.seller_sku
                where ds.account_id=:account_id and ds.asp_id=:asp_id and ds.payload_date between :from_date and :to_date
                {condition_a}
        '''
        sponsored_display_cost = db.session.execute(text(sponsored_display_raw_query), {                 # type: ignore  # noqa: FKA100
            'from_date': from_date,
            'to_date': to_date,
            'account_id': account_id,
            'asp_id': selling_partner_id,
            'category': category,
            'brand': brand,
            'product': product
        }).fetchone()

        # product data
        sponsored_product_raw_query = f'''
                select coalesce(sum(spend),0) as cost
                from az_sponsored_product pr
                left join az_item_master im
                ON im.seller_sku=pr.advertised_sku
                where pr.account_id=:account_id and pr.asp_id=:asp_id and pr.payload_date between :from_date and :to_date
                {condition_a}
        '''
        sponsored_product_cost = db.session.execute(text(sponsored_product_raw_query), {                 # type: ignore  # noqa: FKA100
            'from_date': from_date,
            'to_date': to_date,
            'account_id': account_id,
            'asp_id': selling_partner_id,
            'category': category,
            'brand': brand,
            'product': product
        }).fetchone()

        if calculate_brand:
            # brand data

            sponsored_brand_raw_query = f'''
                select coalesce(sum(cost * clicks),0) as cost
                from az_sponsored_brand
                where account_id=:account_id and asp_id=:asp_id and payload_date between :from_date and :to_date
                {brand_query}
            '''

            sponsored_brand_cost = db.session.execute(text(sponsored_brand_raw_query), {                 # type: ignore  # noqa: FKA100
                'from_date': from_date,
                'to_date': to_date,
                'account_id': account_id,
                'asp_id': selling_partner_id
            }).fetchone()

            return sponsored_brand_cost.cost, sponsored_display_cost.cost, sponsored_product_cost.cost

        return 0, sponsored_display_cost.cost, sponsored_product_cost.cost

    @staticmethod
    def __get_profit_loss_graph_metrics(account_id: str, selling_partner_id: str, from_date: str, to_date: str, product: Optional[tuple], brand: Optional[tuple],
                                        category: Optional[tuple]):
        """function to get metrics for graph in profit and loss section"""

        condition_list = []
        condition_a = ''

        if category:
            condition_list.append('i.category IN :category')

        if brand:
            condition_list.append('i.brand IN :brand')

        if product:
            condition_list.append('i.asin IN :product')

        if condition_list:
            condition_a = 'where ' + ' and '.join(condition_list)

        raw_query = f'''
                with finance_summary as (
                select az_order_id ,
                seller_sku,
                shipment_date,
                refund_date,
                case
                    when TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between :from_date and :to_date then coalesce(gross_sales,0)
                    else 0 end gross_sales,
                case
                    when TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between :from_date and :to_date and units_sold>0 then coalesce(units_sold,0)
                    else 0 end units_sold,
                case
                    when units_sold > 0 then coalesce(-market_place_fee,0)
                    else 0
                    end market_place_fee,
                case
                    when TO_DATE(SUBSTRING(refund_date, 1, 10), :date_format) between :from_date and :to_date and units_returned > 0 then coalesce(-returns,0)
                    else 0
                    end refund,
                case
                    when TO_DATE(SUBSTRING(refund_date, 1, 10), :date_format) between :from_date and :to_date and units_returned > 0 then coalesce(units_returned,0)
                    else 0 end units_returned
                from az_product_performance
                where account_id = :account_id and asp_id = :asp_id and
                shipment_date is not null and
                TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) between :from_date and :to_date
            )
            select TO_DATE(SUBSTRING(shipment_date, 1, 10), :date_format) as shp_date, coalesce(sum(gross_sales),0) as gross_sales, coalesce(sum(market_place_fee),0) as market_place_fee, coalesce(sum(refund),0) as refund, coalesce(sum(units_sold),0) as units_sold,
            coalesce(sum(units_returned),0) as units_returned, coalesce(sum((units_sold - units_returned) * cogs),0) as total_cogs
            from finance_summary f
            left join az_item_master i
            on f.seller_sku = i.seller_sku and i.account_id = :account_id and i.selling_partner_id = :asp_id
            {condition_a}
            group by shp_date
        '''

        profit_loss_graph = db.session.execute(text(raw_query), {                 # type: ignore  # noqa: FKA100
            'from_date': from_date,
            'to_date': to_date,
            'account_id': account_id,
            'asp_id': selling_partner_id,
            'date_format': 'YYYY-MM-DD',
            'category': category,
            'brand': brand,
            'product': product
        }).fetchall()

        return profit_loss_graph

    @staticmethod
    def __get_sales_statistics_bar_graph_metrics(account_id: str, selling_partner_id: str, from_date: str, to_date: str, category: Optional[tuple] = None, brand: Optional[tuple] = None, product: Optional[tuple] = None):
        """method to calculate bar graph metrics"""

        if category or brand or product:
            raw_query = f'''
                    select
                    payload_date as date,
                    coalesce(SUM(ordered_product_sales_amount),0) as gross_sales,
                    coalesce(SUM(units_ordered),0) as units_sold
                    from az_sales_traffic_asin
                    where account_id = :account_id and asp_id = :asp_id
                    {f" AND az_sales_traffic_asin.category IN :category" if category else ""}
                    {f" AND az_sales_traffic_asin.brand IN :brand" if brand else ""}
                    {f" AND az_sales_traffic_asin.child_asin IN :product" if product else ""}
                    and payload_date between :from_date and :to_date
                    GROUP BY payload_date
                    order by payload_date
                    '''
        else:
            raw_query = f'''
                    select
                    date,
                    coalesce(ordered_product_sales_amount,0) as gross_sales,
                    coalesce(units_ordered,0) as units_sold
                    from az_sales_traffic_summary
                    where account_id = :account_id and asp_id = :asp_id
                    {f" AND az_sales_traffic_summary.brand IN :brand" if brand else ""}
                    and date between :from_date and :to_date
                    order by date
                    '''

        sales_statistics_graph = db.session.execute(text(raw_query), {                 # type: ignore  # noqa: FKA100
            'from_date': from_date,
            'to_date': to_date,
            'account_id': account_id,
            'asp_id': selling_partner_id,
            'category': category,
            'brand': brand,
            'product': product
        }).fetchall()

        return sales_statistics_graph

    @staticmethod
    def __get_gross_sales_units_sold_bar_graph(account_id: str, selling_partner_id: str, from_date: str, to_date: str, category: Optional[tuple] = None, brand: Optional[tuple] = None, product: Optional[tuple] = None):
        """method to calculate gross sales and units sold for bar graph section"""

        if category or brand or product:
            raw_query = f'''
                    select
                    coalesce(SUM(ordered_product_sales_amount),0) as gross_sales,
                    coalesce(SUM(units_ordered),0) as units_sold
                    from az_sales_traffic_asin
                    where account_id = :account_id and asp_id = :asp_id
                    {f" AND az_sales_traffic_asin.category IN :category" if category else ""}
                    {f" AND az_sales_traffic_asin.brand IN :brand" if brand else ""}
                    {f" AND az_sales_traffic_asin.child_asin IN :product" if product else ""}
                    and payload_date between :from_date and :to_date
                    '''
        else:
            raw_query = f'''
                    select
                    coalesce(sum(ordered_product_sales_amount),0) as gross_sales,
                    coalesce(sum(units_ordered),0) as units_sold
                    from az_sales_traffic_summary
                    where account_id = :account_id and asp_id = :asp_id
                    {f" AND az_sales_traffic_summary.brand IN :brand" if brand else ""}
                    and date between :from_date and :to_date
                    '''

        metrics = db.session.execute(text(raw_query), {                 # type: ignore  # noqa: FKA100
            'from_date': from_date,
            'to_date': to_date,
            'account_id': account_id,
            'asp_id': selling_partner_id,
            'category': category,
            'brand': brand,
            'product': product
        }).fetchone()

        return metrics

    @staticmethod
    def __get_sales_stats_by_hour(account_id: str, asp_id: str, from_date: str, to_date: str, category: Optional[tuple] = None, brand: Optional[tuple] = None, product: Optional[tuple] = None):
        """Query to fetch sales per hour"""

        from_date = datetime.strptime(from_date, '%Y-%m-%d').date()     # type: ignore  # noqa: FKA100
        to_date = datetime.strptime(to_date, '%Y-%m-%d').date()         # type: ignore  # noqa: FKA100

        raw_query = f'''
                    SELECT
                        hourly_sales->>'result' as sales_data
                    FROM
                        az_sales_traffic_asin
                    WHERE account_id = :account_id and asp_id = :asp_id
                    AND payload_date BETWEEN :from_date and :to_date
                    {f" AND az_sales_traffic_asin.category IN :category" if category else ""}
                    {f" AND az_sales_traffic_asin.brand IN :brand" if brand else ""}
                    {f" AND az_sales_traffic_asin.child_asin IN :product" if product else ""}
                    AND hourly_sales IS NOT NULL
                    AND hourly_sales->'result' IS NOT NULL
                    '''

        sales_stat = db.session.execute(text(raw_query), {                 # type: ignore  # noqa: FKA100
            'from_date': from_date,
            'to_date': to_date,
            'account_id': account_id,
            'asp_id': asp_id,
            'category': category,
            'brand': brand,
            'product': product
        }).fetchall()

        sales_stat = [row[0] for row in sales_stat]
        return sales_stat

    @staticmethod
    def __get_sales_statistics_bar_graph_metrics_order_report(account_id: str, selling_partner_id: str, from_date: str, to_date: str, category: Optional[tuple] = None, brand: Optional[tuple] = None, product: Optional[tuple] = None):
        """method to calculate bar graph metrics"""
        """Source has been now changed from order reports to az_sales_traffic_asin"""

        raw_query = f'''
                select
                payload_date as date,
                COALESCE(SUM(ordered_product_sales_amount),0) as gross_sales,
                COALESCE(SUM(CAST(hourly_sales->'objects'->>'orderCount' AS INTEGER)), 0) AS total_orders,
                COALESCE(SUM(units_ordered),0) as units_sold
                from az_sales_traffic_asin
                where account_id = :account_id and asp_id = :asp_id
                AND payload_date between :from_date and :to_date
                AND hourly_sales IS NOT NULL
                AND hourly_sales->'objects'->>'orderCount' IS NOT NULL
                {f" AND az_sales_traffic_asin.category IN :category" if category else ""}
                {f" AND az_sales_traffic_asin.brand IN :brand" if brand else ""}
                {f" AND az_sales_traffic_asin.child_asin IN :product" if product else ""}
                group by payload_date
                '''

        # raw_query = f'''
        #         select
        #         cast(purchase_date AS DATE) as date,
        #         COALESCE(SUM(COALESCE(item_price, 0) + COALESCE(item_tax, 0)), 0) AS gross_sales,
        #         coalesce(SUM(quantity),0) as units_sold
        #         from az_order_report
        #         where account_id = :account_id and selling_partner_id = :asp_id
        #         {f" AND az_order_report.category IN :category" if category else ""}
        #         {f" AND az_order_report.brand IN :brand" if brand else ""}
        #         {f" AND az_order_report.asin IN :product" if product else ""}
        #         and cast(purchase_date AS DATE) between :from_date and :to_date
        #         group by date
        #         order by date
        #         '''

        sales_statistics_graph = db.session.execute(text(raw_query), {                 # type: ignore  # noqa: FKA100
            'from_date': from_date,
            'to_date': to_date,
            'account_id': account_id,
            'asp_id': selling_partner_id,
            'category': category,
            'brand': brand,
            'product': product
        }).fetchall()

        return sales_statistics_graph
