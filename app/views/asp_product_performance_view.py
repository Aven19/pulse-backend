"""Contains Amazon Product related API definitions."""

import csv
from datetime import datetime
import io

from app import config_data
from app import export_csv_q
from app import logger
from app.helpers.constants import AzProductPerformanceColumn
from app.helpers.constants import AzRefundInsightsColumn
from app.helpers.constants import AzSalesByRegionColumn
from app.helpers.constants import EntityType
from app.helpers.constants import FBA_RETURNS_MAX_REFUND_CLAIM_DAYS
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import PAGE_DEFAULT
from app.helpers.constants import PAGE_LIMIT
from app.helpers.constants import QueueName
from app.helpers.constants import QueueTaskStatus
from app.helpers.constants import ResponseMessageKeys
from app.helpers.decorators import api_time_logger
from app.helpers.decorators import brand_filter
from app.helpers.decorators import token_required
from app.helpers.utility import convert_to_numeric
from app.helpers.utility import enum_validator
from app.helpers.utility import field_type_validator
from app.helpers.utility import format_float_values
from app.helpers.utility import get_current_date
from app.helpers.utility import get_pagination_meta
from app.helpers.utility import required_validator
from app.helpers.utility import send_json_response
from app.models.az_product_performance import AzProductPerformance
from app.models.queue_task import QueueTask
from dateutil import parser
from flask import request
from flask import Response
from pytz import utc
from workers.export_csv_data_worker import ExportCsvDataWorker


class AspProductPerformanceView:
    """class for Product Performance view"""

    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def get_product_performance(user_object, account_object, allowed_brands):  # type: ignore  # noqa: C901
        """Endpoint for getting product performance details"""
        try:
            account_id = account_object.uuid
            asp_id = account_object.asp_id

            marketplace = request.args.get('marketplace')
            from_date = request.args.get('from_date')
            to_date = request.args.get('to_date')
            category = request.args.getlist('category')
            brand = request.args.getlist('brand') if 'brand' in request.args and request.args.getlist(
                'brand') else allowed_brands
            product = request.args.getlist('product')
            page = request.args.get(key='page', default=PAGE_DEFAULT)
            size = request.args.get(key='size', default=PAGE_LIMIT)
            sort_order = request.args.get('sort_order')
            sort_by = request.args.get('sort_by')

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
            if page:
                params['page'] = page
            if size:
                params['size'] = size
            if sort_order:
                params['sort_order'] = sort_order
            if sort_by:
                params['sort_by'] = sort_by

            field_types = {'marketplace': str, 'from_date': str, 'to_date': str, 'product': list,
                           'brand': list, 'category': list, 'page': int, 'size': int, 'sort_order': str, 'sort_by': str}

            required_fields = ['marketplace', 'from_date', 'to_date']

            enum_fields = {
                'marketplace': (marketplace, 'ProductMarketplace'),
                'sort_order': (sort_order, 'SortingOrder'),
                'sort_by': (sort_by, 'AzProductPerformanceColumn')
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

            items, total_count, total_request_count = AzProductPerformance.get_performance(
                account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date, category=tuple(category), brand=tuple(brand), product=tuple(product),
                sort_order=sort_order, sort_by=AzProductPerformanceColumn.get_column(sort_by), page=page, size=size)

            data = {}

            if items:
                result = []
                for get_item in items:
                    _asin = get_item.asin
                    _sku = get_item.seller_sku if get_item.seller_sku is not None else None
                    _item_name = get_item.item_name.strip() if get_item.item_name is not None else None
                    _item_image = get_item.face_image.strip(
                    ) if get_item.face_image is not None else None
                    _brand = get_item.brand.strip() if get_item.brand is not None else None
                    _category = get_item.category.strip() if get_item.category is not None else None

                    current_gross_sales = float(
                        get_item.total_gross_sales) if get_item.total_gross_sales is not None else 0.0
                    current_units_sold = int(
                        get_item.total_units_sold) if get_item.total_units_sold is not None else 0
                    current_refunds = float(
                        get_item.total_refunds) if get_item.total_refunds is not None else 0.0
                    current_units_returned = int(
                        get_item.total_units_returned) if get_item.total_units_returned is not None else 0
                    current_return_rate = float(
                        get_item.returns_rate) if get_item.returns_rate is not None else 0.0
                    current_market_place_fee = float(
                        get_item.market_place_fee) if get_item.market_place_fee is not None else 0.0
                    current_cogs = float(
                        get_item.cogs) if get_item.cogs is not None else 0.0
                    current_profit = float(
                        get_item.profit) if get_item.profit is not None else 0.0
                    current_margin = float(
                        get_item.margin) if get_item.margin is not None else 0.0
                    current_roi = float(
                        get_item.current_roi) if get_item.current_roi is not None else 0.0
                    current_order_count = int(
                        get_item.unique_orders) if get_item.unique_orders is not None else 0
                    average_selling_price = float(
                        get_item.average_selling_price) if get_item.average_selling_price is not None else 0.0

                    gross_sales_growth = float(
                        get_item.total_gross_sales_percentage_growth) if get_item.total_gross_sales_percentage_growth is not None else 0.0
                    units_sold_growth = int(
                        get_item.total_units_sold_percentage_growth) if get_item.total_units_sold_percentage_growth is not None else 0
                    refunds_growth = float(
                        get_item.total_refunds_percentage) if get_item.total_refunds_percentage is not None else 0.0
                    units_returned_growth = int(
                        get_item.total_units_returned_percentage_growth) if get_item.total_units_returned_percentage_growth is not None else 0
                    return_rate_growth = float(
                        get_item.returns_rate_growth) if get_item.returns_rate_growth is not None else 0.0
                    market_place_fee_growth = float(
                        get_item.total_market_place_fee_percentage_growth) if get_item.total_market_place_fee_percentage_growth is not None else 0.0
                    cogs_growth = float(
                        get_item.cogs_percentage_growth) if get_item.cogs_percentage_growth is not None else 0.0
                    profit_growth = float(
                        get_item.profit_percentage_growth) if get_item.profit_percentage_growth is not None else 0.0
                    growth_margin = float(
                        get_item.growth_margin) if get_item.growth_margin is not None else 0.0
                    growth_roi = float(
                        get_item.growth_roi) if get_item.growth_roi is not None else 0.0
                    growth_order_count = float(
                        get_item.total_unique_orders_percentage_growth) if get_item.total_unique_orders_percentage_growth is not None else 0.0
                    average_selling_price_growth = float(
                        get_item.average_selling_price_growth) if get_item.average_selling_price_growth is not None else 0.0

                    item_data = {
                        '_asin': _asin,
                        '_sku': _sku,
                        '_category': _category,
                        '_brand': _brand,
                        '_product_name': _item_name,
                        '_product_image': _item_image,
                        'asp': {
                            'current': average_selling_price,
                            'growth_percentage': average_selling_price_growth,
                        },
                        'roi': {
                            'current': current_roi,
                            'growth_percentage': growth_roi,
                        },
                        'margin': {
                            'current': current_margin,
                            'growth_percentage': growth_margin,
                        },
                        'cogs': {
                            'current': current_cogs,
                            'growth_percentage': cogs_growth,
                        },
                        'gross_sales': {
                            'current': current_gross_sales,
                            'growth_percentage': gross_sales_growth,
                        },
                        'units_sold': {
                            'current': current_units_sold,
                            'growth_percentage': units_sold_growth,
                        },
                        'refunds': {
                            'current': current_refunds,
                            'growth_percentage': refunds_growth,
                        },
                        'units_returned': {
                            'current': current_units_returned,
                            'growth_percentage': units_returned_growth,
                        },
                        'return_rate': {
                            'current': current_return_rate,
                            'growth_percentage': return_rate_growth,
                        },
                        'market_place_fee': {
                            'current': current_market_place_fee,
                            'growth_percentage': market_place_fee_growth,
                        },
                        'profit': {
                            'current': current_profit,
                            'growth_percentage': profit_growth,
                        },
                        'total_orders': {
                            'current': current_order_count,
                            'growth_percentage': growth_order_count,
                        }
                    }
                    result.append(item_data)

                get_all_performance_total = AzProductPerformance.get_performance(
                    account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date, category=tuple(category), brand=tuple(brand), product=tuple(product),
                    sort_order=sort_order, sort_by=AzProductPerformanceColumn.get_column(sort_by), page=page, size=size, calculate_total=True)

                total_performance_dict = {
                    'totals': {
                        'gross_sales': 0,
                        'total_orders': 0,
                        'units_sold': 0,
                        'refunds': 0,
                        'units_returned': 0,
                        'return_rate': 0,
                        'market_place_fee': 0,
                        'cogs': 0,
                        'profit': 0,
                        'asp': 0,
                        'margin': 0,
                    }
                }

                fields_to_sum = ['gross_sales', 'total_orders',
                                 'refunds', 'market_place_fee', 'cogs']
                for field in fields_to_sum:
                    total_performance_dict['totals'][field] = sum(
                        float(getattr(pt, field, 0)) if getattr(
                            pt, field, None) is not None else 0
                        for pt in get_all_performance_total
                    )

                total_units_sold = sum(int(
                    pt.total_units_sold) if pt.total_units_sold is not None else 0 for pt in get_all_performance_total)
                total_units_returned = sum(int(
                    pt.total_units_returned) if pt.total_units_returned is not None else 0 for pt in get_all_performance_total)

                total_gross_sales = total_performance_dict['totals']['gross_sales']
                total_refunds = total_performance_dict['totals']['refunds']
                market_place_fee = total_performance_dict['totals']['market_place_fee']
                cogs = total_performance_dict['totals']['cogs']

                total_performance_dict['totals']['profit'] = total_gross_sales - \
                    abs(total_refunds) - abs(market_place_fee) - abs(cogs)
                total_performance_dict['totals']['margin'] = total_performance_dict['totals']['profit'] / \
                    total_gross_sales - \
                    abs(total_refunds) * 100 if (total_gross_sales
                                                 - abs(total_refunds)) != 0 else 0.0
                total_performance_dict['totals']['units_sold'] = total_units_sold
                total_performance_dict['totals']['units_returned'] = total_units_returned
                total_performance_dict['totals']['asp'] = (
                    total_gross_sales / total_units_sold) if total_units_sold != 0 else 0
                total_performance_dict['totals']['return_rate'] = (
                    total_units_returned / total_units_sold) * 100 if total_units_sold != 0 else 0

                objects = {
                    'totals': format_float_values(total_performance_dict['totals']),
                    'pagination_metadata': get_pagination_meta(current_page=1 if page is None else int(page), page_size=int(size), total_items=total_request_count)
                }

                data = {
                    'result': result,
                    'objects': objects
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
                f'GET -> Failed getting product performance: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    def get_product_performance_day_graph(user_object, account_object):
        """Endpoint for getting product performance details graph data day wise"""
        try:
            account_id = account_object.uuid
            asp_id = account_object.asp_id

            marketplace = request.args.get('marketplace')
            from_date = request.args.get('from_date')
            to_date = request.args.get('to_date')
            seller_sku = request.view_args.get('seller_sku')

            field_types = {'marketplace': str, 'from_date': str,
                           'to_date': str, 'seller_sku': str}

            required_fields = ['marketplace', 'from_date', 'to_date']

            enum_fields = {
                'marketplace': (marketplace, 'ProductMarketplace')
            }

            valid_enum = enum_validator(enum_fields)

            if valid_enum['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=valid_enum['data'])

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

            items = AzProductPerformance.get_performance_day_detail(
                account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date, seller_sku=seller_sku)

            if items:
                result = []
                for get_item in items:
                    item_data = {
                        'date': get_item.shipped_date.strftime('%Y-%m-%d'),
                        'gross_sales': float(get_item.total_gross_sales) if get_item.total_gross_sales is not None else 0.0,
                        'refunds': float(get_item.total_refunds) if get_item.total_refunds is not None else 0.0,
                        'units_sold': int(get_item.total_units_sold) if get_item.total_units_sold is not None else 0,
                        'units_returned': int(get_item.total_units_returned) if get_item.total_units_returned is not None else 0,
                        'asp': float(get_item.average_selling_price) if get_item.average_selling_price is not None else 0.0,
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
                f'GET -> Failed getting product performance graph data day wise: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    def get_product_performance_heatmap(user_object, account_object):
        """Endpoint for getting product performance heatmap"""

        try:
            account_id = account_object.uuid
            asp_id = account_object.asp_id

            marketplace = request.args.get('marketplace')
            from_date = request.args.get('from_date')
            to_date = request.args.get('to_date')
            seller_sku = request.view_args.get('seller_sku')

            field_types = {'marketplace': str, 'from_date': str,
                           'to_date': str, 'seller_sku': str}

            required_fields = ['marketplace', 'from_date', 'to_date']

            enum_fields = {
                'marketplace': (marketplace, 'ProductMarketplace')
            }

            valid_enum = enum_validator(enum_fields)

            if valid_enum['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=valid_enum['data'])

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

            regional_sales = AzProductPerformance.get_heatmap(
                account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date, sku=seller_sku)

            if regional_sales:

                region_stats_object = {'sku': seller_sku,
                                       'total_units_sold': 0, 'total_gross_sales': 0, 'total_units_returned': 0, 'total_refunds': 0}

                region_stats_list = []

                for region in regional_sales:

                    region_stats = {}

                    units_sold = region.total_units_sold if region.total_units_sold else 0
                    gross_sales = region.total_gross_sales if region.total_gross_sales else 0
                    units_returned = region.total_units_returned if region.total_units_returned else 0
                    refunds = region.total_refunds if region.total_refunds else 0
                    state = region.state if region.state else 'Unallocated'
                    zone = region.zone if region.zone else 'Unallocated'

                    region_stats['units_sold'] = convert_to_numeric(units_sold)
                    region_stats['gross_sales'] = convert_to_numeric(
                        gross_sales)
                    region_stats['units_returned'] = convert_to_numeric(
                        units_returned)
                    region_stats['refunds'] = convert_to_numeric(refunds)
                    region_stats['state'] = state
                    region_stats['zone'] = zone

                    region_stats_object['total_units_sold'] += convert_to_numeric(
                        units_sold)
                    region_stats_object['total_gross_sales'] += convert_to_numeric(
                        gross_sales)
                    region_stats_object['total_units_returned'] += convert_to_numeric(
                        units_returned)
                    region_stats_object['total_refunds'] += convert_to_numeric(
                        refunds)

                    region_stats_list.append(region_stats)

                result_dict = {'object': {
                    'regional_sales_statistics': region_stats_object}, 'result': region_stats_list}

                return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=result_dict, error=None)

            return send_json_response(
                http_status=404,
                response_status=False,
                message_key=ResponseMessageKeys.NO_DATA_FOUND.value,
                error=None
            )

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed getting product performance heatmap: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def get_product_sales_by_region(user_object, account_object, allowed_brands):  # type: ignore  # noqa: C901
        """Endpoint for getting various metrics for product performance section"""

        try:
            account_id = account_object.uuid
            asp_id = account_object.asp_id

            from_date = request.args.get('from_date')
            to_date = request.args.get('to_date')
            marketplace = request.args.get('marketplace')
            pincode = request.args.get(key='pincode', default=None)
            state_name = request.args.get(key='state_name', default=None)
            category = request.args.getlist('category')   # type: ignore  # noqa: FKA100
            brand = request.args.getlist('brand') if 'brand' in request.args and request.args.getlist('brand') else allowed_brands  # type: ignore  # noqa: FKA100
            product = request.args.getlist('product')  # type: ignore  # noqa: FKA100

            sort_order = request.args.get(key='sort_order', default=None)
            sort_by = request.args.get(key='sort_by', default=None)

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

            if pincode:
                params['pincode'] = pincode
            if state_name:
                params['state_name'] = state_name

            if sort_order:
                params['sort_order'] = sort_order
            if sort_by:
                params['sort_by'] = sort_by

            field_types = {'marketplace': str, 'from_date': str, 'to_date': str,
                           'category': list, 'brand': list, 'product': list,
                           'pincode': int, 'state_name': str,
                           'sort_order': str, 'sort_by': str}

            required_fields = ['marketplace', 'from_date', 'to_date']

            enum_fields = {
                'marketplace': (marketplace, 'ProductMarketplace'),
                'sort_order': (sort_order, 'SortingOrder'),
                'sort_by': (sort_by, 'AzSalesByRegionColumn')
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

            filters = {}
            if pincode:
                filters['pincode'] = pincode
            if state_name:
                filters['state_name'] = state_name

            regional_sales, total_count, total_request_count = AzProductPerformance.get_sales_by_region(account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date,
                                                                                                        filters=filters, sort_by=AzSalesByRegionColumn.get_column(sort_by), sort_order=sort_order,
                                                                                                        category=tuple(category), brand=tuple(brand), product=tuple(product))

            data = {}
            if regional_sales:
                result = []
                zonal_sales_list = []
                sum_gross_sales = 0
                sum_units_sold = 0
                sum_refund_sales = 0
                sum_units_returned = 0

                for sales in regional_sales:
                    # asin = sales.asin.strip() if sales.asin is not None else ''
                    state_name = sales.state.strip() if sales.state is not None else ''
                    zone = sales.zone.strip() if sales.zone is not None else ''

                    gross_sales_value = float(
                        sales.total_gross_sales) if sales.total_gross_sales is not None else 0.0

                    units_sold_value = int(
                        sales.total_units_sold) if sales.total_units_sold is not None else 0

                    refunds_value = float(
                        sales.total_refunds) if sales.total_refunds is not None else 0.0

                    units_returned_value = int(
                        sales.total_units_returned) if sales.total_units_returned is not None else 0

                    abs_refunds_value = abs(
                        float(sales.total_refunds)) if sales.total_refunds is not None else 0.0

                    market_place_fee = float(
                        sales.market_place_fee) if sales.market_place_fee is not None else 0.0

                    # abs_market_place_fee = abs(float(
                    #     sales.market_place_fee)) if sales.market_place_fee is not None else 0.0

                    cogs = float(
                        sales.cogs) if sales.cogs is not None else 0.0

                    total_expense = market_place_fee + cogs

                    net_profit = gross_sales_value - \
                        abs_refunds_value - abs(total_expense)

                    # For prior
                    prior_gross_sales_value = float(
                        sales.prior_total_gross_sales) if sales.prior_total_gross_sales is not None else 0.0

                    abs_prior_refunds_value = abs(float(
                        sales.prior_total_refunds)) if sales.prior_total_refunds is not None else 0.0

                    prior_units_sold_value = int(
                        sales.prior_total_units_sold) if sales.prior_total_units_sold is not None else 0

                    prior_units_returned_value = int(
                        sales.prior_total_units_returned) if sales.prior_total_units_returned is not None else 0

                    prior_market_place_fee = float(
                        sales.prior_market_place_fee) if sales.prior_market_place_fee is not None else 0.0

                    # abs_prior_market_place_fee = abs(float(
                    #     sales.prior_market_place_fee)) if sales.prior_market_place_fee is not None else 0.0

                    prior_cogs = float(
                        sales.prior_cogs) if sales.prior_cogs is not None else 0.0

                    prior_total_expense = prior_market_place_fee + prior_cogs

                    prior_net_profit = prior_gross_sales_value - \
                        abs_prior_refunds_value - abs(prior_total_expense)

                    product_data = {
                        '_zone': zone,
                        '_state_name': state_name,
                        'gross_sales': {
                            'current': gross_sales_value,
                            'percentage_growth': (gross_sales_value - prior_gross_sales_value) / prior_gross_sales_value * 100 if prior_gross_sales_value is not None and prior_gross_sales_value != 0 and gross_sales_value is not None and gross_sales_value != 0 else 0.0
                        },
                        'units_sold': {
                            'current': units_sold_value,
                            'percentage_growth': (units_sold_value - prior_units_sold_value) / prior_units_sold_value * 100 if prior_units_sold_value is not None and prior_units_sold_value != 0 and units_sold_value is not None and units_sold_value != 0 else 0
                        },
                        'refunds': {
                            'current': refunds_value,
                            'percentage_growth': (abs_refunds_value - abs_prior_refunds_value) / abs_prior_refunds_value * 100 if abs_prior_refunds_value is not None and abs_prior_refunds_value != 0 and abs_refunds_value is not None and abs_refunds_value != 0 else 0.0
                        },
                        'units_returned': {
                            'current': units_returned_value,
                            'percentage_growth': (units_returned_value - prior_units_returned_value) / prior_units_returned_value * 100 if prior_units_returned_value is not None and prior_units_returned_value != 0 and units_returned_value is not None and units_returned_value != 0 else 0
                        },
                        'expenses': {
                            'current': total_expense,
                            'percentage_growth': (total_expense - prior_total_expense) / prior_total_expense * 100 if prior_total_expense is not None and prior_total_expense != 0 and total_expense is not None and total_expense != 0 else 0.0
                        },
                        'net_profit': {
                            'current': net_profit,
                            'percentage_growth': (net_profit - prior_net_profit) / prior_net_profit * 100 if prior_net_profit is not None and prior_net_profit != 0 and net_profit is not None and net_profit != 0 else 0.0
                        }
                    }

                    sum_gross_sales += gross_sales_value
                    sum_refund_sales += refunds_value
                    sum_units_sold += units_sold_value
                    sum_units_returned += units_returned_value
                    result.append(product_data)

                for product_data in result:
                    state_name = product_data['_state_name']
                    product_data_gross_sales = product_data['gross_sales']['current']
                    product_data_refunds = product_data['refunds']['current']
                    product_data_units_sold = product_data['units_sold']['current']
                    product_data_units_returned = product_data['units_returned']['current']

                    product_data['gross_sales']['percentange_over_total_gross'] = product_data_gross_sales / \
                        sum_gross_sales * 100 if sum_gross_sales != 0 else 0.0
                    product_data['refunds']['percentange_over_total_gross'] = product_data_refunds / \
                        sum_refund_sales * 100 if sum_refund_sales != 0 else 0.0

                    product_data['units_sold']['percentange_over_total_gross'] = product_data_units_sold / \
                        sum_units_sold * 100 if sum_units_sold != 0 else 0
                    product_data['units_returned']['percentange_over_total_gross'] = product_data_units_returned / \
                        sum_units_returned * 100 if sum_units_returned != 0 else 0

                objects = {}

                zonal_sales_statistics, zonal_total_count, zonal_total_request_count = AzProductPerformance.get_sales_by_region(account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date,
                                                                                                                                zone=True, filters=filters, sort_by=AzSalesByRegionColumn.get_column(sort_by), sort_order=sort_order,
                                                                                                                                category=tuple(category), brand=tuple(brand), product=tuple(product))

                zonal_sales_list = []
                zonal_total_gross_sales = 0
                zonal_total_refund_sales = 0
                zonal_total_units_sold = 0
                zonal_total_units_returned = 0

                if zonal_sales_statistics:
                    for zone_stats in zonal_sales_statistics:

                        _gross_sales_value = float(
                            zone_stats.total_gross_sales) if zone_stats.total_gross_sales is not None else 0.0

                        _refunds_value = float(
                            zone_stats.total_refunds) if zone_stats.total_refunds is not None else 0.0

                        _units_sold_value = int(
                            zone_stats.total_units_sold) if zone_stats.total_units_sold is not None else 0

                        _units_returned_value = int(
                            zone_stats.total_units_returned) if zone_stats.total_units_returned is not None else 0

                        _prior_gross_sales_value = float(
                            zone_stats.prior_total_gross_sales) if zone_stats.prior_total_gross_sales is not None else 0.0

                        _prior_refunds_value = float(
                            zone_stats.prior_total_refunds) if zone_stats.prior_total_refunds is not None else 0.0

                        _prior_units_sold_value = int(
                            zone_stats.prior_total_units_sold) if zone_stats.prior_total_units_sold is not None else 0

                        _prior_units_returned_value = int(
                            zone_stats.prior_total_units_returned) if zone_stats.prior_total_units_returned is not None else 0

                        zonal_sales_dict = {
                            'zone': zone_stats.zone if zone_stats.zone is not None else 'Unallocated',
                            'gross_sales': {
                                'current': float(_gross_sales_value) if _gross_sales_value is not None else 0.0,
                                'percentage_growth': (_gross_sales_value - _prior_gross_sales_value) / _prior_gross_sales_value * 100 if _prior_gross_sales_value is not None and _prior_gross_sales_value != 0 and _gross_sales_value is not None and _gross_sales_value != 0 else 0.0
                            },
                            'refunds': {
                                'current': float(_refunds_value) if _refunds_value is not None else 0.0,
                                'percentage_growth': (abs(_refunds_value) - abs(_prior_refunds_value)) / abs(_prior_refunds_value) * 100 if _prior_refunds_value is not None and _prior_refunds_value != 0 and _refunds_value is not None and _refunds_value != 0 else 0.0
                            },
                            'units_sold': {
                                'current': int(_units_sold_value) if _units_sold_value is not None else 0,
                                'percentage_growth': (_units_sold_value - _prior_units_sold_value) / _prior_units_sold_value * 100 if _prior_units_sold_value is not None and _prior_units_sold_value != 0 and _units_sold_value is not None and _units_sold_value != 0 else 0
                            },
                            'units_returned': {
                                'current': int(_units_returned_value) if _units_returned_value is not None else 0,
                                'percentage_growth': (_units_returned_value - _prior_units_returned_value) / _prior_units_returned_value * 100 if _prior_units_returned_value is not None and _prior_units_returned_value != 0 and _units_returned_value is not None and _units_returned_value != 0 else 0
                            }
                        }
                        zonal_sales_list.append(zonal_sales_dict)
                        zonal_total_gross_sales += _gross_sales_value
                        zonal_total_refund_sales += _refunds_value
                        zonal_total_units_sold += _units_sold_value
                        zonal_total_units_returned += _units_returned_value

                objects.update({
                    'zonal_sales_statistics': zonal_sales_list,
                    'zonal_total_gross_sales': zonal_total_gross_sales,
                    'zonal_total_refund_sales': zonal_total_refund_sales,
                    'zonal_total_units_sold': zonal_total_units_sold,
                    'zonal_total_units_returned': zonal_total_units_returned
                })

                data = {'results': result, 'objects': objects}

                return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=data, error=None)

            return send_json_response(
                http_status=404,
                response_status=False,
                message_key=ResponseMessageKeys.NO_DATA_FOUND.value,
                error=None
            )

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed getting Sales By Region Data: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def export(user_object, account_object, allowed_brands):   # type: ignore  # noqa: C901
        """Endpoint for getting product performance details"""
        try:
            logged_in_user = user_object.id
            account_id = account_object.uuid
            asp_id = account_object.asp_id

            marketplace = request.args.get('marketplace')
            from_date = request.args.get('from_date')
            to_date = request.args.get('to_date')
            category = request.args.getlist('category')
            brand = request.args.getlist('brand') if 'brand' in request.args and request.args.getlist(
                'brand') else allowed_brands
            product = request.args.getlist('product')
            sort_order = request.args.get('sort_order')
            sort_by = request.args.get('sort_by')

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

            field_types = {'marketplace': str, 'from_date': str, 'to_date': str, 'product': list,
                           'brand': list, 'category': list, 'page': int, 'size': int, 'sort_order': str, 'sort_by': str}

            required_fields = ['marketplace', 'from_date', 'to_date']

            enum_fields = {
                'marketplace': (marketplace, 'ProductMarketplace'),
                'sort_order': (sort_order, 'SortingOrder'),
                'sort_by': (sort_by, 'AzProductPerformanceColumn')
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

            data = {
                'logged_in_user': logged_in_user,
                'account_id': account_id,
                'asp_id': asp_id
            }

            data.update(params)

            queue_task = QueueTask.add_queue_task(queue_name=QueueName.EXPORT_CSV,
                                                  account_id=account_id,
                                                  owner_id=logged_in_user,
                                                  status=QueueTaskStatus.NEW.value,
                                                  entity_type=EntityType.PRODUCT_PERFORMANCE.value,
                                                  param=str(data), input_attachment_id=None, output_attachment_id=None)

            if queue_task:
                queue_task_dict = {
                    'job_id': queue_task.id,
                    'queue_name': queue_task.queue_name,
                    'status': QueueTaskStatus.get_status(queue_task.status),
                    'entity_type': EntityType.get_type(queue_task.entity_type)
                }
                data.update(queue_task_dict)
                queue_task.param = str(data)
                queue_task.save()

                export_csv_q.enqueue(ExportCsvDataWorker.export_csv, data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100

                return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.EXPORT_QUEUED.value, data=queue_task_dict, error=None)

            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed getting product performance: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def get_refund_insights(user_object, account_object, allowed_brands):   # type: ignore  # noqa: C901
        """Endpoint for getting Refund Insights"""
        try:
            account_id = account_object.uuid
            asp_id = account_object.asp_id

            from_date = request.args.get('from_date')
            to_date = request.args.get('to_date')
            marketplace = request.args.get('marketplace')
            page = request.args.get(key='page', default=PAGE_DEFAULT)
            size = request.args.get(key='size', default=PAGE_LIMIT)
            sort_order = request.args.get(key='sort_order', default=None)
            sort_by = request.args.get(key='sort_by', default=None)
            brand = request.args.getlist('brand') if 'brand' in request.args and request.args.getlist(
                'brand') else allowed_brands

            # validation
            params = {}
            if marketplace:
                params['marketplace'] = marketplace
            if from_date:
                params['from_date'] = from_date
            if to_date:
                params['to_date'] = to_date
            if page:
                params['page'] = page
            if size:
                params['size'] = size
            if sort_order:
                params['sort_order'] = sort_order
            if sort_by:
                params['sort_by'] = sort_by
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

            field_types = {'marketplace': str, 'from_date': str, 'to_date': str,
                           'page': int, 'size': int,
                           'sort_order': str, 'sort_by': str}

            required_fields = ['marketplace']

            enum_fields = {
                'marketplace': (marketplace, 'ProductMarketplace'),
                'sort_order': (sort_order, 'SortingOrder'),
                'sort_by': (sort_by, 'AzRefundInsightsColumn')
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

            get_unclaimed_refunds, total_count, total_request_count = AzProductPerformance.get_unclaimed_refunds(
                account_id=account_id, asp_id=asp_id, sort_by=AzRefundInsightsColumn.get_column(sort_by), sort_order=sort_order, page=page, size=size, brand=tuple(brand))

            if get_unclaimed_refunds:
                result = []
                for unclaimed_refund in get_unclaimed_refunds:
                    item_data = {
                        # 'az_order_id': unclaimed_refund.az_order_id,
                        'report_date': get_current_date(),
                        'report_type': unclaimed_refund.report_type,
                        'seller': unclaimed_refund.seller,
                        'refundable_items': int(unclaimed_refund.refundable_items) if unclaimed_refund.refundable_items is not None else 0,
                        'potential_refund': float(unclaimed_refund.potential_refund) if unclaimed_refund.potential_refund is not None else 0
                        # 'potential_refund': float(unclaimed_refund.return_value - unclaimed_refund.reverse_fba_fee) * 0.25 if unclaimed_refund.return_value is not None else 0.0
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
                f'GET -> Failed getting Refund Insights: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def export_unclaimend_report(user_object, account_object, allowed_brands):   # type: ignore  # noqa: C901
        """Endpoint for exporting Refund Insights by az_order_id"""
        try:
            account_id = account_object.uuid
            asp_id = account_object.asp_id

            # az_order_id = request.args.get('az_order_id')
            report_type = request.args.get('report_type')
            marketplace = request.args.get('marketplace')
            brand = request.args.getlist('brand') if 'brand' in request.args and request.args.getlist(
                'brand') else allowed_brands

            # validation
            params = {}
            if marketplace:
                params['marketplace'] = marketplace
            # if az_order_id:
            #     params['az_order_id'] = az_order_id
            if report_type:
                params['report_type'] = report_type
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

            field_types = {'marketplace': str, 'report_type': str}

            required_fields = ['marketplace', 'report_type']

            enum_fields = {
                'marketplace': (marketplace, 'ProductMarketplace'),
                'report_type': (report_type, 'AzFbaReturnsReportType')
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

            get_unclaimed_refunds, total_count, total_request_count = AzProductPerformance.get_unclaimed_refunds(
                account_id=account_id, asp_id=asp_id, report_type=report_type)

            result = []

            if get_unclaimed_refunds:
                for unclaimed_refund in get_unclaimed_refunds:
                    return_date = parser.isoparse(unclaimed_refund.report_date)
                    current_date = datetime.now(utc)
                    fc_id = ''
                    if unclaimed_refund.fc_id is not None:
                        fc_id = unclaimed_refund.fc_id
                    elif unclaimed_refund.shipment_fc_id is not None:
                        fc_id = unclaimed_refund.shipment_fc_id

                    fnsku = ''
                    if unclaimed_refund.fnsku is not None:
                        fnsku = unclaimed_refund.fnsku
                    elif unclaimed_refund.shipment_fnsku is not None:
                        fnsku = unclaimed_refund.shipment_fnsku

                    item = {
                        'Order date': unclaimed_refund.order_date,
                        'Return Date': unclaimed_refund.report_date,
                        'Order id': unclaimed_refund.az_order_id,
                        'Item Name': unclaimed_refund.item_name,
                        'ASIN': unclaimed_refund.asin,
                        'SKU': unclaimed_refund.seller_sku,
                        'FNSKU': fnsku,
                        'Return Value': float(unclaimed_refund.return_value) if unclaimed_refund.return_value is not None else 0.0,
                        'Return Qty': int(unclaimed_refund.refundable_items) if unclaimed_refund.refundable_items is not None else 0,
                        'Return Notes': unclaimed_refund.return_notes if unclaimed_refund.return_notes is not None else '',
                        'Return Reason': unclaimed_refund.return_reason if unclaimed_refund.return_reason is not None else '',
                        'FC id': fc_id,
                        'Potential Refund Value': float(unclaimed_refund.potential_refund) if unclaimed_refund.potential_refund is not None else 0.0,
                        # 'Days after refund day': (current_date - return_date).days,
                        'Days left to claim': FBA_RETURNS_MAX_REFUND_CLAIM_DAYS - (current_date - return_date).days
                    }
                    result.append(item)

                output = io.StringIO()
                csv_writer = csv.DictWriter(output, fieldnames=item.keys())

                # Write the CSV header
                csv_writer.writeheader()

                # Write the data rows
                csv_writer.writerows(result)

                # Create a response with the CSV data
                response = Response(output.getvalue(), content_type='text/csv')
                response.headers[
                    'Content-Disposition'] = f'attachment; filename={report_type}-claim-refund.csv'

                return response

            return send_json_response(
                http_status=404,
                response_status=False,
                message_key=ResponseMessageKeys.NO_DATA_FOUND.value,
                error=None
            )

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed Exporting Refund Insights: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )
