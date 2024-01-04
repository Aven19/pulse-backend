"""Class containing definitions related to marketing reports."""

from datetime import datetime
from typing import Optional

from app import config_data
from app import db
from app import export_csv_q
from app import logger
from app.helpers.constants import EntityType
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import PAGE_DEFAULT
from app.helpers.constants import PAGE_LIMIT
from app.helpers.constants import QueueName
from app.helpers.constants import QueueTaskStatus
from app.helpers.constants import ResponseMessageKeys
from app.helpers.decorators import api_time_logger
from app.helpers.decorators import brand_filter
from app.helpers.decorators import token_required
from app.helpers.queue_helper import add_queue_task_and_enqueue
from app.helpers.utility import convert_string_to_datetime
from app.helpers.utility import convert_to_numeric
from app.helpers.utility import date_to_string
from app.helpers.utility import enum_validator
from app.helpers.utility import field_type_validator
from app.helpers.utility import get_first_last_date
from app.helpers.utility import get_pagination_meta
from app.helpers.utility import get_prior_to_from_date
from app.helpers.utility import is_same_month_year
from app.helpers.utility import required_validator
from app.helpers.utility import send_json_response
from app.models.az_performance_zone import AzPerformanceZone
from app.models.az_product_performance import AzProductPerformance
from app.models.az_sponsored_brand import AzSponsoredBrand
from app.models.az_sponsored_display import AzSponsoredDisplay
from app.models.az_sponsored_product import AzSponsoredProduct
from app.models.queue_task import QueueTask
from app.views.dashboard_view import DashboardView
from flask import request
from sqlalchemy import text
from workers.export_csv_data_worker import ExportCsvDataWorker


class MarketingReportView:
    """class for Marketing Report View"""

    @staticmethod
    def get_ad_impact_statistics(account_id: str, asp_id: str, from_date: str, to_date: str, category: Optional[tuple] = None, brand: Optional[tuple] = None,
                                 product: Optional[tuple] = None):
        """Get Ad Impact data - sales, spends, etc"""

        from_date = datetime.strptime(from_date, '%Y-%m-%d').date()     # type: ignore  # noqa: FKA100
        to_date = datetime.strptime(to_date, '%Y-%m-%d').date()         # type: ignore  # noqa: FKA100

        params = {
            'from_date': from_date,
            'to_date': to_date,
            'account_id': account_id,
            'asp_id': asp_id,
            'category': category,
            'brand': brand,
            'product': product
        }

        raw_query = f'''
                    SELECT
                    max(page_views) as page_views,
                    sum(pp.gross_sales) as total_gross_sales,
                    SUM(pp.units_sold) as total_units_sold,
                    (SUM(pp.units_sold) - SUM(pp.units_returned)) * MAX(im.cogs) AS cogs,
                    SUM(market_place_fee)::Numeric(10,2) as amazon_fees,
                    SUM(CASE WHEN pp.summary_analysis->>'_other_fee' IS NOT NULL
                    THEN (pp.summary_analysis->>'_other_fee')::NUMERIC(10,2) ELSE 0 END) AS other_fees,
                    SUM(case
                        WHEN CAST(pp.refund_date AS DATE) between :from_date and :to_date and pp.units_returned > 0 then coalesce(pp.returns,0)
                        ELSE 0
                    END) as refund
                    from az_product_performance as pp
                    LEFT JOIN
                    az_item_master as im
                    on pp.seller_sku=im.seller_sku
                    LEFT JOIN (
                            SELECT
                            child_asin as asin,
                            sum(page_views) as page_views
                            FROM az_sales_traffic_asin
                            WHERE
                                account_id = '{account_id}'
                                AND asp_id = '{asp_id}'
                                AND payload_date BETWEEN :from_date AND :to_date
                            GROUP BY child_asin
                    ) as sub_query_az_sales_traffic_asin
                    on im.asin=sub_query_az_sales_traffic_asin.asin
                    WHERE
                    pp.account_id = '{account_id}'
                    AND pp.asp_id = '{asp_id}'
                    AND
                    pp.units_sold!=0
                    AND CAST(pp.shipment_date AS DATE) BETWEEN '{from_date}' AND '{to_date}'
        '''

        if category:
            raw_query += ' AND im.category IN :category'

        if brand:
            raw_query += ' AND im.brand IN :brand'

        if product:
            raw_query += ' AND im.asin IN :product'

        results = db.session.execute(text(raw_query + ' group by im.asin'), params).fetchall()  # type: ignore  # noqa: FKA100

        total_count_result = len(results)

        return results, total_count_result

    @staticmethod
    def __get_reimbursement_fee_metrics(account_id: str, selling_partner_id: str, from_date: str, to_date: str, product: Optional[tuple], brand: Optional[tuple],
                                        category: Optional[tuple]):
        """function to get reimbursement metrics"""

        condition_a = ''

        if category:
            condition_a += ' AND im.category IN :category'

        if brand:
            condition_a += ' AND im.brand IN :brand'

        if product:
            condition_a += ' AND im.asin IN :product'

        raw_query = f'''
                SELECT coalesce(sum(cast(fe.finance_value as numeric)),0) as reimbursement_value
                    FROM az_financial_event fe
                    left join az_item_master im
                    on fe.seller_sku=im.seller_sku and im.account_id=:account_id and im.selling_partner_id=:asp_id
                    where fe.account_id=:account_id and fe.asp_id =:asp_id and
                    TO_DATE(SUBSTRING(fe.posted_date, 1, 10), :date_format) between :from_date and :to_date and
                    fe.event_type not in ('RefundEventList','ShipmentEventList','ServiceFeeEventList') and
                    fe.finance_type = 'REVERSAL_REIMBURSEMENT' and
                    fe.az_order_id is null and
                    fe.seller_sku is not null and
                    fe.finance_type is not null
                    {condition_a}
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
        }).fetchone()

        return reimbursement.reimbursement_value

    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def get_ad_graph_data(user_object, account_object, allowed_brands):
        """endpoint for updating cog of one item"""
        try:
            account_id = account_object.uuid
            asp_id = account_object.asp_id

            from_date = request.args.get('from_date')
            to_date = request.args.get('to_date')
            marketplace = request.args.get('marketplace')
            category = request.args.getlist('category')
            brand = request.args.getlist('brand') if 'brand' in request.args and request.args.getlist(
                'brand') else allowed_brands
            product = request.args.getlist('product')

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

            field_types = {'marketplace': str, 'from_date': str, 'to_date': str,
                           'category': list, 'brand': list, 'product': list,
                           'pincode': int, 'state_name': str, }

            required_fields = ['marketplace', 'from_date', 'to_date']

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

            total_gross_sales = 0.0
            cogs = 0.0
            amazon_fees = 0.0
            other_fees = 0.0

            ad_summary_report, total_request_count = MarketingReportView.get_ad_impact_statistics(
                account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))

            if ad_summary_report:
                organic_sales = 0.0
                net_profit = 0.0
                refund = 0.0
                for get_ad_report in ad_summary_report:
                    _total_gross_sales = float(
                        get_ad_report.total_gross_sales) if get_ad_report.total_gross_sales is not None else 0.0
                    _cogs = float(
                        get_ad_report.cogs) if get_ad_report.cogs is not None else 0.0
                    _amazon_fees = float(
                        get_ad_report.amazon_fees) if get_ad_report.amazon_fees is not None else 0.0
                    _amazon_fees = float(
                        get_ad_report.amazon_fees) if get_ad_report.amazon_fees is not None else 0.0
                    _other_fees = float(
                        get_ad_report.other_fees) if get_ad_report.other_fees is not None else 0.0
                    _refund = float(
                        get_ad_report.refund) if get_ad_report.refund is not None else 0.0
                    total_gross_sales += _total_gross_sales
                    cogs += _cogs
                    amazon_fees += _amazon_fees
                    other_fees += _other_fees
                    refund += _refund

                total_ad_sales, total_ad_spends, total_ad_impressions, total_ad_clicks = MarketingReportView.get_ad_totals(
                    account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))

                organic_sales = total_gross_sales - total_ad_sales

                # getting other fee from dashboard view
                other_fees_breakdown, other_fees = DashboardView.get_other_fee_metrics(account_id=account_id, selling_partner_id=asp_id, from_date=from_date,
                                                                                       to_date=to_date, product=tuple(product), brand=tuple(brand), category=tuple(category))

                # getting reimbursement value from dashboard view
                reimbursement_breakdown, reimbursement_value = DashboardView.get_reimbursement_fee_metrics(
                    account_id=account_id, selling_partner_id=asp_id, from_date=from_date, to_date=to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))

                # calculating net_profit
                net_profit = organic_sales + total_ad_sales + reimbursement_value - \
                    abs(refund) - abs(cogs) - abs(amazon_fees) - \
                    total_ad_spends - abs(_other_fees)

                prepare_data = {
                    'total_gross_sales': total_gross_sales,
                    'ad_sales': total_ad_sales,
                    'ad_spends': -abs(total_ad_spends),
                    'cogs': -abs(cogs),
                    'amazon_fees': amazon_fees,
                    'other_fees': other_fees,
                    'organic_sales': organic_sales,
                    'net_profit': net_profit,
                    'other_revenue': reimbursement_value,
                    'refund': refund
                }

                data = {
                    'result': prepare_data
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
                f'GET -> Failed Fetching AD graph Data: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def get_sales_period(user_object, account_object, allowed_brands):
        """Endpoint for marketing report sales period"""
        try:
            account_id = account_object.uuid
            asp_id = account_object.asp_id

            from_date = request.args.get('from_date')
            to_date = request.args.get('to_date')
            marketplace = request.args.get('marketplace')
            category = request.args.getlist('category')
            brand = request.args.getlist('brand') if 'brand' in request.args and request.args.getlist(
                'brand') else allowed_brands
            product = request.args.getlist('product')

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

            field_types = {'marketplace': str, 'from_date': str, 'to_date': str,
                           'category': list, 'brand': list, 'product': list,
                           'pincode': int, 'state_name': str, }

            required_fields = ['marketplace', 'from_date', 'to_date']

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

            total_ad_sales, total_ad_spends, total_ad_impressions, total_ad_clicks = MarketingReportView.get_ad_totals(
                account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))

            prior_from_date, prior_to_date = get_prior_to_from_date(
                from_date=from_date, to_date=to_date)

            yesterday_total_ad_sales, yesterday_total_ad_spends, yesterday_total_ad_impressions, yesterday_total_ad_clicks = MarketingReportView.get_ad_totals(
                account_id=account_id, asp_id=asp_id, from_date=prior_from_date, to_date=prior_to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))

            ad_summary_report, total_request_count = MarketingReportView.get_ad_impact_statistics(
                account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))

            total_gross_sales = 0
            total_units_sold = 0
            ctr = 0
            conversion_rate = 0
            cpc = 0
            roas = 0
            acos = 0
            tacos = 0
            total_page_views = 0

            if ad_summary_report:
                for get_ad_report in ad_summary_report:
                    _total_gross_sales = float(
                        get_ad_report.total_gross_sales) if get_ad_report.total_gross_sales is not None else 0.0
                    _total_units_sold = int(
                        get_ad_report.total_units_sold) if get_ad_report.total_units_sold is not None else 0
                    total_gross_sales += _total_gross_sales
                    total_units_sold += _total_units_sold
                    total_page_views += int(
                        get_ad_report.page_views) if get_ad_report.page_views is not None else 0

            ctr = int(total_ad_clicks) / \
                int(total_ad_impressions) if int(
                    total_ad_impressions) > 0 else 0.0

            conversion_rate = int(
                total_units_sold) / int(total_page_views) if total_page_views > 0 else 0.0
            cpc = total_ad_spends / \
                int(total_ad_clicks) if int(total_ad_clicks) > 0 else 0.0
            roas = total_ad_sales / total_ad_spends if total_ad_spends > 0 else 0.0
            acos = (total_ad_spends
                    / total_ad_sales) if total_ad_sales > 0 else 0.0
            tacos = (total_ad_spends
                     / total_gross_sales) if total_gross_sales > 0 else 0.0

            yesterday_ad_summary_report, yesterday_total_request_count = MarketingReportView.get_ad_impact_statistics(
                account_id=account_id, asp_id=asp_id, from_date=prior_from_date, to_date=prior_to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))

            total_gross_sales_yesterday = 0
            total_units_sold_yesterday = 0
            total_page_views_yesterday = 0
            ctr_yesterday = 0
            conversion_rate_yesterday = 0
            cpc_yesterday = 0
            roas_yesterday = 0
            acos_yesterday = 0
            tacos_yesterday = 0

            if yesterday_ad_summary_report:
                for get_yesterday_ad_report in yesterday_ad_summary_report:
                    _total_gross_sales_yesterday = float(
                        get_yesterday_ad_report.total_gross_sales) if get_yesterday_ad_report.total_gross_sales is not None else 0.0
                    _total_units_sold_yesterday = int(
                        get_yesterday_ad_report.total_units_sold) if get_yesterday_ad_report.total_units_sold is not None else 0.0
                    total_gross_sales_yesterday += _total_gross_sales_yesterday
                    total_units_sold_yesterday += _total_units_sold_yesterday
                    total_page_views_yesterday += int(
                        get_yesterday_ad_report.page_views) if get_yesterday_ad_report.page_views is not None else 0

            ctr_yesterday = int(yesterday_total_ad_clicks) / int(
                yesterday_total_ad_impressions) if int(yesterday_total_ad_impressions) > 0 else 0.0

            conversion_rate = int(
                total_units_sold) / int(total_page_views) if total_page_views > 0 else 0.0

            conversion_rate_yesterday = int(total_units_sold_yesterday) / int(
                total_page_views_yesterday) if int(total_page_views_yesterday) > 0 else 0.0

            cpc_yesterday = yesterday_total_ad_spends / \
                int(yesterday_total_ad_clicks) if int(
                    yesterday_total_ad_clicks) > 0 else 0.0
            roas_yesterday = yesterday_total_ad_sales / \
                yesterday_total_ad_spends if yesterday_total_ad_spends > 0 else 0.0
            acos_yesterday = (yesterday_total_ad_spends
                              / yesterday_total_ad_sales) if yesterday_total_ad_sales > 0 else 0.0
            tacos_yesterday = (yesterday_total_ad_spends
                               / total_gross_sales_yesterday) if total_gross_sales_yesterday > 0 else 0.0

            data = {
                'result': {
                    'total_sales': {
                        'current': total_gross_sales,
                        'prior': total_gross_sales_yesterday,
                        'difference': total_gross_sales - total_gross_sales_yesterday,
                        'growth_percentage': (total_gross_sales - total_gross_sales_yesterday) / total_gross_sales_yesterday * 100 if total_gross_sales_yesterday is not None and total_gross_sales_yesterday != 0 else 0
                    },
                    'total_ad_sales': {
                        'current': total_ad_sales,
                        'prior': yesterday_total_ad_sales,
                        'difference': total_ad_sales - yesterday_total_ad_sales,
                        'growth_percentage': (total_ad_sales - yesterday_total_ad_sales) / yesterday_total_ad_sales * 100 if yesterday_total_ad_sales is not None and yesterday_total_ad_sales != 0 else 0
                    },
                    'total_units_sold': {
                        'current': total_units_sold,
                        'prior': total_units_sold_yesterday,
                        'difference': total_units_sold - total_units_sold_yesterday,
                        'growth_percentage': (total_units_sold - total_units_sold_yesterday) / total_units_sold_yesterday * 100 if total_units_sold_yesterday is not None and total_units_sold_yesterday != 0 else 0
                    },
                    'total_ad_spends': {
                        'current': total_ad_spends,
                        'prior': yesterday_total_ad_spends,
                        'difference': total_ad_spends - yesterday_total_ad_spends,
                        'growth_percentage': (total_ad_spends - yesterday_total_ad_spends) / yesterday_total_ad_spends * 100 if yesterday_total_ad_spends is not None and yesterday_total_ad_spends != 0 else 0
                    },
                    'total_ad_impressions': {
                        'current': total_ad_impressions,
                        'prior': yesterday_total_ad_impressions,
                        'difference': total_ad_impressions - yesterday_total_ad_impressions,
                        'growth_percentage': (total_ad_impressions - yesterday_total_ad_impressions) / yesterday_total_ad_impressions * 100 if yesterday_total_ad_impressions is not None and yesterday_total_ad_impressions != 0 else 0
                    },
                    'total_page_views': {
                        'current': total_page_views,
                        'prior': total_page_views_yesterday,
                        'difference': total_page_views - total_page_views_yesterday,
                        'growth_percentage': (total_page_views - total_page_views_yesterday) / total_page_views_yesterday * 100 if total_page_views_yesterday is not None and total_page_views_yesterday != 0 else 0
                    },
                    'total_ad_clicks': {
                        'current': int(total_ad_clicks),
                        'prior': int(yesterday_total_ad_clicks),
                        'difference': int(total_ad_clicks) - int(yesterday_total_ad_clicks),
                        'growth_percentage': (int(total_ad_clicks) - int(yesterday_total_ad_clicks)) / int(yesterday_total_ad_clicks) * 100 if int(yesterday_total_ad_clicks) is not None and int(yesterday_total_ad_clicks) != 0 else 0
                    },
                    'ctr': {
                        'current': ctr * 100,  # for Percentage multiplied by 100
                        'prior': ctr_yesterday * 100,  # for Percentage multiplied by 100
                        # for Percentage multiplied by 100
                        'difference': (ctr - ctr_yesterday) * 100,
                        'growth_percentage': (ctr - ctr_yesterday) / ctr_yesterday * 100 if ctr_yesterday is not None and ctr_yesterday != 0 else 0
                    },
                    'conversion_rate': {
                        'current': conversion_rate * 100,
                        'prior': conversion_rate_yesterday * 100,
                        'difference': (conversion_rate - conversion_rate_yesterday) * 100,
                        'growth_percentage': (conversion_rate - conversion_rate_yesterday) / conversion_rate_yesterday * 100 if conversion_rate_yesterday is not None and conversion_rate_yesterday != 0 else 0
                    },
                    'cpc': {
                        'current': cpc,
                        'prior': cpc_yesterday,
                        'difference': cpc - cpc_yesterday,
                        'growth_percentage': (cpc - cpc_yesterday) / cpc_yesterday * 100 if cpc_yesterday is not None and cpc_yesterday != 0 else 0
                    },
                    'roas': {
                        'current': roas,
                        'prior': roas_yesterday,
                        'difference': roas - roas_yesterday,
                        'growth_percentage': (roas - roas_yesterday) / roas_yesterday * 100 if roas_yesterday is not None and roas_yesterday != 0 else 0
                    },
                    'acos': {
                        'current': acos * 100,
                        'prior': acos_yesterday * 100,
                        'difference': (acos - acos_yesterday) * 100,
                        'growth_percentage': (acos - acos_yesterday) / acos_yesterday * 100 if acos_yesterday is not None and acos_yesterday != 0 else 0
                    },
                    'tacos': {
                        'current': tacos * 100,
                        'prior': tacos_yesterday * 100,
                        'difference': (tacos - tacos_yesterday) * 100,
                        'growth_percentage': (tacos - tacos_yesterday) / tacos_yesterday * 100 if tacos_yesterday is not None and tacos_yesterday != 0 else 0
                    }
                }
            }

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=data, error=None)

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed while fetching Marketing report Sales Period  data: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def get_costvs_metrics(user_object, account_object, allowed_brands):  # type: ignore  # noqa: C901
        """Endpoint for marketing report sales period"""
        try:
            account_id = account_object.uuid
            asp_id = account_object.asp_id

            from_date = request.args.get('from_date')
            to_date = request.args.get('to_date')
            marketplace = request.args.get('marketplace')
            category = request.args.getlist('category')
            brand = request.args.getlist('brand') if 'brand' in request.args and request.args.getlist(
                'brand') else allowed_brands
            product = request.args.getlist('product')

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

            field_types = {'marketplace': str, 'from_date': str, 'to_date': str,
                           'category': list, 'brand': list, 'product': list,
                           'pincode': int, 'state_name': str, }

            required_fields = ['marketplace', 'from_date', 'to_date']

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

            get_product_ad_data = AzSponsoredProduct.get_ad_summary_by_day(
                account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))
            get_display_ad_data = AzSponsoredDisplay.get_ad_summary_by_day(
                account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))

            get_mr_sales_info = AzProductPerformance.get_mr_sales_info(
                account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))

            mr_sales_info_map = {date_to_string(
                mr_sales_info.shipment_date): mr_sales_info for mr_sales_info in get_mr_sales_info}

            ad_summary = []

            all_ad_data = get_product_ad_data + get_display_ad_data

            for ad_data in all_ad_data:
                date = date_to_string(ad_data.payload_date)
                item = {
                    '_date': date,
                    'total_ad_sales': float(ad_data.sales) if ad_data.sales is not None and ad_data.sales != 0 else 0.0,
                    'total_ad_impressions': int(ad_data.impressions) if ad_data.impressions is not None and ad_data.impressions != 0 else 0,
                    'total_ad_clicks': int(ad_data.clicks) if ad_data.clicks is not None and ad_data.clicks != 0 else 0,
                    'total_ad_spends': float(ad_data.spends) if ad_data.spends is not None and ad_data.spends != 0 else 0.0
                }

                found = False
                for summary_item in ad_summary:
                    if summary_item['_date'] == date:
                        found = True
                        # Update the existing summary with new values
                        summary_item['total_ad_sales'] += item['total_ad_sales']
                        summary_item['total_ad_impressions'] += item['total_ad_impressions']
                        summary_item['total_ad_clicks'] += item['total_ad_clicks']
                        summary_item['total_ad_spends'] += item['total_ad_spends']

                        # Merge the mr_sales_info data if available
                        if date in mr_sales_info_map:
                            mr_sales_entry = mr_sales_info_map[date]
                            summary_item['total_gross_sales'] = float(
                                mr_sales_entry.gross_sales) if mr_sales_entry.gross_sales is not None and mr_sales_entry.gross_sales != 0 else 0.0
                            summary_item['total_page_views'] = int(
                                mr_sales_entry.page_views) if mr_sales_entry.page_views is not None and mr_sales_entry.page_views != 0 else 0
                            summary_item['total_units_sold'] = int(
                                mr_sales_entry.total_units_sold) if mr_sales_entry.total_units_sold is not None and mr_sales_entry.total_units_sold != 0 else 0

                        break

                if not found:
                    ad_summary.append(item)

            # Add missing dates from mr_sales_info_map to ad_summary
            for date, mr_sales_entry in mr_sales_info_map.items():
                if date not in [summary_item['_date'] for summary_item in ad_summary]:
                    item = {
                        '_date': date,
                        'total_ad_sales': 0.0,
                        'total_ad_impressions': 0,
                        'total_ad_clicks': 0,
                        'total_ad_spends': 0.0,
                        'total_gross_sales': float(mr_sales_entry.gross_sales) if mr_sales_entry.gross_sales is not None and mr_sales_entry.gross_sales != 0 else 0.0,
                        'total_page_views': int(mr_sales_entry.page_views) if mr_sales_entry.page_views is not None and mr_sales_entry.page_views != 0 else 0,
                        'total_units_sold': int(
                            mr_sales_entry.total_units_sold) if mr_sales_entry.total_units_sold is not None and mr_sales_entry.total_units_sold != 0 else 0
                    }
                    ad_summary.append(item)

            ad_summary = sorted(ad_summary, key=lambda x: x['_date'])

            for summary_item in ad_summary:
                total_ad_clicks = summary_item['total_ad_clicks']
                total_ad_impressions = summary_item['total_ad_impressions']
                total_ad_spends = summary_item['total_ad_spends']
                total_ad_sales = summary_item['total_ad_sales']
                total_gross_sales = summary_item.get('total_gross_sales', 0)            # type: ignore  # noqa: FKA100
                total_units_sold = summary_item.get('total_units_sold', 0)              # type: ignore  # noqa: FKA100
                total_page_views = summary_item.get('page_views', 0)              # type: ignore  # noqa: FKA100

                conversion_rate = total_units_sold / total_page_views * \
                    100 if total_page_views > 0 else 0.0
                ctr = total_ad_clicks / total_ad_impressions if total_ad_impressions > 0 else 0.0
                cpc = total_ad_spends / total_ad_clicks if total_ad_clicks > 0 else 0.0
                roas = total_ad_sales / total_ad_spends if total_ad_spends > 0 else 0.0
                acos = (total_ad_spends / total_ad_sales) * \
                    100 if total_ad_sales > 0 else 0.0
                tacos = (total_ad_spends / total_gross_sales) * \
                    100 if total_gross_sales > 0 else 0.0

                # for percentage multiplied by 100
                summary_item['ctr'] = ctr * 100
                summary_item['cpc'] = cpc
                summary_item['roas'] = roas
                summary_item['acos'] = acos
                summary_item['tacos'] = tacos
                summary_item['conversion_rate'] = conversion_rate
                summary_item['total_sales'] = total_gross_sales

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=ad_summary, error=None)

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed while fetching Marketing report Sales Period  data: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def get_performance(user_object, account_object, allowed_brands):
        """Endpoint for marketing report product performance"""
        try:
            account_id = account_object.uuid
            asp_id = account_object.asp_id

            from_date = request.args.get('from_date')
            to_date = request.args.get('to_date')
            marketplace = request.args.get('marketplace')
            category = request.args.getlist('category')
            brand = request.args.getlist('brand') if 'brand' in request.args and request.args.getlist(
                'brand') else allowed_brands
            product = request.args.getlist('product')

            page = request.args.get(key='page', default=PAGE_DEFAULT)
            size = request.args.get(key='size', default=PAGE_LIMIT)

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

            field_types = {'marketplace': str, 'from_date': str, 'to_date': str,
                           'category': list, 'brand': list, 'product': list, 'page': int, 'size': int}

            required_fields = ['marketplace', 'from_date', 'to_date']

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

            product_performance_data, total_count, total_count_result = AzProductPerformance.get_product_performance_by_ad(account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date,
                                                                                                                           category=tuple(category), brand=tuple(brand), product=tuple(product), page=page, size=size)

            data = {}
            if product_performance_data:
                result = []
                for performance_data in product_performance_data:

                    finance_total_units_sold = performance_data.total_units_sold if performance_data.total_units_sold is not None else 0

                    # total_gross_sales = 0
                    total_units_sold = 0

                    # (product, brand, display)
                    total_ad_sales = 0
                    organic_sales = 0
                    percentage_organic_sales = 0

                    # sales and traffic table
                    page_views = performance_data.page_views if performance_data.page_views is not None else 0
                    sessions = performance_data.sessions if performance_data.sessions is not None else 0

                    total_ad_clicks = 0
                    ctr = 0
                    cpc = 0
                    roas = 0
                    acos = 0
                    tacos = 0

                    # not clear
                    # total_organic_sessions = 0

                    # Only for brand logic
                    # ********************************************************************************************************
                    total_asb_clicks = 0
                    # total_asb_cost = 0
                    total_asb_impressions = 0
                    total_asb_spends = 0
                    total_asb_sales = 0

                    # if brand and performance_data.brand:
                    #     get_brand_data = AzSponsoredBrand.get_brand_performance(account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date,
                    #                                                             brand=performance_data.brand)
                    #     for brand_data in get_brand_data:
                    #         total_asb_sales += float(
                    #             brand_data.sales) if brand_data.sales is not None else 0.0
                    #         total_asb_clicks += int(
                    #             brand_data.clicks) if brand_data.clicks is not None else 0
                    #         total_asb_cost += float(
                    #             brand_data.cost) if brand_data.cost is not None else 0.0
                    #         total_asb_impressions += int(
                    #             brand_data.impressions) if brand_data.impressions is not None else 0
                    #         total_asb_spends += float(
                    #             brand_data.spends) if brand_data.spends is not None else 0.0

                    # ********************************************************************************************************

                    total_gross_sales = float(
                        performance_data.total_gross_sales) if performance_data.total_gross_sales is not None else 0.0

                    total_asp_ad_sales = float(
                        performance_data.total_asp_ad_sales) if performance_data.total_asp_ad_sales is not None else 0.0
                    total_asd_ad_sales = float(
                        performance_data.total_asd_ad_sales) if performance_data.total_asd_ad_sales is not None else 0.0
                    total_asp_ad_clicks = int(
                        performance_data.total_asp_ad_clicks) if performance_data.total_asp_ad_clicks is not None else 0
                    total_asd_ad_clicks = int(
                        performance_data.total_asd_ad_clicks) if performance_data.total_asd_ad_clicks is not None else 0
                    total_asp_units_sold = int(
                        performance_data.total_asp_units_sold) if performance_data.total_asp_units_sold is not None else 0
                    total_asd_units_sold = int(
                        performance_data.total_asd_units_sold) if performance_data.total_asd_units_sold is not None else 0
                    total_asp_ad_orders = int(
                        performance_data.total_asp_ad_orders) if performance_data.total_asp_ad_orders is not None else 0
                    total_asd_ad_orders = int(
                        performance_data.total_asd_ad_orders) if performance_data.total_asd_ad_orders is not None else 0

                    total_asp_ad_impressions = int(
                        performance_data.total_asp_ad_impressions) if performance_data.total_asp_ad_impressions is not None else 0
                    total_asd_ad_impressions = int(
                        performance_data.total_asd_ad_impressions) if performance_data.total_asd_ad_impressions is not None else 0

                    total_asp_ad_spends = float(
                        performance_data.total_asp_ad_spends) if performance_data.total_asp_ad_spends is not None else 0.0
                    total_asd_ad_spends = float(
                        performance_data.total_asd_ad_spends) if performance_data.total_asd_ad_spends is not None else 0.0

                    # (product, display, brand)
                    total_ad_sales = total_asp_ad_sales + total_asd_ad_sales + total_asb_sales
                    total_ad_clicks = total_asp_ad_clicks + total_asd_ad_clicks + total_asb_clicks
                    total_ad_spends = total_asp_ad_spends + total_asd_ad_spends + total_asb_spends
                    total_ad_impressions = total_asp_ad_impressions + \
                        total_asd_ad_impressions + total_asb_impressions

                    total_units_sold = total_asp_units_sold + total_asd_units_sold
                    total_order_from_ad = total_asp_ad_orders + total_asd_ad_orders

                    organic_sales = total_gross_sales - total_ad_sales
                    percentage_organic_sales = organic_sales / \
                        total_gross_sales * 100 if total_gross_sales is not None and total_gross_sales != 0 else 0

                    ctr = int(total_ad_clicks) / \
                        int(total_ad_impressions) if int(
                            total_ad_impressions) > 0 else 0.0

                    conversion_rate = int(
                        finance_total_units_sold) / int(page_views) * 100 if page_views > 0 and finance_total_units_sold > 0 else 0.0

                    cpc = total_ad_spends / \
                        int(total_ad_clicks) if int(
                            total_ad_clicks) > 0 else 0.0
                    roas = total_ad_sales / total_ad_spends if total_ad_spends > 0 else 0.0
                    acos = total_ad_spends / total_ad_sales * 100 if total_ad_sales > 0 else 0.0
                    tacos = total_ad_spends / total_gross_sales * \
                        100 if total_gross_sales > 0 else 0.0

                    total_organic_units = int(
                        finance_total_units_sold) - int(total_units_sold)

                    ad_conversion_rate = total_order_from_ad / \
                        total_ad_clicks * 100 if total_ad_clicks > 0 else 0.0

                    prepare_data = {
                        '_asin': performance_data.asin.strip() if performance_data.asin is not None else '',
                        '_sku': performance_data.seller_sku.strip() if performance_data.seller_sku is not None else '',
                        '_category': performance_data.category.strip() if performance_data.category is not None else '',
                        '_subcategory': performance_data.subcategory.strip() if performance_data.subcategory is not None else '',
                        '_brand': performance_data.brand.strip() if performance_data.brand is not None else '',
                        '_product_name': performance_data.item_name if performance_data.item_name is not None else '',
                        '_product_image': performance_data.face_image if performance_data.face_image is not None else '',
                        'total_gross_sales': total_gross_sales,
                        'total_units_sold': finance_total_units_sold,
                        'sales_from_ads': total_ad_sales,
                        'order_from_ads': total_order_from_ad,
                        'units_from_ads': total_units_sold,
                        'organic_sales': organic_sales,
                        'organic_sales_percentage': percentage_organic_sales,
                        'organic_units': total_organic_units,
                        'organic_sessions': sessions - total_ad_clicks,
                        'page_views': page_views,
                        'sessions': sessions,
                        'impressions': total_ad_impressions,
                        'total_ad_clicks': total_ad_clicks,
                        'total_ad_spends': total_ad_spends,
                        'ctr': ctr * 100,
                        'cpc': cpc,
                        'roas': roas,
                        'acos': acos,
                        'tacos': tacos,
                        'conversion_rate': conversion_rate,
                        'ad_conversion_rate': ad_conversion_rate,
                        'category_rank': performance_data.category_rank if performance_data.category_rank is not None else '',
                        'subcategory_rank': performance_data.subcategory_rank if performance_data.subcategory_rank is not None else ''
                    }
                    result.append(prepare_data)

                objects = {
                    'pagination_metadata': get_pagination_meta(current_page=1 if page is None else int(page), page_size=int(size), total_items=total_count_result)
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
                f'GET -> Failed while fetching Marketing report Product Performance data: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def get_performance_export(user_object, account_object, allowed_brands):   # type: ignore  # noqa: C901
        """Endpoint for getting product performance details"""
        try:
            logged_in_user = user_object.id
            account_id = account_object.uuid
            asp_id = account_object.asp_id

            from_date = request.args.get('from_date')
            to_date = request.args.get('to_date')
            marketplace = request.args.get('marketplace')
            category = request.args.getlist('category')
            brand = request.args.getlist('brand') if 'brand' in request.args and request.args.getlist(
                'brand') else allowed_brands
            product = request.args.getlist('product')

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

            field_types = {'marketplace': str, 'from_date': str, 'to_date': str,
                           'category': list, 'brand': list, 'product': list}

            required_fields = ['marketplace', 'from_date', 'to_date']

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
                                                  entity_type=EntityType.MR_PRODUCT_PERFORMANCE.value,
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
    def get_ad_totals(account_id: str, asp_id: str, from_date: str, to_date: str, category: Optional[tuple] = None, brand: Optional[tuple] = None, product: Optional[tuple] = None):
        """Gets the total ad sales, spends, impressions, and clicks for a given account, asp_id, from_date, to_date, category, brand, and product.

        Args:
            account_id (str): The account ID.
            asp_id (str): The asp ID.
            from_date (str): The start date.
            to_date (str): The end date.
            category (list[str]): The list of categories.
            brand (list[str]): The list of brands.
            product (list[str]): The list of products.

        Returns:
            total_ad_sales (float): The total ad sales.
            total_ad_spends (float): The total ad spends.
            total_ad_impressions (int): The total ad impressions.
            total_ad_clicks (int): The total ad clicks.
        """

        # Get the ad sales, spends, impressions, and clicks for each data type.
        ad_sp_data = AzSponsoredProduct.get_ad_stats(
            account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date, category=category, brand=brand, product=product)

        """Calculate Brand Only when brand provided"""
        ad_sb_data = []

        calculate_brand = True
        if category or product:
            calculate_brand = False

        if calculate_brand:
            ad_sb_data = AzSponsoredBrand.get_ad_stats(
                account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date, category=category, brand=brand, product=product)

        ad_sd_data = AzSponsoredDisplay.get_ad_stats(
            account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date, category=category, brand=brand, product=product)

        # Initialize the totals.
        total_ad_sales = 0.0
        total_ad_spends = 0.0
        total_ad_impressions = 0
        total_ad_clicks = 0

        # Iterate over the ad data and update the totals.
        for ad_data in [ad_sp_data, ad_sb_data, ad_sd_data]:
            if ad_data:
                for _ad in ad_data:
                    _ad_sales = float(
                        _ad.sales) if _ad.sales is not None else 0.0
                    _ad_spends = float(
                        _ad.spends) if _ad.spends is not None else 0.0
                    _ad_impressions = float(
                        _ad.impressions) if _ad.impressions is not None else 0
                    _ad_clicks = float(
                        _ad.clicks) if _ad.clicks is not None else 0

                    total_ad_sales += _ad_sales
                    total_ad_spends += _ad_spends
                    total_ad_impressions += _ad_impressions
                    total_ad_clicks += _ad_clicks

        return total_ad_sales, total_ad_spends, total_ad_impressions, total_ad_clicks

    @staticmethod
    @api_time_logger
    @token_required
    def create_ad_performance_by_zone(user_object, account_object):                # type: ignore  # noqa: C901
        """Endpoint for generating marketing report performance by zone"""

        try:

            from_date = request.args.get('from_date')
            to_date = request.args.get('to_date')

            # validation
            params = {}
            if from_date:
                params['from_date'] = from_date
            if to_date:
                params['to_date'] = to_date

            field_types = {'from_date': str, 'to_date': str,
                           }

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

            dates_list = get_first_last_date(
                from_date=from_date, to_date=to_date)

            if not dates_list:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=True, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=None)

            for dates in dates_list:

                data = {}

                data.update(
                    {
                        'start_datetime': dates.get('first_day'),
                        'end_datetime': dates.get('last_day')
                        # 'start_datetime': '2023-08-01',
                        # 'end_datetime': '2023-08-20'
                    }
                )

                # queuing performance zone
                add_queue_task_and_enqueue(queue_name=QueueName.AZ_PERFORMANCE_ZONE, account_id=account_object.uuid,
                                           logged_in_user=user_object.id, entity_type=EntityType.MR_PERFORMANCE_ZONE.value, data=data)

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=data, error=None)

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed while fetching Zone based product ads performance: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def get_ad_performance_by_zone(user_object, account_object, allowed_brands):                # type: ignore  # noqa: C901
        """Endpoint for marketing report product performance"""

        try:
            account_id = account_object.uuid
            asp_id = account_object.asp_id

            from_date = request.args.get('from_date')
            to_date = request.args.get('to_date')
            marketplace = request.args.get('marketplace')
            category = request.args.getlist('category')
            brand = request.args.getlist('brand') if 'brand' in request.args and request.args.getlist(
                'brand') else allowed_brands
            product = request.args.getlist('product')
            zone = request.args.get('zone')
            sort_by = request.args.get('sort_by')
            sort_order = request.args.get('sort_order')
            page = request.args.get('page', default=PAGE_DEFAULT)
            size = request.args.get('size', default=PAGE_LIMIT)

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
            if zone:
                params['zone'] = zone
            if sort_by:
                params['sort_by'] = sort_by
            if sort_order:
                params['sort_order'] = sort_order
            if page:
                params['page'] = page
            if size:
                params['size'] = size

            field_types = {'marketplace': str, 'from_date': str, 'to_date': str,
                           'zone': str, 'sort_by': str, 'sort_order': str, 'page': int, 'size': int, 'category': list, 'brand': list, 'product': list}

            required_fields = ['marketplace', 'from_date', 'to_date',
                               'zone', 'sort_by', 'sort_order', 'page', 'size']

            enum_fields = {
                'marketplace': (marketplace, 'ProductMarketplace'),
                'zone': (zone, 'MarketingReportZones'),
                'sort_order': (sort_order, 'SortingOrder')
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

            zone = zone.upper()
            sort_by = sort_by.lower()

            # validate date range
            same_month = is_same_month_year(
                from_date=from_date, to_date=to_date)

            if not same_month:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=True, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=None)

            metrics_month = convert_string_to_datetime(
                input_string=from_date).month

            data, total_count = AzPerformanceZone.get_performance_zone(
                account_id=account_id, asp_id=asp_id, metrics_month=metrics_month, zone=zone, sort_by=sort_by, sort_order=sort_order, page=int(page), size=int(size), category=tuple(category), brand=tuple(brand), product=tuple(product))

            result_dict = {'result': [], 'objects': {}}

            if data:

                for performance in data:

                    performance_dict = {
                        '_asin': performance.asin,
                        '_brand': performance.brand,
                        '_category': performance.category,
                        '_subcategory': performance.sub_category,
                        '_product_image': performance.product_image,
                        '_product_name': performance.product_name,
                        '_sku': performance.sku,
                        'category_rank': convert_to_numeric(performance.category_rank),
                        'subcategory_rank': convert_to_numeric(performance.subcategory_rank),
                        'total_gross_sales': convert_to_numeric(performance.total_sales),
                        'total_units_sold': convert_to_numeric(performance.total_units_sold),
                        'sales_from_ads': convert_to_numeric(performance.sales_from_ads),
                        'order_from_ads': convert_to_numeric(performance.order_from_ads),
                        'units_from_ads': convert_to_numeric(performance.total_ad_units_sold),
                        'organic_sales': convert_to_numeric(performance.organic_sales),
                        'organic_sales_percentage': convert_to_numeric(performance.percentage_organic_sales),
                        'organic_units': convert_to_numeric(performance.organic_units),
                        'cpc': convert_to_numeric(performance.cpc),
                        'total_ad_spends': convert_to_numeric(performance.spend),
                        'impressions': convert_to_numeric(performance.impressions),
                        'page_views': convert_to_numeric(performance.page_views),
                        'sessions': convert_to_numeric(performance.sessions),
                        'organic_sessions': convert_to_numeric(performance.organic_sessions),
                        'total_ad_clicks': convert_to_numeric(performance.clicks_from_ads),
                        'ctr': convert_to_numeric(performance.ctr),
                        'roas': convert_to_numeric(performance.roas),
                        'acos': convert_to_numeric(performance.acos),
                        'tacos': convert_to_numeric(performance.tacos),
                        'conversion_rate': convert_to_numeric(performance.conversion_rate),
                        'ad_conversion_rate': convert_to_numeric(performance.ad_conversion_rate),
                    }

                    result_dict['result'].append(performance_dict)

                objects = {
                    'pagination_metadata': get_pagination_meta(current_page=1 if page is None else int(page), page_size=int(size), total_items=total_count)
                }

                result_dict['objects'] = objects

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
                f'GET -> Failed while fetching Zone based product ads performance for dashboard: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def export_performance_by_zone(user_object, account_object, allowed_brands):  # type: ignore  # noqa: C901
        """export method to output the response as csv/excel"""

        try:
            logged_in_user = user_object.id
            account_id = account_object.uuid
            asp_id = account_object.asp_id

            from_date = request.args.get('from_date')
            to_date = request.args.get('to_date')
            marketplace = request.args.get('marketplace')
            category = request.args.getlist('category')
            brand = request.args.getlist('brand') if 'brand' in request.args and request.args.getlist(
                'brand') else allowed_brands
            product = request.args.getlist('product')
            zone = request.args.get('zone')
            sort_by = request.args.get('sort_by')
            sort_order = request.args.get('sort_order')

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
            if zone:
                params['zone'] = zone.upper()
            if sort_by:
                params['sort_by'] = sort_by.lower()
            if sort_order:
                params['sort_order'] = sort_order

            field_types = {'marketplace': str, 'from_date': str, 'to_date': str,
                           'zone': str, 'sort_by': str, 'sort_order': str, 'category': list, 'brand': list, 'product': list}

            required_fields = ['marketplace', 'from_date', 'to_date',
                               'zone', 'sort_by', 'sort_order']

            enum_fields = {
                'marketplace': (marketplace, 'ProductMarketplace'),
                'zone': (zone, 'MarketingReportZones'),
                'sort_order': (sort_order, 'SortingOrder')
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

            # validate date range
            same_month = is_same_month_year(
                from_date=from_date, to_date=to_date)

            if not same_month:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=True, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=None)

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
                                                  entity_type=EntityType.ADS_PERFORMANCE_BY_ZONE.value,
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
                f'GET -> Failed exporting performance by zone: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )
