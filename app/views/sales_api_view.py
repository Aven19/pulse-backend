from datetime import datetime
from datetime import timedelta
from typing import Optional

from app import db
from app import logger
from app.helpers.constants import ASpReportProcessingStatus
from app.helpers.constants import ASpReportType
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import ResponseMessageKeys
from app.helpers.constants import SalesAPIGranularity
from app.helpers.decorators import api_time_logger
from app.helpers.decorators import token_required
from app.helpers.utility import generate_uuid
from app.helpers.utility import get_asp_market_place_ids
from app.helpers.utility import get_previous_day_date
from app.helpers.utility import required_validator
from app.helpers.utility import send_json_response
from app.helpers.utility import string_to_bool
from app.models.account import Account
from app.models.az_report import AzReport
from app.models.az_sales_traffic_summary import AzSalesTrafficSummary
from app.models.user import User
from flask import request
from providers.amazon_sp_client import AmazonReportEU


class AZSalesView:
    """class for amazon sales api"""

    @staticmethod
    @api_time_logger
    @token_required
    def get_sales(user_object, account_object):
        """get sales from amazon sales api """

        try:

            account_id = account_object.uuid
            asp_id = account_object.asp_id

            from_date = request.args.get('from_date')
            to_date = request.args.get('to_date')
            seller_sku = request.args.get('seller_sku')
            asin = request.args.get('asin')

            last_three_days = request.args.get('last_three_days', True)  # noqa: FKA100

            credentials = account_object.retrieve_asp_credentials(account_object)[
                0]

            response = AZSalesView.call_sales_api(
                account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date, credentials=credentials, last_three_days=last_three_days, seller_sku=seller_sku, asin=asin)

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response, error=None)

        except Exception as exception_error:
            logger.error(
                f'Exception occured while getting sales api data : {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

    @staticmethod
    @api_time_logger
    def cancel_reports():
        """Cancel all sales reports"""

        try:

            reports = AzReport.cancel_old_reports(
                report_type=ASpReportType.SALES_ORDER_METRICS.value, max_age_minutes=0)

            result_dict = {'result': []}

            if reports:

                for report in reports:
                    _report = {
                        'id': report.id,
                        'account_id': report.account_id,
                        'asp_id': report.asp_id,
                        'type': report.type,
                        'status': report.status,
                        'queue_id': report.queue_id
                    }

                    result_dict['result'].append(_report)

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=result_dict, error=None)

        except Exception as exception_error:
            logger.error(
                f'Exception occured while cancelling sales api : {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

    @staticmethod
    @api_time_logger
    def sync_sales():
        """Sync Sales Api for a particular date"""

        try:

            from_date = request.args.get('from_date')
            to_date = request.args.get('to_date')

            users = db.session.query(User.id, Account.uuid, Account.asp_id, Account.asp_credentials).join(Account, (User.id == Account.primary_user_id) & (Account.uuid.isnot(None)) & (Account.asp_id.isnot(None)) & (Account.asp_credentials.isnot(None))).all()  # type: ignore  # noqa: FKA100

            result_dict = {'result': []}

            for user in users:
                user_id, account_id, asp_id, asp_credentials = user

                ref_id = generate_uuid()

                report = AzReport.add(account_id=account_id, seller_partner_id=asp_id, type=ASpReportType.SALES_ORDER_METRICS.value,
                                      reference_id=ref_id, request_start_time=from_date, request_end_time=to_date, status=ASpReportProcessingStatus.NEW.value)

                _report = {
                    'id': report.id,
                    'account_id': report.account_id,
                    'asp_id': report.asp_id,
                    'type': report.type,
                    'status': report.status,
                }

                result_dict['result'].append(_report)

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=result_dict, error=None)

        except Exception as exception_error:
            logger.error(
                f'Exception occured while syncing sales api : {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

    @staticmethod
    def get_datetime_from_interval(interval: str):
        """retrieve from_datetime and to_datetime from interval"""

        start_str, end_str = interval.split('--')
        start_datetime = datetime.fromisoformat(start_str)
        end_datetime = datetime.fromisoformat(end_str)

        # Adjust datetime objects to the desired time zone (e.g., UTC)
        # Adjust based on your specific offset
        time_zone_offset = timedelta(hours=6, minutes=0)
        start_datetime += time_zone_offset
        end_datetime += time_zone_offset

        # Format datetime objects to '2023-11-16T00:00:00--2023-11-16T01:00:00'
        # formatted_interval = f'{start_datetime.isoformat()}--{end_datetime.isoformat()}'

        # Convert the string format to '12:00AM - 01:00AM'
        formatted_time_range = f"{start_datetime.strftime('%I:%M%p')} - {end_datetime.strftime('%I:%M%p')}"

        return start_datetime.strftime('%Y-%m-%d'), formatted_time_range

    @staticmethod
    def call_sales_api(account_id: str, asp_id: str, to_date: str, credentials: dict, from_date: Optional[str] = None, last_three_days=True, seller_sku: Optional[str] = None, asin: Optional[str] = None):
        """call sales api and add/update db. By default last_three_days will be fetched if to_date falls under last 3 dates"""

        try:

            last_three_days = string_to_bool(last_three_days)

            fetch_sales_api = False

            if last_three_days:
                """By default last_three_days will be fetched if to_date falls under last 3 dates"""
                """to_date is required"""

                logger.info(
                    'Fetching sales api data for last 3 days including today')

                required_fields = ['to_date']
                is_valid = required_validator(
                    request_data={'to_date': to_date}, required_fields=required_fields)

                if is_valid['is_error']:
                    raise Exception(
                        'Sales api error: Check parameters passed to the function')

                to_date_obj = datetime.strptime(to_date, '%Y-%m-%d').date()             # noqa: FKA100
                sales_data_to_date_str = datetime.now().date()
                sales_data_from_date = sales_data_to_date_str - \
                    timedelta(days=2)

                sales_data_from_date_str = get_previous_day_date(
                    date=sales_data_from_date)

                if sales_data_from_date <= to_date_obj <= sales_data_to_date_str:
                    logger.info('sales api is required')

                    fetch_sales_api = True

            elif not last_three_days:
                """if last_three_days is False and date range is passed"""
                """from_date, to_date is required"""

                logger.info('Sales api fetched for custom date range')

                required_fields = ['from_date', 'to_date']
                is_valid = required_validator(
                    request_data={'from_date': from_date, 'to_date': to_date}, required_fields=required_fields)

                if is_valid['is_error']:
                    raise Exception(
                        'Sales api error: Check parameters passed to the function')

                sales_data_from_date_str = from_date
                sales_data_from_date_obj = datetime.strptime(sales_data_from_date_str, '%Y-%m-%d').date()             # noqa: FKA100
                sales_data_from_date_str = get_previous_day_date(
                    date=sales_data_from_date_obj)

                sales_data_to_date_str = to_date

                fetch_sales_api = True

            if fetch_sales_api:

                payload_interval = f'{sales_data_from_date_str}T18:00:00-00:30--{sales_data_to_date_str}T18:00:00-00:30'
                logger.info(f'**payload_interval:{payload_interval}')

                params = {
                    'interval': payload_interval,
                    'marketplaceIds': get_asp_market_place_ids(),
                    'granularity': SalesAPIGranularity.HOUR.value
                }

                if seller_sku:
                    params['sku'] = seller_sku
                if asin:
                    params['asin'] = asin

                # creating AmazonReportEU object and passing the credentials
                report = AmazonReportEU(credentials=credentials)

                # calling create report function of report object and passing the payload
                sales_response = report.get_sales(params=params, web_call=True)

                status_code = sales_response.status_code
                response = sales_response.json()

                if not str(status_code) == HttpStatusCode.OK.value:
                    raise Exception(
                        f'sales api response status code: {status_code} with message {response}')

                payload = response.get('payload')

                if response and payload:

                    total_sales = 0
                    unit_count = 0
                    order_count = 0
                    date_total_sales_dict = {}
                    date_unit_count_dict = {}
                    date_order_count_dict = {}
                    date_hourly_sales_dict = {}
                    result_dict = {}

                    for data in payload:

                        interval = data.get('interval')                         # noqa: FKA100
                        payload_date, interval_range = AZSalesView.get_datetime_from_interval(
                            interval=interval)

                        data_total_sales = data.get(                            # noqa: FKA100
                            'totalSales', 0).get('amount', 0)                   # noqa: FKA100
                        data_unit_count = data.get('unitCount', 0)              # noqa: FKA100
                        data_order_count = data.get('orderCount', 0)            # noqa: FKA100
                        # data_order_item_count = data.get('orderItemCount',0)

                        date_total_sales_dict[payload_date] = date_total_sales_dict.get(            # noqa: FKA100
                            payload_date, 0) + data_total_sales
                        date_unit_count_dict[payload_date] = date_unit_count_dict.get(              # noqa: FKA100
                            payload_date, 0) + data_unit_count
                        date_order_count_dict[payload_date] = date_order_count_dict.get(            # noqa: FKA100
                            payload_date, 0) + data_order_count

                        total_sales += data_total_sales
                        unit_count += data_unit_count
                        order_count += data_order_count

                        data['formatted_interval_range'] = interval_range
                        data['formatted_payload_date'] = payload_date

                        date_hourly_sales_dict[payload_date] = date_hourly_sales_dict.get(          # noqa: FKA100
                            payload_date, [])
                        date_hourly_sales_dict[payload_date].append(data)

                    for payload_date in date_total_sales_dict.keys():

                        hourly_sales_dict = date_hourly_sales_dict.get(                     # noqa: FKA100
                            payload_date)
                        total_sales = date_total_sales_dict.get(                            # noqa: FKA100
                            payload_date, 0)
                        unit_count = date_unit_count_dict.get(payload_date, 0)              # noqa: FKA100
                        order_count = date_order_count_dict.get(                            # noqa: FKA100
                            payload_date, 0)

                        hourly_sales = {
                            'objects': {
                                'total_sales': total_sales,
                                'unit_count': unit_count,
                                'order_count': order_count
                            },
                            'result': {'payload': hourly_sales_dict}
                        }

                        result_dict[payload_date] = hourly_sales

                        AzSalesTrafficSummary.insert_or_update_sales_data(account_id=account_id, asp_id=asp_id, date=payload_date,
                                                                          total_sales=total_sales, unit_count=unit_count, hourly_sales=hourly_sales)

                        logger.info(
                            f'sales summary inserted/updated with sales api data for date : {payload_date}')

                    logger.info('***** successfully called sales api')
                    return result_dict

                raise Exception(
                    'Sales api error: Payload key not found in the response')

            else:
                logger.info('Sales api info: no requirement for sales api')
                return None

        except Exception:
            raise
