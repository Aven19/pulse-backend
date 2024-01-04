""" This class handles tasks related to analyzing and reporting on the performance of products. """
import os
import time
import traceback

from app import app
from app import config_data
from app import logger
from app.helpers.constants import CalculationLevelEnum
from app.helpers.constants import MarketingReportZones
from app.helpers.constants import QueueTaskStatus
from app.helpers.constants import TimePeriod
from app.helpers.utility import convert_string_to_datetime
from app.helpers.utility import convert_to_numeric
from app.helpers.utility import date_to_string
from app.helpers.utility import get_from_to_date_by_timestamp
from app.models.account import Account
from app.models.az_performance_zone import AzPerformanceZone
from app.models.az_product_performance import AzProductPerformance
from app.models.az_sponsored_brand import AzSponsoredBrand
from app.models.queue_task import QueueTask
from providers.mail import send_error_notification
import xlsxwriter


class PerformanceByZoneWorker:

    """Class responsible for performance zone"""

    @classmethod
    def performance_by_zone_export(cls, data):
        """To export product performance data"""

        asp_id = data['asp_id']
        account_id = data['account_id']
        from_date = data['from_date']
        category = data.get('category', [])     # type: ignore  # noqa: FKA100
        brand = data.get('brand', [])           # type: ignore  # noqa: FKA100
        product = data.get('product', [])       # type: ignore  # noqa: FKA100
        zone = data.get('zone')                 # type: ignore  # noqa: FKA100
        sort_by = data.get('sort_by')           # type: ignore  # noqa: FKA100
        sort_order = data.get('sort_order')     # type: ignore  # noqa: FKA100

        metrics_month = convert_string_to_datetime(
            input_string=from_date).month

        data, total_count = AzPerformanceZone.get_performance_zone(
            account_id=account_id, asp_id=asp_id, metrics_month=metrics_month, zone=zone, sort_by=sort_by, sort_order=sort_order, page=None, size=None, category=tuple(category), brand=tuple(brand), product=tuple(product))

        result_list = []

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

                result_list.append(performance_dict)

        if result_list:

            directory_path = f"{config_data.get('UPLOAD_FOLDER')}{'tmp/csv_exports/'}{asp_id.lower()}"
            os.makedirs(directory_path, exist_ok=True)
            logger.info(
                'Directory path for performance by zone export: %s', directory_path)

            file_name = str(zone).lower() + '_export_' + str(int(time.time()))
            export_file_path = f'{directory_path}/{file_name}.xlsx'

            workbook = xlsxwriter.Workbook(export_file_path)
            worksheet = workbook.add_worksheet()

            column_names = ['asin', '_brand', 'category', 'product_image', 'product_name', 'sku',
                            'subcategory', 'acos', 'category_rank', 'conversion_rate', 'cpc', 'ctr', 'impressions',
                            'order_from_ads', 'organic_sales', 'organic_sales_percentage', 'organic_sessions', 'organic_units', 'page_views', 'roas',
                            'sales_from_ads', 'sessions', 'subcategory_rank', 'tacos', 'total_ad_clicks', 'total_ad_spends', 'total_gross_sales', 'units_from_ads']

            for col, header in enumerate(column_names):
                worksheet.write(0, col, header)    # type: ignore  # noqa: FKA100

            for row_id, row_data in enumerate(result_list, start=1):
                for col_id, col_name in enumerate(column_names):
                    worksheet.write(row_id, col_id, row_data.get(col_name, ''))    # type: ignore  # noqa: FKA100

            workbook.close()

            return file_name, export_file_path
        else:
            return None, None

    @classmethod
    def get_performance_zone(cls, data):

        with app.app_context():
            job_id = data.get('job_id')

            if job_id is None:
                logger.error(
                    "Queue Task with job_id '{}' not found".format(job_id))
                return

            queue_task = QueueTask.get_by_id(job_id)

            if not queue_task:
                logger.error(
                    "Queue Task with job_id '{}' not found".format(job_id))
                return

            queue_task.status = QueueTaskStatus.RUNNING.value
            queue_task.save()

            account_id = queue_task.account_id
            # user_id = queue_task.owner_id
            default_sync = data.get('default_sync', False)  # type: ignore  # noqa: FKA100

            account = Account.get_by_uuid(uuid=account_id)

            if not account:
                logger.error(
                    "Queue Task with job_id '{}' failed. Account : {} not found".format(job_id, account_id))
                raise Exception

            try:

                asp_id = account.asp_id

                if default_sync:
                    start_datetime, end_datetime = get_from_to_date_by_timestamp(
                        TimePeriod.LAST_30_DAYS.value)

                else:
                    start_datetime = data.get('start_datetime')
                    end_datetime = data.get('end_datetime')

                    if start_datetime is None or end_datetime is None:
                        logger.error(
                            "Queue Task with job_id '{}' failed. start_datetime : {}, end_datetime : {}".format(job_id, start_datetime, end_datetime))
                        raise Exception

                start_datetime = convert_string_to_datetime(start_datetime)
                end_datetime = convert_string_to_datetime(end_datetime)

                start_datetime = date_to_string(start_datetime)
                end_datetime = date_to_string(end_datetime)

                result_dict_account_level = PerformanceByZoneWorker.__calculate_performance_by_zone_metrics(
                    account_id=account_id, asp_id=asp_id, from_date=start_datetime, to_date=end_datetime, level=CalculationLevelEnum.ACCOUNT.value)

                if not result_dict_account_level and queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                result_dict_product_level = PerformanceByZoneWorker.__calculate_performance_by_zone_metrics(
                    account_id=account_id, asp_id=asp_id, from_date=start_datetime, to_date=end_datetime, level=CalculationLevelEnum.PRODUCT.value)

                if not result_dict_product_level and queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                if queue_task:
                    queue_task.status = QueueTaskStatus.COMPLETED.value
                    queue_task.save()

            except Exception as e:
                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                logger.error(
                    'Error while getting performance by zone: ' + str(e))
                logger.error(traceback.format_exc())

    @staticmethod
    def __calculate_performance_by_zone_metrics(account_id: str, asp_id: str, from_date: str, to_date: str, level: str):
        """calculate metrics for performance zones and check eligibility for respective zones"""

        # cache_key = f'{RedisCacheKeys.ADS_PERFORMANCE_BY_ZONE.value}_{account_id}_{asp_id}_{from_date}_{to_date}'
        # cached_object = r.get(cache_key)
        # cached_object = False
        # if cached_object:
        #     cached_object = pickle.loads(cached_object)
        #     performance_by_zone = cached_object.get('performance_by_zone')
        #     total_count_result = cached_object.get('total_count_result')
        # else:
        #     performance_by_zone, total_count, total_count_result = AzProductPerformance.get_performance_by_zone(account_id=account_id,
        #                                                                                                         asp_id=asp_id, from_date=from_date, to_date=to_date, category=category, brand=brand, product=product)
        #     cache_dict = {'performance_by_zone': performance_by_zone,
        #                   'total_count_result': total_count_result}
        #     r.set(name=cache_key, value=pickle.dumps(
        #         cache_dict), ex=TimeInSeconds.SIXTY_MIN.value)

        try:
            performance_by_zone, total_count, total_count_result = AzProductPerformance.get_performance_by_zone(account_id=account_id,
                                                                                                                asp_id=asp_id, from_date=from_date, to_date=to_date)
            # calculate brand data or not depending on filters

            total_orders = 0
            total_page_views = 0
            total_impressions = 0

            calculate_brand = False
            if level == CalculationLevelEnum.ACCOUNT.value:
                calculate_brand = True

            # calculate elibility metrics for zone
            if performance_by_zone:

                brand_performance_dict_a = {}
                for performance in performance_by_zone:

                    # aggregating data from display and product
                    p_total_orders = convert_to_numeric(
                        performance.total_orders)
                    page_views = convert_to_numeric(performance.page_views)
                    impressions_from_ads = convert_to_numeric(
                        performance.as_product_impressions) + convert_to_numeric(performance.as_display_impressions)

                    # calculating brand data
                    if calculate_brand:
                        brand = performance.brand

                        if brand in brand_performance_dict_a:
                            brand_performance = brand_performance_dict_a.get(
                                brand)
                        else:
                            brand_performance = AzSponsoredBrand.get_brand_performance_by_zone(
                                account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date, brand=brand)

                            brand_performance_dict_a[brand] = brand_performance

                        if brand_performance:
                            # aggregating brand ads data with product and display
                            impressions_from_ads = impressions_from_ads + \
                                convert_to_numeric(
                                    brand_performance.impressions)

                    total_orders += p_total_orders
                    total_page_views += page_views
                    total_impressions += impressions_from_ads

                # elibility criteria metrics
                average_conversion_rate = 0 if total_page_views == 0 else (
                    total_orders * 100) / total_page_views
                average_asin_impressions = 0 if total_count_result == 0 else (
                    total_impressions / total_count_result)
                average_asin_page_views = 0 if total_count_result == 0 else (
                    total_page_views / total_count_result)

                # calculating performance metrics for each product in item master
                brand_performance_dict_b = {}
                for performance in performance_by_zone:

                    # aggregating data from display and product
                    total_orders = convert_to_numeric(performance.total_orders)
                    page_views = convert_to_numeric(performance.page_views)
                    total_gross_sales = convert_to_numeric(
                        performance.total_gross_sales)
                    total_units_sold = convert_to_numeric(
                        performance.total_units_sold)
                    total_ad_units_sold = convert_to_numeric(
                        performance.as_product_units_sold + performance.as_display_units_sold)

                    sessions = convert_to_numeric(performance.sessions)
                    spend_from_ads = convert_to_numeric(
                        performance.as_product_spends) + convert_to_numeric(performance.as_display_spends)
                    sales_from_ads = convert_to_numeric(
                        performance.as_product_sales) + convert_to_numeric(performance.as_display_sales)
                    clicks_from_ads = convert_to_numeric(
                        performance.as_product_clicks) + convert_to_numeric(performance.as_display_clicks)
                    impressions_from_ads = convert_to_numeric(
                        performance.as_product_impressions) + convert_to_numeric(performance.as_display_impressions)

                    order_from_ads = convert_to_numeric(
                        performance.as_product_orders + performance.as_display_orders)
                    organic_units = convert_to_numeric(
                        performance.total_units_sold) - convert_to_numeric(total_ad_units_sold)

                    # calculating brand data
                    if calculate_brand:
                        brand = performance.brand

                        if brand in brand_performance_dict_b:
                            brand_performance = brand_performance_dict_b.get(
                                brand)
                        else:
                            brand_performance = AzSponsoredBrand.get_brand_performance_by_zone(
                                account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date, brand=brand)

                            brand_performance_dict_b[brand] = brand_performance

                        if brand_performance:
                            # aggregating brand ads data with product and display
                            spend_from_ads = spend_from_ads + \
                                convert_to_numeric(brand_performance.spends)
                            sales_from_ads = sales_from_ads + \
                                convert_to_numeric(brand_performance.sales)
                            clicks_from_ads = clicks_from_ads + \
                                convert_to_numeric(brand_performance.clicks)
                            impressions_from_ads = impressions_from_ads + \
                                convert_to_numeric(
                                    brand_performance.impressions)

                    organic_session = sessions - clicks_from_ads

                    # checking if meets eligiblity criteria for the zone
                    asin_conversion_rate = 0 if page_views == 0 else (
                        total_orders * 100) / page_views

                    optimal_zone = False
                    opportunity_zone = False
                    work_in_progress_zone = False

                    if asin_conversion_rate > 0.2 and impressions_from_ads >= (3 * average_asin_impressions) and page_views >= (3 * average_asin_page_views):
                        optimal_zone = True
                        zone = MarketingReportZones.OPTIMAL_ZONE.value
                    elif asin_conversion_rate >= average_conversion_rate:
                        opportunity_zone = True
                        zone = MarketingReportZones.OPPORTUNITY_ZONE.value,
                    elif asin_conversion_rate < average_conversion_rate:
                        work_in_progress_zone = True
                        zone = MarketingReportZones.WORK_IN_PROGRESS_ZONE.value,

                    if optimal_zone or opportunity_zone or work_in_progress_zone:
                        organic_sales = total_gross_sales - sales_from_ads

                        percentage_organic_sales = 0 if total_gross_sales == 0 else organic_sales / \
                            total_gross_sales * 100

                        click_through_rate = 0 if impressions_from_ads == 0 else (
                            clicks_from_ads * 100) / impressions_from_ads
                        cost_per_click = 0 if clicks_from_ads == 0 else (
                            spend_from_ads / clicks_from_ads)
                        roas = 0 if spend_from_ads == 0 else (
                            sales_from_ads / spend_from_ads)
                        acos = 0 if sales_from_ads == 0 else spend_from_ads / sales_from_ads * 100
                        tacos = 0 if total_gross_sales == 0 else spend_from_ads / total_gross_sales * 100

                        conversion_rate = 0 if page_views == 0 else total_units_sold / page_views * 100

                        ad_conversion_rate = 0 if clicks_from_ads == 0 else order_from_ads / clicks_from_ads * 100

                        # add to performance table

                        AzPerformanceZone.add_update(account_id=account_id, asp_id=asp_id,
                                                     level=level,
                                                     zone=zone,
                                                     metrics_date=from_date,
                                                     asin=performance.asin,
                                                     sku=performance.seller_sku,
                                                     product_name=performance.item_name,
                                                     product_image=performance.face_image,
                                                     category=performance.category,
                                                     sub_category=performance.sub_category,
                                                     category_rank=performance.category_rank,
                                                     subcategory_rank=performance.subcategory_rank,
                                                     brand=performance.brand,
                                                     total_sales=convert_to_numeric(
                                                         total_gross_sales),
                                                     total_units_sold=convert_to_numeric(
                                                         total_units_sold),
                                                     total_ad_units_sold=convert_to_numeric(
                                                         total_ad_units_sold),
                                                     order_from_ads=convert_to_numeric(
                                                         order_from_ads),
                                                     sales_from_ads=convert_to_numeric(
                                                         sales_from_ads),
                                                     organic_sales=convert_to_numeric(
                                                         organic_sales),
                                                     organic_units=convert_to_numeric(
                                                         organic_units),
                                                     organic_sessions=convert_to_numeric(
                                                         organic_session),
                                                     percentage_organic_sales=convert_to_numeric(
                                                         percentage_organic_sales),
                                                     page_views=convert_to_numeric(
                                                         page_views),
                                                     sessions=convert_to_numeric(
                                                         sessions),
                                                     clicks_from_ads=convert_to_numeric(
                                                         clicks_from_ads),
                                                     ctr=convert_to_numeric(
                                                         click_through_rate),
                                                     cpc=convert_to_numeric(
                                                         cost_per_click),
                                                     spend=convert_to_numeric(
                                                         spend_from_ads),
                                                     roas=convert_to_numeric(
                                                         roas),
                                                     acos=convert_to_numeric(
                                                         acos),
                                                     tacos=convert_to_numeric(
                                                         tacos),
                                                     ad_conversion_rate=convert_to_numeric(
                                                         ad_conversion_rate),
                                                     conversion_rate=convert_to_numeric(
                                                         conversion_rate),
                                                     impressions=convert_to_numeric(
                                                         impressions_from_ads)
                                                     )

                return True

        except Exception as e:
            error_message = f'Error while calculating metrics for performance zones at {level} level' + str(
                e)
            logger.error(error_message)
            logger.error(traceback.format_exc())
            send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject=f'Error while calculating metrics for performance zones at {level} level',
                                    template='emails/slack_email.html', data={}, error_message=error_message, traceback_info=traceback.format_exc())
            return False
