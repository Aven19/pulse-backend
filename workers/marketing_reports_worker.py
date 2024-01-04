""" This class handles tasks related to analyzing and reporting on the performance of products in Marketing Reports. """
import os
import time

from app import config_data
from app import logger
from app.models.az_product_performance import AzProductPerformance
import xlsxwriter


class MarketingReportsWorker:

    """Class responsible for performing product performance analysis in Marketing Reports"""

    @classmethod
    def product_performance_export(cls, data):
        """To export marketing reports product performance data"""

        asp_id = data['asp_id']
        account_id = data['account_id']
        from_date = data['from_date']
        to_date = data['to_date']
        category = data.get('category') if data.get(
            'category') is not None else ''
        brand = data.get('brand') if data.get('brand') is not None else ''
        product = data.get('product') if data.get(
            'product') is not None else ''

        product_performance_data, total_count, total_count_result = AzProductPerformance.get_product_performance_by_ad(account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date,
                                                                                                                       category=tuple(category), brand=tuple(brand), product=tuple(product))

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

        if result:

            directory_path = f"{config_data.get('UPLOAD_FOLDER')}{'tmp/csv_exports/'}{asp_id.lower()}"
            os.makedirs(directory_path, exist_ok=True)
            logger.info(
                'Directory path for Marketing Reports Product Performance export: %s', directory_path)

            file_name = 'Marketing_reports_product_performance_export_' + \
                str(int(time.time()))
            export_file_path = f'{directory_path}/{file_name}.xlsx'

            workbook = xlsxwriter.Workbook(export_file_path)
            worksheet = workbook.add_worksheet()

            column_names = [
                'SKU', 'Category', 'Sub Category', 'Brand', 'Product', 'Product Name', 'Product Image',
                'Total Gross Sales',
                'Total Units Sold',
                'Sales from Ads',
                'Order from Ads',
                'Units from Ads',
                'Organic Sales',
                'Organic Sales %',
                'Organic Units',
                'CPC',
                'Total Ad Spends',
                'Impressions',
                'Page Views',
                'Sessions',
                'Organic Sessions',
                'Total Ad Clicks',
                'CTR',
                'ROAS',
                'ACOS',
                'Tacos',
                'Conversion Rate',
                'Ad Conversion Rate'
            ]

            for col, header in enumerate(column_names):
                worksheet.write(0, col, header)    # type: ignore  # noqa: FKA100

            for row, item in enumerate(result, start=1):
                data_fields = [
                    '_sku', '_category', '_subcategory', '_brand', '_asin', '_product_name', '_product_image',
                    'total_gross_sales', 'total_units_sold',
                    'sales_from_ads', 'order_from_ads', 'units_from_ads',
                    'organic_sales', 'organic_sales_percentage', 'organic_units', 'cpc',
                    'total_ad_spends', 'impressions', 'page_views', 'sessions', 'organic_sessions',
                    'total_ad_clicks', 'ctr', 'roas', 'acos', 'tacos', 'conversion_rate', 'ad_conversion_rate'
                ]

                for col, field in enumerate(data_fields):
                    worksheet.write(row, col, item[field])    # type: ignore  # noqa: FKA100

            workbook.close()

            return file_name, export_file_path
        else:
            return None, None
