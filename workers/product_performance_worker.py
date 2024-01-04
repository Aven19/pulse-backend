""" This class handles tasks related to analyzing and reporting on the performance of products. """
import os

from app import config_data
from app import logger
from app.helpers.constants import AzProductPerformanceColumn
from app.models.az_product_performance import AzProductPerformance
import xlsxwriter


class ProductPerformanceWorker:

    """Class responsible for performing product performance analysis"""

    @classmethod
    def product_performance_export(cls, data):
        """To export product performance data"""

        asp_id = data['asp_id']
        account_id = data['account_id']
        from_date = data['from_date']
        to_date = data['to_date']
        category = data.get('category') if data.get(
            'category') is not None else ''
        brand = data.get('brand') if data.get('brand') is not None else ''
        product = data.get('product') if data.get(
            'product') is not None else ''
        sort_order = data.get('sort_order')
        sort_by = data.get('sort_by')

        items, total_count, total_request_count = AzProductPerformance.get_performance(
            account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date, category=tuple(category), brand=tuple(brand), product=tuple(product),
            sort_order=sort_order, sort_by=AzProductPerformanceColumn.get_column(sort_by))

        if items:
            result = []
            for get_item in items:
                _asin = get_item.asin
                _sku = get_item.seller_sku if get_item.seller_sku is not None else None
                _item_name = get_item.item_name.strip() if get_item.item_name is not None else None
                _brand = get_item.brand.strip() if get_item.brand is not None else None
                _category = get_item.category.strip() if get_item.category is not None else None

                current_gross_sales = float(
                    get_item.total_gross_sales) if get_item.total_gross_sales is not None else 0.0
                current_total_orders = int(
                    get_item.unique_orders) if get_item.unique_orders is not None else 0
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
                average_selling_price = float(
                    get_item.average_selling_price) if get_item.average_selling_price is not None else 0.0

                item_data = {
                    '_asin': _asin,
                    '_sku': _sku,
                    '_category': _category,
                    '_brand': _brand,
                    '_product_name': _item_name,
                    'asp_current': average_selling_price,
                    'margin_current': current_margin,
                    'cogs_current': current_cogs,
                    'gross_sales_current': current_gross_sales,
                    'unique_orders': current_total_orders,
                    'units_sold_current': current_units_sold,
                    'refunds_current': current_refunds,
                    'units_returned_current': current_units_returned,
                    'return_rate_current': current_return_rate,
                    'market_place_fee_current': current_market_place_fee,
                    'profit_current': current_profit
                }
                result.append(item_data)

            directory_path = f"{config_data.get('UPLOAD_FOLDER')}{'tmp/csv_exports/'}{asp_id.lower()}"
            os.makedirs(directory_path, exist_ok=True)
            logger.info(
                'Directory path for product performance export: %s', directory_path)

            file_name = 'product_performance_export'
            export_file_path = f'{directory_path}/{file_name}.xlsx'

            workbook = xlsxwriter.Workbook(export_file_path)
            worksheet = workbook.add_worksheet()

            column_names = [
                'SKU', 'Category', 'Brand', 'Product', 'Product Name',
                'Gross Sales',
                'Total Orders',
                'Units Sold',
                'Refunds',
                'Units Returned',
                'Return Rate',
                'Market Place Fee',
                'COGS',
                'Profit',
                'ASP',
                'Margin'
            ]

            for col, header in enumerate(column_names):
                worksheet.write(0, col, header)    # type: ignore  # noqa: FKA100

            for row, item in enumerate(result, start=1):
                data_fields = [
                    '_sku', '_category', '_brand', '_asin', '_product_name',
                    'gross_sales_current',
                    'unique_orders',
                    'units_sold_current',
                    'refunds_current',
                    'units_returned_current',
                    'return_rate_current',
                    'market_place_fee_current',
                    'cogs_current',
                    'profit_current',
                    'asp_current',
                    'margin_current'
                ]

                for col, field in enumerate(data_fields):
                    worksheet.write(row, col, item[field])    # type: ignore  # noqa: FKA100

            workbook.close()

            return file_name, export_file_path
        else:
            return None, None
