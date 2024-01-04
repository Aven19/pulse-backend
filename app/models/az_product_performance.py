"""
    Database model for storing Financial Events in database is written in this File along with its methods.
"""
from datetime import datetime
import time
from typing import Any
from typing import Optional

from app import db
from app.helpers.constants import AzFbaReturnsReportType
from app.helpers.constants import SortingOrder
from app.helpers.utility import get_prior_to_from_date
from app.models.az_item_master import AzItemMaster
from app.models.base import Base
from sqlalchemy import text


class AzProductPerformance(Base):
    """
        Financial Event model to store events in database
    """
    __tablename__ = 'az_product_performance'

    id = db.Column(db.BigInteger, primary_key=True)
    account_id = db.Column(db.String(36), nullable=False)
    asp_id = db.Column(db.String(255))
    category = db.Column(db.String(250), nullable=True)
    brand = db.Column(db.String(250), nullable=True)
    az_order_id = db.Column(db.String(255))
    seller_order_id = db.Column(db.String(255))
    seller_sku = db.Column(db.String(255))
    expenses = db.Column(db.Numeric, nullable=True)
    gross_sales = db.Column(db.Numeric, nullable=True)
    units_sold = db.Column(db.BigInteger)
    units_returned = db.Column(db.BigInteger)
    market_place_fee = db.Column(db.Numeric, nullable=True)
    forward_fba_fee = db.Column(db.Numeric, nullable=True)
    reverse_fba_fee = db.Column(db.Numeric, nullable=True)
    returns = db.Column(db.Numeric, nullable=True)
    shipment_date = db.Column(db.String(255))
    refund_date = db.Column(db.String(255))
    summary_analysis = db.Column(db.JSON, nullable=True)
    created_at = db.Column(db.BigInteger)
    updated_at = db.Column(db.BigInteger, nullable=True)
    deactivated_at = db.Column(db.BigInteger, nullable=True)

    @staticmethod
    def create(account_id: str, az_order_id: str, asp_id: str, seller_order_id=None, seller_sku=None, gross_sales=None,
               expenses=None, units_sold=None, units_returned=None, market_place_fee=None, forward_fba_fee=None,
               reverse_fba_fee=None, returns=None, shipment_date=None, refund_date=None, summary_analysis=None):
        """ Create a new product with its performance calculations"""

        category, brand = None, None

        if seller_sku:
            category, brand = AzItemMaster.get_category_brand_by_sku(
                account_id=account_id, asp_id=asp_id, seller_sku=seller_sku)

        product = AzProductPerformance(
            account_id=account_id,
            asp_id=asp_id,
            category=category,
            brand=brand,
            az_order_id=az_order_id,
            seller_order_id=seller_order_id,
            seller_sku=seller_sku,
            gross_sales=gross_sales,
            expenses=expenses,
            units_sold=units_sold,
            units_returned=units_returned,
            market_place_fee=market_place_fee,
            forward_fba_fee=forward_fba_fee,
            reverse_fba_fee=reverse_fba_fee,
            returns=returns,
            shipment_date=shipment_date,
            refund_date=refund_date,
            summary_analysis=summary_analysis,
            created_at=int(time.time())
        )

        db.session.add(product)
        return product.save()

    @classmethod
    def update(cls, account_id: str, az_order_id: str, asp_id: str, seller_sku: str, seller_order_id=None, gross_sales=None, expenses=None,
               units_sold=None, units_returned=None, market_place_fee=None, forward_fba_fee=None, reverse_fba_fee=None, returns=None, shipment_date=None, refund_date=None, summary_analysis=None):
        """ Update product performance """

        category, brand = None, None

        if seller_sku:
            category, brand = AzItemMaster.get_category_brand_by_sku(
                account_id=account_id, asp_id=asp_id, seller_sku=seller_sku)

        product = db.session.query(cls).filter(
            cls.account_id == account_id, cls.az_order_id == az_order_id, cls.asp_id == asp_id, cls.seller_sku == seller_sku).first()

        if product:

            if category is not None:
                product.category = category

            if brand is not None:
                product.brand = brand

            if seller_order_id is not None:
                product.seller_order_id = seller_order_id

            if gross_sales is not None:
                product.gross_sales = gross_sales

            if expenses is not None:
                product.expenses = expenses

            if units_sold is not None:
                product.units_sold = units_sold

            if units_returned is not None:
                product.units_returned = units_returned

            if market_place_fee is not None:
                product.market_place_fee = market_place_fee

            if forward_fba_fee is not None:
                product.forward_fba_fee = forward_fba_fee

            if reverse_fba_fee is not None:
                product.reverse_fba_fee = reverse_fba_fee

            if returns is not None:
                product.returns = returns

            if shipment_date is not None:
                product.shipment_date = shipment_date

            if refund_date is not None:
                product.refund_date = refund_date

            if summary_analysis is not None:
                product.summary_analysis = summary_analysis

            product.updated_at = int(time.time())
            db.session.commit()

        return product

    @classmethod
    def get_by_az_order_id(cls, account_id: str, asp_id: str, az_order_id: str, seller_sku: Optional[str] = None) -> Any:
        """Filter record by Amazon order id"""

        query = db.session.query(cls).filter(
            cls.account_id == account_id, cls.asp_id == asp_id, cls.az_order_id == az_order_id)

        if seller_sku is not None:
            query = query.filter(cls.seller_sku == seller_sku)

        return query.first()

    @classmethod
    def get_performance(cls, account_id: str, asp_id: str, from_date: str, to_date: str, category: Optional[tuple] = None, brand: Optional[tuple] = None,
                        product: Optional[tuple] = None, sort_by: Any = None, sort_order: Any = None, page: Any = None, size: Any = None, calculate_total: Optional[bool] = False):
        """Get Product Performance - sales, refund, market place fee, etc"""

        prior_from_date, prior_to_date = get_prior_to_from_date(
            from_date=from_date, to_date=to_date)

        from_date = datetime.strptime(from_date, '%Y-%m-%d').date()     # type: ignore  # noqa: FKA100
        to_date = datetime.strptime(to_date, '%Y-%m-%d').date()         # type: ignore  # noqa: FKA100

        params = {
            'from_date': from_date,
            'to_date': to_date,
            'prior_from_date': prior_from_date,
            'prior_to_date': prior_to_date,
            'account_id': account_id,
            'asp_id': asp_id,
            'category': category,
            'brand': brand,
            'product': product
        }

        if calculate_total:
            raw_query = '''
                        SELECT
                        im.asin,
                        im.item_name,
                        im.brand,
                        im.category,
                        im.face_image,
                        c.seller_sku,
                        c.total_gross_sales as gross_sales,
                        c.order_count as total_orders,
                        c.total_units_sold,
                        c.total_refunds as refunds,
                        c.total_units_returned,
                        c.market_place_fee,
                        (c.total_units_sold - c.total_units_returned) * ac.product_cost AS cogs
                        FROM (
                        SELECT
                            pp.seller_sku,
                            SUM(pp.gross_sales) AS total_gross_sales,
                            SUM(pp.units_sold) AS total_units_sold,
                            count(pp.az_order_id) AS order_count,
                            SUM(CASE WHEN CAST(pp.refund_date AS DATE) BETWEEN :from_date AND :to_date THEN pp.returns ELSE 0 END) AS total_refunds,
                            SUM(CASE WHEN CAST(pp.refund_date AS DATE) BETWEEN :from_date AND :to_date THEN pp.units_returned ELSE 0 END) AS total_units_returned,
                            SUM(CASE
                                WHEN (CAST(pp.shipment_date AS DATE) BETWEEN :from_date AND :to_date) OR
                                    (CAST(pp.refund_date AS DATE) BETWEEN :from_date AND :to_date) THEN pp.market_place_fee
                                ELSE 0
                            END) AS market_place_fee
                            FROM az_product_performance AS pp
                            WHERE
                            pp.account_id = :account_id
                            AND pp.asp_id = :asp_id
                            AND pp.units_sold != 0
                            AND CAST(pp.shipment_date AS DATE) BETWEEN :from_date AND :to_date
                            GROUP BY pp.seller_sku
                        ) c
                        LEFT JOIN az_item_master AS im ON c.seller_sku = im.seller_sku
                        INNER JOIN (
                            SELECT
                                im.asin,
                                SUM(im.cogs) AS product_cost
                            FROM az_item_master AS im
                            WHERE im.account_id = :account_id
                            AND im.selling_partner_id = :asp_id
                            GROUP BY im.asin
                        ) ac ON im.asin = ac.asin
                        WHERE
                        im.asin != ''
            '''

        else:

            raw_query = '''
                        SELECT
                        im.asin,
                        im.item_name,
                        im.brand,
                        im.category,
                        im.face_image,
                        c.seller_sku,
                        c.total_gross_sales as total_gross_sales,
                        (c.total_gross_sales/nullif(c.total_units_sold,0)) as average_selling_price,
                        CASE
                            WHEN
                            (_p._previous_gross_sales/nullif(_p._previous_units_sold,0)) > 0
                            THEN
                            (
                            (c.total_gross_sales/nullif(c.total_units_sold,0))
                            -
                            (_p._previous_gross_sales/nullif(_p._previous_units_sold,0))
                            )
                            /
                            (_p._previous_gross_sales/nullif(_p._previous_units_sold,0))
                            ELSE 0.00
                        END AS average_selling_price_growth,
                        ((c.total_gross_sales - ABS(c.total_refunds) - ABS(c.market_place_fee) - ((c.total_units_sold - c.total_units_returned) * ac.product_cost)) / nullif(c.total_gross_sales,0)) as current_roi,
                        CASE
                            WHEN
                            ((_p._previous_gross_sales - ABS(_p._previous_refunds) - ABS(_p._previous_market_place_fee) - ((_p._previous_units_sold - _p._previous_units_returned) * ac.product_cost)) / nullif(_p._previous_gross_sales,0)) > 0
                            THEN
                            (
                            ((c.total_gross_sales - ABS(c.total_refunds) - ABS(c.market_place_fee) - ((c.total_units_sold - c.total_units_returned) * ac.product_cost)) / nullif(c.total_gross_sales,0))
                            -
                            ((_p._previous_gross_sales - ABS(_p._previous_refunds) - ABS(_p._previous_market_place_fee) - ((_p._previous_units_sold - _p._previous_units_returned) * ac.product_cost)) / nullif(_p._previous_gross_sales,0))
                            )
                            /
                            ((_p._previous_gross_sales - ABS(_p._previous_refunds) - ABS(_p._previous_market_place_fee) - ((_p._previous_units_sold - _p._previous_units_returned) * ac.product_cost)) / nullif(_p._previous_gross_sales,0))
                            ELSE 0.00
                        END AS growth_roi,
                        c.order_count as unique_orders,
                        CASE
                            WHEN _p._previous_order_count > 0 THEN (c.order_count - _p._previous_order_count) * 100 / _p._previous_order_count
                            ELSE 0.00
                        END AS total_unique_orders_percentage_growth,
                        CASE
                            WHEN
                            (c.total_gross_sales - ABS(c.total_refunds) - ABS(c.market_place_fee) - ((c.total_units_sold - c.total_units_returned) * ac.product_cost)) > 0
                            THEN
                            ((c.total_gross_sales - ABS(c.total_refunds) - ABS(c.market_place_fee) - ((c.total_units_sold - c.total_units_returned) * ac.product_cost))
                            /
                            nullif((c.total_gross_sales - ABS(c.total_refunds)), 0)) * 100
                            ELSE 0
                        END AS margin,
                        CASE
                            WHEN (c.total_gross_sales - ABS(c.total_refunds) - ABS(c.market_place_fee) - ((c.total_units_sold - c.total_units_returned) * ac.product_cost)) > 0
                            THEN
                                (
                                    (
                                        (c.total_gross_sales - ABS(c.total_refunds) - ABS(c.market_place_fee) - ((c.total_units_sold - c.total_units_returned) * ac.product_cost)) -
                                        (_p._previous_gross_sales - ABS(_p._previous_refunds) - ABS(_p._previous_market_place_fee) - ((_p._previous_units_sold - _p._previous_units_returned) * ac.product_cost))
                                    ) /
                                    nullif(
                                        (_p._previous_gross_sales - ABS(_p._previous_refunds) - ABS(_p._previous_market_place_fee) - ((_p._previous_units_sold - _p._previous_units_returned) * ac.product_cost)),
                                        0
                                    )
                                ) * 100
                            ELSE 0
                        END AS growth_margin,
                        CASE
                            WHEN _p._previous_gross_sales > 0 THEN CAST((c.total_gross_sales - _p._previous_gross_sales) / _p._previous_gross_sales * 100 AS NUMERIC(10, 2))
                            ELSE 0.00
                        END AS total_gross_sales_percentage_growth,
                        c.total_units_sold,
                        CASE
                            WHEN _p._previous_units_sold > 0 THEN CAST((c.total_units_sold - _p._previous_units_sold) / _p._previous_units_sold * 100 AS NUMERIC(10, 2))
                            ELSE 0.00
                        END AS total_units_sold_percentage_growth,
                        c.total_refunds,
                        CASE
                            WHEN _p._previous_refunds > 0 THEN CAST((ABS(c.total_refunds) - ABS(_p._previous_refunds)) / ABS(_p._previous_refunds) * 100 AS NUMERIC(10, 2))
                            ELSE 0.00
                        END AS total_refunds_percentage,
                        c.total_units_returned,
                        CASE
                            WHEN _p._previous_units_returned > 0 THEN CAST((c.total_units_returned - _p._previous_units_returned) / _p._previous_units_returned * 100 AS NUMERIC(10, 2))
                            ELSE 0.00
                        END AS total_units_returned_percentage_growth,
                        (c.total_units_returned / c.total_units_sold) * 100 AS returns_rate,
                        CASE
                            WHEN ((c.total_units_returned / c.total_units_sold) * 100) IS NOT NULL AND ((_p._previous_units_returned / NULLIF(_p._previous_units_sold, 0)) * 100) IS NOT NULL AND ((_p._previous_units_returned / NULLIF(_p._previous_units_sold, 0)) * 100) > 0
                            THEN CAST(
                            (((c.total_units_returned / c.total_units_sold) * 100) - ((_p._previous_units_returned / NULLIF(_p._previous_units_sold, 0)) * 100) ) / ((_p._previous_units_returned / NULLIF(_p._previous_units_sold, 0)) * 100) * 100 AS NUMERIC(10, 2))
                            ELSE 0.00
                        END AS returns_rate_growth,
                        c.market_place_fee,
                        CASE
                            WHEN _p._previous_market_place_fee is not NULL AND _p._previous_market_place_fee <> 0
                            THEN
                                (ABS(c.market_place_fee) - ABS(_p._previous_market_place_fee)) / NULLIF(ABS(_p._previous_market_place_fee), 0) * 100
                            ELSE 0.00
                        END AS total_market_place_fee_percentage_growth,
                        (c.total_units_sold - c.total_units_returned) * ac.product_cost AS cogs,
                        CASE
                            WHEN ((c.total_units_sold - c.total_units_returned) * ac.product_cost) > 0 AND ((_p._previous_units_sold - _p._previous_units_returned) * ac.product_cost) > 0
                            THEN
                            CAST((((c.total_units_sold - c.total_units_returned) * ac.product_cost) - ((_p._previous_units_sold - _p._previous_units_returned) * ac.product_cost)) / ((_p._previous_units_sold - _p._previous_units_returned) * ac.product_cost) * 100 AS NUMERIC(10, 2))
                            ELSE 0.00
                        END AS cogs_percentage_growth,
                        (c.total_gross_sales - ABS(c.total_refunds) - ABS(c.market_place_fee) - ((c.total_units_sold - c.total_units_returned) * ac.product_cost)) AS profit,
                        CASE
                            WHEN (c.total_gross_sales - ABS(c.total_refunds) - ABS(c.market_place_fee) - ((c.total_units_sold - c.total_units_returned) * ac.product_cost)) > 0
                            THEN CAST((c.total_gross_sales - ABS(c.total_refunds) - ABS(c.market_place_fee) - ((c.total_units_sold - c.total_units_returned) * ac.product_cost) - (_p._previous_gross_sales - ABS(_p._previous_refunds) - ABS(_p._previous_market_place_fee) - ((_p._previous_units_sold - _p._previous_units_returned) * ac.product_cost))) / (_p._previous_gross_sales - ABS(_p._previous_refunds) - ABS(_p._previous_market_place_fee) - ((_p._previous_units_sold - _p._previous_units_returned) * ac.product_cost)) * 100 AS NUMERIC(10, 2))
                            ELSE 0.00
                        END AS profit_percentage_growth
                        FROM (
                        SELECT
                            pp.seller_sku,
                            SUM(pp.gross_sales) AS total_gross_sales,
                            SUM(pp.units_sold) AS total_units_sold,
                            count(pp.az_order_id) AS order_count,
                            SUM(CASE WHEN CAST(pp.refund_date AS DATE) BETWEEN :from_date AND :to_date THEN pp.returns ELSE 0 END) AS total_refunds,
                            SUM(CASE WHEN CAST(pp.refund_date AS DATE) BETWEEN :from_date AND :to_date THEN pp.units_returned ELSE 0 END) AS total_units_returned,
                            SUM(CASE
                                WHEN (CAST(pp.shipment_date AS DATE) BETWEEN :from_date AND :to_date) OR
                                    (CAST(pp.refund_date AS DATE) BETWEEN :from_date AND :to_date) THEN pp.market_place_fee
                                ELSE 0
                            END) AS market_place_fee
                            FROM az_product_performance AS pp
                            WHERE
                            pp.account_id = :account_id
                            AND pp.asp_id = :asp_id
                            AND pp.units_sold != 0
                            AND CAST(pp.shipment_date AS DATE) BETWEEN :from_date AND :to_date
                            GROUP BY pp.seller_sku
                        ) c
                        LEFT JOIN (
                        SELECT
                            pp.seller_sku,
                            SUM(pp.gross_sales) AS _previous_gross_sales,
                            SUM(pp.units_sold) AS _previous_units_sold,
                            count(pp.az_order_id) AS _previous_order_count,
                            SUM(CASE WHEN CAST(pp.refund_date AS DATE) BETWEEN :prior_from_date AND :prior_to_date THEN pp.returns ELSE 0 END) AS _previous_refunds,
                            SUM(CASE WHEN CAST(pp.refund_date AS DATE) BETWEEN :prior_from_date AND :prior_to_date THEN pp.units_returned ELSE 0 END) AS _previous_units_returned,
                            SUM(CASE
                                WHEN (CAST(pp.shipment_date AS DATE) BETWEEN :prior_from_date AND :prior_to_date) OR
                                    (CAST(pp.refund_date AS DATE) BETWEEN :prior_from_date AND :prior_to_date) THEN pp.market_place_fee
                                ELSE 0
                            END) AS _previous_market_place_fee
                        FROM az_product_performance AS pp
                        WHERE pp.account_id = :account_id
                        AND pp.asp_id = :asp_id
                        AND pp.units_sold != 0
                        AND CAST(pp.shipment_date AS DATE) BETWEEN :prior_from_date AND :prior_to_date
                        GROUP BY pp.seller_sku
                        ) _p ON c.seller_sku = _p.seller_sku
                        LEFT JOIN az_item_master AS im ON c.seller_sku = im.seller_sku
                        INNER JOIN (
                            SELECT
                                im.asin,
                                SUM(im.cogs) AS product_cost
                            FROM az_item_master AS im
                            WHERE im.account_id = :account_id
                            AND im.selling_partner_id = :asp_id
                            GROUP BY im.asin
                        ) ac ON im.asin = ac.asin
                        WHERE
                        im.asin != ''
            '''

        if category:
            raw_query += ' AND im.category IN :category'

        if brand:
            raw_query += ' AND im.brand IN :brand'

        if product:
            raw_query += ' AND im.asin IN :product'

        if not calculate_total:

            count_query = f'SELECT COUNT(*) FROM ({raw_query}) AS total_count_query'

            total_count_result = db.session.execute(text(count_query), params).scalar()  # type: ignore  # noqa: FKA100

            if sort_by is not None and sort_order is not None:
                if sort_order == SortingOrder.ASC.value:
                    raw_query = raw_query + f' ORDER BY {sort_by} ASC'
                else:
                    raw_query = raw_query + f' ORDER BY {sort_by} DESC'

            if page and size:
                page = int(page) - 1
                size = int(size)
                raw_query = raw_query + f' LIMIT {size} OFFSET {page * size}'

            results = db.session.execute(text(raw_query + ';'), params).fetchall()  # type: ignore  # noqa: FKA100

            total_count = len(results)

            return results, total_count, total_count_result

        else:

            return db.session.execute(text(raw_query + ';'), params).fetchall()  # type: ignore  # noqa: FKA100

    @classmethod
    def get_performance_day_detail(cls, account_id: str, asp_id: str, from_date: str, to_date: str, seller_sku: str):
        """Get Product Performance details by day - sales, refund, units sold, etc"""

        from_date = datetime.strptime(from_date, '%Y-%m-%d').date()     # type: ignore  # noqa: FKA100
        to_date = datetime.strptime(to_date, '%Y-%m-%d').date()         # type: ignore  # noqa: FKA100

        params = {
            'from_date': from_date,
            'to_date': to_date,
            'account_id': account_id,
            'asp_id': asp_id,
            'seller_sku': seller_sku
        }

        raw_query = '''
                    SELECT
                        SUM(gross_sales)::numeric(10,2) as total_gross_sales,
                        SUM(units_sold)::int as total_units_sold,
                        SUM(units_returned)::int as total_units_returned,
                        SUM(returns)::numeric(10,2) as total_refunds,
                        (SUM(gross_sales)/NULLIF(SUM(units_sold),0))::numeric(10,2) as average_selling_price,
                        CAST(shipment_date as DATE) as shipped_date
                    FROM az_product_performance
                        WHERE
                        account_id = :account_id
                        AND
                        asp_id = :asp_id
                        AND
                        seller_sku = :seller_sku
                        AND
                        CAST(shipment_date as DATE) BETWEEN :from_date and :to_date
                        AND
                        shipment_date is not null
                    group by
                        shipped_date
                    ORDER by shipped_date asc
        '''

        results = db.session.execute(text(raw_query + ';'), params).fetchall()  # type: ignore  # noqa: FKA100

        return results

    @classmethod
    def get_sales_by_region(cls, account_id: str, asp_id: str, from_date: str, to_date: str,
                            category: Optional[tuple] = None, brand: Optional[tuple] = None, product: Optional[tuple] = None,
                            zone=False, filters: Any = None, sort_by: Any = None, sort_order: Any = None, page: Any = None, size: Any = None):
        """Get Sales by Region, If zone is true it will give sales by zone"""

        prior_from_date, prior_to_date = get_prior_to_from_date(
            from_date=from_date, to_date=to_date)

        from_date = datetime.strptime(from_date, '%Y-%m-%d').date()     # type: ignore  # noqa: FKA100
        to_date = datetime.strptime(to_date, '%Y-%m-%d').date()         # type: ignore  # noqa: FKA100

        params = {
            'from_date': from_date,
            'to_date': to_date,
            'prior_from_date': prior_from_date,
            'prior_to_date': prior_to_date,
            'account_id': account_id,
            'asp_id': asp_id,
            'category': category,
            'brand': brand,
            'product': product
        }

        filter_conditions = []

        if filters:
            # Assuming 'filters' is a dictionary containing 'pincode' and 'state_name' filters
            # if 'pincode' in filters and 'state_name' in filters:
            #     # Use the OR condition when both 'pincode' and 'state_name' are present
            #     filter_conditions.append(
            #         f"(pcm.pincode = '{filters['pincode']}' OR pcm.state_name = '{filters['state_name']}')")
            # elif 'pincode' in filters:
            #     # Use only 'pincode' filter condition
            #     filter_conditions.append(
            #         f"pcm.pincode = '{filters['pincode']}'")
            if 'state_name' in filters:
                # Use only 'state_name' filter condition
                filter_conditions.append(
                    f"pcm.state_name = '{filters['state_name']}'")

        # filter_cases = ' AND '.join(filter_conditions)

        raw_query = f'''
                    SELECT
                        SUM(CASE WHEN cast(app.shipment_date as DATE) BETWEEN :from_date AND :to_date THEN app.units_sold ELSE 0 END)::INT AS total_units_sold,
                        SUM(CASE WHEN cast(app.shipment_date as DATE) BETWEEN :from_date AND :to_date THEN app.gross_sales ELSE 0 END)::NUMERIC(10, 2) AS total_gross_sales,
                        SUM(CASE WHEN cast(app.refund_date as DATE) BETWEEN :from_date AND :to_date THEN app.units_returned ELSE 0 END)::INT AS total_units_returned,
                        SUM(CASE WHEN cast(app.refund_date as DATE) BETWEEN :from_date AND :to_date THEN app.returns ELSE 0 END)::NUMERIC(10, 2) AS total_refunds,
                        SUM(CASE WHEN CAST(app.refund_date AS DATE) BETWEEN :from_date AND :to_date THEN app.market_place_fee ELSE 0 END)::NUMERIC(10, 2) AS market_place_fee,
                        SUM((CASE WHEN cast(app.shipment_date as DATE) BETWEEN :from_date AND :to_date THEN app.units_sold ELSE 0 END - CASE WHEN cast(app.refund_date as DATE) BETWEEN :from_date AND :to_date THEN app.units_returned ELSE 0 END) * im.cogs) AS cogs,

                        SUM(CASE WHEN cast(app.shipment_date as DATE) BETWEEN :prior_from_date AND :prior_to_date THEN app.units_sold ELSE 0 END)::INT AS prior_total_units_sold,
                        SUM(CASE WHEN cast(app.shipment_date as DATE) BETWEEN :prior_from_date AND :prior_to_date THEN app.gross_sales ELSE 0 END)::NUMERIC(10, 2) AS prior_total_gross_sales,
                        SUM(CASE WHEN cast(app.refund_date as DATE) BETWEEN :prior_from_date AND :prior_to_date THEN app.units_returned ELSE 0 END)::INT AS prior_total_units_returned,
                        SUM(CASE WHEN cast(app.refund_date as DATE) BETWEEN :prior_from_date AND :prior_to_date THEN app.returns ELSE 0 END)::NUMERIC(10, 2) AS prior_total_refunds,
                        SUM(CASE WHEN CAST(app.refund_date AS DATE) BETWEEN :prior_from_date AND :prior_to_date THEN app.market_place_fee ELSE 0 END)::NUMERIC(10, 2) AS prior_market_place_fee,
                        SUM((CASE WHEN cast(app.shipment_date as DATE) BETWEEN :prior_from_date AND :prior_to_date THEN app.units_sold ELSE 0 END - CASE WHEN cast(app.refund_date as DATE) BETWEEN :prior_from_date AND :prior_to_date THEN app.units_returned ELSE 0 END) * im.cogs) AS prior_cogs,

                        {f" INITCAP(pcm.state_name) as state," if not zone else ""}
                        INITCAP(pcm.zone) as zone
                    FROM
                        az_product_performance as app
                    LEFT JOIN
                        az_item_master as im ON app.seller_sku = im.seller_sku
                    LEFT JOIN
                        az_order_report as aor ON app.az_order_id = aor.amazon_order_id
                    LEFT JOIN
                        postal_code_master AS pcm ON CAST(CAST(aor.ship_postal_code AS NUMERIC) AS BIGINT) = pcm.pincode
                    WHERE
                        (cast(app.shipment_date as DATE) BETWEEN :prior_from_date AND :to_date
                        OR cast(app.refund_date as DATE) BETWEEN :prior_from_date AND :to_date)
                        AND app.account_id = :account_id
                        AND app.asp_id = :asp_id
                        AND app.seller_sku IS NOT NULL
                        {f" AND im.category IN :category" if category else ""}
                        {f" AND im.brand IN :brand" if brand else ""}
                        {f" AND im.asin IN :product" if product else ""}
                    GROUP BY
                        {f" INITCAP(pcm.state_name)," if not zone else ""}
                        INITCAP(pcm.zone)
                    '''

        count_query = f'SELECT COUNT(*) FROM ({raw_query}) AS total_count_query'
        total_count_result = db.session.execute(text(count_query), params).scalar()     # type: ignore  # noqa: FKA100

        if sort_by is not None and sort_order is not None:
            if sort_order == SortingOrder.ASC.value:
                raw_query = raw_query + f' ORDER BY {sort_by} ASC'
            else:
                raw_query = raw_query + f' ORDER BY {sort_by} DESC'

        results = db.session.execute(text(raw_query + ';'), params).fetchall()     # type: ignore  # noqa: FKA100

        total_count = len(results)

        return results, total_count, total_count_result

    @classmethod
    def get_heatmap(cls, account_id: str, asp_id: str, from_date: str, to_date: str, sku: str, zone=False):
        """Get product performance heatmap , state and zone wise"""

        from_date = datetime.strptime(from_date, '%Y-%m-%d').date()     # type: ignore  # noqa: FKA100
        to_date = datetime.strptime(to_date, '%Y-%m-%d').date()         # type: ignore  # noqa: FKA100

        params = {
            'from_date': from_date,
            'to_date': to_date,
            'account_id': account_id,
            'asp_id': asp_id,
            'sku': sku
        }

        raw_query = f'''
                    SELECT
                        SUM(CASE WHEN cast(app.shipment_date as DATE) BETWEEN :from_date AND :to_date THEN app.units_sold ELSE 0 END)::INT AS total_units_sold,
                        SUM(CASE WHEN cast(app.shipment_date as DATE) BETWEEN :from_date AND :to_date THEN app.gross_sales ELSE 0 END)::NUMERIC(10, 2) AS total_gross_sales,
                        SUM(CASE WHEN cast(app.refund_date as DATE) BETWEEN :from_date AND :to_date THEN app.units_returned ELSE 0 END)::INT AS total_units_returned,
                        SUM(CASE WHEN cast(app.refund_date as DATE) BETWEEN :from_date AND :to_date THEN app.returns ELSE 0 END)::NUMERIC(10, 2) AS total_refunds,
                        {f" INITCAP(pcm.state_name) as state," if not zone else ""}
                        INITCAP(pcm.zone) as zone
                    FROM
                        az_product_performance as app
                    LEFT JOIN
                        az_item_master as im ON app.seller_sku = im.seller_sku
                    LEFT JOIN
                        az_order_report as aor ON app.az_order_id = aor.amazon_order_id
                    LEFT JOIN
                        postal_code_master AS pcm ON CAST(CAST(aor.ship_postal_code AS NUMERIC) AS BIGINT) = pcm.pincode
                    WHERE
                        (cast(app.shipment_date as DATE) BETWEEN :from_date AND :to_date
                        OR cast(app.refund_date as DATE) BETWEEN :from_date AND :to_date)
                        AND app.account_id = :account_id
                        AND app.asp_id = :asp_id
                        AND im.seller_sku = :sku
                    GROUP BY
                        {f" INITCAP(pcm.state_name)," if not zone else ""}
                        INITCAP(pcm.zone)
                    '''

        results = db.session.execute(text(raw_query + ';'), params).fetchall()     # type: ignore  # noqa: FKA100

        return results

    @classmethod
    def get_sales_by_region_pin_state(cls, account_id: str, asp_id: str, from_date: str, to_date: str,
                                      category: Optional[tuple], brand: Optional[tuple], product: Optional[tuple]):
        """Get Pincode and State Name for Sales by Region"""

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
                        SUM(gross_sales),
                        state_name
                    FROM (
                        SELECT
                            sum(app.gross_sales) as gross_sales,
                            pcm.state_name
                        FROM
                            az_product_performance AS app
                        LEFT JOIN
                            az_order_report AS aor ON app.az_order_id = aor.amazon_order_id
                        JOIN
                            postal_code_master AS pcm ON CAST(CAST(aor.ship_postal_code AS NUMERIC) AS BIGINT) = pcm.pincode
                        JOIN
                            az_item_master AS im ON aor.sku = im.seller_sku
                        WHERE
                        app.account_id = :account_id
                        AND app.asp_id = :asp_id
                        AND app.units_sold !=0
                        AND CAST(app.shipment_date AS DATE) BETWEEN :from_date AND :to_date
                        {f" AND im.category IN :category" if category else ""}
                        {f" AND im.brand IN :brand" if brand else ""}
                        {f" AND im.asin IN :product" if product else ""}
                        GROUP BY
                            pcm.state_name
                    ) AS c GROUP BY state_name
        '''

        results = db.session.execute(text(raw_query + ';'), params).fetchall()     # type: ignore  # noqa: FKA100

        return results

    @classmethod
    def get_zonal_sales_stats(cls, account_id: str, asp_id: str, from_date: str, to_date: str,
                              category: Optional[tuple], brand: Optional[tuple], product: Optional[tuple]):
        """Get Zone Level Sales statistics"""

        prior_from_date, prior_to_date = get_prior_to_from_date(
            from_date=from_date, to_date=to_date)

        from_date = datetime.strptime(from_date, '%Y-%m-%d').date()     # type: ignore  # noqa: FKA100
        to_date = datetime.strptime(to_date, '%Y-%m-%d').date()     # type: ignore  # noqa: FKA100

        params = {
            'from_date': from_date,
            'to_date': to_date,
            'prior_from_date': prior_from_date,
            'prior_to_date': prior_to_date,
            'account_id': account_id,
            'asp_id': asp_id,
            'category': category,
            'brand': brand,
            'product': product
        }

        raw_query = f'''
                    SELECT
                        zone,
                        total_gross_sales,
                        total_refunds,
                        CASE
                            WHEN _p._p_total_gross_sales > 0
                            THEN ABS(CAST((c.total_gross_sales - _p._p_total_gross_sales) / _p._p_total_gross_sales * 100 AS NUMERIC(10, 2)))
                            ELSE 0.00
                        END AS total_gross_sales_percentage_growth,
                        CASE
                            WHEN _p._p_total_refunds > 0 THEN ABS(CAST((c.total_refunds - _p._p_total_refunds) / _p._p_total_refunds * 100 AS NUMERIC(10, 2)))
                            ELSE 0.00
                        END AS total_refunds_percentage
                    FROM (
                        SELECT
                        pcm.zone,
                        SUM(app.gross_sales)::NUMERIC(10, 2) AS total_gross_sales,
                        ABS(SUM(CASE WHEN CAST(app.refund_date AS DATE) BETWEEN :from_date AND :to_date THEN app.returns ELSE 0 END))::NUMERIC(10, 2) AS total_refunds
                        FROM
                            az_product_performance AS app
                        JOIN
                            az_order_report AS aor ON app.az_order_id = aor.amazon_order_id
                        JOIN
                            postal_code_master AS pcm ON CAST(CAST(aor.ship_postal_code AS NUMERIC) AS BIGINT) = pcm.pincode
                        JOIN
                            az_item_master AS im ON aor.sku = im.seller_sku
                        WHERE
                            app.units_sold != 0
                            AND app.account_id = :account_id
                            AND app.asp_id = :asp_id
                            AND CAST(app.shipment_date AS DATE) BETWEEN :from_date AND :to_date
                            {f" AND im.category IN :category" if category else ""}
                            {f" AND im.brand IN :brand" if brand else ""}
                            {f" AND im.asin IN :product" if product else ""}
                        GROUP BY
                        pcm.zone
                    ) AS c
                    LEFT JOIN
                    (
                        SELECT
                        pcm.zone as _p_zone,
                        SUM(app.gross_sales)::NUMERIC(10, 2) AS _p_total_gross_sales,
                        ABS(SUM(CASE WHEN CAST(app.refund_date AS DATE) BETWEEN :prior_from_date AND :prior_to_date THEN app.returns ELSE 0 END))::NUMERIC(10, 2) AS _p_total_refunds
                        FROM
                            az_product_performance AS app
                        JOIN
                            az_order_report AS aor ON app.az_order_id = aor.amazon_order_id
                        JOIN
                            postal_code_master AS pcm ON CAST(CAST(aor.ship_postal_code AS NUMERIC) AS BIGINT) = pcm.pincode
                        JOIN
                            az_item_master AS im ON aor.sku = im.seller_sku
                        WHERE
                            app.units_sold != 0
                            AND app.account_id = :account_id
                            AND app.asp_id = :asp_id
                            AND CAST(app.shipment_date AS DATE) BETWEEN :prior_from_date AND :prior_to_date
                            {f" AND im.category IN :category" if category else ""}
                            {f" AND im.brand IN :brand" if brand else ""}
                            {f" AND im.asin IN :product" if product else ""}
                        GROUP BY
                        pcm.zone
                    ) AS _p on c.zone=_p._p_zone
        '''

        results = db.session.execute(text(raw_query + ';'), params).fetchall()    # type: ignore  # noqa: FKA100

        return results

    @staticmethod
    def get_product_performance_by_ad(account_id: str, asp_id: str, from_date: str, to_date: str, category: Optional[tuple] = None, brand: Optional[tuple] = None,
                                      product: Optional[tuple] = None, filters: Any = None, sort: Any = None, page: Any = None, size: Any = None):
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

        raw_query = '''
                    SELECT
                    im.item_name,
                    im.face_image,
                    im.seller_sku,
                    im.asin,
                    im.brand,
                    im.category,
                    im.subcategory,
                    im.category_rank,
                    im.subcategory_rank,
                    total_gross_sales,
                    total_units_sold,
                    asp_ad_sales as total_asp_ad_sales,
                    asd_ad_sales as total_asd_ad_sales,
                    asp_ad_clicks as total_asp_ad_clicks,
                    asd_ad_clicks as total_asd_ad_clicks,
                    asp_ad_spends as total_asp_ad_spends,
                    asd_ad_spends as total_asd_ad_spends,
                    asp_ad_impressions as total_asp_ad_impressions,
                    asd_ad_impressions as total_asd_ad_impressions,
                    asp_ad_orders as total_asp_ad_orders,
                    asp_units_sold as total_asp_units_sold,
                    asd_ad_orders as total_asd_ad_orders,
                    asd_units_sold as total_asd_units_sold,
                    sub_query_az_sales_traffic_asin.page_views as page_views,
                    sub_query_az_sales_traffic_asin.units_ordered as units_ordered,
                    sub_query_az_sales_traffic_asin.sessions as sessions
                    FROM az_item_master as im
                    LEFT JOIN
                    (
                        SELECT
                        app.seller_sku,
                        sum(app.gross_sales) as total_gross_sales,
                        sum(app.units_sold) as total_units_sold
                        from az_product_performance as app
                        WHERE
                        app.account_id = :account_id
                        AND app.asp_id = :asp_id
                        AND CAST(app.shipment_date AS DATE) BETWEEN :from_date AND :to_date
                        and app.seller_sku is not null
                        group by
                        app.seller_sku
                    ) as sub_query_app
                    on im.seller_sku=sub_query_app.seller_sku
                    LEFT JOIN
                    (
                        select
                        asp.advertised_sku as asp_advertised_sku,
                        SUM(asp.clicks) as asp_ad_clicks,
                        SUM(asp.spend) as asp_ad_spends,
                        SUM(asp.sales_7d) as asp_ad_sales,
                        SUM(asp.impressions) as asp_ad_impressions,
                        SUM(asp.purchases_7d) as asp_ad_orders,
                        SUM(asp.units_sold_clicks_7d) as asp_units_sold
                        from az_sponsored_product as asp
                        WHERE
                        asp.account_id = :account_id
                        AND asp.asp_id = :asp_id
                        AND asp.payload_date BETWEEN :from_date AND :to_date
                        group by advertised_sku
                    ) as sub_query_asp
                    on im.seller_sku=sub_query_asp.asp_advertised_sku
                    LEFT JOIN
                    (
                        select
                        asd.sku as asd_advertised_sku,
                        SUM(asd.clicks) as asd_ad_clicks,
                        SUM(asd.attributed_sales_14d) as asd_ad_sales,
                        SUM(asd.cost * asd.clicks) as asd_ad_spends,
                        SUM(asd.impressions) as asd_ad_impressions,
                        SUM(asd.attributed_conversions_7d) as asd_ad_orders,
                        SUM(asd.attributed_units_ordered_7d) as asd_units_sold
                        from az_sponsored_display as asd
                        WHERE
                        asd.account_id = :account_id
                        AND asd.asp_id = :asp_id
                        AND asd.payload_date BETWEEN :from_date AND :to_date
                        group by sku
                    ) as sub_query_asd
                    on im.seller_sku=sub_query_asd.asd_advertised_sku
                    LEFT JOIN (
                        SELECT
                        child_asin as asin,
                        SUM(units_ordered) as units_ordered,
                        SUM(page_views) as page_views,
                        SUM(sessions) as sessions
                        FROM az_sales_traffic_asin
                        WHERE
                        payload_date BETWEEN :from_date AND :to_date
                        and asp_id = :asp_id
                        and account_id = :account_id
                        GROUP BY child_asin
                    ) as sub_query_az_sales_traffic_asin
                    on im.asin=sub_query_az_sales_traffic_asin.asin
                    WHERE im.seller_sku in (select seller_sku from az_product_performance where CAST(shipment_date AS DATE) BETWEEN :from_date AND :to_date group by seller_sku)
                    AND im.account_id = :account_id
                    AND im.selling_partner_id = :asp_id
        '''

        if category:
            raw_query += ' AND im.category IN :category'

        if brand:
            raw_query += ' AND im.brand IN :brand'

        if product:
            raw_query += ' AND im.asin IN :product'

        count_query = f'SELECT COUNT(*) FROM ({raw_query}) AS total_count_query'
        total_count_result = db.session.execute(text(count_query), params).scalar()     # type: ignore  # noqa: FKA100

        if page and size:
            page = int(page) - 1
            size = int(size)
            raw_query = raw_query + f' LIMIT {size} OFFSET {page * size}'

        results = db.session.execute(text(raw_query + ';'), params).fetchall()     # type: ignore  # noqa: FKA100

        total_count = len(results)

        return results, total_count, total_count_result

    @staticmethod
    def get_performance_by_zone(account_id: str, asp_id: str, from_date: str, to_date: str):
        """Get Ad Impact data for different market zones like optimal, opportunity and work in progress zones"""

        from_date = datetime.strptime(from_date, '%Y-%m-%d').date()     # type: ignore  # noqa: FKA100
        to_date = datetime.strptime(to_date, '%Y-%m-%d').date()         # type: ignore  # noqa: FKA100

        params = {
            'from_date': from_date,
            'to_date': to_date,
            'account_id': account_id,
            'asp_id': asp_id,
        }

        raw_query = '''
                    SELECT
                    max(im.item_name) as item_name,
                    max(im.face_image) as face_image,
                    max(im.seller_sku) as seller_sku,
                    max(im.asin) as asin,
                    max(im.brand) as brand,
                    max(im.category) as category,
                    max(im.subcategory) as sub_category,
                    max(im.category_rank) as category_rank,
                    max(im.subcategory_rank) as subcategory_rank,
                    coalesce(sum(total_orders),0) as total_orders,
                    coalesce(sum(total_gross_sales),0) as total_gross_sales,
                    coalesce(sum(total_units_sold),0) as total_units_sold,
                    coalesce(sum(asp_ad_sales),0) as as_product_sales,
                    coalesce(sum(asd_ad_sales),0) as as_display_sales,
                    coalesce(sum(asp_ad_clicks),0) as as_product_clicks,
                    coalesce(sum(asd_ad_clicks),0) as as_display_clicks,
                    coalesce(sum(asp_ad_spends),0) as as_product_spends,
                    coalesce(sum(asd_ad_spends),0) as as_display_spends,
                    coalesce(sum(asp_ad_impressions),0) as as_product_impressions,
                    coalesce(sum(asd_ad_impressions),0) as as_display_impressions,
                    coalesce(sum(asp_ad_orders),0) as as_product_orders,
                    coalesce(sum(asp_units_sold),0) as as_product_units_sold,
                    coalesce(sum(asd_ad_orders),0) as as_display_orders,
                    coalesce(sum(asd_units_sold),0) as as_display_units_sold,
                    coalesce(max(page_views),0) as page_views,
                    coalesce(max(sessions),0) as sessions,
                    coalesce(max(units_ordered),0) as units_ordered,
                    coalesce(sum((total_orders * 100)/nullif(page_views,0)),0) as asin_conversion_rate
                    FROM az_item_master as im
                    LEFT JOIN
                    (
                        SELECT
                        app.seller_sku,
                        sum(app.gross_sales) as total_gross_sales,
                        sum(app.units_sold) as total_units_sold,
                        count(app.az_order_id) as total_orders
                        from az_product_performance as app
                        WHERE
                        app.account_id = :account_id
                        AND app.asp_id = :asp_id
                        AND CAST(app.shipment_date AS DATE) BETWEEN :from_date AND :to_date
                        and app.seller_sku is not null
                        group by
                        app.seller_sku
                    ) as sub_query_app
                    on im.seller_sku=sub_query_app.seller_sku
                    LEFT JOIN
                    (
                        select
                        asp.advertised_sku as asp_advertised_sku,
                        SUM(asp.clicks) as asp_ad_clicks,
                        SUM(asp.spend) as asp_ad_spends,
                        SUM(asp.sales_7d) as asp_ad_sales,
                        SUM(asp.impressions) as asp_ad_impressions,
                        SUM(asp.purchases_7d) as asp_ad_orders,
                        SUM(asp.units_sold_clicks_7d) as asp_units_sold
                        from az_sponsored_product as asp
                        WHERE
                        asp.account_id = :account_id
                        AND asp.asp_id = :asp_id
                        AND asp.payload_date BETWEEN :from_date AND :to_date
                        group by advertised_sku
                    ) as sub_query_asp
                    on im.seller_sku=sub_query_asp.asp_advertised_sku
                    LEFT JOIN
                    (
                        select
                        asd.sku as asd_advertised_sku,
                        SUM(asd.clicks) as asd_ad_clicks,
                        SUM(asd.attributed_sales_14d) as asd_ad_sales,
                        SUM(asd.cost * asd.clicks) as asd_ad_spends,
                        SUM(asd.impressions) as asd_ad_impressions,
                        SUM(asd.attributed_conversions_7d) as asd_ad_orders,
                        SUM(asd.attributed_units_ordered_7d) as asd_units_sold
                        from az_sponsored_display as asd
                        WHERE
                        asd.account_id = :account_id
                        AND asd.asp_id = :asp_id
                        AND asd.payload_date BETWEEN :from_date AND :to_date
                        group by sku
                    ) as sub_query_asd
                    on im.seller_sku=sub_query_asd.asd_advertised_sku
                    LEFT JOIN (
                        SELECT
                        child_asin as asin,
                        SUM(page_views) as page_views,
                        SUM(sessions) as sessions,
                        SUM(units_ordered) as units_ordered
                        FROM az_sales_traffic_asin
                        WHERE
                        account_id = :account_id
                        AND asp_id = :asp_id
                        AND payload_date BETWEEN :from_date AND :to_date
                        GROUP BY child_asin
                    ) as sub_query_az_sales_traffic_asin
                    on im.asin=sub_query_az_sales_traffic_asin.asin
                    where im.account_id = :account_id
                    AND im.selling_partner_id = :asp_id
                    group by im.asin
        '''

        count_query = f'SELECT COUNT(*) FROM ({raw_query}) AS total_count_query'
        total_count_result = db.session.execute(text(count_query), params).scalar()     # type: ignore  # noqa: FKA100

        # if page and size:
        #     page = int(page) - 1
        #     size = int(size)
        #     raw_query = raw_query + f' LIMIT {size} OFFSET {page * size}'

        results = db.session.execute(text(raw_query + ';'), params).fetchall()     # type: ignore  # noqa: FKA100

        total_count = len(results)

        return results, total_count, total_count_result

    @classmethod
    def get_mr_sales_info(cls, account_id: str, asp_id: str, from_date: str, to_date: str, category: Optional[tuple] = None, brand: Optional[tuple] = None,
                          product: Optional[tuple] = None):
        """Gets ad impact statistics for a given time period."""

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
                    select
                    shipment_date,
                    sum(gross_sales)::numeric(10,2) as gross_sales,
                    sum(page_views) as page_views,
                    sum(total_units_sold) as total_units_sold
                    from(
                    SELECT
                    cast(app.shipment_date as DATE),
                    sum(app.gross_sales)::numeric(10,2) as gross_sales,
                    max(sts.page_views) as page_views,
                    sum(app.units_sold) as total_units_sold
                    from az_product_performance as app
                    left join az_item_master as im
                    on app.seller_sku=im.seller_sku
                    left join
                    (
                        SELECT child_asin, sum(page_views) as page_views, payload_date from
                        az_sales_traffic_asin
                        where payload_date between :from_date and :to_date
                        {f" AND az_sales_traffic_asin.child_asin IN :product" if product else ""}
                        AND az_sales_traffic_asin.account_id = :account_id
                        AND az_sales_traffic_asin.asp_id = :asp_id
                        group by child_asin, payload_date
                    ) as sts on im.asin=sts.child_asin and cast(app.shipment_date as DATE)=sts.payload_date
                    where
                    cast(app.shipment_date as DATE) between :from_date and :to_date
                    {f" AND im.category IN :category" if category else ""}
                    {f" AND im.brand IN :brand" if brand else ""}
                    {f" AND im.asin IN :product" if product else ""}
                    AND app.account_id = :account_id
                    AND app.asp_id = :asp_id
                    group by cast(app.shipment_date as DATE), asin
                    ) as subquery
                    group by shipment_date
                    '''

        results = db.session.execute(text(raw_query + ';'), params).fetchall()  # type: ignore  # noqa: FKA100

        return results

    @classmethod
    def get_unclaimed_refunds(cls, account_id: str, asp_id: str, report_type: Optional[str] = None, sort_by: Any = None, sort_order: Any = None, page: Any = None, size: Any = None, brand: Optional[tuple] = None):
        """Get Unclaimed refunds for refund insights"""

        params = {
            'account_id': account_id,
            'asp_id': asp_id,
            'report_type': report_type,
            'brand': brand
        }

        if report_type is not None:

            raw_query = f'''
                            SELECT
                            app.shipment_date as order_date,
                            app.az_order_id,
                            app.refund_date as report_date,
                            COALESCE(fba_returns.detailed_disposition, '{AzFbaReturnsReportType.LOST.value}') as report_type,
                            app.asp_id as seller,
                            fba_reimbursements.amount_total as potential_refund,
                            app.units_returned as refundable_items,
                            fba_returns.order_id as returns_order_id,
                            {'im.seller_sku, im.item_name, im.asin, fba_returns.fnsku, ABS(app.returns) as return_value, ABS(app.reverse_fba_fee) as reverse_fba_fee, (ABS(app.returns)::numeric - ABS(app.reverse_fba_fee)::numeric) * 0.25 as potential_refund, fba_returns.detailed_disposition as return_notes, fba_returns.reason as return_reason, fba_returns.fulfillment_center_id as fc_id, ' if report_type is not None else ''}
                            fba_reimbursements.az_order_id as reimbursements_az_order_id,
                            shipment_info.fulfillment_center_id as shipment_fc_id,
                            shipment_info.fnsku as shipment_fnsku
                            FROM az_product_performance app
                            LEFT JOIN az_fba_returns as fba_returns
                            ON app.az_order_id=fba_returns.order_id
                            LEFT JOIN az_fba_reimbursements as fba_reimbursements
                            ON app.az_order_id=fba_reimbursements.az_order_id
                            {'LEFT JOIN az_item_master as im on app.seller_sku=im.seller_sku ' if report_type is not None else ''}
                            LEFT JOIN (
                                SELECT
                                    amazon_order_id,
                                    fulfillment_center_id,
                                    fnsku
                                FROM az_fba_customer_shipment_sales
                                group by amazon_order_id, fulfillment_center_id, fnsku
                            ) AS shipment_info
                            ON app.az_order_id = shipment_info.amazon_order_id
                            WHERE
                            app.asp_id = :asp_id
                            AND app.account_id =:account_id
                            AND app.shipment_date IS NOT NULL
                            {f" AND app.brand IN :brand" if brand else ""}
                            {f" AND COALESCE(fba_returns.detailed_disposition, '{AzFbaReturnsReportType.LOST.value}') = :report_type"  if report_type is not None else ""}
                            AND app.refund_date IS NOT NULL
                            AND cast(app.refund_date as DATE) BETWEEN CURRENT_TIMESTAMP - INTERVAL '75 DAY' AND CURRENT_TIMESTAMP - INTERVAL '45 DAY'
                            AND (fba_returns.detailed_disposition != 'SELLABLE'
                            OR (fba_returns.order_id is null and fba_reimbursements.az_order_id is null))
                            ORDER BY app.refund_date asc
                        '''

        else:
            raw_query = f'''
                            SELECT
                            COALESCE(fba_returns.detailed_disposition, 'LOST') as report_type,
                            app.asp_id as seller,
                            SUM(ABS(app.returns) - ABS(app.reverse_fba_fee))::Numeric(10,2) * 0.25 as potential_refund,
                            SUM(app.units_returned) as refundable_items
                            FROM az_product_performance app
                            LEFT JOIN az_fba_returns as fba_returns
                            ON app.az_order_id=fba_returns.order_id
                            LEFT JOIN az_fba_reimbursements as fba_reimbursements
                            ON app.az_order_id=fba_reimbursements.az_order_id
                            WHERE
                            app.asp_id = :asp_id
                            AND app.account_id =:account_id
                            {f" AND app.brand IN :brand" if brand else ""}
                            AND
                            app.shipment_date IS NOT NULL
                            AND app.refund_date IS NOT NULL
                            AND cast(app.refund_date as DATE) BETWEEN CURRENT_TIMESTAMP - INTERVAL '75 DAY' AND CURRENT_TIMESTAMP - INTERVAL '45 DAY'
                            AND (fba_returns.detailed_disposition != 'SELLABLE'
                            OR (fba_returns.order_id is null and fba_reimbursements.az_order_id is null))
                            group by COALESCE(fba_returns.detailed_disposition, 'LOST'), app.asp_id
                        '''

        count_query = f'SELECT COUNT(*) FROM ({raw_query}) AS total_count_query'
        total_count_result = db.session.execute(text(count_query), params).scalar()     # type: ignore  # noqa: FKA100

        if sort_by is not None and sort_order is not None:
            if sort_order == SortingOrder.ASC.value:
                raw_query = raw_query + f' ORDER BY {sort_by} ASC'
            else:
                raw_query = raw_query + f' ORDER BY {sort_by} DESC'

        if page and size:
            page = int(page) - 1
            size = int(size)
            raw_query = raw_query + f' LIMIT {size} OFFSET {page * size}'

        results = db.session.execute(text(raw_query + ';'), params).fetchall()     # type: ignore  # noqa: FKA100

        total_count = len(results)

        return results, total_count, total_count_result
