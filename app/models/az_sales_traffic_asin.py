"""
    Database model for storing sales and traffic by asin data
"""
from datetime import datetime
import time
from typing import Any
from typing import Optional

from app import db
from app.helpers.constants import DbAnomalies
from app.models.az_item_master import AzItemMaster
from app.models.base import Base
from sqlalchemy import text
# from datetime import timedelta


class AzSalesTrafficAsin(Base):
    """
        SalestrafficAsin model to store sales traffic by asin data in database
    """
    __tablename__ = 'az_sales_traffic_asin'

    id = db.Column(db.BigInteger, primary_key=True)
    account_id = db.Column(db.String(36))
    asp_id = db.Column(db.String(255))
    category = db.Column(db.String(250), nullable=True)
    brand = db.Column(db.String(250), nullable=True)
    parent_asin = db.Column(db.String(255))
    child_asin = db.Column(db.String(255))
    payload_date = db.Column(db.Date)
    units_ordered = db.Column(db.Integer)
    units_ordered_b2b = db.Column(db.Integer)
    ordered_product_sales_amount = db.Column(db.Numeric)
    ordered_product_sales_amount_b2b = db.Column(db.Numeric)
    ordered_product_sales_currency_code = db.Column(db.String(50))
    ordered_product_sales_currency_code_b2b = db.Column(db.String(50))
    total_order_items = db.Column(db.Integer)
    total_order_items_b2b = db.Column(db.Integer)
    browser_sessions = db.Column(db.Integer)
    browser_sessions_b2b = db.Column(db.Integer)
    mobile_app_sessions = db.Column(db.Integer)
    mobile_app_sessions_b2b = db.Column(db.Integer)
    sessions = db.Column(db.Integer)
    sessions_b2b = db.Column(db.Integer)
    browser_session_percentage = db.Column(db.Numeric)
    browser_session_percentage_b2b = db.Column(db.Numeric)
    mobile_app_session_percentage = db.Column(db.Numeric)
    mobile_app_session_percentage_b2b = db.Column(db.Numeric)
    session_percentage = db.Column(db.Numeric)
    session_percentage_b2b = db.Column(db.Numeric)
    browser_page_views = db.Column(db.Integer)
    browser_page_views_b2b = db.Column(db.Integer)
    mobile_app_page_views = db.Column(db.Integer)
    mobile_app_page_views_b2b = db.Column(db.Integer)
    page_views = db.Column(db.Integer)
    page_views_b2b = db.Column(db.Integer)
    browser_page_views_percentage = db.Column(db.Numeric)
    browser_page_views_percentage_b2b = db.Column(db.Numeric)
    mobile_app_page_views_percentage = db.Column(db.Numeric)
    mobile_app_page_views_percentage_b2b = db.Column(db.Numeric)
    page_views_percentage = db.Column(db.Numeric)
    page_views_percentage_b2b = db.Column(db.Numeric)
    buy_box_percentage = db.Column(db.Numeric)
    buy_box_percentage_b2b = db.Column(db.Numeric)
    unit_session_percentage = db.Column(db.Numeric)
    unit_session_percentage_b2b = db.Column(db.Numeric)
    asin_granularity = db.Column(db.String(50))
    hourly_sales = db.Column(db.JSON, nullable=True)
    created_at = db.Column(db.BigInteger)
    updated_at = db.Column(db.BigInteger)

    @classmethod
    def add(cls, account_id: str, asp_id: str, parent_asin: str, child_asin: str, payload_date: str, units_ordered: int,
            units_ordered_b2b: int, ordered_product_sales_amount: float, ordered_product_sales_amount_b2b: float,
            ordered_product_sales_currency_code: str, ordered_product_sales_currency_code_b2b: str, total_order_items: int,
            total_order_items_b2b: int, browser_sessions: int, browser_sessions_b2b: int, mobile_app_sessions: int,
            mobile_app_sessions_b2b: int, sessions: int, sessions_b2b: int, browser_session_percentage: float,
            browser_session_percentage_b2b: float, mobile_app_session_percentage: float, mobile_app_session_percentage_b2b: float,
            session_percentage: float, session_percentage_b2b: float, browser_page_views: int, browser_page_views_b2b: int,
            mobile_app_page_views: int, mobile_app_page_views_b2b: int, page_views: int, page_views_b2b: int,
            browser_page_views_percentage: float, browser_page_views_percentage_b2b: float, mobile_app_page_views_percentage: float,
            mobile_app_page_views_percentage_b2b: float, page_views_percentage: float, page_views_percentage_b2b: float,
            buy_box_percentage: float, buy_box_percentage_b2b: float, unit_session_percentage: float, unit_session_percentage_b2b: float,
            asin_granularity: str):
        """Create a new sales traffic asin entry"""

        category, brand = None, None

        if child_asin:
            category, brand = AzItemMaster.get_category_brand_by_asin(
                account_id=account_id, asp_id=asp_id, asin=child_asin)

        sales_traffic_asin = cls()
        sales_traffic_asin.account_id = account_id
        sales_traffic_asin.asp_id = asp_id
        sales_traffic_asin.category = category
        sales_traffic_asin.brand = brand
        sales_traffic_asin.parent_asin = parent_asin
        sales_traffic_asin.child_asin = child_asin
        sales_traffic_asin.payload_date = payload_date
        sales_traffic_asin.units_ordered = units_ordered
        sales_traffic_asin.units_ordered_b2b = units_ordered_b2b
        sales_traffic_asin.ordered_product_sales_amount = ordered_product_sales_amount
        sales_traffic_asin.ordered_product_sales_amount_b2b = ordered_product_sales_amount_b2b
        sales_traffic_asin.ordered_product_sales_currency_code = ordered_product_sales_currency_code
        sales_traffic_asin.ordered_product_sales_currency_code_b2b = ordered_product_sales_currency_code_b2b
        sales_traffic_asin.total_order_items = total_order_items
        sales_traffic_asin.total_order_items_b2b = total_order_items_b2b
        sales_traffic_asin.browser_sessions = browser_sessions
        sales_traffic_asin.browser_sessions_b2b = browser_sessions_b2b
        sales_traffic_asin.mobile_app_sessions = mobile_app_sessions
        sales_traffic_asin.mobile_app_sessions_b2b = mobile_app_sessions_b2b
        sales_traffic_asin.sessions = sessions
        sales_traffic_asin.sessions_b2b = sessions_b2b
        sales_traffic_asin.browser_session_percentage = browser_session_percentage
        sales_traffic_asin.browser_session_percentage_b2b = browser_session_percentage_b2b
        sales_traffic_asin.mobile_app_session_percentage = mobile_app_session_percentage
        sales_traffic_asin.mobile_app_session_percentage_b2b = mobile_app_session_percentage_b2b
        sales_traffic_asin.session_percentage = session_percentage
        sales_traffic_asin.session_percentage_b2b = session_percentage_b2b
        sales_traffic_asin.browser_page_views = browser_page_views
        sales_traffic_asin.browser_page_views_b2b = browser_page_views_b2b
        sales_traffic_asin.mobile_app_page_views = mobile_app_page_views
        sales_traffic_asin.mobile_app_page_views_b2b = mobile_app_page_views_b2b
        sales_traffic_asin.page_views = page_views
        sales_traffic_asin.page_views_b2b = page_views_b2b
        sales_traffic_asin.browser_page_views_percentage = browser_page_views_percentage
        sales_traffic_asin.browser_page_views_percentage_b2b = browser_page_views_percentage_b2b
        sales_traffic_asin.mobile_app_page_views_percentage = mobile_app_page_views_percentage
        sales_traffic_asin.mobile_app_page_views_percentage_b2b = mobile_app_page_views_percentage_b2b
        sales_traffic_asin.page_views_percentage = page_views_percentage
        sales_traffic_asin.page_views_percentage_b2b = page_views_percentage_b2b
        sales_traffic_asin.buy_box_percentage = buy_box_percentage
        sales_traffic_asin.buy_box_percentage_b2b = buy_box_percentage_b2b
        sales_traffic_asin.unit_session_percentage = unit_session_percentage
        sales_traffic_asin.unit_session_percentage_b2b = unit_session_percentage_b2b
        sales_traffic_asin.asin_granularity = asin_granularity
        sales_traffic_asin.created_at = int(time.time())
        sales_traffic_asin.save()

        return sales_traffic_asin

    @classmethod
    def insert_or_update(cls, account_id: str, asp_id: str, parent_asin: str, child_asin: str, payload_date: str, units_ordered: int,
                         units_ordered_b2b: int, ordered_product_sales_amount: float, ordered_product_sales_amount_b2b: float,
                         ordered_product_sales_currency_code: str, ordered_product_sales_currency_code_b2b: str, total_order_items: int,
                         total_order_items_b2b: int, browser_sessions: int, browser_sessions_b2b: int, mobile_app_sessions: int,
                         mobile_app_sessions_b2b: int, sessions: int, sessions_b2b: int, browser_session_percentage: float,
                         browser_session_percentage_b2b: float, mobile_app_session_percentage: float, mobile_app_session_percentage_b2b: float,
                         session_percentage: float, session_percentage_b2b: float, browser_page_views: int, browser_page_views_b2b: int,
                         mobile_app_page_views: int, mobile_app_page_views_b2b: int, page_views: int, page_views_b2b: int,
                         browser_page_views_percentage: float, browser_page_views_percentage_b2b: float, mobile_app_page_views_percentage: float,
                         mobile_app_page_views_percentage_b2b: float, page_views_percentage: float, page_views_percentage_b2b: float,
                         buy_box_percentage: float, buy_box_percentage_b2b: float, unit_session_percentage: float, unit_session_percentage_b2b: float,
                         asin_granularity: str):
        """insert or update a new sales traffic asin entry"""

        category, brand = None, None

        if child_asin:
            category, brand = AzItemMaster.get_category_brand_by_asin(
                account_id=account_id, asp_id=asp_id, asin=child_asin)

        sales_traffic = db.session.query(cls).filter(cls.account_id == account_id,
                                                     cls.asp_id == asp_id, cls.parent_asin == parent_asin, cls.child_asin == child_asin, cls.payload_date == payload_date).first()

        current_time = int(time.time())
        record = DbAnomalies.UPDATE.value

        if sales_traffic == None:
            record = DbAnomalies.INSERTION.value
            sales_traffic = cls(account_id=account_id,
                                asp_id=asp_id, parent_asin=parent_asin, child_asin=child_asin, payload_date=payload_date, created_at=current_time)

        sales_traffic.category = category
        sales_traffic.brand = brand
        sales_traffic.units_ordered = units_ordered
        sales_traffic.units_ordered_b2b = units_ordered_b2b
        sales_traffic.ordered_product_sales_amount = ordered_product_sales_amount
        sales_traffic.ordered_product_sales_amount_b2b = ordered_product_sales_amount_b2b
        sales_traffic.ordered_product_sales_currency_code = ordered_product_sales_currency_code
        sales_traffic.ordered_product_sales_currency_code_b2b = ordered_product_sales_currency_code_b2b
        sales_traffic.total_order_items = total_order_items
        sales_traffic.total_order_items_b2b = total_order_items_b2b
        sales_traffic.browser_sessions = browser_sessions
        sales_traffic.browser_sessions_b2b = browser_sessions_b2b
        sales_traffic.mobile_app_sessions = mobile_app_sessions
        sales_traffic.mobile_app_sessions_b2b = mobile_app_sessions_b2b
        sales_traffic.sessions = sessions
        sales_traffic.sessions_b2b = sessions_b2b
        sales_traffic.browser_session_percentage = browser_session_percentage
        sales_traffic.browser_session_percentage_b2b = browser_session_percentage_b2b
        sales_traffic.mobile_app_session_percentage = mobile_app_session_percentage
        sales_traffic.mobile_app_session_percentage_b2b = mobile_app_session_percentage_b2b
        sales_traffic.session_percentage = session_percentage
        sales_traffic.session_percentage_b2b = session_percentage_b2b
        sales_traffic.browser_page_views = browser_page_views
        sales_traffic.browser_page_views_b2b = browser_page_views_b2b
        sales_traffic.mobile_app_page_views = mobile_app_page_views
        sales_traffic.mobile_app_page_views_b2b = mobile_app_page_views_b2b
        sales_traffic.page_views = page_views
        sales_traffic.page_views_b2b = page_views_b2b
        sales_traffic.browser_page_views_percentage = browser_page_views_percentage
        sales_traffic.browser_page_views_percentage_b2b = browser_page_views_percentage_b2b
        sales_traffic.mobile_app_page_views_percentage = mobile_app_page_views_percentage
        sales_traffic.mobile_app_page_views_percentage_b2b = mobile_app_page_views_percentage_b2b
        sales_traffic.page_views_percentage = page_views_percentage
        sales_traffic.page_views_percentage_b2b = page_views_percentage_b2b
        sales_traffic.buy_box_percentage = buy_box_percentage
        sales_traffic.buy_box_percentage_b2b = buy_box_percentage_b2b
        sales_traffic.unit_session_percentage = unit_session_percentage
        sales_traffic.unit_session_percentage_b2b = unit_session_percentage_b2b
        sales_traffic.asin_granularity = asin_granularity
        sales_traffic.updated_at = current_time

        if record == DbAnomalies.INSERTION.value:
            sales_traffic.save()
        else:
            db.session.commit()

        return sales_traffic

    @classmethod
    def get_glance_summary(cls, account_id: str, asp_id: str, from_date: Optional[str] = None, to_date: Optional[str] = None, category: Optional[tuple] = None, brand: Optional[tuple] = None,
                           product: Optional[tuple] = None, sort_by: Any = None, sort_order: Any = None):
        """Get Glance view summary"""

        if from_date and to_date:
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
                            TO_CHAR(payload_date, 'Mon-YY') AS month,
                            payload_date,
                            SUM(ordered_product_sales_amount) AS total_sales,
                            CASE
                                WHEN SUM(total_units_ordered) = 0 THEN NULL
                                ELSE (SUM(ordered_product_sales_amount) / SUM(total_units_ordered))::NUMERIC(10, 2)
                            END AS asp,
                            SUM(total_units_ordered) AS units_ordered,
                            (SUM(total_units_ordered) / SUM(total_gv) * 100)::NUMERIC(10, 2) AS conversion,
                            SUM(total_gv) AS total_gv
                        FROM (
                            SELECT
                                DATE_TRUNC('month', payload_date) AS payload_date,
                                SUM(page_views) AS page_views,
                                SUM(page_views * (buy_box_percentage / 100))::NUMERIC(10, 2) AS total_gv,
                                SUM(units_ordered) AS total_units_ordered,
                                SUM(ordered_product_sales_amount) AS ordered_product_sales_amount
                            FROM
                                public.az_sales_traffic_asin
                                LEFT join az_item_master as im
                                ON az_sales_traffic_asin.child_asin=im.asin
                            WHERE
                                az_sales_traffic_asin.asp_id = :asp_id
                                AND az_sales_traffic_asin.account_id =:account_id
                                AND payload_date < DATE_TRUNC('month', CURRENT_DATE)
                                {f" AND payload_date BETWEEN :from_date and :to_date" if from_date else "AND payload_date  >  CURRENT_DATE - INTERVAL '3 months'"}
                                {f" AND im.category IN :category" if category else ""}
                                {f" AND im.brand IN :brand" if brand else ""}
                                {f" AND im.asin IN :product" if product else ""}
                            GROUP BY
                                DATE_TRUNC('month', payload_date), child_asin
                        ) AS asin_summary
                        GROUP BY
                            month, payload_date
                        ORDER BY payload_date ASC
                    '''

        count_query = f'SELECT COUNT(*) FROM ({raw_query}) AS total_count_query'
        total_count_result = db.session.execute(text(count_query), params).scalar()     # type: ignore  # noqa: FKA100

        results = db.session.execute(text(raw_query + ';'), params).fetchall()     # type: ignore  # noqa: FKA100

        total_count = len(results)

        return results, total_count, total_count_result

    @classmethod
    def get_asin_approx_sales(cls, account_id: str, asp_id: str, product: Optional[tuple], from_date: str):
        """Get Approx Sales for asin"""

        from_date = datetime.strptime(from_date, '%Y-%m-%d').date()     # type: ignore  # noqa: FKA100

        params = {
            'account_id': account_id,
            'asp_id': asp_id,
            'product': product,
            'from_date': from_date
        }

        raw_query = '''
                        SELECT
                            child_asin,
                            CASE
                                WHEN SUM(units_ordered / NULLIF(buy_box_percentage / 100, 0)) != 0
                                THEN CAST(SUM(units_ordered / NULLIF(buy_box_percentage / 100, 0)) AS NUMERIC(10, 2))
                                ELSE 0.00
                            END AS average_sales
                        FROM
                            az_sales_traffic_asin
                        WHERE
                            account_id = :account_id
                            AND asp_id = :asp_id
                            AND
                            child_asin IN :product
                            AND
                            payload_date >= :from_date - INTERVAL '30 days'
                            AND payload_date <= :from_date
                        group by child_asin
                    '''

        results = db.session.execute(text(raw_query + ';'), params).fetchall()  # type: ignore  # noqa: FKA100

        return results

    @classmethod
    def insert_or_update_sales_data(cls, account_id: str, asp_id: str, date: str, asin: str, hourly_sales: dict, total_sales: Optional[float] = None, unit_count: Optional[int] = None, sku: Optional[str] = None, category: Optional[tuple] = None, brand: Optional[tuple] = None):
        """insert or update sales api data"""

        sales_traffic_asin = db.session.query(cls).filter(cls.account_id == account_id,
                                                          cls.asp_id == asp_id, cls.payload_date == date, cls.child_asin == asin).first()

        current_time = int(time.time())
        record = DbAnomalies.UPDATE.value

        if sales_traffic_asin == None:
            record = DbAnomalies.INSERTION.value
            sales_traffic_asin = cls(account_id=account_id,
                                     asp_id=asp_id, payload_date=date, parent_asin=asin, child_asin=asin, category=category, brand=brand, created_at=current_time)

        # three_days_ago_date = (datetime.now() - timedelta(days=3)).date()
        # payload_date = sales_traffic_asin.payload_date

        # if payload_date > three_days_ago_date:
        #     sales_traffic_asin.ordered_product_sales_amount = total_sales
        #     sales_traffic_asin.units_ordered = unit_count

        sales_traffic_asin.ordered_product_sales_amount = total_sales
        sales_traffic_asin.units_ordered = unit_count

        sales_traffic_asin.hourly_sales = hourly_sales
        sales_traffic_asin.category = category
        sales_traffic_asin.brand = brand
        sales_traffic_asin.updated_at = current_time

        if record == DbAnomalies.INSERTION.value:
            sales_traffic_asin.save()
        else:
            db.session.commit()

        return sales_traffic_asin
