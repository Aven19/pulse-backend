"""
    Database model for storing sales and traffic summary (bydate)
"""
from datetime import datetime
from datetime import timedelta
import time
from typing import Optional

from app import db
from app.helpers.constants import DbAnomalies
from app.models.base import Base


class AzSalesTrafficSummary(Base):
    """
        SalesTrafficSummary model to sales traffic by date data in database
    """
    __tablename__ = 'az_sales_traffic_summary'

    id = db.Column(db.BigInteger, primary_key=True)
    account_id = db.Column(db.String(36))
    asp_id = db.Column(db.String(255))
    category = db.Column(db.String(250), nullable=True)
    brand = db.Column(db.String(250), nullable=True)
    date = db.Column(db.Date, nullable=False)
    ordered_product_sales_amount = db.Column(db.Numeric)
    ordered_product_sales_currency_code = db.Column(db.String(50))
    ordered_product_sales_amount_b2b = db.Column(db.Numeric)
    ordered_product_sales_currency_code_b2b = db.Column(db.String(50))
    units_ordered = db.Column(db.Integer)
    units_ordered_b2b = db.Column(db.Integer)
    total_order_items = db.Column(db.Integer)
    total_order_items_b2b = db.Column(db.Integer)
    average_sales_per_order_item_amount = db.Column(db.Numeric)
    average_sales_per_order_item_currency_code = db.Column(db.String(50))
    average_sales_per_order_item_amount_b2b = db.Column(db.Numeric)
    average_sales_per_order_item_currency_code_b2b = db.Column(db.String(50))
    average_units_per_order_item = db.Column(db.Numeric)
    average_units_per_order_item_b2b = db.Column(db.Numeric)
    average_selling_price_amount = db.Column(db.Numeric)
    average_selling_price_currency_code = db.Column(db.String(50))
    average_selling_price_amount_b2b = db.Column(db.Numeric)
    average_selling_price_currency_code_b2b = db.Column(db.String(50))
    units_refunded = db.Column(db.Integer)
    refund_rate = db.Column(db.Numeric)
    claims_granted = db.Column(db.Integer)
    claims_amount_amount = db.Column(db.Numeric)
    claims_amount_currency_code = db.Column(db.String(50))
    shipped_product_sales_amount = db.Column(db.Numeric)
    shipped_product_sales_currency_code = db.Column(db.String(50))
    units_shipped = db.Column(db.Integer)
    orders_shipped = db.Column(db.Integer)
    browser_page_views = db.Column(db.BigInteger)
    browser_page_views_b2b = db.Column(db.BigInteger)
    mobile_app_page_views = db.Column(db.BigInteger)
    mobile_app_page_views_b2b = db.Column(db.BigInteger)
    page_views = db.Column(db.BigInteger)
    page_views_b2b = db.Column(db.BigInteger)
    browser_sessions = db.Column(db.BigInteger)
    browser_sessions_b2b = db.Column(db.BigInteger)
    mobile_app_sessions = db.Column(db.BigInteger)
    mobile_app_sessions_b2b = db.Column(db.BigInteger)
    sessions = db.Column(db.BigInteger)
    sessions_b2b = db.Column(db.BigInteger)
    buy_box_percentage = db.Column(db.Numeric)
    buy_box_percentage_b2b = db.Column(db.Numeric)
    order_item_session_percentage = db.Column(db.Numeric)
    order_item_session_percentage_b2b = db.Column(db.Numeric)
    unit_session_percentage = db.Column(db.Numeric)
    unit_session_percentage_b2b = db.Column(db.Numeric)
    average_offer_count = db.Column(db.Integer)
    average_parent_items = db.Column(db.Integer)
    feedback_received = db.Column(db.Integer)
    negative_feedback_received = db.Column(db.Integer)
    received_negative_feedback_rate = db.Column(db.Numeric)
    date_granularity = db.Column(db.String(50))
    hourly_sales = db.Column(db.JSON, nullable=True)
    created_at = db.Column(db.BigInteger)
    updated_at = db.Column(db.BigInteger)

    @classmethod
    def add(cls, account_id: str, asp_id: str, date: str, ordered_product_sales_amount: float, ordered_product_sales_currency_code: str, units_ordered: int,
            ordered_product_sales_amount_b2b: float, ordered_product_sales_currency_code_b2b: str, units_ordered_b2b: int, total_order_items: int,
            total_order_items_b2b: int, average_sales_per_order_item_amount: float, average_sales_per_order_item_currency_code: str,
            average_sales_per_order_item_amount_b2b: float, average_sales_per_order_item_currency_code_b2b: str, average_units_per_order_item: float,
            average_units_per_order_item_b2b: float, average_selling_price_amount: float, average_selling_price_currency_code: str,
            average_selling_price_amount_b2b: float, average_selling_price_currency_code_b2b: str, units_refunded: int, refund_rate: float,
            claims_granted: int, claims_amount_amount: float, claims_amount_currency_code: str, shipped_product_sales_amount: float,
            shipped_product_sales_currency_code: str, units_shipped: int, orders_shipped: int, browser_page_views: int, browser_page_views_b2b: int,
            mobile_app_page_views: int, mobile_app_page_views_b2b: int, page_views: int, page_views_b2b: int, browser_sessions: int,
            browser_sessions_b2b: int, mobile_app_sessions: int, mobile_app_sessions_b2b: int, sessions: int, sessions_b2b: int,
            buy_box_percentage: float, buy_box_percentage_b2b: float, order_item_session_percentage: float, order_item_session_percentage_b2b: float,
            unit_session_percentage: float, unit_session_percentage_b2b: float, average_offer_count: int, average_parent_items: int,
            feedback_received: int, negative_feedback_received: int, received_negative_feedback_rate: float, date_granularity: str):
        """Create a new sales traffic summary"""
        sales_traffic_summary = cls()
        sales_traffic_summary.account_id = account_id
        sales_traffic_summary.asp_id = asp_id
        sales_traffic_summary.date = date
        sales_traffic_summary.ordered_product_sales_amount = ordered_product_sales_amount
        sales_traffic_summary.ordered_product_sales_currency_code = ordered_product_sales_currency_code
        sales_traffic_summary.ordered_product_sales_amount_b2b = ordered_product_sales_amount_b2b
        sales_traffic_summary.ordered_product_sales_currency_code_b2b = ordered_product_sales_currency_code_b2b
        sales_traffic_summary.units_ordered = units_ordered
        sales_traffic_summary.units_ordered_b2b = units_ordered_b2b
        sales_traffic_summary.total_order_items = total_order_items
        sales_traffic_summary.total_order_items_b2b = total_order_items_b2b
        sales_traffic_summary.average_sales_per_order_item_amount = average_sales_per_order_item_amount
        sales_traffic_summary.average_sales_per_order_item_currency_code = average_sales_per_order_item_currency_code
        sales_traffic_summary.average_sales_per_order_item_amount_b2b = average_sales_per_order_item_amount_b2b
        sales_traffic_summary.average_sales_per_order_item_currency_code_b2b = average_sales_per_order_item_currency_code_b2b
        sales_traffic_summary.average_units_per_order_item = average_units_per_order_item
        sales_traffic_summary.average_units_per_order_item_b2b = average_units_per_order_item_b2b
        sales_traffic_summary.average_selling_price_amount = average_selling_price_amount
        sales_traffic_summary.average_selling_price_currency_code = average_selling_price_currency_code
        sales_traffic_summary.average_selling_price_amount_b2b = average_selling_price_amount_b2b
        sales_traffic_summary.average_selling_price_currency_code_b2b = average_selling_price_currency_code_b2b
        sales_traffic_summary.units_refunded = units_refunded
        sales_traffic_summary.refund_rate = refund_rate
        sales_traffic_summary.claims_granted = claims_granted
        sales_traffic_summary.claims_amount_amount = claims_amount_amount
        sales_traffic_summary.claims_amount_currency_code = claims_amount_currency_code
        sales_traffic_summary.shipped_product_sales_amount = shipped_product_sales_amount
        sales_traffic_summary.shipped_product_sales_currency_code = shipped_product_sales_currency_code
        sales_traffic_summary.units_shipped = units_shipped
        sales_traffic_summary.orders_shipped = orders_shipped
        sales_traffic_summary.browser_page_views = browser_page_views
        sales_traffic_summary.browser_page_views_b2b = browser_page_views_b2b
        sales_traffic_summary.mobile_app_page_views = mobile_app_page_views
        sales_traffic_summary.mobile_app_page_views_b2b = mobile_app_page_views_b2b
        sales_traffic_summary.page_views = page_views
        sales_traffic_summary.page_views_b2b = page_views_b2b
        sales_traffic_summary.browser_sessions = browser_sessions
        sales_traffic_summary.browser_sessions_b2b = browser_sessions_b2b
        sales_traffic_summary.mobile_app_sessions = mobile_app_sessions
        sales_traffic_summary.mobile_app_sessions_b2b = mobile_app_sessions_b2b
        sales_traffic_summary.sessions = sessions
        sales_traffic_summary.sessions_b2b = sessions_b2b
        sales_traffic_summary.buy_box_percentage = buy_box_percentage
        sales_traffic_summary.buy_box_percentage_b2b = buy_box_percentage_b2b
        sales_traffic_summary.order_item_session_percentage = order_item_session_percentage
        sales_traffic_summary.order_item_session_percentage_b2b = order_item_session_percentage_b2b
        sales_traffic_summary.unit_session_percentage = unit_session_percentage
        sales_traffic_summary.unit_session_percentage_b2b = unit_session_percentage_b2b
        sales_traffic_summary.average_offer_count = average_offer_count
        sales_traffic_summary.average_parent_items = average_parent_items
        sales_traffic_summary.feedback_received = feedback_received
        sales_traffic_summary.negative_feedback_received = negative_feedback_received
        sales_traffic_summary.received_negative_feedback_rate = received_negative_feedback_rate
        sales_traffic_summary.date_granularity = date_granularity
        sales_traffic_summary.created_at = int(time.time())
        sales_traffic_summary.save()

        return sales_traffic_summary

    @classmethod
    def insert_or_update(cls, account_id: str, asp_id: str, date: str, ordered_product_sales_amount: float, ordered_product_sales_currency_code: str, units_ordered: int,
                         ordered_product_sales_amount_b2b: float, ordered_product_sales_currency_code_b2b: str, units_ordered_b2b: int, total_order_items: int,
                         total_order_items_b2b: int, average_sales_per_order_item_amount: float, average_sales_per_order_item_currency_code: str,
                         average_sales_per_order_item_amount_b2b: float, average_sales_per_order_item_currency_code_b2b: str, average_units_per_order_item: float,
                         average_units_per_order_item_b2b: float, average_selling_price_amount: float, average_selling_price_currency_code: str,
                         average_selling_price_amount_b2b: float, average_selling_price_currency_code_b2b: str, units_refunded: int, refund_rate: float,
                         claims_granted: int, claims_amount_amount: float, claims_amount_currency_code: str, shipped_product_sales_amount: float,
                         shipped_product_sales_currency_code: str, units_shipped: int, orders_shipped: int, browser_page_views: int, browser_page_views_b2b: int,
                         mobile_app_page_views: int, mobile_app_page_views_b2b: int, page_views: int, page_views_b2b: int, browser_sessions: int,
                         browser_sessions_b2b: int, mobile_app_sessions: int, mobile_app_sessions_b2b: int, sessions: int, sessions_b2b: int,
                         buy_box_percentage: float, buy_box_percentage_b2b: float, order_item_session_percentage: float, order_item_session_percentage_b2b: float,
                         unit_session_percentage: float, unit_session_percentage_b2b: float, average_offer_count: int, average_parent_items: int,
                         feedback_received: int, negative_feedback_received: int, received_negative_feedback_rate: float, date_granularity: str):
        """insert or update a new sales traffic summary"""

        sales_traffic_summary = db.session.query(cls).filter(cls.account_id == account_id,
                                                             cls.asp_id == asp_id, cls.date == date).first()

        current_time = int(time.time())
        record = DbAnomalies.UPDATE.value

        if sales_traffic_summary == None:
            record = DbAnomalies.INSERTION.value
            sales_traffic_summary = cls(account_id=account_id,
                                        asp_id=asp_id, date=date, created_at=current_time)

        sales_traffic_summary.ordered_product_sales_amount = ordered_product_sales_amount
        sales_traffic_summary.ordered_product_sales_currency_code = ordered_product_sales_currency_code
        sales_traffic_summary.ordered_product_sales_amount_b2b = ordered_product_sales_amount_b2b
        sales_traffic_summary.ordered_product_sales_currency_code_b2b = ordered_product_sales_currency_code_b2b
        sales_traffic_summary.units_ordered = units_ordered
        sales_traffic_summary.units_ordered_b2b = units_ordered_b2b
        sales_traffic_summary.total_order_items = total_order_items
        sales_traffic_summary.total_order_items_b2b = total_order_items_b2b
        sales_traffic_summary.average_sales_per_order_item_amount = average_sales_per_order_item_amount
        sales_traffic_summary.average_sales_per_order_item_currency_code = average_sales_per_order_item_currency_code
        sales_traffic_summary.average_sales_per_order_item_amount_b2b = average_sales_per_order_item_amount_b2b
        sales_traffic_summary.average_sales_per_order_item_currency_code_b2b = average_sales_per_order_item_currency_code_b2b
        sales_traffic_summary.average_units_per_order_item = average_units_per_order_item
        sales_traffic_summary.average_units_per_order_item_b2b = average_units_per_order_item_b2b
        sales_traffic_summary.average_selling_price_amount = average_selling_price_amount
        sales_traffic_summary.average_selling_price_currency_code = average_selling_price_currency_code
        sales_traffic_summary.average_selling_price_amount_b2b = average_selling_price_amount_b2b
        sales_traffic_summary.average_selling_price_currency_code_b2b = average_selling_price_currency_code_b2b
        sales_traffic_summary.units_refunded = units_refunded
        sales_traffic_summary.refund_rate = refund_rate
        sales_traffic_summary.claims_granted = claims_granted
        sales_traffic_summary.claims_amount_amount = claims_amount_amount
        sales_traffic_summary.claims_amount_currency_code = claims_amount_currency_code
        sales_traffic_summary.shipped_product_sales_amount = shipped_product_sales_amount
        sales_traffic_summary.shipped_product_sales_currency_code = shipped_product_sales_currency_code
        sales_traffic_summary.units_shipped = units_shipped
        sales_traffic_summary.orders_shipped = orders_shipped
        sales_traffic_summary.browser_page_views = browser_page_views
        sales_traffic_summary.browser_page_views_b2b = browser_page_views_b2b
        sales_traffic_summary.mobile_app_page_views = mobile_app_page_views
        sales_traffic_summary.mobile_app_page_views_b2b = mobile_app_page_views_b2b
        sales_traffic_summary.page_views = page_views
        sales_traffic_summary.page_views_b2b = page_views_b2b
        sales_traffic_summary.browser_sessions = browser_sessions
        sales_traffic_summary.browser_sessions_b2b = browser_sessions_b2b
        sales_traffic_summary.mobile_app_sessions = mobile_app_sessions
        sales_traffic_summary.mobile_app_sessions_b2b = mobile_app_sessions_b2b
        sales_traffic_summary.sessions = sessions
        sales_traffic_summary.sessions_b2b = sessions_b2b
        sales_traffic_summary.buy_box_percentage = buy_box_percentage
        sales_traffic_summary.buy_box_percentage_b2b = buy_box_percentage_b2b
        sales_traffic_summary.order_item_session_percentage = order_item_session_percentage
        sales_traffic_summary.order_item_session_percentage_b2b = order_item_session_percentage_b2b
        sales_traffic_summary.unit_session_percentage = unit_session_percentage
        sales_traffic_summary.unit_session_percentage_b2b = unit_session_percentage_b2b
        sales_traffic_summary.average_offer_count = average_offer_count
        sales_traffic_summary.average_parent_items = average_parent_items
        sales_traffic_summary.feedback_received = feedback_received
        sales_traffic_summary.negative_feedback_received = negative_feedback_received
        sales_traffic_summary.received_negative_feedback_rate = received_negative_feedback_rate
        sales_traffic_summary.date_granularity = date_granularity
        sales_traffic_summary.updated_at = current_time

        if record == DbAnomalies.INSERTION.value:
            sales_traffic_summary.save()
        else:
            db.session.commit()

        return sales_traffic_summary

    @classmethod
    def get_by_date(cls, account_id: str, asp_id: str, from_date: str, to_date: str):
        """get by sales traffic summary by date"""

        return db.session.query(cls).filter(cls.account_id == account_id, cls.asp_id == asp_id, cls.date.between(from_date, to_date)).all()        # noqa: FKA100

    @classmethod
    def insert_or_update_sales_data(cls, account_id: str, asp_id: str, date: str, total_sales: float, unit_count: int, hourly_sales: Optional[dict] = None):
        """insert or update sales api data"""

        sales_traffic_summary = db.session.query(cls).filter(cls.account_id == account_id,
                                                             cls.asp_id == asp_id, cls.date == date).first()

        current_time = int(time.time())
        record = DbAnomalies.UPDATE.value

        if sales_traffic_summary == None:
            record = DbAnomalies.INSERTION.value
            sales_traffic_summary = cls(account_id=account_id,
                                        asp_id=asp_id, date=date, created_at=current_time)

        # three_days_ago_date = (datetime.now() - timedelta(days=3)).date()
        # payload_date = sales_traffic_summary.date

        # if payload_date > three_days_ago_date:
        #     sales_traffic_summary.ordered_product_sales_amount = total_sales
        #     sales_traffic_summary.units_ordered = unit_count

        sales_traffic_summary.ordered_product_sales_amount = total_sales
        sales_traffic_summary.units_ordered = unit_count

        if hourly_sales:
            sales_traffic_summary.hourly_sales = hourly_sales

        sales_traffic_summary.updated_at = current_time

        if record == DbAnomalies.INSERTION.value:
            sales_traffic_summary.save()
        else:
            db.session.commit()

        return sales_traffic_summary

    @classmethod
    def get_recent_sales_data(cls, account_id: str, asp_id: str):
        """method to retrieve sales data from sales traffic summary for last 3 days"""

        to_date = datetime.now().date()
        from_date = to_date - timedelta(days=2)

        return db.session.query(cls).filter(cls.account_id == account_id, cls.asp_id == asp_id, cls.date.between(from_date, to_date)).all()        # noqa: FKA100

    @classmethod
    def get_hourly_sales_by_single_date(cls, account_id: str, asp_id: str, from_date: str, to_date: str):
        """method to retrieve hourly_sales json by date"""

        return db.session.query(cls.hourly_sales).filter(cls.account_id == account_id, cls.asp_id == asp_id, cls.date.between(from_date, to_date), cls.hourly_sales != None).first()        # noqa: FKA100
