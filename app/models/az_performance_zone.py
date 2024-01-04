"""
    Database model for storing Fba Reimbursements Report in database is written in this File along with its methods.
"""
import time
from typing import Optional

from app import db
from app.helpers.constants import CalculationLevelEnum
from app.helpers.constants import DbAnomalies
from app.helpers.constants import SortingOrder
from app.models.base import Base
from sqlalchemy import extract


class AzPerformanceZone(Base):
    """
        Performance zone table to store performance zone metrics
    """
    __tablename__ = 'az_performance_zone'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    account_id = db.Column(db.String(36), nullable=False)
    asp_id = db.Column(db.String(255), nullable=False)
    level = db.Column(db.String(30), nullable=False)
    zone = db.Column(db.String(50), nullable=False)
    metrics_date = db.Column(db.Date)
    asin = db.Column(db.String(50), nullable=True)
    sku = db.Column(db.String(50), nullable=True)
    product_name = db.Column(db.String(255))
    product_image = db.Column(db.Text)
    category = db.Column(db.String(255))
    sub_category = db.Column(db.String(255))
    category_rank = db.Column(db.BigInteger, nullable=True)
    subcategory_rank = db.Column(db.BigInteger, nullable=True)
    brand = db.Column(db.String(255))
    total_sales = db.Column(db.Numeric)
    total_units_sold = db.Column(db.Integer)
    total_ad_units_sold = db.Column(db.Integer)
    order_from_ads = db.Column(db.BigInteger)
    sales_from_ads = db.Column(db.Numeric)
    organic_sales = db.Column(db.Numeric)
    organic_units = db.Column(db.BigInteger)
    organic_sessions = db.Column(db.BigInteger)
    percentage_organic_sales = db.Column(db.Numeric)
    page_views = db.Column(db.Integer)
    sessions = db.Column(db.Integer)
    clicks_from_ads = db.Column(db.Integer)
    ctr = db.Column(db.Numeric)
    cpc = db.Column(db.Numeric)
    spend = db.Column(db.Numeric)
    roas = db.Column(db.Numeric)
    acos = db.Column(db.Numeric)
    tacos = db.Column(db.Numeric)
    conversion_rate = db.Column(db.Numeric)
    ad_conversion_rate = db.Column(db.Numeric)
    impressions = db.Column(db.BigInteger, nullable=True)
    created_at = db.Column(db.BigInteger)
    updated_at = db.Column(db.BigInteger)

    @classmethod
    def add_update(cls, account_id: str, asp_id: str, level: str, zone: str, metrics_date: str, asin: str, sku: str,
                   product_name: str, product_image: str, category: str, sub_category: str, category_rank: int, subcategory_rank: int, brand: str,
                   total_sales: float, total_units_sold: int, total_ad_units_sold: int, order_from_ads: int, sales_from_ads: float, organic_sales: float, organic_units: int,
                   organic_sessions: int, percentage_organic_sales: float, page_views: int,
                   sessions: int, clicks_from_ads: int, ctr: float, cpc: float, spend: float, roas: float, acos: float, tacos: float, conversion_rate: float, ad_conversion_rate: float, impressions: int):
        """Insert if new performance zone record according to date or update it."""

        performance_zone = db.session.query(cls).filter(
            cls.account_id == account_id, cls.asp_id == asp_id, cls.level == level, cls.metrics_date == metrics_date, cls.level == level, cls.zone == zone, cls.asin == asin).first()

        current_time = int(time.time())
        record = DbAnomalies.UPDATE.value

        if performance_zone == None:
            record = DbAnomalies.INSERTION.value
            performance_zone = cls(account_id=account_id, asp_id=asp_id,
                                   metrics_date=metrics_date, level=level, zone=zone, asin=asin, created_at=current_time)

        performance_zone.sku = sku
        performance_zone.product_name = product_name
        performance_zone.product_image = product_image
        performance_zone.category = category
        performance_zone.sub_category = sub_category
        performance_zone.category_rank = category_rank
        performance_zone.subcategory_rank = subcategory_rank
        performance_zone.brand = brand
        performance_zone.total_sales = total_sales
        performance_zone.total_units_sold = total_units_sold
        performance_zone.total_ad_units_sold = total_ad_units_sold
        performance_zone.order_from_ads = order_from_ads
        performance_zone.sales_from_ads = sales_from_ads
        performance_zone.organic_sales = organic_sales
        performance_zone.organic_units = organic_units
        performance_zone.organic_sessions = organic_sessions
        performance_zone.percentage_organic_sales = percentage_organic_sales
        performance_zone.page_views = page_views
        performance_zone.sessions = sessions
        performance_zone.clicks_from_ads = clicks_from_ads
        performance_zone.ctr = ctr
        performance_zone.cpc = cpc
        performance_zone.spend = spend
        performance_zone.roas = roas
        performance_zone.acos = acos
        performance_zone.tacos = tacos
        performance_zone.conversion_rate = conversion_rate
        performance_zone.ad_conversion_rate = ad_conversion_rate
        performance_zone.impressions = impressions

        if record == DbAnomalies.INSERTION.value:
            performance_zone.save()
        else:
            performance_zone.updated_at = current_time
            db.session.commit()

        return performance_zone

    @classmethod
    def get_performance_zone(cls, account_id: str, asp_id: str, metrics_month: int, zone: str, sort_by: str, sort_order: str, page: Optional[int] = None, size: Optional[int] = None, category: Optional[tuple] = None, brand: Optional[tuple] = None, product: Optional[tuple] = None):
        """ returns performance zone products according to the filter and pagination"""
        level = CalculationLevelEnum.ACCOUNT.value
        if brand or category:
            level = CalculationLevelEnum.PRODUCT.value

        if page and size:
            offset = (page - 1) * size

        query = (
            db.session.query(                   # type: ignore  # noqa: FKA100
                cls.asin,
                cls.brand,
                cls.category,
                cls.product_image,
                cls.product_name,
                cls.sku,
                cls.sub_category,
                cls.acos,
                cls.category_rank,
                cls.conversion_rate,
                cls.ad_conversion_rate,
                cls.cpc,
                cls.ctr,
                cls.impressions,
                cls.order_from_ads,
                cls.organic_sales,
                cls.percentage_organic_sales,
                cls.organic_sessions,
                cls.organic_units,
                cls.page_views,
                cls.roas,
                cls.sales_from_ads,
                cls.sessions,
                cls.subcategory_rank,
                cls.tacos,
                cls.clicks_from_ads,
                cls.spend,
                cls.total_sales,
                cls.total_units_sold,
                cls.total_ad_units_sold
            )
            .filter(cls.account_id == account_id, cls.asp_id == asp_id, extract('month', cls.metrics_date) == metrics_month, cls.level == level, cls.zone == zone)      # type: ignore  # noqa: FKA100
        )

        # Filter by category, brand, or product if they are not None
        if category:
            query = query.filter(cls.category.in_(category))
        if brand:
            query = query.filter(cls.brand.in_(brand))
        if product:
            query = query.filter(cls.asin.in_(product))             # type: ignore  # noqa: FKA100

        if sort_order == SortingOrder.ASC.value:
            query = query.order_by(getattr(cls, sort_by))
        else:
            query = query.order_by(getattr(cls, sort_by).desc())

        # count query
        total_count_query = query.statement.with_only_columns(
            db.func.count()).order_by(None)

        total_count = db.session.execute(total_count_query).scalar()

        # main query
        if page and size:
            query = query.offset(offset).limit(size)

        results = query.all()

        return results, total_count
