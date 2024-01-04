"""sponosored display model to store ads data"""

from datetime import datetime
import time
from typing import Optional

from app import db
from app.helpers.constants import DbAnomalies
from app.models.az_item_master import AzItemMaster
from app.models.base import Base
from sqlalchemy import text


class AzSponsoredDisplay(Base):
    """
        sponsored display model to store sponsored display report data in database
    """
    __tablename__ = 'az_sponsored_display'

    id = db.Column(db.BigInteger, primary_key=True)
    asp_id = db.Column(db.String(255))
    az_ads_profile_id = db.Column(db.String(255))
    account_id = db.Column(db.String(36), nullable=False)
    category = db.Column(db.String(250), nullable=True)
    brand = db.Column(db.String(250), nullable=True)
    payload_date = db.Column(db.Date)
    ad_group_id = db.Column(db.BigInteger)
    ad_group_name = db.Column(db.String(255))
    ad_id = db.Column(db.BigInteger)
    asin = db.Column(db.String(255))
    attributed_conversions_14d = db.Column(db.Integer)
    attributed_conversions_14d_same_sku = db.Column(db.Integer)
    attributed_conversions_1d = db.Column(db.Integer)
    attributed_conversions_1d_same_sku = db.Column(db.Integer)
    attributed_conversions_30d = db.Column(db.Integer)
    attributed_conversions_30d_same_sku = db.Column(db.Integer)
    attributed_conversions_7d = db.Column(db.Integer)
    attributed_conversions_7d_same_sku = db.Column(db.Integer)
    attributed_detail_page_view_14d = db.Column(db.Integer)
    attributed_orders_new_to_brand_14d = db.Column(db.Integer)
    attributed_sales_14d = db.Column(db.Integer)
    attributed_sales_14d_same_sku = db.Column(db.Integer)
    attributed_sales_1d = db.Column(db.Integer)
    attributed_sales_1d_same_sku = db.Column(db.Integer)
    attributed_sales_30d = db.Column(db.Integer)
    attributed_sales_30d_same_sku = db.Column(db.Integer)
    attributed_sales_7d = db.Column(db.Integer)
    attributed_sales_7d_same_sku = db.Column(db.Integer)
    attributed_sales_new_to_brand_14d = db.Column(db.Integer)
    attributed_units_ordered_14d = db.Column(db.Integer)
    attributed_units_ordered_1d = db.Column(db.Integer)
    attributed_units_ordered_30d = db.Column(db.Integer)
    attributed_units_ordered_7d = db.Column(db.Integer)
    attributed_units_ordered_new_to_brand_14d = db.Column(db.Integer)
    campaign_id = db.Column(db.BigInteger)
    campaign_name = db.Column(db.String(255))
    clicks = db.Column(db.Integer)
    cost = db.Column(db.Numeric)
    currency = db.Column(db.String(10))
    impressions = db.Column(db.Integer)
    sku = db.Column(db.String(255))
    view_attributed_conversions_14d = db.Column(db.Integer)
    view_impressions = db.Column(db.Integer)
    view_attributed_detail_page_view_14d = db.Column(db.Integer)
    view_attributed_sales_14d = db.Column(db.Integer)
    view_attributed_units_ordered_14d = db.Column(db.Integer)
    view_attributed_orders_new_to_brand_14d = db.Column(db.Integer)
    view_attributed_sales_new_to_brand_14d = db.Column(db.Integer)
    view_attributed_units_ordered_new_to_brand_14d = db.Column(db.Integer)
    attributed_branded_searches_14d = db.Column(db.Integer)
    view_attributed_branded_searches_14d = db.Column(db.Integer)
    video_complete_views = db.Column(db.Integer)
    video_first_quartile_views = db.Column(db.Integer)
    video_midpoint_views = db.Column(db.Integer)
    video_third_quartile_views = db.Column(db.Integer)
    video_unmutes = db.Column(db.Integer)
    vtr = db.Column(db.Numeric)
    vctr = db.Column(db.Numeric)
    avg_impressions_frequency = db.Column(db.Numeric)
    cumulative_reach = db.Column(db.Integer)
    created_at = db.Column(db.BigInteger)
    updated_at = db.Column(db.BigInteger)

    @classmethod
    def add(cls, asp_id: str, account_id: str, payload_date: str, ad_group_id: int, ad_group_name: str, ad_id: int, asin: str, attributed_conversions_14d: int,
            attributed_conversions_14d_same_sku: int, attributed_conversions_1d: int, attributed_conversions_1d_same_sku: int, attributed_conversions_30d: int,
            attributed_conversions_30d_same_sku: int, attributed_conversions_7d: int, attributed_conversions_7d_same_sku: int, attributed_detail_page_view_14d: int,
            attributed_orders_new_to_brand_14d: int, attributed_sales_14d: int, attributed_sales_14d_same_sku: int, attributed_sales_1d: int,
            attributed_sales_1d_same_sku: int, attributed_sales_30d: int, attributed_sales_30d_same_sku: int, attributed_sales_7d: int, attributed_sales_7d_same_sku: int,
            attributed_sales_new_to_brand_14d: int, attributed_units_ordered_14d: int, attributed_units_ordered_1d: int, attributed_units_ordered_30d: int,
            attributed_units_ordered_7d: int, attributed_units_ordered_new_to_brand_14d: int, campaign_id: int, campaign_name: str, clicks: int,
            cost: float, currency: str, impressions: int, sku: str, view_attributed_conversions_14d: int, view_impressions: int, view_attributed_detail_page_view_14d: int,
            view_attributed_sales_14d: int, view_attributed_units_ordered_14d: int, view_attributed_orders_new_to_brand_14d: int, view_attributed_sales_new_to_brand_14d: int,
            view_attributed_units_ordered_new_to_brand_14d: int, attributed_branded_searches_14d: int, view_attributed_branded_searches_14d: int, video_complete_views: int,
            video_first_quartile_views: int, video_midpoint_views: int, video_third_quartile_views: int, video_unmutes: int, vtr: float,
            vctr: float, avg_impressions_frequency: float, cumulative_reach: int):
        """method to add sponsored dispay data in az_sponsored_display table"""

        category, brand = None, None

        if sku:
            category, brand = AzItemMaster.get_category_brand_by_sku(
                account_id=account_id, asp_id=asp_id, seller_sku=sku)

        az_sponsored_display = cls()
        az_sponsored_display.asp_id = asp_id
        az_sponsored_display.account_id = account_id
        az_sponsored_display.category = category
        az_sponsored_display.brand = brand
        az_sponsored_display.payload_date = payload_date
        az_sponsored_display.ad_group_id = ad_group_id
        az_sponsored_display.ad_group_name = ad_group_name
        az_sponsored_display.ad_id = ad_id
        az_sponsored_display.asin = asin
        az_sponsored_display.attributed_conversions_14d = attributed_conversions_14d
        az_sponsored_display.attributed_conversions_14d_same_sku = attributed_conversions_14d_same_sku
        az_sponsored_display.attributed_conversions_1d = attributed_conversions_1d
        az_sponsored_display.attributed_conversions_1d_same_sku = attributed_conversions_1d_same_sku
        az_sponsored_display.attributed_conversions_30d = attributed_conversions_30d
        az_sponsored_display.attributed_conversions_30d_same_sku = attributed_conversions_30d_same_sku
        az_sponsored_display.attributed_conversions_7d = attributed_conversions_7d
        az_sponsored_display.attributed_conversions_7d_same_sku = attributed_conversions_7d_same_sku
        az_sponsored_display.attributed_detail_page_view_14d = attributed_detail_page_view_14d
        az_sponsored_display.attributed_orders_new_to_brand_14d = attributed_orders_new_to_brand_14d
        az_sponsored_display.attributed_sales_14d = attributed_sales_14d
        az_sponsored_display.attributed_sales_14d_same_sku = attributed_sales_14d_same_sku
        az_sponsored_display.attributed_sales_1d = attributed_sales_1d
        az_sponsored_display.attributed_sales_1d_same_sku = attributed_sales_1d_same_sku
        az_sponsored_display.attributed_sales_30d = attributed_sales_30d
        az_sponsored_display.attributed_sales_30d_same_sku = attributed_sales_30d_same_sku
        az_sponsored_display.attributed_sales_7d = attributed_sales_7d
        az_sponsored_display.attributed_sales_7d_same_sku = attributed_sales_7d_same_sku
        az_sponsored_display.attributed_sales_new_to_brand_14d = attributed_sales_new_to_brand_14d
        az_sponsored_display.attributed_units_ordered_14d = attributed_units_ordered_14d
        az_sponsored_display.attributed_units_ordered_1d = attributed_units_ordered_1d
        az_sponsored_display.attributed_units_ordered_30d = attributed_units_ordered_30d
        az_sponsored_display.attributed_units_ordered_7d = attributed_units_ordered_7d
        az_sponsored_display.attributed_units_ordered_new_to_brand_14d = attributed_units_ordered_new_to_brand_14d
        az_sponsored_display.campaign_id = campaign_id
        az_sponsored_display.campaign_name = campaign_name
        az_sponsored_display.clicks = clicks
        az_sponsored_display.cost = cost
        az_sponsored_display.currency = currency
        az_sponsored_display.impressions = impressions
        az_sponsored_display.sku = sku
        az_sponsored_display.view_attributed_conversions_14d = view_attributed_conversions_14d
        az_sponsored_display.view_impressions = view_impressions
        az_sponsored_display.view_attributed_detail_page_view_14d = view_attributed_detail_page_view_14d
        az_sponsored_display.view_attributed_sales_14d = view_attributed_sales_14d
        az_sponsored_display.view_attributed_units_ordered_14d = view_attributed_units_ordered_14d
        az_sponsored_display.view_attributed_orders_new_to_brand_14d = view_attributed_orders_new_to_brand_14d
        az_sponsored_display.view_attributed_sales_new_to_brand_14d = view_attributed_sales_new_to_brand_14d
        az_sponsored_display.view_attributed_units_ordered_new_to_brand_14d = view_attributed_units_ordered_new_to_brand_14d
        az_sponsored_display.attributed_branded_searches_14d = attributed_branded_searches_14d
        az_sponsored_display.view_attributed_branded_searches_14d = view_attributed_branded_searches_14d
        az_sponsored_display.video_complete_views = video_complete_views
        az_sponsored_display.video_first_quartile_views = video_first_quartile_views
        az_sponsored_display.video_midpoint_views = video_midpoint_views
        az_sponsored_display.video_third_quartile_views = video_third_quartile_views
        az_sponsored_display.video_unmutes = video_unmutes
        az_sponsored_display.vtr = vtr
        az_sponsored_display.vctr = vctr
        az_sponsored_display.avg_impressions_frequency = avg_impressions_frequency
        az_sponsored_display.cumulative_reach = cumulative_reach
        az_sponsored_display.created_at = int(time.time())

        az_sponsored_display.save()

        return az_sponsored_display

    @classmethod
    def add_update(cls, asp_id: str, az_ads_profile_id: str, account_id: str, payload_date: str, ad_group_id: int, ad_group_name: str, ad_id: int, asin: str, attributed_conversions_14d: int,
                   attributed_conversions_14d_same_sku: int, attributed_conversions_1d: int, attributed_conversions_1d_same_sku: int, attributed_conversions_30d: int,
                   attributed_conversions_30d_same_sku: int, attributed_conversions_7d: int, attributed_conversions_7d_same_sku: int, attributed_detail_page_view_14d: int,
                   attributed_orders_new_to_brand_14d: int, attributed_sales_14d: int, attributed_sales_14d_same_sku: int, attributed_sales_1d: int,
                   attributed_sales_1d_same_sku: int, attributed_sales_30d: int, attributed_sales_30d_same_sku: int, attributed_sales_7d: int, attributed_sales_7d_same_sku: int,
                   attributed_sales_new_to_brand_14d: int, attributed_units_ordered_14d: int, attributed_units_ordered_1d: int, attributed_units_ordered_30d: int,
                   attributed_units_ordered_7d: int, attributed_units_ordered_new_to_brand_14d: int, campaign_id: int, campaign_name: str, clicks: int,
                   cost: float, currency: str, impressions: int, sku: str, view_attributed_conversions_14d: int, view_impressions: int, view_attributed_detail_page_view_14d: int,
                   view_attributed_sales_14d: int, view_attributed_units_ordered_14d: int, view_attributed_orders_new_to_brand_14d: int, view_attributed_sales_new_to_brand_14d: int,
                   view_attributed_units_ordered_new_to_brand_14d: int, attributed_branded_searches_14d: int, view_attributed_branded_searches_14d: int, video_complete_views: int,
                   video_first_quartile_views: int, video_midpoint_views: int, video_third_quartile_views: int, video_unmutes: int, vtr: float,
                   vctr: float, avg_impressions_frequency: float, cumulative_reach: int):
        """Insert if new sponsored display record according to date or update it."""

        category, brand = None, None

        if sku:
            category, brand = AzItemMaster.get_category_brand_by_sku(
                account_id=account_id, asp_id=asp_id, seller_sku=sku)

        az_sponsored_display = db.session.query(cls).filter(
            cls.account_id == account_id, cls.az_ads_profile_id == az_ads_profile_id, cls.asp_id == asp_id, cls.payload_date == payload_date, cls.ad_id == ad_id, cls.asin == asin, cls.campaign_id == campaign_id).first()

        current_time = int(time.time())
        record = DbAnomalies.UPDATE.value

        if az_sponsored_display == None:
            record = DbAnomalies.INSERTION.value
            az_sponsored_display = cls(account_id=account_id, az_ads_profile_id=az_ads_profile_id, asp_id=asp_id,
                                       payload_date=payload_date, ad_id=ad_id, asin=asin, campaign_id=campaign_id, created_at=current_time)

        az_sponsored_display.category = category
        az_sponsored_display.brand = brand
        az_sponsored_display.ad_group_id = ad_group_id
        az_sponsored_display.ad_group_name = ad_group_name
        az_sponsored_display.attributed_conversions_14d = attributed_conversions_14d
        az_sponsored_display.attributed_conversions_14d_same_sku = attributed_conversions_14d_same_sku
        az_sponsored_display.attributed_conversions_1d = attributed_conversions_1d
        az_sponsored_display.attributed_conversions_1d_same_sku = attributed_conversions_1d_same_sku
        az_sponsored_display.attributed_conversions_30d = attributed_conversions_30d
        az_sponsored_display.attributed_conversions_30d_same_sku = attributed_conversions_30d_same_sku
        az_sponsored_display.attributed_conversions_7d = attributed_conversions_7d
        az_sponsored_display.attributed_conversions_7d_same_sku = attributed_conversions_7d_same_sku
        az_sponsored_display.attributed_detail_page_view_14d = attributed_detail_page_view_14d
        az_sponsored_display.attributed_orders_new_to_brand_14d = attributed_orders_new_to_brand_14d
        az_sponsored_display.attributed_sales_14d = attributed_sales_14d
        az_sponsored_display.attributed_sales_14d_same_sku = attributed_sales_14d_same_sku
        az_sponsored_display.attributed_sales_1d = attributed_sales_1d
        az_sponsored_display.attributed_sales_1d_same_sku = attributed_sales_1d_same_sku
        az_sponsored_display.attributed_sales_30d = attributed_sales_30d
        az_sponsored_display.attributed_sales_30d_same_sku = attributed_sales_30d_same_sku
        az_sponsored_display.attributed_sales_7d = attributed_sales_7d
        az_sponsored_display.attributed_sales_7d_same_sku = attributed_sales_7d_same_sku
        az_sponsored_display.attributed_sales_new_to_brand_14d = attributed_sales_new_to_brand_14d
        az_sponsored_display.attributed_units_ordered_14d = attributed_units_ordered_14d
        az_sponsored_display.attributed_units_ordered_1d = attributed_units_ordered_1d
        az_sponsored_display.attributed_units_ordered_30d = attributed_units_ordered_30d
        az_sponsored_display.attributed_units_ordered_7d = attributed_units_ordered_7d
        az_sponsored_display.attributed_units_ordered_new_to_brand_14d = attributed_units_ordered_new_to_brand_14d
        az_sponsored_display.campaign_name = campaign_name
        az_sponsored_display.clicks = clicks
        az_sponsored_display.cost = cost
        az_sponsored_display.currency = currency
        az_sponsored_display.impressions = impressions
        az_sponsored_display.sku = sku
        az_sponsored_display.view_attributed_conversions_14d = view_attributed_conversions_14d
        az_sponsored_display.view_impressions = view_impressions
        az_sponsored_display.view_attributed_detail_page_view_14d = view_attributed_detail_page_view_14d
        az_sponsored_display.view_attributed_sales_14d = view_attributed_sales_14d
        az_sponsored_display.view_attributed_units_ordered_14d = view_attributed_units_ordered_14d
        az_sponsored_display.view_attributed_orders_new_to_brand_14d = view_attributed_orders_new_to_brand_14d
        az_sponsored_display.view_attributed_sales_new_to_brand_14d = view_attributed_sales_new_to_brand_14d
        az_sponsored_display.view_attributed_units_ordered_new_to_brand_14d = view_attributed_units_ordered_new_to_brand_14d
        az_sponsored_display.attributed_branded_searches_14d = attributed_branded_searches_14d
        az_sponsored_display.view_attributed_branded_searches_14d = view_attributed_branded_searches_14d
        az_sponsored_display.video_complete_views = video_complete_views
        az_sponsored_display.video_first_quartile_views = video_first_quartile_views
        az_sponsored_display.video_midpoint_views = video_midpoint_views
        az_sponsored_display.video_third_quartile_views = video_third_quartile_views
        az_sponsored_display.video_unmutes = video_unmutes
        az_sponsored_display.vtr = vtr
        az_sponsored_display.vctr = vctr
        az_sponsored_display.avg_impressions_frequency = avg_impressions_frequency
        az_sponsored_display.cumulative_reach = cumulative_reach
        az_sponsored_display.updated_at = current_time

        if record == DbAnomalies.INSERTION.value:
            az_sponsored_display.save()
        else:
            db.session.commit()

        return az_sponsored_display

    @classmethod
    def get_ad_stats(cls, account_id: str, asp_id: str, from_date: str, to_date: str, category: Optional[tuple] = None, brand: Optional[tuple] = None,
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

        raw_query = '''
                    SELECT
                    SUM(asd.attributed_sales_14d) as sales,
                    SUM(asd.impressions) as impressions,
                    SUM(asd.clicks) as clicks,
                    SUM(asd.cost) as cost,
                    SUM(asd.cost * asd.clicks) as spends
                    from az_sponsored_display as asd
                    LEFT JOIN
                    az_item_master as im
                    ON im.seller_sku=asd.sku
                    WHERE
                    asd.payload_date BETWEEN :from_date AND :to_date
                    AND asd.account_id = :account_id
                    AND asd.asp_id = :asp_id
                    '''

        if category:
            raw_query += ' AND im.category IN :category'

        if brand:
            raw_query += ' AND im.brand IN :brand'

        if product:
            raw_query += ' AND im.asin IN :product'

        results = db.session.execute(text(raw_query + ';'), params).fetchall()  # type: ignore  # noqa: FKA100

        return results

    @classmethod
    def get_ad_summary_by_day(cls, account_id: str, asp_id: str, from_date: str, to_date: str, category: Optional[tuple] = None, brand: Optional[tuple] = None,
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
                        SELECT
                        asd.payload_date,
                        SUM(asd.attributed_sales_14d) as sales,
                        SUM(asd.impressions) as impressions,
                        SUM(asd.clicks) as clicks,
                        SUM(asd.cost * asd.clicks) as spends
                        from az_sponsored_display as asd
                        LEFT JOIN
                        az_item_master as im
                        ON im.seller_sku=asd.sku
                        WHERE asd.payload_date between :from_date and :to_date
                        AND asd.account_id = :account_id
                        AND asd.asp_id = :asp_id
                        {f" AND im.category IN :category" if category else ""}
                        {f" AND im.brand IN :brand" if brand else ""}
                        {f" AND im.asin IN :product" if product else ""}
                        GROUP BY
                        asd.payload_date
                    '''

        results = db.session.execute(text(raw_query + ';'), params).fetchall()  # type: ignore  # noqa: FKA100

        return results
