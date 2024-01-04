"""sponosored brand model to store ads data"""

from datetime import datetime
import time
from typing import Optional

from app import db
from app.helpers.constants import DbAnomalies
from app.models.base import Base
from sqlalchemy import text


class AzSponsoredBrand(Base):
    """
        sponsored brand model to store sponsored brand report data in database
    """
    __tablename__ = 'az_sponsored_brand'

    id = db.Column(db.BigInteger, primary_key=True)
    asp_id = db.Column(db.String(255))
    az_ads_profile_id = db.Column(db.String(255))
    account_id = db.Column(db.String(36), nullable=False)
    category = db.Column(db.String(250), nullable=True)
    brand = db.Column(db.String(250), nullable=True)
    payload_date = db.Column(db.Date)
    sb_type = db.Column(db.String(20))
    # this was referred as brand - not to be used.
    ad_group_name = db.Column(db.String(255))
    attributed_conversions_14d = db.Column(db.Integer)
    attributed_conversions_14d_same_sku = db.Column(db.Integer)
    attributed_sales_14d = db.Column(db.Integer)
    attributed_sales_14d_same_sku = db.Column(db.Integer)
    campaign_budget = db.Column(db.Numeric)
    campaign_budget_type = db.Column(db.String(50))
    campaign_id = db.Column(db.BigInteger)
    # refer this as brand - newly informed
    campaign_name = db.Column(db.String(255))
    campaign_status = db.Column(db.String(50))
    clicks = db.Column(db.Integer)
    cost = db.Column(db.Numeric)
    impressions = db.Column(db.Integer)
    keyword_bid = db.Column(db.Numeric)
    keyword_id = db.Column(db.BigInteger)
    keyword_status = db.Column(db.String(50))
    keyword_text = db.Column(db.String(255))
    match_type = db.Column(db.String(50))
    sbv_vctr = db.Column(db.Numeric)
    sbv_video_5_second_view_rate = db.Column(db.Numeric)
    sbv_video_5_second_views = db.Column(db.Integer)
    sbv_video_complete_views = db.Column(db.Integer)
    sbv_video_first_quartile_views = db.Column(db.Integer)
    sbv_video_midpoint_views = db.Column(db.Integer)
    sbv_video_third_quartile_views = db.Column(db.Integer)
    sbv_video_unmutes = db.Column(db.Integer)
    sbv_viewable_impressions = db.Column(db.Integer)
    sbv_vtr = db.Column(db.Numeric)
    dpv_14d = db.Column(db.Integer)
    attributed_detail_page_views_clicks_14d = db.Column(db.Integer)
    attributed_order_rate_new_to_brand_14d = db.Column(db.Integer)
    attributed_orders_new_to_brand_14d = db.Column(db.Integer)
    attributed_orders_new_to_brand_percentage_14d = db.Column(db.Numeric)
    attributed_sales_new_to_brand_14d = db.Column(db.Integer)
    attributed_sales_new_to_brand_percentage_14d = db.Column(db.Numeric)
    attributed_units_ordered_new_to_brand_14d = db.Column(db.Integer)
    attributed_units_ordered_new_to_brand_percentage_14d = db.Column(
        db.Numeric)
    attributed_branded_searches_14d = db.Column(db.Integer)
    sbv_currency = db.Column(db.String(10))
    top_of_search_impression_share = db.Column(db.Numeric)
    sbb_applicable_budget_rule_id = db.Column(db.String(255))
    sbb_applicable_budget_rule_name = db.Column(db.String(255))
    sbb_campaign_rule_based_budget = db.Column(db.Numeric)
    sbb_search_term_impression_rank = db.Column(db.Numeric)
    sbb_search_term_impression_share = db.Column(db.Numeric)
    sbb_units_sold_14d = db.Column(db.Integer)
    created_at = db.Column(db.BigInteger)
    updated_at = db.Column(db.BigInteger)

    @classmethod
    def add(cls, asp_id: str, account_id: str, payload_date: str, sb_type: str, ad_group_name: str, attributed_conversions_14d: int, attributed_conversions_14d_same_sku: int, attributed_sales_14d: int,
            attributed_sales_14d_same_sku: int, campaign_budget: float, campaign_budget_type: str, campaign_id: int, campaign_name: str, campaign_status: str,
            clicks: int, cost: float, impressions: int, keyword_bid: float, keyword_id: int, keyword_status: str, keyword_text: str, match_type: str,
            sbv_vctr: float, sbv_video_5_second_view_rate: float, sbv_video_5_second_views: int, sbv_video_complete_views: int, sbv_video_first_quartile_views: int,
            sbv_video_midpoint_views: int, sbv_video_third_quartile_views: int, sbv_video_unmutes: int, sbv_viewable_impressions: int, sbv_vtr: float,
            dpv_14d: int, attributed_detail_page_views_clicks_14d: int, attributed_order_rate_new_to_brand_14d: int, attributed_orders_new_to_brand_14d: int,
            attributed_orders_new_to_brand_percentage_14d: float, attributed_sales_new_to_brand_14d: int, attributed_sales_new_to_brand_percentage_14d: float, attributed_units_ordered_new_to_brand_14d: int,
            attributed_units_ordered_new_to_brand_percentage_14d: float, attributed_branded_searches_14d: int, sbv_currency: str, top_of_search_impression_share: float, sbb_applicable_budget_rule_id: str,
            sbb_applicable_budget_rule_name: str, sbb_campaign_rule_based_budget: float, sbb_search_term_impression_rank: float, sbb_search_term_impression_share: float, sbb_units_sold_14d: int):
        """method to add sponsored brand data in az_sponsored_brand table"""

        az_sponsored_brand = cls()
        az_sponsored_brand.asp_id = asp_id
        az_sponsored_brand.account_id = account_id
        az_sponsored_brand.payload_date = payload_date
        az_sponsored_brand.sb_type = sb_type
        az_sponsored_brand.ad_group_name = ad_group_name
        az_sponsored_brand.attributed_conversions_14d = attributed_conversions_14d
        az_sponsored_brand.attributed_conversions_14d_same_sku = attributed_conversions_14d_same_sku
        az_sponsored_brand.attributed_sales_14d = attributed_sales_14d
        az_sponsored_brand.attributed_sales_14d_same_sku = attributed_sales_14d_same_sku
        az_sponsored_brand.campaign_budget = campaign_budget
        az_sponsored_brand.campaign_budget_type = campaign_budget_type
        az_sponsored_brand.campaign_id = campaign_id
        az_sponsored_brand.campaign_name = campaign_name
        az_sponsored_brand.campaign_status = campaign_status
        az_sponsored_brand.clicks = clicks
        az_sponsored_brand.cost = cost
        az_sponsored_brand.impressions = impressions
        az_sponsored_brand.keyword_bid = keyword_bid
        az_sponsored_brand.keyword_id = keyword_id
        az_sponsored_brand.keyword_status = keyword_status
        az_sponsored_brand.keyword_text = keyword_text
        az_sponsored_brand.match_type = match_type
        az_sponsored_brand.sbv_vctr = sbv_vctr
        az_sponsored_brand.sbv_video_5_second_view_rate = sbv_video_5_second_view_rate
        az_sponsored_brand.sbv_video_5_second_views = sbv_video_5_second_views
        az_sponsored_brand.sbv_video_complete_views = sbv_video_complete_views
        az_sponsored_brand.sbv_video_first_quartile_views = sbv_video_first_quartile_views
        az_sponsored_brand.sbv_video_midpoint_views = sbv_video_midpoint_views
        az_sponsored_brand.sbv_video_third_quartile_views = sbv_video_third_quartile_views
        az_sponsored_brand.sbv_video_unmutes = sbv_video_unmutes
        az_sponsored_brand.sbv_viewable_impressions = sbv_viewable_impressions
        az_sponsored_brand.sbv_vtr = sbv_vtr
        az_sponsored_brand.dpv_14d = dpv_14d
        az_sponsored_brand.attributed_detail_page_views_clicks_14d = attributed_detail_page_views_clicks_14d
        az_sponsored_brand.attributed_order_rate_new_to_brand_14d = attributed_order_rate_new_to_brand_14d
        az_sponsored_brand.attributed_orders_new_to_brand_14d = attributed_orders_new_to_brand_14d
        az_sponsored_brand.attributed_orders_new_to_brand_percentage_14d = attributed_orders_new_to_brand_percentage_14d
        az_sponsored_brand.attributed_sales_new_to_brand_14d = attributed_sales_new_to_brand_14d
        az_sponsored_brand.attributed_sales_new_to_brand_percentage_14d = attributed_sales_new_to_brand_percentage_14d
        az_sponsored_brand.attributed_units_ordered_new_to_brand_14d = attributed_units_ordered_new_to_brand_14d
        az_sponsored_brand.attributed_units_ordered_new_to_brand_percentage_14d = attributed_units_ordered_new_to_brand_percentage_14d
        az_sponsored_brand.attributed_branded_searches_14d = attributed_branded_searches_14d
        az_sponsored_brand.sbv_currency = sbv_currency
        az_sponsored_brand.top_of_search_impression_share = top_of_search_impression_share
        az_sponsored_brand.sbb_applicable_budget_rule_id = sbb_applicable_budget_rule_id
        az_sponsored_brand.sbb_applicable_budget_rule_name = sbb_applicable_budget_rule_name
        az_sponsored_brand.sbb_campaign_rule_based_budget = sbb_campaign_rule_based_budget
        az_sponsored_brand.sbb_search_term_impression_rank = sbb_search_term_impression_rank
        az_sponsored_brand.sbb_search_term_impression_share = sbb_search_term_impression_share
        az_sponsored_brand.sbb_units_sold_14d = sbb_units_sold_14d

        az_sponsored_brand.created_at = int(time.time())

        az_sponsored_brand.save()

        return az_sponsored_brand

    @classmethod
    def add_update(cls, asp_id: str, az_ads_profile_id: str, account_id: str, payload_date: str, sb_type: str, ad_group_name: str, attributed_conversions_14d: int, attributed_conversions_14d_same_sku: int, attributed_sales_14d: int,
                   attributed_sales_14d_same_sku: int, campaign_budget: float, campaign_budget_type: str, campaign_id: int, campaign_name: str, campaign_status: str,
                   clicks: int, cost: float, impressions: int, keyword_bid: float, keyword_id: int, keyword_status: str, keyword_text: str, match_type: str,
                   sbv_vctr: float, sbv_video_5_second_view_rate: float, sbv_video_5_second_views: int, sbv_video_complete_views: int, sbv_video_first_quartile_views: int,
                   sbv_video_midpoint_views: int, sbv_video_third_quartile_views: int, sbv_video_unmutes: int, sbv_viewable_impressions: int, sbv_vtr: float,
                   dpv_14d: int, attributed_detail_page_views_clicks_14d: int, attributed_order_rate_new_to_brand_14d: int, attributed_orders_new_to_brand_14d: int,
                   attributed_orders_new_to_brand_percentage_14d: float, attributed_sales_new_to_brand_14d: int, attributed_sales_new_to_brand_percentage_14d: float, attributed_units_ordered_new_to_brand_14d: int,
                   attributed_units_ordered_new_to_brand_percentage_14d: float, attributed_branded_searches_14d: int, sbv_currency: str, top_of_search_impression_share: float, sbb_applicable_budget_rule_id: str,
                   sbb_applicable_budget_rule_name: str, sbb_campaign_rule_based_budget: float, sbb_search_term_impression_rank: float, sbb_search_term_impression_share: float, sbb_units_sold_14d: int):
        """Insert if new sponsored brand record according to date or update it."""

        az_sponsored_brand = db.session.query(cls).filter(
            cls.account_id == account_id, cls.az_ads_profile_id == az_ads_profile_id, cls.asp_id == asp_id, cls.payload_date == payload_date, cls.sb_type == sb_type, cls.keyword_id == keyword_id,
            cls.campaign_id == campaign_id).first()

        current_time = int(time.time())
        record = DbAnomalies.UPDATE.value

        if az_sponsored_brand == None:
            record = DbAnomalies.INSERTION.value
            az_sponsored_brand = cls(account_id=account_id, az_ads_profile_id=az_ads_profile_id, asp_id=asp_id,
                                     payload_date=payload_date, sb_type=sb_type, keyword_id=keyword_id, campaign_id=campaign_id, created_at=current_time)

        az_sponsored_brand.ad_group_name = ad_group_name
        az_sponsored_brand.attributed_conversions_14d = attributed_conversions_14d
        az_sponsored_brand.attributed_conversions_14d_same_sku = attributed_conversions_14d_same_sku
        az_sponsored_brand.attributed_sales_14d = attributed_sales_14d
        az_sponsored_brand.attributed_sales_14d_same_sku = attributed_sales_14d_same_sku
        az_sponsored_brand.campaign_budget = campaign_budget
        az_sponsored_brand.campaign_budget_type = campaign_budget_type
        az_sponsored_brand.campaign_name = campaign_name
        az_sponsored_brand.campaign_status = campaign_status
        az_sponsored_brand.clicks = clicks
        az_sponsored_brand.cost = cost
        az_sponsored_brand.impressions = impressions
        az_sponsored_brand.keyword_bid = keyword_bid
        az_sponsored_brand.keyword_status = keyword_status
        az_sponsored_brand.keyword_text = keyword_text
        az_sponsored_brand.match_type = match_type
        az_sponsored_brand.sbv_vctr = sbv_vctr
        az_sponsored_brand.sbv_video_5_second_view_rate = sbv_video_5_second_view_rate
        az_sponsored_brand.sbv_video_5_second_views = sbv_video_5_second_views
        az_sponsored_brand.sbv_video_complete_views = sbv_video_complete_views
        az_sponsored_brand.sbv_video_first_quartile_views = sbv_video_first_quartile_views
        az_sponsored_brand.sbv_video_midpoint_views = sbv_video_midpoint_views
        az_sponsored_brand.sbv_video_third_quartile_views = sbv_video_third_quartile_views
        az_sponsored_brand.sbv_video_unmutes = sbv_video_unmutes
        az_sponsored_brand.sbv_viewable_impressions = sbv_viewable_impressions
        az_sponsored_brand.sbv_vtr = sbv_vtr
        az_sponsored_brand.dpv_14d = dpv_14d
        az_sponsored_brand.attributed_detail_page_views_clicks_14d = attributed_detail_page_views_clicks_14d
        az_sponsored_brand.attributed_order_rate_new_to_brand_14d = attributed_order_rate_new_to_brand_14d
        az_sponsored_brand.attributed_orders_new_to_brand_14d = attributed_orders_new_to_brand_14d
        az_sponsored_brand.attributed_orders_new_to_brand_percentage_14d = attributed_orders_new_to_brand_percentage_14d
        az_sponsored_brand.attributed_sales_new_to_brand_14d = attributed_sales_new_to_brand_14d
        az_sponsored_brand.attributed_sales_new_to_brand_percentage_14d = attributed_sales_new_to_brand_percentage_14d
        az_sponsored_brand.attributed_units_ordered_new_to_brand_14d = attributed_units_ordered_new_to_brand_14d
        az_sponsored_brand.attributed_units_ordered_new_to_brand_percentage_14d = attributed_units_ordered_new_to_brand_percentage_14d
        az_sponsored_brand.attributed_branded_searches_14d = attributed_branded_searches_14d
        az_sponsored_brand.sbv_currency = sbv_currency
        az_sponsored_brand.top_of_search_impression_share = top_of_search_impression_share
        az_sponsored_brand.sbb_applicable_budget_rule_id = sbb_applicable_budget_rule_id
        az_sponsored_brand.sbb_applicable_budget_rule_name = sbb_applicable_budget_rule_name
        az_sponsored_brand.sbb_campaign_rule_based_budget = sbb_campaign_rule_based_budget
        az_sponsored_brand.sbb_search_term_impression_rank = sbb_search_term_impression_rank
        az_sponsored_brand.sbb_search_term_impression_share = sbb_search_term_impression_share
        az_sponsored_brand.sbb_units_sold_14d = sbb_units_sold_14d
        az_sponsored_brand.updated_at = current_time

        if record == DbAnomalies.INSERTION.value:
            az_sponsored_brand.save()
        else:
            db.session.commit()

        return az_sponsored_brand

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
            'product': product
        }

        brand = [f'%{brand_data}%' for brand_data in brand]

        raw_query = '''
                        select
                        SUM(asb.attributed_sales_14d) as sales,
                        SUM(asb.impressions) as impressions,
                        SUM(asb.clicks) as clicks,
                        SUM(asb.cost) as cost,
                        SUM(asb.cost * asb.clicks) as spends
                        from az_sponsored_brand as asb
                        WHERE asb.payload_date BETWEEN :from_date AND :to_date
                        AND asb.account_id = :account_id
                        AND asb.asp_id = :asp_id
                    '''

        if brand:
            raw_query += f' AND campaign_name ILIKE any(array{brand})'

        results = db.session.execute(text(raw_query + ';'), params).fetchall()  # type: ignore  # noqa: FKA100

        return results

    @classmethod
    def get_brand_performance(cls, account_id: str, asp_id: str, from_date: str, to_date: str, brand: str):
        """Gets performance by brand."""

        from_date = datetime.strptime(from_date, '%Y-%m-%d').date()     # type: ignore  # noqa: FKA100
        to_date = datetime.strptime(to_date, '%Y-%m-%d').date()         # type: ignore  # noqa: FKA100

        params = {
            'from_date': from_date,
            'to_date': to_date,
            'account_id': account_id,
            'asp_id': asp_id,
            'brand': f'%{brand}%',
        }

        raw_query = '''
                    select
                    payload_date,
                    SUM(attributed_sales_14d) as sales,
                    SUM(clicks * cost) AS spends,
                    sum(clicks) as clicks,
                    sum(cost) as cost,
                    sum(impressions) as impressions
                    from az_sponsored_brand
                    where payload_date BETWEEN '2023-08-04' AND '2023-08-04'
                    AND account_id = :account_id
                    AND asp_id = :asp_id
                    AND campaign_name ILIKE :brand
                    group by
                    payload_date
                    '''

        results = db.session.execute(text(raw_query + ';'), params).fetchall()  # type: ignore  # noqa: FKA100

        return results

    @classmethod
    def get_brand_performance_by_zone(cls, account_id: str, asp_id: str, from_date: str, to_date: str, brand: str):
        """Gets performance by brand. for different zones like optimal,opportunity, work in progress """

        from_date = datetime.strptime(from_date, '%Y-%m-%d').date()     # type: ignore  # noqa: FKA100
        to_date = datetime.strptime(to_date, '%Y-%m-%d').date()         # type: ignore  # noqa: FKA100

        params = {
            'from_date': from_date,
            'to_date': to_date,
            'account_id': account_id,
            'asp_id': asp_id,
            'brand': f'%{brand}%',
        }

        raw_query = '''
                    select
                    coalesce(SUM(attributed_sales_14d),0) as sales,
                    coalesce(SUM(clicks * cost),0) AS spends,
                    coalesce(sum(clicks),0) as clicks,
                    coalesce(sum(cost),0) as cost,
                    coalesce(sum(impressions),0) as impressions
                    from az_sponsored_brand
                    where payload_date BETWEEN :from_date AND :to_date
                    AND account_id = :account_id
                    AND asp_id = :asp_id
                    AND campaign_name ILIKE :brand
                    '''

        results = db.session.execute(text(raw_query + ';'), params).fetchone()  # type: ignore  # noqa: FKA100

        return results
