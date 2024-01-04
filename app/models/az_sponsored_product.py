"""sponosored display model to store ads data"""

from datetime import datetime
import time
from typing import Optional

from app import db
from app.helpers.constants import DbAnomalies
from app.models.az_item_master import AzItemMaster
from app.models.base import Base
from sqlalchemy import text


class AzSponsoredProduct(Base):
    """
        sponsored product model to store sponsored product report data in database
    """
    __tablename__ = 'az_sponsored_product'

    id = db.Column(db.BigInteger, primary_key=True)
    asp_id = db.Column(db.String(255))
    az_ads_profile_id = db.Column(db.String(255))
    account_id = db.Column(db.String(36), nullable=False)
    category = db.Column(db.String(250), nullable=True)
    brand = db.Column(db.String(250), nullable=True)
    payload_date = db.Column(db.Date)
    attributed_sales_same_sku_1d = db.Column(db.Numeric)
    roas_clicks_14d = db.Column(db.Numeric)
    end_date = db.Column(db.Date)
    units_sold_clicks_1d = db.Column(db.Integer)
    attributed_sales_same_sku_14d = db.Column(db.Numeric)
    sales_7d = db.Column(db.Numeric)
    attributed_sales_same_sku_30d = db.Column(db.Numeric)
    kindle_edition_normalized_pages_royalties_14d = db.Column(db.Numeric)
    units_sold_same_sku_1d = db.Column(db.Integer)
    campaign_status = db.Column(db.String(50))
    advertised_sku = db.Column(db.String(255))
    sales_other_sku_7d = db.Column(db.Numeric)
    purchases_same_sku_7d = db.Column(db.Integer)
    campaign_budget_amount = db.Column(db.Integer)
    purchases_7d = db.Column(db.Integer)
    units_sold_same_sku_30d = db.Column(db.Integer)
    cost_per_click = db.Column(db.Numeric)
    units_sold_clicks_14d = db.Column(db.Integer)
    ad_group_name = db.Column(db.String(255))
    campaign_id = db.Column(db.BigInteger)
    click_through_rate = db.Column(db.Numeric)
    kindle_edition_normalized_pages_read_14d = db.Column(db.Numeric)
    acos_clicks_14d = db.Column(db.Numeric)
    units_sold_clicks_30d = db.Column(db.Integer)
    portfolio_id = db.Column(db.BigInteger)
    ad_id = db.Column(db.BigInteger)
    campaign_budget_currency_code = db.Column(db.String(10))
    start_date = db.Column(db.Date)
    roas_clicks_7d = db.Column(db.Numeric)
    units_sold_same_sku_14d = db.Column(db.Integer)
    units_sold_clicks_7d = db.Column(db.Integer)
    attributed_sales_same_sku_7d = db.Column(db.Numeric)
    sales_1d = db.Column(db.Numeric)
    ad_group_id = db.Column(db.BigInteger)
    purchases_same_sku_14d = db.Column(db.Integer)
    units_sold_other_sku_7d = db.Column(db.Integer)
    spend = db.Column(db.Numeric)
    purchases_same_sku_1d = db.Column(db.Integer)
    campaign_budget_type = db.Column(db.String(50))
    advertised_asin = db.Column(db.String(255))
    purchases_1d = db.Column(db.Integer)
    units_sold_same_sku_7d = db.Column(db.Integer)
    cost = db.Column(db.Numeric)
    sales_14d = db.Column(db.Numeric)
    acos_clicks_7d = db.Column(db.Numeric)
    sales_30d = db.Column(db.Numeric)
    impressions = db.Column(db.Integer)
    purchases_same_sku_30d = db.Column(db.Integer)
    purchases_14d = db.Column(db.Integer)
    purchases_30d = db.Column(db.Integer)
    clicks = db.Column(db.Integer)
    campaign_name = db.Column(db.String(255))
    created_at = db.Column(db.BigInteger)
    updated_at = db.Column(db.BigInteger)

    @classmethod
    def add(cls, asp_id: str, account_id: str, payload_date: str, attributed_sales_same_sku_1d: float, roas_clicks_14d: float,
            end_date: str, units_sold_clicks_1d: int, attributed_sales_same_sku_14d: float, sales_7d: float,
            attributed_sales_same_sku_30d: float, kindle_edition_normalized_pages_royalties_14d: float,
            units_sold_same_sku_1d: int, campaign_status: str, advertised_sku: str, sales_other_sku_7d: float,
            purchases_same_sku_7d: int, campaign_budget_amount: int, purchases_7d: int, units_sold_same_sku_30d: int,
            cost_per_click: float, units_sold_clicks_14d: int, ad_group_name: str, campaign_id: int,
            click_through_rate: float, kindle_edition_normalized_pages_read_14d: float, acos_clicks_14d: float,
            units_sold_clicks_30d: int, portfolio_id: int, ad_id: int, campaign_budget_currency_code: str,
            start_date: str, roas_clicks_7d: float, units_sold_same_sku_14d: int, units_sold_clicks_7d: int,
            attributed_sales_same_sku_7d: float, sales_1d: float, ad_group_id: int, purchases_same_sku_14d: int,
            units_sold_other_sku_7d: int, spend: float, purchases_same_sku_1d: int, campaign_budget_type: str,
            advertised_asin: str, purchases_1d: int, units_sold_same_sku_7d: int, cost: float, sales_14d: float,
            acos_clicks_7d: float, sales_30d: float, impressions: int, purchases_same_sku_30d: int, purchases_14d: int,
            purchases_30d: int, clicks: int, campaign_name: str):
        """insert into az_sponsored_product table"""

        category, brand = None, None

        if advertised_sku:
            category, brand = AzItemMaster.get_category_brand_by_sku(
                account_id=account_id, asp_id=asp_id, seller_sku=advertised_sku)

        az_sponsored_product = cls()
        az_sponsored_product.asp_id = asp_id
        az_sponsored_product.account_id = account_id
        az_sponsored_product.category = category
        az_sponsored_product.brand = brand
        az_sponsored_product.payload_date = payload_date
        az_sponsored_product.attributed_sales_same_sku_1d = attributed_sales_same_sku_1d
        az_sponsored_product.roas_clicks_14d = roas_clicks_14d
        az_sponsored_product.end_date = end_date
        az_sponsored_product.units_sold_clicks_1d = units_sold_clicks_1d
        az_sponsored_product.attributed_sales_same_sku_14d = attributed_sales_same_sku_14d
        az_sponsored_product.sales_7d = sales_7d
        az_sponsored_product.attributed_sales_same_sku_30d = attributed_sales_same_sku_30d
        az_sponsored_product.kindle_edition_normalized_pages_royalties_14d = kindle_edition_normalized_pages_royalties_14d
        az_sponsored_product.units_sold_same_sku_1d = units_sold_same_sku_1d
        az_sponsored_product.campaign_status = campaign_status
        az_sponsored_product.advertised_sku = advertised_sku
        az_sponsored_product.sales_other_sku_7d = sales_other_sku_7d
        az_sponsored_product.purchases_same_sku_7d = purchases_same_sku_7d
        az_sponsored_product.campaign_budget_amount = campaign_budget_amount
        az_sponsored_product.purchases_7d = purchases_7d
        az_sponsored_product.units_sold_same_sku_30d = units_sold_same_sku_30d
        az_sponsored_product.cost_per_click = cost_per_click
        az_sponsored_product.units_sold_clicks_14d = units_sold_clicks_14d
        az_sponsored_product.ad_group_name = ad_group_name
        az_sponsored_product.campaign_id = campaign_id
        az_sponsored_product.click_through_rate = click_through_rate
        az_sponsored_product.kindle_edition_normalized_pages_read_14d = kindle_edition_normalized_pages_read_14d
        az_sponsored_product.acos_clicks_14d = acos_clicks_14d
        az_sponsored_product.units_sold_clicks_30d = units_sold_clicks_30d
        az_sponsored_product.portfolio_id = portfolio_id
        az_sponsored_product.ad_id = ad_id
        az_sponsored_product.campaign_budget_currency_code = campaign_budget_currency_code
        az_sponsored_product.start_date = start_date
        az_sponsored_product.roas_clicks_7d = roas_clicks_7d
        az_sponsored_product.units_sold_same_sku_14d = units_sold_same_sku_14d
        az_sponsored_product.units_sold_clicks_7d = units_sold_clicks_7d
        az_sponsored_product.attributed_sales_same_sku_7d = attributed_sales_same_sku_7d
        az_sponsored_product.sales_1d = sales_1d
        az_sponsored_product.ad_group_id = ad_group_id
        az_sponsored_product.purchases_same_sku_14d = purchases_same_sku_14d
        az_sponsored_product.units_sold_other_sku_7d = units_sold_other_sku_7d
        az_sponsored_product.spend = spend
        az_sponsored_product.purchases_same_sku_1d = purchases_same_sku_1d
        az_sponsored_product.campaign_budget_type = campaign_budget_type
        az_sponsored_product.advertised_asin = advertised_asin
        az_sponsored_product.purchases_1d = purchases_1d
        az_sponsored_product.units_sold_same_sku_7d = units_sold_same_sku_7d
        az_sponsored_product.cost = cost
        az_sponsored_product.sales_14d = sales_14d
        az_sponsored_product.acos_clicks_7d = acos_clicks_7d
        az_sponsored_product.sales_30d = sales_30d
        az_sponsored_product.impressions = impressions
        az_sponsored_product.purchases_same_sku_30d = purchases_same_sku_30d
        az_sponsored_product.purchases_14d = purchases_14d
        az_sponsored_product.purchases_30d = purchases_30d
        az_sponsored_product.clicks = clicks
        az_sponsored_product.campaign_name = campaign_name
        az_sponsored_product.created_at = int(time.time())

        az_sponsored_product.save()

        return az_sponsored_product

    @classmethod
    def add_update(cls, asp_id: str, az_ads_profile_id: str, account_id: str, payload_date: str, attributed_sales_same_sku_1d: float, roas_clicks_14d: float,
                   end_date: str, units_sold_clicks_1d: int, attributed_sales_same_sku_14d: float, sales_7d: float,
                   attributed_sales_same_sku_30d: float, kindle_edition_normalized_pages_royalties_14d: float,
                   units_sold_same_sku_1d: int, campaign_status: str, advertised_sku: str, sales_other_sku_7d: float,
                   purchases_same_sku_7d: int, campaign_budget_amount: int, purchases_7d: int, units_sold_same_sku_30d: int,
                   cost_per_click: float, units_sold_clicks_14d: int, ad_group_name: str, campaign_id: int,
                   click_through_rate: float, kindle_edition_normalized_pages_read_14d: float, acos_clicks_14d: float,
                   units_sold_clicks_30d: int, portfolio_id: int, ad_id: int, campaign_budget_currency_code: str,
                   start_date: str, roas_clicks_7d: float, units_sold_same_sku_14d: int, units_sold_clicks_7d: int,
                   attributed_sales_same_sku_7d: float, sales_1d: float, ad_group_id: int, purchases_same_sku_14d: int,
                   units_sold_other_sku_7d: int, spend: float, purchases_same_sku_1d: int, campaign_budget_type: str,
                   advertised_asin: str, purchases_1d: int, units_sold_same_sku_7d: int, cost: float, sales_14d: float,
                   acos_clicks_7d: float, sales_30d: float, impressions: int, purchases_same_sku_30d: int, purchases_14d: int,
                   purchases_30d: int, clicks: int, campaign_name: str):
        """Insert if new sponsored product record according to date or update it."""

        category, brand = None, None

        if advertised_sku:
            category, brand = AzItemMaster.get_category_brand_by_sku(
                account_id=account_id, asp_id=asp_id, seller_sku=advertised_sku)

        az_sponsored_product = db.session.query(cls).filter(
            cls.account_id == account_id, cls.asp_id == asp_id, cls.az_ads_profile_id == az_ads_profile_id, cls.payload_date == payload_date, cls.ad_id == ad_id, cls.advertised_asin == advertised_asin, cls.advertised_sku == advertised_sku).first()

        current_time = int(time.time())
        record = DbAnomalies.UPDATE.value

        if az_sponsored_product == None:
            record = DbAnomalies.INSERTION.value
            az_sponsored_product = cls(account_id=account_id, asp_id=asp_id, az_ads_profile_id=az_ads_profile_id,
                                       payload_date=payload_date, ad_id=ad_id, advertised_asin=advertised_asin, advertised_sku=advertised_sku, created_at=current_time)

        az_sponsored_product.category = category
        az_sponsored_product.brand = brand
        az_sponsored_product.attributed_sales_same_sku_1d = attributed_sales_same_sku_1d
        az_sponsored_product.roas_clicks_14d = roas_clicks_14d
        az_sponsored_product.end_date = end_date
        az_sponsored_product.units_sold_clicks_1d = units_sold_clicks_1d
        az_sponsored_product.attributed_sales_same_sku_14d = attributed_sales_same_sku_14d
        az_sponsored_product.sales_7d = sales_7d
        az_sponsored_product.attributed_sales_same_sku_30d = attributed_sales_same_sku_30d
        az_sponsored_product.kindle_edition_normalized_pages_royalties_14d = kindle_edition_normalized_pages_royalties_14d
        az_sponsored_product.units_sold_same_sku_1d = units_sold_same_sku_1d
        az_sponsored_product.campaign_status = campaign_status
        az_sponsored_product.sales_other_sku_7d = sales_other_sku_7d
        az_sponsored_product.purchases_same_sku_7d = purchases_same_sku_7d
        az_sponsored_product.campaign_budget_amount = campaign_budget_amount
        az_sponsored_product.purchases_7d = purchases_7d
        az_sponsored_product.units_sold_same_sku_30d = units_sold_same_sku_30d
        az_sponsored_product.cost_per_click = cost_per_click
        az_sponsored_product.units_sold_clicks_14d = units_sold_clicks_14d
        az_sponsored_product.ad_group_name = ad_group_name
        az_sponsored_product.campaign_id = campaign_id
        az_sponsored_product.click_through_rate = click_through_rate
        az_sponsored_product.kindle_edition_normalized_pages_read_14d = kindle_edition_normalized_pages_read_14d
        az_sponsored_product.acos_clicks_14d = acos_clicks_14d
        az_sponsored_product.units_sold_clicks_30d = units_sold_clicks_30d
        az_sponsored_product.portfolio_id = portfolio_id
        az_sponsored_product.campaign_budget_currency_code = campaign_budget_currency_code
        az_sponsored_product.start_date = start_date
        az_sponsored_product.roas_clicks_7d = roas_clicks_7d
        az_sponsored_product.units_sold_same_sku_14d = units_sold_same_sku_14d
        az_sponsored_product.units_sold_clicks_7d = units_sold_clicks_7d
        az_sponsored_product.attributed_sales_same_sku_7d = attributed_sales_same_sku_7d
        az_sponsored_product.sales_1d = sales_1d
        az_sponsored_product.ad_group_id = ad_group_id
        az_sponsored_product.purchases_same_sku_14d = purchases_same_sku_14d
        az_sponsored_product.units_sold_other_sku_7d = units_sold_other_sku_7d
        az_sponsored_product.spend = spend
        az_sponsored_product.purchases_same_sku_1d = purchases_same_sku_1d
        az_sponsored_product.campaign_budget_type = campaign_budget_type
        az_sponsored_product.purchases_1d = purchases_1d
        az_sponsored_product.units_sold_same_sku_7d = units_sold_same_sku_7d
        az_sponsored_product.cost = cost
        az_sponsored_product.sales_14d = sales_14d
        az_sponsored_product.acos_clicks_7d = acos_clicks_7d
        az_sponsored_product.sales_30d = sales_30d
        az_sponsored_product.impressions = impressions
        az_sponsored_product.purchases_same_sku_30d = purchases_same_sku_30d
        az_sponsored_product.purchases_14d = purchases_14d
        az_sponsored_product.purchases_30d = purchases_30d
        az_sponsored_product.clicks = clicks
        az_sponsored_product.campaign_name = campaign_name
        az_sponsored_product.updated_at = current_time

        if record == DbAnomalies.INSERTION.value:
            az_sponsored_product.save()
        else:
            db.session.commit()

        return az_sponsored_product

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
                    SUM(asp.sales_7d) as sales,
                    SUM(asp.impressions) as impressions,
                    SUM(asp.clicks) as clicks,
                    SUM(asp.spend) as spends
                    from az_sponsored_product as asp
                    LEFT JOIN
                    az_item_master as im
                    ON im.seller_sku=asp.advertised_sku
                    WHERE asp.payload_date BETWEEN :from_date AND :to_date
                    AND asp.account_id = :account_id
                    AND asp.asp_id = :asp_id
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
                    asp.payload_date,
                    SUM(asp.sales_7d) as sales,
                    SUM(asp.impressions) as impressions,
                    SUM(asp.clicks) as clicks,
                    SUM(asp.spend) as spends
                    from az_sponsored_product as asp
                    LEFT JOIN
                    az_item_master as im
                    ON im.seller_sku=asp.advertised_sku
                    WHERE asp.payload_date between :from_date and :to_date
                    AND asp.account_id = :account_id
                    AND asp.asp_id = :asp_id
                    {f" AND im.category IN :category" if category else ""}
                    {f" AND im.brand IN :brand" if brand else ""}
                    {f" AND im.asin IN :product" if product else ""}
                    GROUP BY
                    asp.payload_date
                    '''

        results = db.session.execute(text(raw_query + ';'), params).fetchall()  # type: ignore  # noqa: FKA100

        return results
