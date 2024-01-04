"""
    Database model for storing item master data in database is written in this File along with its methods.
"""
from datetime import datetime
import time
from typing import Any
from typing import Optional

from app import db
from app import logger
from app.helpers.constants import FulfillmentChannel
from app.helpers.constants import ItemMasterStatus
from app.helpers.constants import SortingOrder
from app.helpers.utility import get_prior_to_from_date
from app.models.base import Base
from sqlalchemy import func
from sqlalchemy import Index
from sqlalchemy import text
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError


class AzItemMaster(Base):
    """
        itemMaster model to store item master data
    """
    __tablename__ = 'az_item_master'

    id = db.Column(db.BigInteger, primary_key=True)
    account_id = db.Column(db.String(36), nullable=False)
    asp_id = db.Column(db.String(255))
    selling_partner_id = db.Column(db.String(255))
    item_name = db.Column(db.String(250), nullable=True)
    item_description = db.Column(db.Text, nullable=True)
    listing_id = db.Column(db.String(100), nullable=True)
    seller_sku = db.Column(db.String(100), nullable=True)
    price = db.Column(db.Numeric, nullable=True)
    asin = db.Column(db.String(100), nullable=False)
    product_id = db.Column(db.String(100), nullable=True)
    fulfillment_channel = db.Column(db.Enum(
        FulfillmentChannel), nullable=True, default=FulfillmentChannel.DEFAULT.name)
    status = db.Column(db.Enum(ItemMasterStatus), nullable=True,
                       default=ItemMasterStatus.INACTIVE.name)
    max_retail_price = db.Column(db.Numeric, nullable=True)
    brand = db.Column(db.String(250), nullable=True)
    brand_updated_at = db.Column(db.BigInteger, nullable=True)
    cogs = db.Column(db.Numeric(10, 2), nullable=True)  # type: ignore  # noqa: FKA100
    cogs_updated_at = db.Column(db.BigInteger, nullable=True)
    category = db.Column(db.String(250), nullable=True)
    subcategory = db.Column(db.String(250), nullable=True)
    category_rank = db.Column(db.BigInteger, nullable=True)
    subcategory_rank = db.Column(db.BigInteger, nullable=True)
    face_image = db.Column(db.Text, nullable=True)
    other_images = db.Column(db.JSON, nullable=True)
    fba_inventory_json = db.Column(db.JSON, nullable=True)
    created_at = db.Column(db.BigInteger, nullable=False)
    updated_at = db.Column(db.BigInteger, nullable=True)
    deleted_at = db.Column(db.BigInteger, nullable=True)

    __table_args__ = (UniqueConstraint('account_id', 'selling_partner_id', 'seller_sku', name='uq_account_selling_partner_id_seller_sku'), Index('ix_unique_combination', 'account_id', 'selling_partner_id', 'seller_sku'))  # type: ignore  # noqa: FKA100

    @classmethod
    def get_total_records(cls, account_id: str, asp_id: str) -> int:
        """
        Get the count of records in the az_item_master table for a specific account_id and asp_id.

        Args:
            account_id (str): The account_id to filter by.
            asp_id (str): The asp_id to filter by.

        Returns:
            int: The count of records matching the given account_id and asp_id.
        """
        count = cls.query.session.query(func.count(cls.id)).filter_by(
            account_id=account_id, selling_partner_id=asp_id).scalar()
        return count

    @classmethod
    def get_all_records(cls, account_id: str, asp_id: str, status: Optional[str] = None):
        """
        Get all records in the az_item_master table that match both account_id and asp_id.

        Args:
            account_id (str): The account_id to filter by.
            asp_id (str): The asp_id to filter by.
            status (Optional[str]): The status to filter by.

        Returns:
            Tuple[List[AzItemMaster], int]: A tuple containing a list of records matching both account_id, asp_id,
                                            and the optional status, and the total count of such records.
        """
        query = cls.query.filter_by(
            account_id=account_id, selling_partner_id=asp_id)

        if status is not None:
            query = query.filter_by(status=status)

        records = query.all()
        total_count = len(records)
        return records, total_count

    @classmethod
    def get_by_seller_sku(cls, account_id: str, asp_id: str, seller_sku: str):
        """Filter record by Seller Sku"""
        return db.session.query(cls).filter(cls.account_id == account_id, cls.selling_partner_id == asp_id, cls.seller_sku == seller_sku).first()

    @classmethod
    def get_category_brand_by_sku(cls, account_id: str, asp_id: str, seller_sku: str):
        """fetch category and brand by sku"""

        brand_category = db.session.query(cls).filter(
            cls.account_id == account_id, cls.selling_partner_id == asp_id, cls.seller_sku == seller_sku).first()

        if brand_category:
            return brand_category.category, brand_category.brand
        return None, None

    @classmethod
    def get_category_brand_by_asin(cls, account_id: str, asp_id: str, asin: str):
        """fetch category and brand by asin"""

        brand_category = db.session.query(cls).filter(
            cls.account_id == account_id, cls.selling_partner_id == asp_id, cls.asin == asin).first()

        if brand_category:
            return brand_category.category, brand_category.brand
        return None, None

    @classmethod
    def update_catalog_item(cls, account_id: str, asp_id: str, asin: str, brand=None, category=None, subcategory=None,
                            category_rank=None, subcategory_rank=None, face_image=None, other_images=None):
        """Filter record by asin and update catalog"""
        item = db.session.query(cls).filter(
            cls.account_id == account_id, cls.selling_partner_id == asp_id, cls.asin == asin).first()

        current_time = int(time.time())
        if item:
            if brand:
                item.brand = brand
                item.brand_updated_at = current_time
            if category:
                item.category = category
            if subcategory:
                item.subcategory = subcategory
            if category_rank:
                item.category_rank = category_rank
            if subcategory_rank:
                item.subcategory_rank = subcategory_rank
            if face_image:
                item.face_image = face_image
            if other_images:
                item.other_images = other_images
            item.updated_at = current_time
            db.session.commit()

        return item

    @classmethod
    def add(cls, account_id: str, selling_partner_id: str, item_name: str, item_description: str, listing_id: str,
            seller_sku: str, price: float, asin: str, product_id: str,
            fulfillment_channel: str, status: str, max_retail_price: float):
        """add data to table"""

        item_master = cls()
        item_master.account_id = account_id
        item_master.selling_partner_id = selling_partner_id
        item_master.item_name = item_name
        item_master.item_description = item_description
        item_master.listing_id = listing_id
        item_master.seller_sku = seller_sku
        item_master.price = price
        item_master.asin = asin
        item_master.product_id = product_id
        item_master.fulfillment_channel = fulfillment_channel
        item_master.status = status
        item_master.max_retail_price = max_retail_price
        item_master.created_at = int(time.time())
        item_master.save()

        return item_master

    @classmethod
    def insert_or_update(cls, account_id: str, selling_partner_id: str, item_name: str, item_description: str, listing_id: str,
                         seller_sku: str, price: float, asin: str, product_id: str,
                         fulfillment_channel: str, status: str, max_retail_price: float):
        """insert or update items"""

        item = db.session.query(cls).filter(cls.account_id == account_id,
                                            cls.selling_partner_id == selling_partner_id, cls.seller_sku == seller_sku).first()

        if item:
            """update if item exists"""
            item.item_name = item_name
            item.item_description = item_description
            item.listing_id = listing_id
            item.price = price
            item.asin = asin
            item.product_id = product_id
            item.fulfillment_channel = fulfillment_channel
            item.status = status
            item.max_retail_price = max_retail_price
            item.updated_at = int(time.time())

            db.session.commit()

            return item

        else:
            """add item if item does not exist already"""
            item_master = cls()
            item_master.account_id = account_id
            item_master.selling_partner_id = selling_partner_id
            item_master.item_name = item_name
            item_master.item_description = item_description
            item_master.listing_id = listing_id
            item_master.seller_sku = seller_sku
            item_master.price = price
            item_master.asin = asin
            item_master.product_id = product_id
            item_master.fulfillment_channel = fulfillment_channel
            item_master.status = status
            item_master.max_retail_price = max_retail_price
            item_master.created_at = int(time.time())
            item_master.save()

            return item_master

    @classmethod
    def update_cogs(cls, account_id: str, sku: str, selling_partner_id: str, cogs: float):
        """Update item master cogs according to id"""

        item = db.session.query(cls).filter(account_id == account_id,
                                            cls.selling_partner_id == selling_partner_id, cls.seller_sku == sku).first()

        item.cogs = cogs

        item.cogs_updated_at = int(time.time())

        db.session.commit()

        return item

    @classmethod
    def update_brand(cls, sku: str, account_id: str, selling_partner_id: str, brand: str):
        """Update item master brand according to id"""

        item = db.session.query(cls).filter(cls.account_id == account_id,
                                            cls.selling_partner_id == selling_partner_id, cls.seller_sku == sku).first()

        item.brand = brand

        item.brand_updated_at = int(time.time())

        db.session.commit()

        return item

    @classmethod
    def get_item_by_sku(cls, sku: str, account_id: str, selling_partner_id: str):
        """get item according to sku from item master table"""

        item = db.session.query(cls).filter(cls.account_id == account_id,
                                            cls.selling_partner_id == selling_partner_id, cls.seller_sku == sku).first()

        return item

    @classmethod
    def update_items(cls, account_id: str, selling_partner_id: str, item_name: str, item_description: str, listing_id: str,
                     price: float, seller_sku: str, asin: str, product_id: str, fulfillment_channel: str, status: str, max_retail_price: float):
        """Update item master items according to sku"""

        item = db.session.query(cls).filter(cls.account_id == account_id,
                                            cls.selling_partner_id == selling_partner_id, cls.seller_sku == seller_sku).first()

        item.item_name = item_name
        item.item_description = item_description
        item.listing_id = listing_id
        item.price = price
        item.asin = asin
        item.product_id = product_id
        item.fulfillment_channel = fulfillment_channel
        item.status = status
        item.max_retail_price = max_retail_price
        item.updated_at = int(time.time())

        db.session.commit()

        return item

    @classmethod
    def get_item_level(cls, account_id: str, asp_id: str, from_date=None, to_date=None, product: Optional[tuple] = None, brand: Optional[tuple] = None, category: Optional[tuple] = None, fba_inventory: Optional[bool] = False, sort_by: Any = None, sort_order: Any = None, page: Any = None, size: Any = None, fulfillment_channel: Optional[str] = None, status: Optional[str] = None):
        """ Get Inventory level data"""

        prior_from_date, prior_to_date = get_prior_to_from_date(
            from_date=from_date, to_date=to_date)

        prior_from_date = datetime.strptime(prior_from_date, '%Y-%m-%d').date()     # type: ignore  # noqa: FKA100
        prior_to_date = datetime.strptime(prior_to_date, '%Y-%m-%d').date()     # type: ignore  # noqa: FKA100

        if from_date and to_date:
            from_date = datetime.strptime(from_date, '%Y-%m-%d').date()     # type: ignore  # noqa: FKA100
            to_date = datetime.strptime(to_date, '%Y-%m-%d').date()         # type: ignore  # noqa: FKA100

        params = {
            'account_id': account_id,
            'asp_id': asp_id,
            'from_date': from_date,
            'to_date': to_date,
            'category': category,
            'brand': brand,
            'product': product,
            'prior_from_date': prior_from_date,
            'prior_to_date': prior_to_date,
            'fulfillment_channel': fulfillment_channel,
            'status': status
        }

        raw_query = f'''
                    SELECT
                    im.brand,
                    im.category,
                    im.face_image,
                    im.seller_sku,
                    im.item_name,
                    im.asin,
                    im.fulfillment_channel,
                    im.status,
                    CAST(im.fba_inventory_json->'inventoryDetails'->>'unfulfillableQuantity' AS JSON) as fba_unfulfillable_breakup,
                    (im.fba_inventory_json->'inventoryDetails'->>'fulfillableQuantity')::int as fba_sellable_quantity,
                    (im.fba_inventory_json->'inventoryDetails'->'unfulfillableQuantity'->>'totalUnfulfillableQuantity')::int as fba_unfulfillable_quantity,
                    (im.fba_inventory_json->'inventoryDetails'->'reservedQuantity'->>'pendingTransshipmentQuantity')::int as fba_in_transit_quantity,
                    (im.fba_inventory_json->'inventoryDetails'->>'fulfillableQuantity')::int +
                    (im.fba_inventory_json->'inventoryDetails'->'reservedQuantity'->>'pendingTransshipmentQuantity')::int +
                    (im.fba_inventory_json->'inventoryDetails'->'unfulfillableQuantity'->>'totalUnfulfillableQuantity')::int
                    as total_fba_quantity,
                    im.max_retail_price::Numeric(10, 2) AS price,
                    COALESCE(im.cogs::Numeric(10, 2), 0) AS product_cost,
                    COALESCE(sq_t.sellable_quantity, 0) as sellable_quantity,
                    COALESCE(uq_t.unfulfillable_quantity, 0) as unfulfillable_quantity,
                    COALESCE(iq_t.in_transit_quantity, 0) as in_transit_quantity,
                    COALESCE(average_sales, 0) as average_sales,
                    COALESCE(avg_daily_sales_30_days, 0) as avg_daily_sales_30_days,
                    COALESCE(avg_daily_sales_30_days_prior, 0) as avg_daily_sales_30_days_prior,
                    CASE WHEN COALESCE(avg_daily_sales_30_days, 0) > 0
                        THEN
                        (COALESCE(sq_t.sellable_quantity, 0) + COALESCE(iq_t.in_transit_quantity, 0) / NULLIF(COALESCE(avg_daily_sales_30_days, 0), 0))
                        ELSE
                        0
                    END as days_of_inventory,
                    CASE WHEN COALESCE(avg_daily_sales_30_days, 0) > 0
                        THEN
                        (COALESCE(sq_t.sellable_quantity, 0) + COALESCE(iq_t.in_transit_quantity, 0) / NULLIF((COALESCE(avg_daily_sales_30_days, 0) * 30) * 100, 0))
                        ELSE
                        0
                    END as in_stock_rate,
                    COALESCE(sq_t.sellable_quantity, 0) + COALESCE(uq_t.unfulfillable_quantity, 0) + COALESCE(iq_t.in_transit_quantity, 0) as total_quantity,
                    COALESCE(sq_t.sellable_quantity * im.cogs::Numeric(10, 2), 0) + COALESCE(uq_t.unfulfillable_quantity * im.cogs::Numeric(10, 2), 0) + COALESCE(iq_t.in_transit_quantity * im.cogs::Numeric(10, 2), 0) as value_of_stock,
                    COALESCE(uq_t.customer_damaged, 0) as customer_damaged,
                    COALESCE(uq_t.warehouse_damaged, 0) as warehouse_damaged,
                    COALESCE(uq_t.expired, 0) as expired,
                    COALESCE(uq_t.distributor_damaged, 0) as distributor_damaged,
                    COALESCE(uq_t.carrier_damaged, 0) as carrier_damaged,
                    COALESCE(uq_t.defective, 0) as defective
                    FROM
                    az_item_master im
                    LEFT JOIN (
                        SELECT
                            account_id,
                            asp_id,
                            child_asin,
                            SUM(CASE WHEN payload_date > (:from_date - INTERVAL '30 days') THEN app.units_ordered ELSE 0 END)::bigint / 30 AS avg_daily_sales_30_days,
                            SUM(CASE WHEN payload_date > (:prior_from_date - INTERVAL '30 days') THEN app.units_ordered ELSE 0 END)::bigint / 30 AS avg_daily_sales_30_days_prior,
                            SUM(
                                CASE WHEN payload_date > (:prior_from_date - INTERVAL '30 days') THEN app.units_ordered / NULLIF(buy_box_percentage / 100, 0) ELSE 0.00 END
                            )::NUMERIC(10, 2) AS average_sales
                        FROM
                            az_sales_traffic_asin as app
                        WHERE
                            app.account_id = :account_id
                            AND
                            app.asp_id = :asp_id
                            AND payload_date BETWEEN (:from_date) - INTERVAL '60 days' AND :from_date
                        GROUP BY
                        app.account_id, app.asp_id, app.child_asin
                    ) pp ON im.asin = pp.child_asin
                    AND im.account_id = pp.account_id
                    AND im.selling_partner_id = pp.asp_id
                    LEFT JOIN
                    (
                        SELECT
                        account_id as sq_account_id,
                        asp_id as sq_asp_id,
                        asin as sq_asin,
                        msku as sq_sku,
                        ending_warehouse_balance as sellable_quantity
                        {',date' if from_date is None else ''}
                        FROM (
                            SELECT
                            account_id,
                            asp_id,
                            asin,
                            msku,
                            {'SUM(ending_warehouse_balance) as ending_warehouse_balance' if from_date is not None else 'ending_warehouse_balance,date,ROW_NUMBER() OVER (PARTITION BY asin ORDER BY date DESC) AS rn'}
                            FROM az_ledger_summary
                            WHERE
                            disposition = 'SELLABLE' {'AND DATE BETWEEN :from_date and :to_date' if from_date is not None else ''} AND
                            account_id = :account_id
                            AND
                            asp_id = :asp_id
                            {'GROUP BY account_id, asp_id, asin, msku' if from_date is not None else ''}
                        ) AS sq
                        {'WHERE rn = 1' if from_date is None else ''}
                    )
                    as sq_t
                    ON im.seller_sku = sq_t.sq_sku
                    AND im.account_id = sq_t.sq_account_id
                    AND im.selling_partner_id = sq_t.sq_asp_id
                    LEFT JOIN
                    (
                        SELECT
                        account_id as uq_account_id,
                        asp_id as uq_asp_id,
                        asin as uq_asin,
                        msku as uq_sku,
                        customer_damaged,
                        warehouse_damaged,
                        expired,
                        distributor_damaged,
                        carrier_damaged,
                        defective,
                        {'ending_warehouse_balance as unfulfillable_quantity' if from_date is not None else 'SUM(ending_warehouse_balance) as unfulfillable_quantity'}
                        FROM (
                            SELECT
                            account_id,
                            asp_id,
                            asin,
                            msku,
                            SUM(CASE WHEN disposition = 'CUSTOMER_DAMAGED' THEN ending_warehouse_balance ELSE 0 END) as customer_damaged,
                            SUM(CASE WHEN disposition = 'WAREHOUSE_DAMAGED' THEN ending_warehouse_balance ELSE 0 END) as warehouse_damaged,
                            SUM(CASE WHEN disposition = 'EXPIRED' THEN ending_warehouse_balance ELSE 0 END) as expired,
                            SUM(CASE WHEN disposition = 'DISTRIBUTOR_DAMAGED' THEN ending_warehouse_balance ELSE 0 END) as distributor_damaged,
                            SUM(CASE WHEN disposition = 'CARRIER_DAMAGED' THEN ending_warehouse_balance ELSE 0 END) as carrier_damaged,
                            SUM(CASE WHEN disposition = 'DEFECTIVE' THEN ending_warehouse_balance ELSE 0 END) as defective,
                            {'SUM(ending_warehouse_balance) as ending_warehouse_balance' if from_date is not None else 'ending_warehouse_balance, date, ROW_NUMBER() OVER (PARTITION BY asin ORDER BY date DESC) AS rn'}
                            FROM az_ledger_summary
                            WHERE disposition != 'SELLABLE' {' AND DATE BETWEEN :from_date and :to_date' if from_date is not None else ''} AND account_id = :account_id AND
                            asp_id = :asp_id
                            {'GROUP BY account_id, asp_id, asin, msku' if from_date is not None else ''}
                        ) AS uq
                        {'WHERE rn = 1 GROUP BY account_id, asp_id, asin' if from_date is None else ''}
                    ) as uq_t ON im.seller_sku = uq_t.uq_sku
                    AND im.account_id = uq_t.uq_account_id
                    AND im.selling_partner_id = uq_t.uq_asp_id
                    LEFT JOIN
                    (
                        SELECT
                        account_id as iq_account_id,
                        asp_id as iq_asp_id,
                        asin as iq_asin,
                        msku as iq_sku,
                        {'in_transit_btw_warehouse as in_transit_quantity' if from_date is not None else 'SUM(in_transit_btw_warehouse) as in_transit_quantity'}
                        FROM (
                            SELECT
                            account_id,
                            asp_id,
                            asin,
                            msku,
                            {'SUM(in_transit_btw_warehouse) as in_transit_btw_warehouse' if from_date is not None else 'in_transit_btw_warehouse, date, ROW_NUMBER() OVER (PARTITION BY asin ORDER BY date DESC) AS rn'}
                            FROM az_ledger_summary
                            WHERE {' DATE BETWEEN :from_date and :to_date AND' if from_date is not None else ''} account_id = :account_id AND
                            asp_id = :asp_id
                            {'GROUP BY account_id, asp_id, asin, msku' if from_date is not None else ''}
                        ) AS iq
                        {'WHERE rn = 1 GROUP BY account_id, asp_id, asin' if from_date is None else ''}
                    ) as iq_t ON im.seller_sku = iq_t.iq_sku
                    AND im.account_id = iq_t.iq_account_id
                    AND im.selling_partner_id = iq_t.iq_asp_id
                    WHERE im.account_id = :account_id and im.selling_partner_id = :asp_id
                    '''

        if category:
            raw_query += ' AND im.category IN :category'

        if brand:
            raw_query += ' AND im.brand IN :brand'

        if product:
            raw_query += ' AND im.asin IN :product'

        if fulfillment_channel:
            raw_query += ' AND im.fulfillment_channel = :fulfillment_channel'

        if status:
            raw_query += ' AND im.status = :status'

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

    @classmethod
    def get_stock_and_units_info(cls, account_id: str, asp_id: str, from_date=None, to_date=None, product: Optional[tuple] = None, brand: Optional[tuple] = None, category: Optional[tuple] = None, fulfillment_channel: Optional[str] = None, status: Optional[str] = None):
        """Get stock and unit level count and growth"""

        params = {
            'account_id': account_id,
            'asp_id': asp_id,
            'category': category,
            'brand': brand,
            'product': product,
            'from_date': from_date,
            'to_date': to_date,
            'fulfillment_channel': fulfillment_channel,
            'status': status
        }

        raw_query = '''
                    SELECT
                    SUM(
                        CASE WHEN ls.date BETWEEN :to_date AND :to_date
                        and ls.disposition = 'SELLABLE'
                        THEN ls.ending_warehouse_balance
                        ELSE 0
                        END
                    )::INT AS sellable_quantity,
                    SUM(
                        CASE WHEN ls.date BETWEEN :to_date AND :to_date
                        and disposition != 'SELLABLE'
                        THEN ls.ending_warehouse_balance
                        ELSE 0
                        END
                    )::INT AS unfulfillable_quantity,
                    SUM(
                        CASE WHEN ls.date BETWEEN :to_date AND :to_date
                        THEN ls.in_transit_btw_warehouse
                        ELSE 0
                        END
                    )::INT AS in_transit_quantity,
                    SUM(
                        CASE WHEN ls.date BETWEEN :from_date AND :from_date
                        and ls.disposition = 'SELLABLE'
                        THEN ls.ending_warehouse_balance
                        ELSE 0
                        END
                    )::INT AS since_yesterday_sellable_quantity,
                    SUM(
                        CASE WHEN ls.date BETWEEN :from_date AND :from_date
                        and ls.disposition != 'SELLABLE'
                        THEN ls.ending_warehouse_balance
                        ELSE 0
                        END
                    )::INT AS since_yesterday_unfulfillable_quantity,
                    SUM(
                        CASE WHEN ls.date BETWEEN :from_date AND :from_date
                        THEN ls.in_transit_btw_warehouse
                        ELSE 0
                        END
                    )::INT AS since_yesterday_in_transit_quantity,
                    SUM(
                        CASE WHEN ls.date BETWEEN :to_date AND :to_date
                        and ls.disposition = 'SELLABLE'
                        THEN ls.ending_warehouse_balance  * im.cogs
                        ELSE 0
                        END
                    )::INT AS sellable_stock,
                    SUM(
                        CASE WHEN ls.date BETWEEN :from_date AND :from_date
                        and ls.disposition = 'SELLABLE'
                        THEN ls.ending_warehouse_balance  * im.cogs
                        ELSE 0
                        END
                    )::INT AS sellable_stock_since_yesterday,
                    SUM(
                        CASE WHEN ls.date BETWEEN :to_date AND :to_date
                        and ls.disposition != 'SELLABLE'
                        THEN ls.ending_warehouse_balance  * im.cogs
                        ELSE 0
                        END
                    )::INT AS unsellable_stock,
                    SUM(
                        CASE WHEN ls.date BETWEEN :from_date AND :from_date
                        and ls.disposition != 'SELLABLE'
                        THEN ls.ending_warehouse_balance  * im.cogs
                        ELSE 0
                        END
                    )::INT AS unsellable_stock_since_yesterday,
                    SUM(
                        CASE WHEN ls.date BETWEEN :to_date AND :to_date
                        THEN ls.in_transit_btw_warehouse  * im.cogs
                        ELSE 0
                        END
                    )::INT AS in_transit_stock,
                    SUM(
                        CASE WHEN ls.date BETWEEN :from_date AND :from_date
                        THEN ls.in_transit_btw_warehouse  * im.cogs
                        ELSE 0
                        END
                    )::INT AS in_transit_stock_since_yesterday
                    from az_ledger_summary as ls
                    left join az_item_master as im
                    on ls.msku=im.seller_sku
                    AND ls.account_id = im.account_id
                    AND ls.asp_id = im.selling_partner_id
                    where date
                    BETWEEN :from_date AND :to_date
                    AND ls.account_id = :account_id
                    AND ls.asp_id = :asp_id
                '''

        if category:
            raw_query += ' AND im.category IN :category'

        if brand:
            raw_query += ' AND im.brand IN :brand'

        if product:
            raw_query += ' AND im.asin IN :product'

        if fulfillment_channel:
            raw_query += ' AND im.fulfillment_channel = :fulfillment_channel'

        if status:
            raw_query += ' AND im.status = :status'

        results = db.session.execute(text(raw_query + ';'), params).fetchall()   # type: ignore  # noqa: FKA100

        return results

    @classmethod
    def get_fba_stock_and_units_info(cls, account_id: str, asp_id: str, from_date=None, to_date=None, product: Optional[tuple] = None, brand: Optional[tuple] = None, category: Optional[tuple] = None, fulfillment_channel: Optional[str] = None, status: Optional[str] = None):
        """Get stock and unit level count and growth"""

        params = {
            'account_id': account_id,
            'asp_id': asp_id,
            'category': category,
            'brand': brand,
            'product': product,
            'from_date': from_date,
            'to_date': to_date,
            'fulfillment_channel': fulfillment_channel,
            'status': status
        }

        raw_query = '''
                    SELECT
                        SUM(COALESCE((im.fba_inventory_json->'inventoryDetails'->>'fulfillableQuantity')::int, 0)) as sellable_quantity,
                        SUM(COALESCE((im.fba_inventory_json->'inventoryDetails'->'unfulfillableQuantity'->>'totalUnfulfillableQuantity')::int, 0)) as unfulfillable_quantity,
                        SUM(COALESCE((im.fba_inventory_json->'inventoryDetails'->'reservedQuantity'->>'pendingTransshipmentQuantity')::int, 0)) as in_transit_quantity,
                        SUM(
                            COALESCE((im.fba_inventory_json->'inventoryDetails'->>'fulfillableQuantity')::int, 0) +
                            COALESCE((im.fba_inventory_json->'inventoryDetails'->'reservedQuantity'->>'pendingTransshipmentQuantity')::int, 0) +
                            COALESCE((im.fba_inventory_json->'inventoryDetails'->'unfulfillableQuantity'->>'totalUnfulfillableQuantity')::int, 0)
                        ) as total_quantity,
                        SUM(COALESCE((im.fba_inventory_json->'inventoryDetails'->>'fulfillableQuantity')::int, 0) * COALESCE(im.cogs::Numeric(10, 2), 0)) AS sellable_stock,
                        SUM(COALESCE((im.fba_inventory_json->'inventoryDetails'->'reservedQuantity'->>'pendingTransshipmentQuantity')::int, 0) * COALESCE(im.cogs::Numeric(10, 2), 0)) AS in_transit_stock,
                        SUM(COALESCE((im.fba_inventory_json->'inventoryDetails'->'unfulfillableQuantity'->>'totalUnfulfillableQuantity')::int, 0) * COALESCE(im.cogs::Numeric(10, 2), 0)) AS unfulfillable_stock
                    FROM az_item_master AS im
                    WHERE
                    im.account_id = :account_id
                    AND
                    im.selling_partner_id = :asp_id
                '''

        if category:
            raw_query += ' AND im.category IN :category'

        if brand:
            raw_query += ' AND im.brand IN :brand'

        if product:
            raw_query += ' AND im.asin IN :product'

        if fulfillment_channel:
            raw_query += ' AND im.fulfillment_channel = :fulfillment_channel'

        if status:
            raw_query += ' AND im.status = :status'

        results = db.session.execute(text(raw_query + ';'), params).fetchall()   # type: ignore  # noqa: FKA100

        return results

    @classmethod
    def get_inventory_summary_by_days(cls, account_id: str, asp_id: str, category: Optional[tuple] = None, brand: Optional[tuple] = None,
                                      product: Optional[tuple] = None, from_date=None, to_date=None, page=None, size=None):
        """Get stock and unit level count and growth"""

        if from_date and to_date:
            from_date = datetime.strptime(from_date, '%Y-%m-%d').date()     # type: ignore  # noqa: FKA100
            to_date = datetime.strptime(to_date, '%Y-%m-%d').date()         # type: ignore  # noqa: FKA100

        params = {
            'account_id': account_id,
            'asp_id': asp_id,
            'category': category,
            'brand': brand,
            'product': product,
            'from_date': from_date,
            'to_date': to_date
        }

        raw_query = f'''
                    SELECT
                        ls.date,
                        units_sold,
                        SUM(CASE WHEN ls.disposition = 'SELLABLE' THEN ls.ending_warehouse_balance ELSE 0 END) AS sellable_quantity,
                        SUM(CASE WHEN ls.disposition != 'SELLABLE' THEN ls.ending_warehouse_balance ELSE 0 END) AS unfulfillable_quantity,
                        SUM(ls.in_transit_btw_warehouse) AS in_transit_quantity,
                        SUM(ls.ending_warehouse_balance + ls.in_transit_btw_warehouse) AS total_quantity
                    FROM az_ledger_summary as ls
                    LEFT JOIN az_item_master as im on im.seller_sku = ls.msku
                    AND im.account_id = ls.account_id
                    AND im.selling_partner_id = ls.asp_id
                    LEFT JOIN (
                        SELECT sum(app.units_sold) as units_sold,
                        cast(app.shipment_date as DATE) as shipped_date,
                        app.seller_sku,
                        app.account_id as app_account_id,
                        app.asp_id as app_asp_id
                        from
                        az_product_performance as app
                        left join az_item_master as im on app.seller_sku=im.seller_sku
                        WHERE cast(app.shipment_date as DATE) >= ({':from_date' if from_date is not None else 'CURRENT_DATE'} - INTERVAL '30 days')
                        {f" AND cast(app.shipment_date as DATE) <= :from_date" if from_date is not None else ""}
                        {f" AND im.category IN :category" if category else ""}
                        {f" AND im.brand IN :brand" if brand else ""}
                        {f" AND im.asin IN :product" if product else ""}
                        group by app.account_id, app.asp_id, app.seller_sku, shipped_date
                    ) as app on im.seller_sku = app.seller_sku and ls.date = app.shipped_date
                    AND im.account_id = app.app_account_id
                    AND im.selling_partner_id = app.app_asp_id
                    WHERE
                    ls.account_id = :account_id
                    AND ls.asp_id = :asp_id
                    AND ls.date >= ({':from_date' if from_date is not None else 'CURRENT_DATE'} - INTERVAL '30 days')
                    {f" AND ls.date <= :from_date" if from_date is not None else ""}
                    {f" AND im.category IN :category" if category else ""}
                    {f" AND im.brand IN :brand" if brand else ""}
                    {f" AND ls.asin IN :product" if product else ""}
                    GROUP BY ls.date, units_sold
                    ORDER BY ls.date asc
                '''

        count_query = f'SELECT COUNT(*) FROM ({raw_query}) AS total_count_query'
        total_count_result = db.session.execute(text(count_query), params).scalar()  # type: ignore  # noqa: FKA100

        if page and size:
            page = int(page) - 1
            size = int(size)
            raw_query = raw_query + f' LIMIT {size} OFFSET {page * size}'

        results = db.session.execute(text(raw_query + ';'), params).fetchall()  # type: ignore  # noqa: FKA100

        total_count = len(results)

        return results, total_count, total_count_result

    @classmethod
    def get_inventory_summary_at_fc_level(cls, account_id: str, asp_id: str, from_date=None, to_date=None,
                                          category: Optional[tuple] = None, brand: Optional[tuple] = None,
                                          product: Optional[tuple] = None):
        """Get stock and unit level count and growth"""

        if from_date and to_date:
            from_date = datetime.strptime(from_date, '%Y-%m-%d').date()     # type: ignore  # noqa: FKA100
            to_date = datetime.strptime(to_date, '%Y-%m-%d').date()         # type: ignore  # noqa: FKA100

        params = {
            'account_id': account_id,
            'asp_id': asp_id,
            'category': category,
            'brand': brand,
            'product': product,
            'from_date': from_date,
            'to_date': to_date
        }

        raw_query = f'''
                    SELECT
                    ls.location as fulfillment_center,
                    SUM(CASE WHEN ls.disposition = 'SELLABLE' THEN ls.ending_warehouse_balance ELSE 0 END) AS sellable_quantity,
                    SUM(CASE WHEN ls.disposition != 'SELLABLE' THEN ls.ending_warehouse_balance ELSE 0 END) AS unfulfillable_quantity,
                    SUM(ls.in_transit_btw_warehouse) AS in_transit_quantity,
                    SUM(ls.ending_warehouse_balance + ls.in_transit_btw_warehouse) AS total_quantity
                    FROM az_ledger_summary as ls
                    LEFT JOIN az_item_master as im on ls.asin = im.asin
                    AND im.account_id=ls.account_id
                    AND im.selling_partner_id=ls.asp_id
                    WHERE
                    ls.account_id = :account_id
                    AND
                    ls.asp_id = :asp_id
                    AND
                    ls.date {'BETWEEN :from_date AND :to_date' if from_date is not None else '=CURRENT_DATE'}
                    {f" AND ls.date <= :from_date" if from_date is not None else ""}
                    {f" AND im.category IN :category" if category else ""}
                    {f" AND im.brand IN :brand" if brand else ""}
                    {f" AND ls.asin IN :product" if product else ""}
                    GROUP BY ls.location
                '''

        count_query = f'SELECT COUNT(*) FROM ({raw_query}) AS total_count_query'
        total_count_result = db.session.execute(text(count_query), params).scalar()  # type: ignore  # noqa: FKA100

        results = db.session.execute(text(raw_query + ';'), params).fetchall()  # type: ignore  # noqa: FKA100

        total_count = len(results)

        return results, total_count, total_count_result

    @classmethod
    def upsert_fba_inventory(cls, records):
        try:
            # Define the insert statement
            insert_stmt = insert(AzItemMaster).values(records)

            # Specify the conflict target and update values
            conflict_target = ['account_id',
                               'selling_partner_id', 'seller_sku']
            update_values = {
                'fba_inventory_json': insert_stmt.excluded.fba_inventory_json}

            # Create the upsert statement
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=conflict_target,
                set_=update_values
            )

            db.session.execute(upsert_stmt)

            db.session.commit()
        except IntegrityError as e:
            db.session.rollback()
            logger.info(f'IntegrityError while using upsert: {e}')
