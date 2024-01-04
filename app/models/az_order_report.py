"""
    Database model for storing Orders in database is written in this File along with its methods.
"""
import time
from typing import Any
from typing import Optional

from app import db
from app.helpers.constants import SortingOrder
from app.models.az_item_master import AzItemMaster
from app.models.base import Base
from sqlalchemy import cast
from sqlalchemy import Date
from sqlalchemy import desc
from sqlalchemy import func
from sqlalchemy.orm import aliased


class AzOrderReport(Base):
    """
        Order model to store order report data in database
    """
    __tablename__ = 'az_order_report'

    id = db.Column(db.BigInteger, primary_key=True)
    account_id = db.Column(db.String(36), nullable=False)
    asp_id = db.Column(db.String(255))
    selling_partner_id = db.Column(db.String(255))
    category = db.Column(db.String(250), nullable=True)
    brand = db.Column(db.String(250), nullable=True)
    amazon_order_id = db.Column(db.String(255))
    merchant_order_id = db.Column(db.String(255))
    purchase_date = db.Column(db.String(255))
    last_updated_date = db.Column(db.String(255))
    order_status = db.Column(db.String(255))
    fulfillment_channel = db.Column(db.String(255))
    sales_channel = db.Column(db.String(255))
    ship_service_level = db.Column(db.String(255))
    product_name = db.Column(db.String(255))
    sku = db.Column(db.String(255))
    asin = db.Column(db.String(255))
    item_status = db.Column(db.String(255))
    quantity = db.Column(db.Integer)
    currency = db.Column(db.String(255))
    item_price = db.Column(db.Numeric(10, 2))  # type: ignore  # noqa: FKA100
    item_tax = db.Column(db.Numeric(10, 2))  # type: ignore  # noqa: FKA100
    shipping_price = db.Column(db.Numeric(10, 2))  # type: ignore  # noqa: FKA100
    shipping_tax = db.Column(db.Numeric(10, 2))  # type: ignore  # noqa: FKA100
    gift_wrap_price = db.Column(db.Numeric(10, 2))  # type: ignore  # noqa: FKA100
    gift_wrap_tax = db.Column(db.Numeric(10, 2))  # type: ignore  # noqa: FKA100
    item_promotion_discount = db.Column(db.Numeric(10, 2))  # type: ignore  # noqa: FKA100
    ship_promotion_discount = db.Column(db.Numeric(10, 2))  # type: ignore  # noqa: FKA100
    ship_city = db.Column(db.String(255))
    ship_state = db.Column(db.String(255))
    ship_postal_code = db.Column(db.String(255))
    ship_country = db.Column(db.String(255))
    created_at = db.Column(db.BigInteger)
    updated_at = db.Column(db.BigInteger)

    @classmethod
    def add(cls, account_id: str, selling_partner_id: str, amazon_order_id: str, merchant_order_id: str, purchase_date: str, last_updated_date: str,
            order_status: str, fulfillment_channel: str, sales_channel: str,
            ship_service_level: str, product_name: str, sku: str, asin: str, item_status: str,
            quantity: int, currency: str, item_price: float, item_tax: float, shipping_price: float,
            shipping_tax: float, gift_wrap_price: float, gift_wrap_tax: float, item_promotion_discount: float,
            ship_promotion_discount: float, ship_city: str, ship_state: str, ship_postal_code: str,
            ship_country: str):
        """Create a new Order"""

        category, brand = None, None
        if sku:
            category, brand = AzItemMaster.get_category_brand_by_sku(
                account_id=account_id, asp_id=selling_partner_id, seller_sku=sku)

        order = cls()
        order.account_id = account_id
        order.selling_partner_id = selling_partner_id
        order.category = category
        order.brand = brand
        order.amazon_order_id = amazon_order_id
        order.merchant_order_id = merchant_order_id
        order.purchase_date = purchase_date
        order.last_updated_date = last_updated_date
        order.order_status = order_status
        order.fulfillment_channel = fulfillment_channel
        order.sales_channel = sales_channel
        order.ship_service_level = ship_service_level
        order.product_name = product_name
        order.sku = sku
        order.asin = asin
        order.item_status = item_status
        order.quantity = quantity
        order.currency = currency
        order.item_price = item_price
        order.item_tax = item_tax
        order.shipping_price = shipping_price
        order.shipping_tax = shipping_tax
        order.gift_wrap_price = gift_wrap_price
        order.gift_wrap_tax = gift_wrap_tax
        order.item_promotion_discount = item_promotion_discount
        order.ship_promotion_discount = ship_promotion_discount
        order.ship_city = ship_city
        order.ship_state = ship_state
        order.ship_postal_code = ship_postal_code
        order.ship_country = ship_country
        # order.gross_sales = sum(i for i in [item_price, item_tax, shipping_price, shipping_tax, gift_wrap_price,
        #                         gift_wrap_tax, item_promotion_discount, ship_promotion_discount] if i is not None)

        order.created_at = int(time.time())
        order.save()

        return order

    @classmethod
    def get_by_amazon_order_id(cls, amazon_order_id: str, selling_partner_id: str):
        """get order by amazon order id"""

        item = db.session.query(cls).filter(
            cls.selling_partner_id == selling_partner_id, cls.amazon_order_id == amazon_order_id).first()

        return item

    @classmethod
    def get_purchased_date_orders(cls, account_id: str, asp_id: str, from_date: str, to_date: str, page: Any = None, size: Any = None, category: Optional[tuple] = None, brand: Optional[tuple] = None, product: Optional[tuple] = None):
        """Get all orders by date"""

        total_count = None

        im_alias = aliased(AzItemMaster)

        purchase_date_cast = cast(cls.purchase_date, Date)

        result_query = db.session.query(cls, cls.item_price, cls.sku, cls.quantity, cls.purchase_date, cls.product_name, cls.asin, im_alias.face_image)  # type: ignore  # noqa: FKA100
        result_query = result_query.join(im_alias, cls.sku == im_alias.seller_sku, isouter=True)  # type: ignore  # noqa: FKA100
        result_query = result_query.filter(cls.account_id == account_id, cls.selling_partner_id == asp_id, purchase_date_cast.between(from_date, to_date), cls.item_price != None, cls.category.in_(category) if category else True, cls.brand.in_(brand) if brand else True, cls.asin.in_(product) if product else True).order_by(cls.purchase_date.desc())  # type: ignore  # noqa: FKA100

        if total_count is None:
            total_count = result_query.count()

        if page and size:
            page = int(page) - 1
            size = int(size)
            result_query = result_query.limit(
                size).offset(page * size)

        result = result_query.all()

        return result, total_count

    @classmethod
    def get_top_least_selling_orders(cls, account_id: str, asp_id: str, from_date: str, to_date: str, sort_order: str, size: Any = None, category: Optional[tuple] = None, brand: Optional[tuple] = None, product: Optional[tuple] = None):
        """Get top/least selling orders by date"""

        im_alias = aliased(AzItemMaster)

        purchase_date_cast = cast(cls.purchase_date, Date)

        result_query = db.session.query(cls.asin, func.sum(cls.item_price).label('gross_sales'),        # type: ignore  # noqa: FKA100
                                        func.max(cls.sku).label('sku'),
                                        func.sum(cls.quantity).label(
                                            'units_sold'),
                                        func.max(cls.product_name).label(
                                            'product_name'),
                                        func.max(im_alias.face_image).label('product_image'))  # type: ignore  # noqa: FKA100
        result_query = result_query.join(im_alias, cls.sku == im_alias.seller_sku, isouter=True)  # type: ignore  # noqa: FKA100
        result_query = result_query.filter(cls.account_id == account_id, cls.selling_partner_id == asp_id, purchase_date_cast.between(from_date, to_date), cls.item_price != None, cls.category.in_(category) if category else True, cls.brand.in_(brand) if brand else True, cls.asin.in_(product) if product else True).group_by(cls.asin)  # type: ignore  # noqa: FKA100

        if sort_order == SortingOrder.ASC.value:
            result_query = result_query.group_by(
                cls.asin).order_by('gross_sales').limit(size)
        elif sort_order == SortingOrder.DESC.value:
            result_query = result_query.group_by(
                cls.asin).order_by(desc('gross_sales')).limit(size)

        products = result_query.all()

        return products

    @classmethod
    def update_orders(cls, account_id: str, selling_partner_id: str, amazon_order_id: str, merchant_order_id: str, purchase_date: str, last_updated_date: str,
                      order_status: str, fulfillment_channel: str, sales_channel: str,
                      ship_service_level: str, product_name: str, sku: str, asin: str, item_status: str,
                      quantity: int, currency: str, item_price: float, item_tax: float, shipping_price: float,
                      shipping_tax: float, gift_wrap_price: float, gift_wrap_tax: float, item_promotion_discount: float,
                      ship_promotion_discount: float, ship_city: str, ship_state: str, ship_postal_code: str,
                      ship_country: str):
        """update orders according to amazon id from orders table"""

        category, brand = None, None

        if sku:
            category, brand = AzItemMaster.get_category_brand_by_sku(
                account_id=account_id, asp_id=selling_partner_id, seller_sku=sku)

        order = db.session.query(cls).filter(
            cls.account_id == account_id, cls.selling_partner_id == selling_partner_id, cls.amazon_order_id == amazon_order_id, cls.sku == sku).first()

        if order:
            order.category = category
            order.brand = brand
            order.amazon_order_id = amazon_order_id
            order.merchant_order_id = merchant_order_id
            order.purchase_date = purchase_date
            order.last_updated_date = last_updated_date
            order.order_status = order_status
            order.fulfillment_channel = fulfillment_channel
            order.sales_channel = sales_channel
            order.ship_service_level = ship_service_level
            order.product_name = product_name
            order.sku = sku
            order.asin = asin
            order.item_status = item_status
            order.quantity = quantity
            order.currency = currency
            order.item_price = item_price
            order.item_tax = item_tax
            order.shipping_price = shipping_price
            order.shipping_tax = shipping_tax
            order.gift_wrap_price = gift_wrap_price
            order.gift_wrap_tax = gift_wrap_tax
            order.item_promotion_discount = item_promotion_discount
            order.ship_promotion_discount = ship_promotion_discount
            order.ship_city = ship_city
            order.ship_state = ship_state
            order.ship_postal_code = ship_postal_code
            order.ship_country = ship_country
        # order.gross_sales = sum(i for i in [item_price, item_tax, shipping_price, shipping_tax, gift_wrap_price,
        #                         gift_wrap_tax, item_promotion_discount, ship_promotion_discount] if i is not None)

            order.updated_at = int(time.time())
            db.session.commit()

        return order

    @classmethod
    def get_by_az_order_id_and_sku(cls, amazon_order_id: str, selling_partner_id: str, sku: str):
        """Get order by amazon order id and sku"""

        return db.session.query(cls).filter(
            cls.selling_partner_id == selling_partner_id, cls.amazon_order_id == amazon_order_id, cls.sku == sku).first()
