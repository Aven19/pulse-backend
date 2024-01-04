"""
    Database model for storing order items data in database is written in this File along with its methods.
"""
import time

from app import db
from app.models.base import Base


class AzOrderItems(Base):
    """
        Order Items data model"
    """
    __tablename__ = 'az_order_item'

    id = db.Column(db.BigInteger, primary_key=True)
    account_id = db.Column(db.String(36), nullable=False)
    asp_id = db.Column(db.String(255))
    selling_partner_id = db.Column(db.String(255))
    category = db.Column(db.String(250), nullable=True)
    brand = db.Column(db.String(250), nullable=True)
    amazon_order_id = db.Column(db.String(255))
    product_info_number_of_items = db.Column(db.Integer)
    item_tax_currency_code = db.Column(db.String(20))
    item_tax_amount = db.Column(db.Numeric)
    quantity_shipped = db.Column(db.Integer)
    item_price_currency_code = db.Column(db.String(20))
    item_price_amount = db.Column(db.Numeric)
    asin = db.Column(db.String(255))
    seller_sku = db.Column(db.String(255))
    title = db.Column(db.String(255))
    is_gift = db.Column(db.Boolean)
    is_transparency = db.Column(db.Boolean)
    quantity_ordered = db.Column(db.Integer)
    promotion_discount_tax_currency_code = db.Column(db.String(20))
    promotion_discount_tax_amount = db.Column(db.Numeric)
    promotion_ids = db.Column(db.String(255))
    promotion_discount_currency_code = db.Column(db.String(20))
    promotion_discount_amount = db.Column(db.Numeric)
    order_item_id = db.Column(db.String(255))
    created_at = db.Column(db.BigInteger, nullable=False)
    updated_at = db.Column(db.BigInteger, nullable=True)
    deleted_at = db.Column(db.BigInteger, nullable=True)

    @classmethod
    def add(cls, selling_partner_id: str, amazon_order_id: str, product_info_number_of_items: int, item_tax_currency_code: str, item_tax_amount: float, quantity_shipped: int, item_price_currency_code: str, item_price_amount: float,
            asin: str, seller_sku: str, title: str, is_gift: bool, is_transparency: bool, quantity_ordered: int,
            promotion_discount_tax_currency_code: str, promotion_discount_tax_amount: float, promotion_ids: str, promotion_discount_currency_code: str,
            promotion_discount_amount: float, order_item_id: str):
        """add order items data to table"""

        order_items = cls()
        order_items.selling_partner_id = selling_partner_id
        order_items.amazon_order_id = amazon_order_id
        order_items.product_info_number_of_items = product_info_number_of_items
        order_items.item_tax_currency_code = item_tax_currency_code
        order_items.item_tax_amount = item_tax_amount
        order_items.quantity_shipped = quantity_shipped
        order_items.item_price_currency_code = item_price_currency_code
        order_items.item_price_amount = item_price_amount
        order_items.asin = asin
        order_items.seller_sku = seller_sku
        order_items.title = title
        order_items.is_gift = is_gift
        order_items.is_transparency = is_transparency
        order_items.quantity_ordered = quantity_ordered
        order_items.promotion_discount_tax_currency_code = promotion_discount_tax_currency_code
        order_items.promotion_discount_tax_amount = promotion_discount_tax_amount
        order_items.promotion_ids = promotion_ids
        order_items.promotion_discount_currency_code = promotion_discount_currency_code
        order_items.promotion_discount_amount = promotion_discount_amount
        order_items.order_item_id = order_item_id
        order_items.created_at = int(time.time())

        order_items.save()

        return order_items

    @classmethod
    def update_order_items(cls, selling_partner_id: str, amazon_order_id: str, product_info_number_of_items: int, item_tax_currency_code: str, item_tax_amount: float, quantity_shipped: int, item_price_currency_code: str, item_price_amount: float,
                           asin: str, seller_sku: str, title: str, is_gift: bool, is_transparency: bool, quantity_ordered: int,
                           promotion_discount_tax_currency_code: str, promotion_discount_tax_amount: float, promotion_ids: str, promotion_discount_currency_code: str,
                           promotion_discount_amount: float, order_item_id: str):
        """update order items data to table"""

        order_item = db.session.query(cls).filter(
            cls.selling_partner_id == selling_partner_id, cls.amazon_order_id == amazon_order_id).first()

        order_item.product_info_number_of_items = product_info_number_of_items
        order_item.item_tax_currency_code = item_tax_currency_code
        order_item.item_tax_amount = item_tax_amount
        order_item.quantity_shipped = quantity_shipped
        order_item.item_price_currency_code = item_price_currency_code
        order_item.item_price_amount = item_price_amount
        order_item.asin = asin
        order_item.seller_sku = seller_sku
        order_item.title = title
        order_item.is_gift = is_gift
        order_item.is_transparency = is_transparency
        order_item.quantity_ordered = quantity_ordered
        order_item.promotion_discount_tax_currency_code = promotion_discount_tax_currency_code
        order_item.promotion_discount_tax_amount = promotion_discount_tax_amount
        order_item.promotion_ids = promotion_ids
        order_item.promotion_discount_currency_code = promotion_discount_currency_code
        order_item.promotion_discount_amount = promotion_discount_amount
        order_item.order_item_id = order_item_id
        order_item.updated_at = int(time.time())
        db.session.commit()

        return order_item

    @classmethod
    def insert_or_update(cls, selling_partner_id: str, amazon_order_id: str, product_info_number_of_items: int, item_tax_currency_code: str, item_tax_amount: float, quantity_shipped: int, item_price_currency_code: str, item_price_amount: float,
                         asin: str, seller_sku: str, title: str, is_gift: bool, is_transparency: bool, quantity_ordered: int,
                         promotion_discount_tax_currency_code: str, promotion_discount_tax_amount: float, promotion_ids: str, promotion_discount_currency_code: str,
                         promotion_discount_amount: float, order_item_id: str):
        """insert or update by amazon id"""

        order_item = db.session.query(cls).filter(
            cls.selling_partner_id == selling_partner_id, cls.amazon_order_id == amazon_order_id).first()

        if not order_item:
            """creating new order if order does not exist"""
            new_order_item = cls()
            new_order_item.selling_partner_id = selling_partner_id
            new_order_item.amazon_order_id = amazon_order_id
            new_order_item.product_info_number_of_items = product_info_number_of_items
            new_order_item.item_tax_currency_code = item_tax_currency_code
            new_order_item.item_tax_amount = item_tax_amount
            new_order_item.quantity_shipped = quantity_shipped
            new_order_item.item_price_currency_code = item_price_currency_code
            new_order_item.item_price_amount = item_price_amount
            new_order_item.asin = asin
            new_order_item.seller_sku = seller_sku
            new_order_item.title = title
            new_order_item.is_gift = is_gift
            new_order_item.is_transparency = is_transparency
            new_order_item.quantity_ordered = quantity_ordered
            new_order_item.promotion_discount_tax_currency_code = promotion_discount_tax_currency_code
            new_order_item.promotion_discount_tax_amount = promotion_discount_tax_amount
            new_order_item.promotion_ids = promotion_ids
            new_order_item.promotion_discount_currency_code = promotion_discount_currency_code
            new_order_item.promotion_discount_amount = promotion_discount_amount
            new_order_item.order_item_id = order_item_id
            new_order_item.created_at = int(time.time())

            new_order_item.save()

            return new_order_item

        else:
            """updating if order item already exists"""
            order_item.product_info_number_of_items = product_info_number_of_items
            order_item.item_tax_currency_code = item_tax_currency_code
            order_item.item_tax_amount = item_tax_amount
            order_item.quantity_shipped = quantity_shipped
            order_item.item_price_currency_code = item_price_currency_code
            order_item.item_price_amount = item_price_amount
            order_item.asin = asin
            order_item.seller_sku = seller_sku
            order_item.title = title
            order_item.is_gift = is_gift
            order_item.is_transparency = is_transparency
            order_item.quantity_ordered = quantity_ordered
            order_item.promotion_discount_tax_currency_code = promotion_discount_tax_currency_code
            order_item.promotion_discount_tax_amount = promotion_discount_tax_amount
            order_item.promotion_ids = promotion_ids
            order_item.promotion_discount_currency_code = promotion_discount_currency_code
            order_item.promotion_discount_amount = promotion_discount_amount
            order_item.order_item_id = order_item_id
            order_item.updated_at = int(time.time())
            db.session.commit()

            return order_item

    @classmethod
    def get_by_amazon_order_id(cls, amazon_order_id: str, selling_partner_id: str):
        """get order by amazon id"""

        item = db.session.query(cls).filter(
            cls.selling_partner_id == selling_partner_id, cls.amazon_order_id == amazon_order_id).first()

        return item
