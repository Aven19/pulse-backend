"""
    Database model for storing orders data in database is written in this File along with its methods.
"""
import time

from app import db
from app.models.base import Base


class AzOrders(Base):
    """
        orders api data table
    """
    __tablename__ = 'az_order'

    id = db.Column(db.BigInteger, primary_key=True)
    account_id = db.Column(db.String(36), nullable=False)
    asp_id = db.Column(db.String(255))
    selling_partner_id = db.Column(db.String(255))
    category = db.Column(db.String(250), nullable=True)
    brand = db.Column(db.String(250), nullable=True)
    buyer_email = db.Column(db.String(255))
    amazon_order_id = db.Column(db.String(255))
    earliest_ship_date = db.Column(db.DateTime)
    sales_channel = db.Column(db.String, nullable=True)
    order_status = db.Column(db.String(255), nullable=True)
    number_of_items_shipped = db.Column(db.Integer)
    order_type = db.Column(db.String(255), nullable=True)
    is_premium_order = db.Column(db.Boolean)
    is_prime = db.Column(db.Boolean)
    fulfillment_channel = db.Column(db.String(255), nullable=True)
    number_of_items_unshipped = db.Column(db.Integer)
    has_regulated_items = db.Column(db.Boolean)
    is_replacement_order = db.Column(db.Boolean)
    is_sold_by_ab = db.Column(db.Boolean)
    latest_ship_date = db.Column(db.DateTime)
    ship_service_level = db.Column(db.String(255))
    is_ispu = db.Column(db.Boolean)
    marketplace_id = db.Column(db.String(100))
    purchase_date = db.Column(db.DateTime)
    shipping_address_state_or_region = db.Column(db.String(255))
    shipping_address_postal_code = db.Column(db.Integer)
    shipping_address_city = db.Column(db.String(255))
    shipping_address_country_code = db.Column(db.String(20))
    is_access_point_order = db.Column(db.Boolean)
    seller_order_id = db.Column(db.String(255))
    payment_method = db.Column(db.String(255), nullable=True)
    is_business_order = db.Column(db.Boolean)
    order_total_currency_code = db.Column(db.String(20))
    order_total_amount = db.Column(db.Numeric)
    payment_method_details = db.Column(db.String(100))
    is_global_express_enabled = db.Column(db.Boolean)
    last_update_date = db.Column(db.DateTime)
    shipment_service_level_category = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.BigInteger, nullable=False)
    updated_at = db.Column(db.BigInteger, nullable=True)
    deleted_at = db.Column(db.BigInteger, nullable=True)

    @classmethod
    def add(cls, selling_partner_id: str, buyer_email: str, amazon_order_id: str, earliest_ship_date: str, sales_channel: str, order_status: str, number_of_items_shipped: int,
            order_type: str, is_premium_order: bool, is_prime: bool, fulfillment_channel: str, number_of_items_unshipped: int, has_regulated_items: bool,
            is_replacement_order: bool, is_sold_by_ab: bool, latest_ship_date: str, ship_service_level: str, is_ispu: bool, marketplace_id: str,
            purchase_date: str, shipping_address_state_or_region: str, shipping_address_postal_code: int, shipping_address_city: str, shipping_address_country_code: str,
            is_access_point_order: bool, seller_order_id: str, payment_method: str, is_business_order: bool, order_total_currency_code: str,
            order_total_amount: float, payment_method_details: str, is_global_express_enabled: bool, last_update_date: bool, shipment_service_level_category: str):
        """add getOrders data to table"""

        get_orders = cls()
        get_orders.selling_partner_id = selling_partner_id
        get_orders.buyer_email = buyer_email
        get_orders.amazon_order_id = amazon_order_id
        get_orders.earliest_ship_date = earliest_ship_date
        get_orders.sales_channel = sales_channel
        get_orders.order_status = order_status
        get_orders.number_of_items_shipped = number_of_items_shipped
        get_orders.order_type = order_type
        get_orders.is_premium_order = is_premium_order
        get_orders.is_prime = is_prime
        get_orders.fulfillment_channel = fulfillment_channel
        get_orders.number_of_items_unshipped = number_of_items_unshipped
        get_orders.has_regulated_items = has_regulated_items
        get_orders.is_replacement_order = is_replacement_order
        get_orders.is_sold_by_ab = is_sold_by_ab
        get_orders.latest_ship_date = latest_ship_date
        get_orders.ship_service_level = ship_service_level
        get_orders.is_ispu = is_ispu
        get_orders.marketplace_id = marketplace_id
        get_orders.purchase_date = purchase_date
        get_orders.shipping_address_state_or_region = shipping_address_state_or_region
        get_orders.shipping_address_postal_code = shipping_address_postal_code
        get_orders.shipping_address_city = shipping_address_city
        get_orders.shipping_address_country_code = shipping_address_country_code
        get_orders.is_access_point_order = is_access_point_order
        get_orders.seller_order_id = seller_order_id
        get_orders.payment_method = payment_method
        get_orders.is_business_order = is_business_order
        get_orders.order_total_currency_code = order_total_currency_code
        get_orders.order_total_amount = order_total_amount
        get_orders.payment_method_details = payment_method_details
        get_orders.is_global_express_enabled = is_global_express_enabled
        get_orders.last_update_date = last_update_date
        get_orders.shipment_service_level_category = shipment_service_level_category
        get_orders.created_at = int(time.time())
        get_orders.save()

        return get_orders

    @classmethod
    def get_by_amazon_order_id(cls, amazon_order_id: str, selling_partner_id: str):
        """get order by amazon id"""

        item = db.session.query(cls).filter(
            cls.selling_partner_id == selling_partner_id, cls.amazon_order_id == amazon_order_id).first()

        return item

    @classmethod
    def update_orders(cls, selling_partner_id: str, buyer_email: str, amazon_order_id: str, earliest_ship_date: str, sales_channel: str, order_status: str, number_of_items_shipped: int,
                      order_type: str, is_premium_order: bool, is_prime: bool, fulfillment_channel: str, number_of_items_unshipped: int, has_regulated_items: bool,
                      is_replacement_order: bool, is_sold_by_ab: bool, latest_ship_date: str, ship_service_level: str, is_ispu: bool, marketplace_id: str,
                      purchase_date: str, shipping_address_state_or_region: str, shipping_address_postal_code: int, shipping_address_city: str, shipping_address_country_code: str,
                      is_access_point_order: bool, seller_order_id: str, payment_method: str, is_business_order: bool, order_total_currency_code: str,
                      order_total_amount: float, payment_method_details: str, is_global_express_enabled: bool, last_update_date: bool, shipment_service_level_category: str):
        """update orders according to amazon id from orders table"""

        orders = db.session.query(cls).filter(
            cls.selling_partner_id == selling_partner_id, cls.amazon_order_id == amazon_order_id).first()

        orders.buyer_email = buyer_email
        orders.earliest_ship_date = earliest_ship_date
        orders.sales_channel = sales_channel
        orders.order_status = order_status
        orders.number_of_items_shipped = number_of_items_shipped
        orders.order_type = order_type
        orders.is_premium_order = is_premium_order
        orders.is_prime = is_prime
        orders.fulfillment_channel = fulfillment_channel
        orders.number_of_items_unshipped = number_of_items_unshipped
        orders.has_regulated_items = has_regulated_items
        orders.is_replacement_order = is_replacement_order
        orders.is_sold_by_ab = is_sold_by_ab
        orders.latest_ship_date = latest_ship_date
        orders.ship_service_level = ship_service_level
        orders.is_ispu = is_ispu
        orders.marketplace_id = marketplace_id
        orders.purchase_date = purchase_date
        orders.shipping_address_state_or_region = shipping_address_state_or_region
        orders.shipping_address_postal_code = shipping_address_postal_code
        orders.shipping_address_city = shipping_address_city
        orders.shipping_address_country_code = shipping_address_country_code
        orders.is_access_point_order = is_access_point_order
        orders.seller_order_id = seller_order_id
        orders.payment_method = payment_method
        orders.is_business_order = is_business_order
        orders.order_total_currency_code = order_total_currency_code
        orders.order_total_amount = order_total_amount
        orders.payment_method_details = payment_method_details
        orders.is_global_express_enabled = is_global_express_enabled
        orders.last_update_date = last_update_date
        orders.shipment_service_level_category = shipment_service_level_category
        orders.updated_at = int(time.time())
        db.session.commit()

        return orders

    @classmethod
    def insert_or_update(cls, selling_partner_id: str, account_id: str, buyer_email: str, amazon_order_id: str, earliest_ship_date: str, sales_channel: str, order_status: str, number_of_items_shipped: int,
                         order_type: str, is_premium_order: bool, is_prime: bool, fulfillment_channel: str, number_of_items_unshipped: int, has_regulated_items: bool,
                         is_replacement_order: bool, is_sold_by_ab: bool, latest_ship_date: str, ship_service_level: str, is_ispu: bool, marketplace_id: str,
                         purchase_date: str, shipping_address_state_or_region: str, shipping_address_postal_code: int, shipping_address_city: str, shipping_address_country_code: str,
                         is_access_point_order: bool, seller_order_id: str, payment_method: str, is_business_order: bool, order_total_currency_code: str,
                         order_total_amount: float, payment_method_details: str, is_global_express_enabled: bool, last_update_date: bool, shipment_service_level_category: str):
        """insert or update orders"""

        order = db.session.query(cls).filter(
            cls.selling_partner_id == selling_partner_id, cls.account_id == account_id, cls.amazon_order_id == amazon_order_id).first()

        if order:
            """update if order exists"""
            order.buyer_email = buyer_email
            order.earliest_ship_date = earliest_ship_date
            order.sales_channel = sales_channel
            order.order_status = order_status
            order.number_of_items_shipped = number_of_items_shipped
            order.order_type = order_type
            order.is_premium_order = is_premium_order
            order.is_prime = is_prime
            order.fulfillment_channel = fulfillment_channel
            order.number_of_items_unshipped = number_of_items_unshipped
            order.has_regulated_items = has_regulated_items
            order.is_replacement_order = is_replacement_order
            order.is_sold_by_ab = is_sold_by_ab
            order.latest_ship_date = latest_ship_date
            order.ship_service_level = ship_service_level
            order.is_ispu = is_ispu
            order.marketplace_id = marketplace_id
            order.purchase_date = purchase_date
            order.shipping_address_state_or_region = shipping_address_state_or_region
            order.shipping_address_postal_code = shipping_address_postal_code
            order.shipping_address_city = shipping_address_city
            order.shipping_address_country_code = shipping_address_country_code
            order.is_access_point_order = is_access_point_order
            order.seller_order_id = seller_order_id
            order.payment_method = payment_method
            order.is_business_order = is_business_order
            order.order_total_currency_code = order_total_currency_code
            order.order_total_amount = order_total_amount
            order.payment_method_details = payment_method_details
            order.is_global_express_enabled = is_global_express_enabled
            order.last_update_date = last_update_date
            order.shipment_service_level_category = shipment_service_level_category
            order.updated_at = int(time.time())
            db.session.commit()

            return order

        else:
            new_order = cls()
            new_order.selling_partner_id = selling_partner_id
            new_order.account_id = account_id
            new_order.buyer_email = buyer_email
            new_order.amazon_order_id = amazon_order_id
            new_order.earliest_ship_date = earliest_ship_date
            new_order.sales_channel = sales_channel
            new_order.order_status = order_status
            new_order.number_of_items_shipped = number_of_items_shipped
            new_order.order_type = order_type
            new_order.is_premium_order = is_premium_order
            new_order.is_prime = is_prime
            new_order.fulfillment_channel = fulfillment_channel
            new_order.number_of_items_unshipped = number_of_items_unshipped
            new_order.has_regulated_items = has_regulated_items
            new_order.is_replacement_order = is_replacement_order
            new_order.is_sold_by_ab = is_sold_by_ab
            new_order.latest_ship_date = latest_ship_date
            new_order.ship_service_level = ship_service_level
            new_order.is_ispu = is_ispu
            new_order.marketplace_id = marketplace_id
            new_order.purchase_date = purchase_date
            new_order.shipping_address_state_or_region = shipping_address_state_or_region
            new_order.shipping_address_postal_code = shipping_address_postal_code
            new_order.shipping_address_city = shipping_address_city
            new_order.shipping_address_country_code = shipping_address_country_code
            new_order.is_access_point_order = is_access_point_order
            new_order.seller_order_id = seller_order_id
            new_order.payment_method = payment_method
            new_order.is_business_order = is_business_order
            new_order.order_total_currency_code = order_total_currency_code
            new_order.order_total_amount = order_total_amount
            new_order.payment_method_details = payment_method_details
            new_order.is_global_express_enabled = is_global_express_enabled
            new_order.last_update_date = last_update_date
            new_order.shipment_service_level_category = shipment_service_level_category
            new_order.created_at = int(time.time())
            new_order.save()

            return new_order
