"""
    Database model for storing Fba Customer Shipment Sales Report in database is written in this File along with its methods.
"""
import time

from app import db
from app.helpers.constants import DbAnomalies
from app.models.az_item_master import AzItemMaster
from app.models.base import Base


class AzFbaCustomerShipmentSales(Base):
    """
        Fba Customer Shipment Sales Report model to store returns report in database
    """
    __tablename__ = 'az_fba_customer_shipment_sales'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    account_id = db.Column(db.String(36), nullable=False)
    asp_id = db.Column(db.String(255))
    brand = db.Column(db.String(250), nullable=True)
    category = db.Column(db.String(250), nullable=True)
    shipment_date = db.Column(db.String(255))
    sku = db.Column(db.String(255))
    fnsku = db.Column(db.String(255))
    asin = db.Column(db.String(255))
    fulfillment_center_id = db.Column(db.String(255))
    quantity = db.Column(db.Integer)
    currency = db.Column(db.String(255))
    amazon_order_id = db.Column(db.String(255))
    item_price_per_unit = db.Column(db.Numeric(10, 2))  # type: ignore  # noqa: FKA100
    shipping_price = db.Column(db.Numeric(10, 2))  # type: ignore  # noqa: FKA100
    gift_wrap_price = db.Column(db.Numeric(10, 2))  # type: ignore  # noqa: FKA100
    ship_city = db.Column(db.String(255))
    ship_state = db.Column(db.String(255))
    ship_postal_code = db.Column(db.String(255))
    created_at = db.Column(db.BigInteger)
    updated_at = db.Column(db.BigInteger)

    @classmethod
    def get_by_order_id(cls, order_id: str):
        """Filter records by Order Id."""
        return db.session.query(cls).filter(cls.order_id == order_id).first()

    @classmethod
    def add_or_update(cls, account_id: str, asp_id: str,
                      shipment_date: str,
                      sku: str,
                      fnsku: str,
                      asin: str,
                      fulfillment_center_id: str,
                      quantity: int,
                      amazon_order_id: str,
                      currency: str,
                      item_price_per_unit: float,
                      shipping_price: float,
                      gift_wrap_price: float,
                      ship_city: str,
                      ship_state: str,
                      ship_postal_code: str
                      ):
        """Insert or update a FBA Customer Shipment Sales Report"""

        category, brand = None, None

        if sku:
            category, brand = AzItemMaster.get_category_brand_by_sku(
                account_id=account_id, asp_id=asp_id, seller_sku=sku)

        fba_customer_shipment_sales = db.session.query(cls).filter(
            cls.account_id == account_id, cls.asp_id == asp_id,
            cls.amazon_order_id == amazon_order_id, cls.shipment_date == shipment_date,
            cls.sku == sku, cls.asin == asin, cls.fulfillment_center_id == fulfillment_center_id).first()
        current_time = int(time.time())
        record = DbAnomalies.UPDATE.value

        if fba_customer_shipment_sales == None:
            record = DbAnomalies.INSERTION.value
            fba_customer_shipment_sales = cls(account_id=account_id, asp_id=asp_id, amazon_order_id=amazon_order_id, shipment_date=shipment_date,
                                              sku=sku, asin=asin, fulfillment_center_id=fulfillment_center_id, created_at=current_time)

        fba_customer_shipment_sales.category = category
        fba_customer_shipment_sales.brand = brand
        fba_customer_shipment_sales.quantity = quantity
        fba_customer_shipment_sales.fnsku = fnsku
        fba_customer_shipment_sales.currency = currency
        fba_customer_shipment_sales.item_price_per_unit = item_price_per_unit
        fba_customer_shipment_sales.shipping_price = shipping_price
        fba_customer_shipment_sales.gift_wrap_price = gift_wrap_price
        fba_customer_shipment_sales.ship_city = ship_city
        fba_customer_shipment_sales.ship_state = ship_state
        fba_customer_shipment_sales.ship_postal_code = ship_postal_code

        if record == DbAnomalies.INSERTION.value:
            fba_customer_shipment_sales.save()
        else:
            fba_customer_shipment_sales.updated_at = current_time
            db.session.commit()

        return fba_customer_shipment_sales
