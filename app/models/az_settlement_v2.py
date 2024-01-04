"""
    Database model for storing Settlement Report v2 in database is written in this File along with its methods.
"""
from datetime import datetime
import time

from app import db
from app.models.base import Base


class AzSettlementV2(Base):
    """
        Model for storing Settlement Report v2 data in database
    """
    __tablename__ = 'az_settlement_v2'

    id = db.Column(db.Integer, primary_key=True)
    account_id = db.Column(db.String(36))
    asp_id = db.Column(db.String(255))
    selling_partner_id = db.Column(db.String(255))
    category = db.Column(db.String(250), nullable=True)
    brand = db.Column(db.String(250), nullable=True)
    settlement_id = db.Column(db.String(255), nullable=False)
    settlement_start_date = db.Column(db.DateTime, nullable=True)
    settlement_end_date = db.Column(db.DateTime, nullable=True)
    deposit_date = db.Column(db.DateTime, nullable=True)
    total_amount = db.Column(db.Numeric(10, 2), nullable=True)  # type: ignore  # noqa: FKA100
    currency = db.Column(db.String(10))
    transaction_type = db.Column(db.String(100))
    order_id = db.Column(db.String(255), nullable=True)
    merchant_order_id = db.Column(db.String(255), nullable=True)
    adjustment_id = db.Column(db.String(255), nullable=True)
    shipment_id = db.Column(db.String(255), nullable=True)
    marketplace_name = db.Column(db.String(255), nullable=True)
    amount_details = db.Column(db.JSON)
    fulfillment_id = db.Column(db.String(255), nullable=True)
    posted_date = db.Column(db.Date, nullable=True)
    posted_date_time = db.Column(db.DateTime, nullable=True)
    order_item_code = db.Column(db.String(255), nullable=True)
    merchant_order_item_id = db.Column(db.String(255), nullable=True)
    merchant_adjustment_item_id = db.Column(db.String(255), nullable=True)
    sku = db.Column(db.String(255), nullable=True)
    quantity_purchased = db.Column(db.Integer, nullable=True)
    promotion_id = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.BigInteger)
    updated_at = db.Column(db.BigInteger)

    def __repr__(self) -> str:
        """
            Object Representation Method for custom object representation on console or log
        """
        return '<id {}>'.format(self.id)

    @classmethod
    def add(cls, selling_partner_id: str, settlement_id: str, settlement_start_date: str, settlement_end_date: str, deposit_date: str, total_amount: float, currency: str, transaction_type: str, order_id: str, merchant_order_id: str, adjustment_id: str, shipment_id: str, marketplace_name: str, amount_details: dict, fulfillment_id: str, posted_date: str, posted_date_time: str, order_item_code: str, merchant_order_item_id: str, merchant_adjustment_item_id: str, sku: str, quantity_purchased: int, promotion_id: str):
        """Create a new Settlement Report v2"""
        settlement_report_v2 = cls()
        settlement_report_v2.selling_partner_id = selling_partner_id,
        settlement_report_v2.settlement_id = settlement_id,
        settlement_report_v2.settlement_start_date = settlement_start_date,
        settlement_report_v2.settlement_end_date = settlement_end_date,
        settlement_report_v2.deposit_date = deposit_date,
        settlement_report_v2.total_amount = total_amount,
        settlement_report_v2.currency = currency,
        settlement_report_v2.transaction_type = transaction_type,
        settlement_report_v2.order_id = order_id,
        settlement_report_v2.merchant_order_id = merchant_order_id,
        settlement_report_v2.adjustment_id = adjustment_id,
        settlement_report_v2.shipment_id = shipment_id,
        settlement_report_v2.marketplace_name = marketplace_name,
        settlement_report_v2.amount_details = amount_details,
        settlement_report_v2.fulfillment_id = fulfillment_id,
        settlement_report_v2.posted_date = posted_date,
        settlement_report_v2.posted_date_time = posted_date_time,
        settlement_report_v2.order_item_code = order_item_code,
        settlement_report_v2.merchant_order_item_id = merchant_order_item_id,
        settlement_report_v2.merchant_adjustment_item_id = merchant_adjustment_item_id,
        settlement_report_v2.sku = sku,
        settlement_report_v2.quantity_purchased = quantity_purchased,
        settlement_report_v2.promotion_id = promotion_id,
        settlement_report_v2.created_at = int(time.time())
        settlement_report_v2.save()

        return settlement_report_v2

    @classmethod
    def get_settlement_by_date(cls, selling_partner_id: str, start_date: str, end_date: str):
        """
        return settlement report data by date
        """
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()   # type: ignore  # noqa: FKA100
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()    # type: ignore  # noqa: FKA100

        settlements = db.session.query(cls).filter(cls.selling_partner_id == selling_partner_id, cls.posted_date >= start_date, cls.posted_date <= end_date).all()     # type: ignore  # noqa: FKA100

        return settlements

    @classmethod
    def get_by_order_id(cls, selling_partner_id: str, order_id: str):
        """
        return settlement report data by order_id
        """
        settlement = db.session.query(cls).filter(cls.selling_partner_id == selling_partner_id, cls.order_id == order_id).first()  # type: ignore  # noqa: FKA100

        return settlement

    @classmethod
    def update_by_order_id(cls, selling_partner_id: str, settlement_id: str, settlement_start_date: str, settlement_end_date: str, deposit_date: str, total_amount: float, currency: str, transaction_type: str, order_id: str, merchant_order_id: str, adjustment_id: str, shipment_id: str, marketplace_name: str, amount_details: dict, fulfillment_id: str, posted_date: str, posted_date_time: str, order_item_code: str, merchant_order_item_id: str, merchant_adjustment_item_id: str, sku: str, quantity_purchased: int, promotion_id: str):
        """update settlement by order id"""

        settlement = db.session.query(cls).filter(
            cls.selling_partner_id == selling_partner_id, cls.order_id == order_id).first()

        settlement.selling_partner_id = selling_partner_id
        settlement.settlement_id = settlement_id
        settlement.settlement_start_date = settlement_start_date
        settlement.settlement_end_date = settlement_end_date
        settlement.deposit_date = deposit_date
        settlement.total_amount = total_amount
        settlement.currency = currency
        settlement.transaction_type = transaction_type
        settlement.order_id = order_id
        settlement.merchant_order_id = merchant_order_id
        settlement.adjustment_id = adjustment_id
        settlement.shipment_id = shipment_id
        settlement.marketplace_name = marketplace_name
        settlement.amount_details = amount_details
        settlement.fulfillment_id = fulfillment_id
        settlement.posted_date = posted_date
        settlement.posted_date_time = posted_date_time
        settlement.order_item_code = order_item_code
        settlement.merchant_order_item_id = merchant_order_item_id
        settlement.merchant_adjustment_item_id = merchant_adjustment_item_id
        settlement.sku = sku
        settlement.quantity_purchased = quantity_purchased
        settlement.promotion_id = promotion_id
        settlement.updated_at = int(time.time())

        db.session.commit()

        return settlement
