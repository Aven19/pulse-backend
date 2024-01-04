'settlement model to store settlement report data'

import time

from app import db
from app.models.base import Base


class AzSettlement(Base):
    """
        settlement model to store settlement report data in database
    """
    __tablename__ = 'az_settlement'

    id = db.Column(db.BigInteger, primary_key=True)
    account_id = db.Column(db.String(36), nullable=False)
    asp_id = db.Column(db.String(255))
    category = db.Column(db.String(250), nullable=True)
    brand = db.Column(db.String(250), nullable=True)
    selling_partner_id = db.Column(db.String(255))
    settlement_id = db.Column(db.BigInteger, nullable=False)
    settlement_start_date = db.Column(db.DateTime, nullable=True)
    settlement_end_date = db.Column(db.DateTime, nullable=True)
    deposit_date = db.Column(db.DateTime, nullable=True)
    total_amount = db.Column(db.Numeric, nullable=True)
    currency = db.Column(db.String(10))
    transaction_type = db.Column(db.String(100))
    order_id = db.Column(db.String(255))
    merchant_order_id = db.Column(db.String(255))
    adjustment_id = db.Column(db.String(255))
    shipment_id = db.Column(db.String(255), nullable=True)
    marketplace_name = db.Column(db.String(255), nullable=True)
    shipment_fee_type = db.Column(db.String(255))
    shipment_fee_amount = db.Column(db.Numeric)
    order_fee_type = db.Column(db.String(255))
    order_fee_amount = db.Column(db.Numeric)
    fulfillment_id = db.Column(db.String(255))
    posted_date = db.Column(db.Date)
    order_item_code = db.Column(db.String(255))
    merchant_order_item_id = db.Column(db.String(255))
    merchant_adjustment_item_id = db.Column(db.String(255))
    sku = db.Column(db.String(255))
    quantity_purchased = db.Column(db.Integer)
    price_type = db.Column(db.String(255))
    price_amount = db.Column(db.Numeric)
    item_related_fee_type = db.Column(db.String(255))
    item_related_fee_amount = db.Column(db.Numeric)
    misc_fee_amount = db.Column(db.Numeric)
    other_fee_amount = db.Column(db.Numeric)
    other_fee_reason_description = db.Column(db.Text)
    promotion_id = db.Column(db.String(255))
    promotion_type = db.Column(db.String(255))
    promotion_amount = db.Column(db.Numeric)
    direct_payment_type = db.Column(db.String(255))
    direct_payment_amount = db.Column(db.Numeric)
    other_amount = db.Column(db.Numeric)
    created_at = db.Column(db.BigInteger)
    updated_at = db.Column(db.BigInteger)

    @classmethod
    def add(cls, selling_partner_id: str, settlement_id: str, settlement_start_date: str, settlement_end_date: str, deposit_date: str, total_amount: float, currency: str, transaction_type: str, order_id: str, merchant_order_id: str, adjustment_id: str, shipment_id: str, marketplace_name: str, shipment_fee_type: str, shipment_fee_amount: float, order_fee_type: str, order_fee_amount: float, fulfillment_id: str, posted_date: str, order_item_code: str, merchant_order_item_id: str, merchant_adjustment_item_id: str, sku: str, quantity_purchased: int, price_type: str, price_amount: float,
            item_related_fee_type: str, item_related_fee_amount: float, misc_fee_amount: float, other_fee_amount: float, other_fee_reason_description: str, promotion_id: str, promotion_type: str, promotion_amount: float, direct_payment_type: str, direct_payment_amount: float, other_amount: float):
        """Create a new Settlement Report v2"""
        settlement = cls()
        settlement.selling_partner_id = selling_partner_id
        settlement.settlement_id = settlement_id,
        settlement.settlement_start_date = settlement_start_date,
        settlement.settlement_end_date = settlement_end_date,
        settlement.deposit_date = deposit_date,
        settlement.total_amount = total_amount,
        settlement.currency = currency,
        settlement.transaction_type = transaction_type,
        settlement.order_id = order_id,
        settlement.merchant_order_id = merchant_order_id,
        settlement.adjustment_id = adjustment_id,
        settlement.shipment_id = shipment_id,
        settlement.marketplace_name = marketplace_name,
        settlement.shipment_fee_type = shipment_fee_type,
        settlement.shipment_fee_amount = shipment_fee_amount
        settlement.order_fee_type = order_fee_type
        settlement.order_fee_amount = order_fee_amount
        settlement.fulfillment_id = fulfillment_id
        settlement.posted_date = posted_date
        settlement.order_item_code = order_item_code,
        settlement.merchant_order_item_id = merchant_order_item_id,
        settlement.merchant_adjustment_item_id = merchant_adjustment_item_id,
        settlement.sku = sku,
        settlement.quantity_purchased = quantity_purchased,
        settlement.price_type = price_type
        settlement.price_amount = price_amount
        settlement.item_related_fee_type = item_related_fee_type
        settlement.item_related_fee_amount = item_related_fee_amount
        settlement.misc_fee_amount = misc_fee_amount
        settlement.other_fee_amount = other_fee_amount
        settlement.other_fee_reason_description = other_fee_reason_description
        settlement.promotion_id = promotion_id,
        settlement.promotion_type = promotion_type
        settlement.promotion_amount = promotion_amount
        settlement.direct_payment_type = direct_payment_type
        settlement.direct_payment_amount = direct_payment_amount
        settlement.other_amount = other_amount
        settlement.created_at = int(time.time())

        settlement.save()

        return settlement
