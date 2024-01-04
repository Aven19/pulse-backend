"""
    Database model for storing returns in database is written in this File along with its methods.
"""
from app import db
from app.models.base import Base


class AzReturn(Base):
    """
        Return model to store returns in database
    """
    __tablename__ = 'az_return'

    id = db.Column(db.Integer, primary_key=True)
    account_id = db.Column(db.String(36), nullable=False)
    asp_id = db.Column(db.String(255))
    selling_partner_id = db.Column(db.String(255))
    category = db.Column(db.String(250), nullable=True)
    brand = db.Column(db.String(250), nullable=True)
    order_id = db.Column(db.String(255))
    order_date = db.Column(db.Date)
    return_request_date = db.Column(db.Date)
    return_request_status = db.Column(db.String(255))
    amazon_rma_id = db.Column(db.String(255))
    merchant_rma_id = db.Column(db.String(255))
    label_type = db.Column(db.String(255))
    label_cost = db.Column(db.Float)
    currency_code = db.Column(db.String(3))
    return_carrier = db.Column(db.String(255))
    tracking_id = db.Column(db.String(255))
    label_to_be_paid_by = db.Column(db.String(255))
    a_to_z_claim = db.Column(db.Boolean)
    is_prime = db.Column(db.Boolean)
    asin = db.Column(db.String(10))
    merchant_sku = db.Column(db.String(255))
    item_name = db.Column(db.String(255))
    return_quantity = db.Column(db.Integer)
    return_reason = db.Column(db.String(255))
    in_policy = db.Column(db.Boolean)
    return_type = db.Column(db.String(255))
    resolution = db.Column(db.String(255))
    invoice_number = db.Column(db.String(255))
    return_delivery_date = db.Column(db.Date)
    order_amount = db.Column(db.Float)
    order_quantity = db.Column(db.Integer)
    safet_action_reason = db.Column(db.String(255))
    safet_claim_id = db.Column(db.String(255))
    safet_claim_state = db.Column(db.String(255))
    safet_claim_creation_time = db.Column(db.DateTime)
    safet_claim_reimbursement_amount = db.Column(db.Float)
    refunded_amount = db.Column(db.Float)
