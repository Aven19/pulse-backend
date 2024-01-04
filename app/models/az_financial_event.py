"""
    Database model for storing Financial Events in database is written in this File along with its methods.
"""
from typing import Any

from app import db
from app.models.base import Base


class AzFinancialEvent(Base):
    """
        Financial Event model to store events in database
    """
    __tablename__ = 'az_financial_event'

    id = db.Column(db.BigInteger, primary_key=True)
    account_id = db.Column(db.String(36), nullable=False)
    asp_id = db.Column(db.String(255))
    category = db.Column(db.String(250), nullable=True)
    brand = db.Column(db.String(250), nullable=True)
    az_order_id = db.Column(db.String(255))
    seller_order_id = db.Column(db.String(255))
    seller_sku = db.Column(db.String(255), nullable=True)
    market_place = db.Column(db.String(255))
    posted_date = db.Column(db.String(255))
    event_type = db.Column(db.String(255))
    event_json = db.Column(db.JSON)
    finance_type = db.Column(db.String(255))
    finance_value = db.Column(db.String(255))
    created_at = db.Column(db.BigInteger)
    updated_at = db.Column(db.BigInteger, nullable=True)
    deactivated_at = db.Column(db.BigInteger, nullable=True)

    @classmethod
    def get_by_az_order_id(cls, account_id: str, asp_id: str, az_order_id: str) -> Any:
        """Filter record by Amazon order id"""
        return db.session.query(cls).filter(cls.account_id == account_id, cls.asp_id == asp_id,
                                            cls.az_order_id == az_order_id).order_by(cls.posted_date.asc()).all()
