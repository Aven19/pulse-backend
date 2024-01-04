"""
    Database model for storing payment details in database is written in this File along with its methods.
"""
import time
from typing import Optional

from app import db
from app.helpers.constants import DbAnomalies
from app.models.base import Base
from sqlalchemy import desc


class Payment(Base):
    """
        Plans model to store different plans and details in database
    """
    __tablename__ = 'payment'

    id = db.Column(db.BigInteger, primary_key=True)
    user_id = db.Column(db.BigInteger, nullable=False)
    # account_id = db.Column(db.BigInteger, nullable=False)
    account_id = db.Column(db.String(36), nullable=False)
    amount = db.Column(db.Numeric)
    status = db.Column(db.String(50))
    verified = db.Column(db.String(20))
    currency = db.Column(db.String(20))
    reference_order_id = db.Column(db.String(255))
    reference_payment_id = db.Column(db.String(255))
    reference_subscription_id = db.Column(db.String(255))
    request = db.Column(db.JSON)
    response = db.Column(db.JSON)
    payment_mode = db.Column(db.String(50))
    created_at = db.Column(db.BigInteger)
    updated_at = db.Column(db.BigInteger)

    def __repr__(self) -> str:
        """
            Object Representation Method for custom object representation on console or log
        """
        return '<id {}>'.format(self.id)

    @classmethod
    def add(cls, user_id: int, account_id: str, amount: float, status: str, currency: str, reference_order_id: str, reference_payment_id: str, reference_subscription_id: str, payment_mode: str, response: dict, verified: Optional[str] = None, request: Optional[dict] = None):
        """add payment entry"""

        payment = cls()
        payment.user_id = user_id
        payment.account_id = account_id
        payment.amount = amount
        payment.verified = verified
        payment.status = status
        payment.currency = currency
        payment.reference_order_id = reference_order_id
        payment.reference_payment_id = reference_payment_id
        payment.reference_subscription_id = reference_subscription_id
        payment.payment_mode = payment_mode
        payment.request = request
        payment.response = response
        payment.created_at = int(time.time())

        payment.save()

        return payment

    @classmethod
    def get_by_reference_subscription_id(cls, reference_subscription_id: str, account_id=None, user_id=None):
        """get payment by reference_subscription_id"""
        query = db.session.query(cls).filter(
            cls.reference_subscription_id == reference_subscription_id)

        if account_id is not None:
            query = query.filter(cls.account_id == account_id)

        if user_id is not None:
            query = query.filter(cls.user_id == user_id)

        return query.order_by(desc(cls.updated_at)).first()

    @classmethod
    def get_by_reference_payment_id(cls, reference_payment_id: str):
        """get payment by reference_payment_id"""
        return db.session.query(cls).filter(cls.reference_payment_id == reference_payment_id).first()

    @classmethod
    def get_by_reference_order_id(cls, reference_order_id: str):
        """get payment by reference_order_id"""
        return db.session.query(cls).filter(cls.reference_order_id == reference_order_id).first()

    @classmethod
    def add_update(cls, user_id: int, account_id: str, amount: float, status: str, currency: str, reference_payment_id: str, payment_mode: str, response: dict):
        """method to add/update the payment table"""

        payment = db.session.query(cls).filter(cls.user_id == user_id, cls.account_id
                                               == account_id, cls.reference_payment_id == reference_payment_id).first()

        current_time = int(time.time())
        record = DbAnomalies.UPDATE.value

        if payment == None:
            record = DbAnomalies.INSERTION.value
            payment = cls(user_id=user_id, account_id=account_id,
                          reference_payment_id=reference_payment_id, created_at=current_time)

        payment.amount = amount
        payment.status = status
        payment.currency = currency
        payment.payment_mode = payment_mode
        payment.response = response
        payment.updated_at = current_time

        if record == DbAnomalies.INSERTION.value:
            payment.save()
        else:
            db.session.commit()

        return payment
