"""
    Database model for storing user subscription details in database is written in this File along with its methods.
"""
import time
from typing import Any

from app import db
from app import logger
from app.helpers.constants import DbAnomalies
from app.helpers.constants import SubscriptionStates
from app.models.base import Base


class Subscription(Base):
    """
        Subscription model to store different plans and details in database
    """
    __tablename__ = 'subscription'

    id = db.Column(db.BigInteger, primary_key=True)
    # account_id = db.Column(db.BigInteger, nullable=False)
    account_id = db.Column(db.String(36), nullable=False, index=True)
    reference_subscription_id = db.Column(db.String(50), nullable=False)
    plan_id = db.Column(db.BigInteger, nullable=False)
    payment_id = db.Column(db.BigInteger)
    status = db.Column(db.String(50), nullable=False)
    start_date = db.Column(db.DateTime, nullable=False)
    end_date = db.Column(db.DateTime, nullable=False)
    created_at = db.Column(db.BigInteger)
    updated_at = db.Column(db.BigInteger)
    deactivated_at = db.Column(db.BigInteger)

    def __repr__(self) -> str:
        """
            Object Representation Method for custom object representation on console or log
        """
        return '<id {}>'.format(self.id)

    @classmethod
    def add(cls, account_id: str, reference_subscription_id: str, plan_id: int, payment_id: int, status: str, start_date: Any, end_date: Any):
        """method to add subscription entry"""

        logger.info('Inside add subscription method')
        logger.info(f'Start Date: {start_date}')
        logger.info(f'End Date: {end_date}')
        subscription = cls()
        subscription.account_id = account_id
        subscription.reference_subscription_id = reference_subscription_id
        subscription.plan_id = plan_id
        subscription.payment_id = payment_id
        subscription.status = status
        subscription.start_date = start_date
        subscription.end_date = end_date
        subscription.created_at = int(time.time())

        subscription.save()

        return subscription

    @classmethod
    def add_update(cls, account_id: str, reference_subscription_id: str, plan_id: int, payment_id: int, status: str, start_date: Any, end_date: Any):
        """method to add/update the payment table"""

        logger.info('Inside add update subscription method')
        logger.info(f'Start Date: {start_date}')
        logger.info(f'End Date: {end_date}')

        subscription = db.session.query(cls).filter(
            cls.account_id == account_id).first()

        current_time = int(time.time())
        record = DbAnomalies.UPDATE.value

        if subscription == None:
            record = DbAnomalies.INSERTION.value
            subscription = cls(account_id=account_id, created_at=current_time)

        subscription.reference_subscription_id = reference_subscription_id
        subscription.plan_id = plan_id
        subscription.payment_id = payment_id
        subscription.status = status
        subscription.start_date = start_date
        subscription.end_date = end_date
        subscription.updated_at = current_time

        if record == DbAnomalies.INSERTION.value:
            subscription.save()
        else:
            db.session.commit()

        return subscription

    @classmethod
    def get_by_reference_subscription_id(cls, account_id: str, reference_subscription_id: str):
        """return susbcription by reference subscription id"""

        return db.session.query(cls).filter(cls.account_id == account_id, cls.reference_subscription_id == reference_subscription_id).first()

    @classmethod
    def get_status_by_account_id(cls, account_id: str):
        """return susbcription by reference subscription id"""

        subscription = db.session.query(cls).filter(
            cls.account_id == account_id, cls.status == SubscriptionStates.ACTIVE.value).first()

        current_time = int(time.time())

        if subscription:

            end_datetime = subscription.end_date

            if current_time <= int(end_datetime.timestamp()):
                return True

        return False

    @classmethod
    def get_by_account_id(cls, account_id: str):
        """return susbcription by reference subscription id"""

        subscription = db.session.query(cls).filter(
            cls.account_id == account_id, cls.status == SubscriptionStates.ACTIVE.value).first()

        current_time = int(time.time())

        if subscription:

            end_datetime = subscription.end_date

            if current_time <= int(end_datetime.timestamp()):
                return subscription

        return None

    @classmethod
    def set_status_inactive(cls, account_id: str):
        """set status as inactive for all active subscriptions of an account_id"""
        logger.info('**** set status inactive *******')
        db.session.query(cls).filter(cls.account_id == account_id, cls.status == SubscriptionStates.ACTIVE.value).update(
            {'status': SubscriptionStates.INACTIVE.value}, synchronize_session=False)
