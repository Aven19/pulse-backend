"""
    Database model for storing plans details in database is written in this File along with its methods.
"""
import time

from app import db
from app.helpers.constants import DbAnomalies
from app.models.base import Base


class Plan(Base):
    """
        Plan model to store different plans and details in database
    """
    __tablename__ = 'plan'

    id = db.Column(db.BigInteger, primary_key=True)
    reference_plan_id = db.Column(db.String(255), nullable=True, unique=True)
    name = db.Column(db.String(255), nullable=False)
    period = db.Column(db.String(255))
    status = db.Column(db.String(50), nullable=False)
    amount = db.Column(db.Numeric)
    currency = db.Column(db.String(20))
    discount = db.Column(db.Numeric)
    feature = db.Column(db.JSON)
    description = db.Column(db.Text)
    created_at = db.Column(db.BigInteger)
    updated_at = db.Column(db.BigInteger)

    def __repr__(self) -> str:
        """
            Object Representation Method for custom object representation on console or log
        """
        return '<id {}>'.format(self.id)

    @classmethod
    def add_update(cls, name: str, period: str, status: str, amount: float, currency: str, feature: dict, reference_plan_id=None, description=None, discount=None):
        """method to add data in plan table"""

        plan = db.session.query(cls).filter(
            cls.reference_plan_id == reference_plan_id).first()

        current_time = int(time.time())
        record = DbAnomalies.UPDATE.value

        if plan == None:
            record = DbAnomalies.INSERTION.value
            plan = cls(reference_plan_id=reference_plan_id,
                       created_at=current_time)

        plan.name = name
        plan.period = period
        plan.status = status
        plan.amount = amount
        plan.currency = currency
        plan.feature = feature
        plan.description = description
        plan.discount = discount
        plan.updated_at = current_time

        if record == DbAnomalies.INSERTION.value:
            plan.save()
        else:
            db.session.commit()

        return plan

    @classmethod
    def get_all_plans(cls):
        """method to get all the plans from Plans table"""

        return db.session.query(cls).order_by(cls.id).all()

    @classmethod
    def get_by_reference_plan_id(cls, reference_plan_id: str):
        """method to get plan by plan_id"""

        return db.session.query(cls).filter(cls.reference_plan_id == reference_plan_id).first()
