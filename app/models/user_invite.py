"""
    Database model for storing user invite data in database is written in this File along with its methods.
"""
import time
from typing import Any
from typing import Optional

from app import db
from app.helpers.constants import UserInviteStatus
from app.models.base import Base
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.postgresql import ARRAY


class UserInvite(Base):
    """
        User invite model to store invite details in database
    """
    __tablename__ = 'user_invite'

    id = db.Column(db.BigInteger, primary_key=True)
    uuid = db.Column(db.String(36), nullable=False, unique=True)
    first_name = db.Column(db.String(255), nullable=False)
    last_name = db.Column(db.String(255), nullable=True)
    email = db.Column(db.String(255), nullable=False)
    status = db.Column(db.String(20), nullable=False)
    invited_by_user_id = db.Column(db.Integer, nullable=False)
    invited_by_account_id = db.Column(db.Integer, nullable=False)
    brand = db.Column(ARRAY(db.String), nullable=False, default=[])  # type: ignore  # noqa: FKA100
    category = db.Column(ARRAY(db.String), nullable=False, default=[])  # type: ignore  # noqa: FKA100
    created_at = db.Column(db.BigInteger)
    last_sent_at = db.Column(db.BigInteger)
    updated_at = db.Column(db.BigInteger)
    __table_args__ = (UniqueConstraint('invited_by_account_id',                     # type: ignore  # noqa: FKA100
                      'email', name='uq_invited_by_account_id_email'),)

    def __repr__(self) -> str:
        """
            Object Representation Method for custom object representation on console or log
        """
        return '<id {}>'.format(self.id)

    @classmethod
    def add(cls, uuid: str, first_name: str, email: str, invited_by_user_id: int, invited_by_account_id: int, brand=list, category=list, last_name: Optional[str] = None):
        """add invite details to user_invite table"""

        user_invite = cls()
        user_invite.uuid = uuid
        user_invite.first_name = first_name
        user_invite.last_name = last_name
        user_invite.email = email
        user_invite.status = UserInviteStatus.PENDING.value
        user_invite.invited_by_user_id = invited_by_user_id
        user_invite.invited_by_account_id = invited_by_account_id
        user_invite.brand = brand
        user_invite.category = category
        user_invite.created_at = int(time.time())
        user_invite.last_sent_at = int(time.time())
        user_invite.save()

        return user_invite

    @classmethod
    def get_user_invite(cls, email: str, invited_by_user_id: int, invited_by_account_id) -> Any:
        """
            Class method to fetch/select the log records from the database table by given email_id, user_id and account_id.
        """
        return db.session.query(cls).filter(cls.email == email, cls.invited_by_user_id == invited_by_user_id, cls.invited_by_account_id == invited_by_account_id).first()

    @classmethod
    def get_by_email(cls, email: str) -> Any:
        """
            Class method to fetch/select the log records from the database table by given email_id.
        """
        return db.session.query(cls).filter(cls.email == email).first()

    @classmethod
    def update_status(cls, uuid: str, status: str):
        """method to update status of invitation"""

        invited_user = cls.get_by_uuid(uuid=uuid)

        if invited_user:

            invited_user.status = status
            invited_user.updated_at = int(time.time())

            db.session.commit()

        return invited_user

    @classmethod
    def update_invite(cls, uuid: str, first_name: str, brand: list, category: list, last_name: Optional[str]):
        """method to update last_sent_at"""

        invited_user = db.session.query(cls).filter(cls.uuid == uuid).first()

        current_time = int(time.time())
        if invited_user:
            invited_user.first_name = first_name
            invited_user.last_name = last_name
            invited_user.brand = brand
            invited_user.category = category
            invited_user.last_sent_at = current_time
            invited_user.updated_at = current_time

            db.session.commit()

        return invited_user
