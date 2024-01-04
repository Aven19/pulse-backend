"""
    Database model for storing user account in database is written in this File along with its methods.
"""
import time
from typing import Any
from typing import Optional

from app import db
from app.models.base import Base
from sqlalchemy import ForeignKey
from sqlalchemy import Index
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import backref
from sqlalchemy.orm import relationship


class UserAccount(Base):
    """
        User account model to store users in database
    """
    __tablename__ = 'user_account'

    id = db.Column(db.BigInteger, primary_key=True)
    user_id = db.Column(db.BigInteger, ForeignKey('user.id'))  # type: ignore  # noqa: FKA100
    account_id = db.Column(db.BigInteger, ForeignKey('account.id'))  # type: ignore  # noqa: FKA100
    brand = db.Column(ARRAY(db.String), nullable=False, default=[])  # type: ignore  # noqa: FKA100
    category = db.Column(ARRAY(db.String), nullable=False, default=[])  # type: ignore  # noqa: FKA100
    created_at = db.Column(db.BigInteger)
    updated_at = db.Column(db.BigInteger)
    deactivated_at = db.Column(db.BigInteger, nullable=True)
    # user = relationship('User', backref='user_accounts')
    # account = relationship('Account', backref='account_users')
    user = relationship('User', backref=backref(
        'user_account', cascade='all, delete-orphan'))
    account = relationship('Account', backref=backref(
        'user_account', cascade='all, delete-orphan'))

    __table_args__ = (                                          # type: ignore  # noqa: FKA100
        Index('ix_user_id_account_id', 'user_id', 'account_id'),)   # type: ignore  # noqa: FKA100

    def __repr__(self) -> str:
        """
            Object Representation Method for custom object representation on console or log
        """
        return '<id {}>'.format(self.id)

    @classmethod
    def add(cls, user_id: int, account_id: int, brand: Optional[list] = None, category: Optional[list] = None) -> Any:
        """Map user with account"""

        user_account = db.session.query(cls).filter(
            cls.user_id == user_id, cls.account_id == account_id).first()

        if not user_account:
            created_at = int(time.time())
            new_user_account = cls(
                user_id=user_id, account_id=account_id, brand=brand, category=category, created_at=created_at, updated_at=created_at)
            db.session.add(new_user_account)
            db.session.commit()

        return user_account

    @classmethod
    def update(cls, user_id: int, account_id: int, brand: Optional[list] = None, category: Optional[list] = None) -> Any:
        """Update User account details"""

        user_account = db.session.query(cls).filter(
            cls.user_id == user_id, cls.account_id == account_id).first()

        if user_account:
            user_account.updated_at = int(time.time())
            user_account.category = category
            user_account.brand = brand
            db.session.commit()

        return user_account

    @classmethod
    def is_user_account_exists(cls, user_id: int, account_id: int):
        """check if user-account mapping already exists"""

        return db.session.query(cls).filter(cls.user_id == user_id, cls.account_id == account_id).first()

    @classmethod
    def get_brand(cls, user_id: int, account_id: int):
        """
        Retrieve the brand for a specific user and account.

        :param user_id: The user's ID.
        :param account_id: The account's ID.
        :return: The brand associated with the user and account, or None if not found.
        """
        result = db.session.query(cls.brand).filter(
            cls.user_id == user_id, cls.account_id == account_id).first()
        return result.brand if result is not None else None

    @classmethod
    def is_active(cls, user_id: int):
        """check if user is active in atleast one account"""

        return db.session.query(cls.user_id, cls.account_id).filter(cls.user_id == user_id, cls.deactivated_at == None).first()     # type: ignore  # noqa: FKA100
