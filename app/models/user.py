"""
    Database model for storing users in database is written in this File along with its methods.
"""
from collections.abc import Iterable
import time
from typing import Any
from typing import Optional

from app import db
from app.helpers.utility import hash_password
from app.models.base import Base
from app.models.user_account import UserAccount
from app.models.user_invite import UserInvite
from sqlalchemy import func
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy.orm import relationship


class User(Base):
    """
        User model to store users in database
    """
    __tablename__ = 'user'

    id = db.Column(db.BigInteger, primary_key=True)
    first_name = db.Column(db.String(100), nullable=True)
    last_name = db.Column(db.String(100), nullable=True)
    email = db.Column(db.String(255), unique=True, index=True)
    password = db.Column(db.String(255), nullable=True)
    google_auth = db.Column(db.JSON, nullable=True)
    amazon_auth = db.Column(db.JSON, nullable=True)
    last_login_at = db.Column(db.BigInteger, nullable=True)
    created_at = db.Column(db.BigInteger)
    updated_at = db.Column(db.BigInteger)
    deactivated_at = db.Column(db.BigInteger, nullable=True)

    accounts = relationship(
        'Account', secondary='user_account', back_populates='users')

    def __repr__(self) -> str:
        """
            Object Representation Method for custom object representation on console or log
        """
        return '<id {}>'.format(self.id)

    @classmethod
    def serialize(cls, queryset):
        """
        Serializer method, created to convert the Model Object to JSON data.
        """
        users_data = []
        account_dict = []

        if not isinstance(queryset, Iterable):
            queryset = [queryset]

        for user_obj in queryset:
            for account in user_obj.accounts:
                az_ads_profile_ids = account.az_ads_profile_ids if account.az_ads_profile_ids is not None else None

                account_details = {
                    'account_id': account.uuid,
                    'legal_name': account.legal_name,
                    'display_name': account.display_name,
                    'asp_marketplace': account.asp_marketplace,
                    'asp_id': account.asp_id,
                    'selling_partner_id': account.asp_id,
                    'asp_id_connected_at': account.asp_id_connected_at,
                    'az_ads_profile_id': az_ads_profile_ids,
                    'az_ads_account_name': next(
                        (get_profile['accountInfo']['name'] for get_profile in account.az_ads_account_info
                         if get_profile.get('profileId') == az_ads_profile_ids), None) if account.az_ads_account_info is not None else None,
                    'is_primary': user_obj.id == account.primary_user_id,
                    'profile_attachment_id': account.detail.get('file_attachment_id') if account.detail is not None else None
                }

                account_dict.append(account_details)

            user_details_obj = {
                'id': user_obj.id,
                'first_name': user_obj.first_name,
                'last_name': user_obj.last_name,
                'email_id': user_obj.email,
                'created_at': user_obj.created_at
            }

            users_data.append(user_details_obj)

        return users_data, account_dict

    @classmethod
    def get_by_email(cls, email: str) -> Any:
        """
            Class method to fetch/select the log records from the database table by given email_id.
        """
        return db.session.query(cls).filter(cls.email == email).first()

    @classmethod
    def add(cls, email: str, first_name=None, last_name=None, password=None, google_auth=None, amazon_auth=None):
        """Create new user"""

        created_at = int(time.time())

        new_user = cls(email=email, password=hash_password(password), first_name=first_name, last_name=last_name,
                       google_auth=google_auth, amazon_auth=amazon_auth, last_login_at=created_at, created_at=created_at, updated_at=created_at)

        db.session.add(new_user)
        db.session.commit()
        return new_user

    @classmethod
    def get_member_details(cls, account_id: str, page: int, size: int, q: Optional[str] = None, sort_key: Optional[str] = None, sort_order: Optional[str] = None):
        """method to get member details for member page"""

        query = db.session.query(                       # type: ignore  # noqa: FKA100
            cls.first_name,
            cls.last_name,
            cls.email,
            UserAccount.deactivated_at,
            UserInvite.invited_by_user_id,
            UserInvite.invited_by_account_id,
            UserAccount.brand
        ).outerjoin(                                            # type: ignore  # noqa: FKA100
            UserAccount, cls.id == UserAccount.user_id
        ).outerjoin(                                                # type: ignore  # noqa: FKA100
            UserInvite, (cls.email == UserInvite.email) & (
                UserAccount.account_id == UserInvite.invited_by_account_id)
        ).filter(
            UserAccount.account_id == account_id
        )

        if q:
            query = query.filter(or_(                           # type: ignore  # noqa: FKA100
                cls.first_name.ilike(f'%{q}%'),
                cls.last_name.ilike(f'%{q}%'),
                cls.email.ilike(f'%{q}%')
            ))

        if sort_key and sort_order:

            if sort_key.lower() == 'brand':
                sort_column = UserInvite.brand
            else:
                sort_column = getattr(cls, sort_key)

            if sort_order.lower() == 'asc':
                query = query.order_by(sort_column)
            else:
                query = query.order_by(sort_column.desc())

        # Create a subquery to count the rows with applied filters

        total_count_query = select(func.count()).select_from(query)

        total_count_result = db.session.execute(total_count_query).scalar()

        if page and size:
            page = int(page) - 1
            size = int(size)
            query = query.limit(size).offset(page * size)

        members = query.all()

        return members, total_count_result
