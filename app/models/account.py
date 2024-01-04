"""
    Database model for storing Account in database is written in this File along with its methods.
"""
from collections.abc import Iterable
import time
from typing import Any
from typing import List

from app import config_data
from app import db
from app import logger
from app.helpers.utility import generate_uuid
from app.models.base import Base
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import relationship


class Account(Base):
    """
        Account model to store account in database
    """
    __tablename__ = 'account'

    id = db.Column(db.BigInteger, primary_key=True)
    uuid = db.Column(db.String(36), nullable=False, unique=True, index=True)
    legal_name = db.Column(db.String(100), nullable=True)
    display_name = db.Column(db.String(100), nullable=True)
    detail = db.Column(db.JSON, nullable=True)
    primary_user_id = db.Column(db.BigInteger, nullable=True)
    asp_id = db.Column(db.String(100), nullable=True)
    asp_marketplace = db.Column(db.String(100), nullable=True)
    asp_marketplace_id = db.Column(ARRAY(db.String), default=[])
    asp_id_connected_at = db.Column(db.BigInteger, nullable=True)
    asp_sync_started_at = db.Column(db.BigInteger, nullable=True)
    asp_sync_completed_at = db.Column(db.BigInteger, nullable=True)
    asp_credentials = db.Column(db.JSON)
    fsp_id = db.Column(db.String(100), nullable=True)
    fsp_credentials = db.Column(db.JSON)
    ssp_id = db.Column(db.String(100), nullable=True)
    ssp_credentials = db.Column(db.JSON)
    az_ads_profile_ids = db.Column(ARRAY(db.String), nullable=True, default=[])
    az_ads_account_info = db.Column(db.JSON)
    az_ads_credentials = db.Column(db.JSON)
    created_at = db.Column(db.BigInteger)
    updated_at = db.Column(db.BigInteger)
    deactivated_at = db.Column(db.BigInteger, nullable=True)

    users = relationship('User', secondary='user_account',
                         back_populates='accounts')

    def __repr__(self) -> str:
        """
            Object Representation Method for custom object representation on console or log
        """
        return '<id {}>'.format(self.id)

    @classmethod
    def retrieve_asp_credentials(cls, queryset):
        """
        Retrieve ASP credentials from a queryset and return them as a list of dictionaries.

        Args:
            queryset: A queryset containing the account objects.

        Returns:
            List of ASP credentials as dictionaries.

        """

        credentials = []

        if not isinstance(queryset, Iterable):
            queryset = [queryset]

        for account_obj in queryset:
            logger.info('*' * 200)
            logger.info(
                'Inside Model logging Amazon seller partner credentials')
            logger.info('Line 79: Seller Partner Id: {asp_id}'.format(
                asp_id=account_obj.asp_id))
            logger.info('Line 80: Credentials: \n{credentials}'.format(
                credentials=account_obj.asp_credentials))

            cred = {
                'seller_partner_id': account_obj.asp_id,
                'refresh_token': account_obj.asp_credentials.get('refresh_token'),
                'client_id': config_data.get('SP_LWA_APP_ID'),
                'client_secret': config_data.get('SP_LWA_CLIENT_SECRET')
            }
            logger.info(
                'Line 88: Cred: \n{credentials}'.format(credentials=cred))

            credentials.append(cred)

        return credentials

    @classmethod
    def retrieve_az_ads_credentials(cls, queryset: Any, az_ads_profile_id: int):
        """
        Retrieve Amzon ads credentials from a queryset and return them as a list of dictionaries.

        Args:
            queryset: A queryset containing the account objects.

        Returns:
            List of Amzon ads credentials as dictionaries.

        """

        credentials = []

        if not isinstance(queryset, Iterable):
            queryset = [queryset]

        for account_obj in queryset:
            cred_array = account_obj.az_ads_credentials

            for cred in cred_array:
                if az_ads_profile_id == cred.get('az_ads_profile_id'):
                    _cred = {
                        'az_ads_profile_id': cred.get('az_ads_profile_id'),
                        'refresh_token': cred.get('refresh_token'),
                        'client_id': config_data.get('AZ_AD_CLIENT_ID'),
                        'client_secret': config_data.get(
                            'AZ_AD_CLIENT_SECRET')
                    }

            credentials.append(_cred)

        return credentials

    @classmethod
    def get_by_user_id(cls, primary_user_id: int) -> Any:
        """Filter record by seller_partner_id"""
        return db.session.query(cls).filter(cls.primary_user_id == primary_user_id).first()

    @classmethod
    def get_all_az_ads_profile_ids(cls) -> List[str]:
        """Retrieve all az_ads_profile_ids"""
        results = db.session.query(cls.az_ads_profile_ids).filter(
            cls.az_ads_profile_ids.isnot(None)).all()

        ids = [item for result in results for item in (result[0] or [])]

        return ids

    @classmethod
    def get_by_asp_id(cls, asp_id: str) -> Any:
        """Filter record by seller_partner_id"""
        return db.session.query(cls).filter(cls.asp_id == asp_id).first()

    @classmethod
    def get_by_amazon_seller_partner_id(cls, primary_user_id: int, seller_partner_id: str) -> Any:
        """Filter record by seller_partner_id"""
        return db.session.query(cls).filter(
            cls.primary_user_id == primary_user_id,
            cls.asp_id == seller_partner_id
        ).first()

    @classmethod
    def add(cls, legal_name=None, display_name=None, primary_user_id=None,
            asp_id=None, asp_credentials=None, fsp_id=None, fsp_credentials=None,
            ssp_id=None, ssp_credentials=None) -> Any:
        """Add a new account"""
        created_at = int(time.time())

        account = cls(uuid=generate_uuid(), primary_user_id=primary_user_id, legal_name=legal_name, display_name=display_name,
                      asp_id=asp_id, asp_credentials=asp_credentials, fsp_id=fsp_id, fsp_credentials=fsp_credentials,
                      ssp_id=ssp_id, ssp_credentials=ssp_credentials, created_at=created_at, updated_at=created_at)

        db.session.add(account)
        db.session.commit()

        return account

    @classmethod
    def update(cls, primary_user_id=None, legal_name=None, display_name=None,
               asp_id=None, asp_credentials=None, fsp_credentials=None,
               ssp_credentials=None) -> Any:
        """Update account"""

        account = db.session.query(cls).filter(
            cls.primary_user_id == primary_user_id).first()

        if account:

            if legal_name:
                account.legal_name = legal_name

            if display_name:
                account.display_name = display_name

            if asp_id and asp_credentials:
                account.asp_credentials = asp_credentials

            if fsp_credentials:
                account.fsp_credentials = fsp_credentials

            if ssp_credentials:
                account.ssp_credentials = ssp_credentials

            account.updated_at = int(time.time())

            db.session.commit()

        return account

    @classmethod
    def update_az_ads_info(cls, account_id=None, az_ads_credentials=None, az_ads_account_info=None) -> Any:
        """Update Ad's credentials info"""

        account = db.session.query(cls).filter(
            cls.uuid == account_id).first()

        if account:

            if az_ads_credentials:
                account.az_ads_credentials = None
                db.session.commit()
                account.az_ads_credentials = az_ads_credentials

            if az_ads_account_info:
                account.az_ads_account_info = az_ads_account_info

            account.updated_at = int(time.time())

            db.session.commit()

        return account

    @classmethod
    def is_primary_user(cls, user_id: int, account_id: str) -> Any:
        """check if user is a primary user of the account by account uuid"""
        return db.session.query(cls).filter(cls.uuid == account_id, cls.primary_user_id == user_id).first()

    @classmethod
    def is_account_and_primary_user(cls, user_id: int):
        """check if a user has an account in which they are primary user"""
        return db.session.query(cls).filter(cls.primary_user_id == user_id).first()

    @classmethod
    def get_all_az_ads_account_info(cls) -> Any:
        """Retrieve all az_ads_profile_ids"""
        results = db.session.query(cls.primary_user_id, cls.uuid, cls.az_ads_account_info).filter(cls.az_ads_account_info.isnot(None)).all()  # type: ignore  # noqa: FKA100

        return results
