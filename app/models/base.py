"""Contains some basic definitions that can be extended by other models."""

from typing import Any
from typing import Optional
from uuid import uuid4

from app import db
from app import logger
from sqlalchemy import and_
from sqlalchemy import update


class Base(db.Model):
    """Base modal for all other modal that contains some basic methods that can be extended by other modals."""
    __abstract__ = True

    @classmethod
    def get_by_id(cls, id: Any) -> Any:
        """Filter record by id."""
        if isinstance(id, list):
            return db.session.query(cls).filter(cls.id.in_(id)).all()
        return db.session.query(cls).filter(cls.id == id).first()
        # return cls.query.filter_by(id=id).first()

    @classmethod
    def get_all(cls) -> list:
        """Return all records of table."""
        return db.session.query(cls).all()

    @classmethod
    def create_uuid(cls) -> Any:
        """creates a random uuid and checks if exists in the cls table: if yes the it recursively calls itself if not the returns the uuid."""
        uuid = str(uuid4())
        exists = db.session.query(cls).filter(cls.uuid == uuid).first()
        if exists:
            cls.create_uuid()
        else:
            return uuid

    @classmethod
    def get_def_exp_by_uuid(cls, uuid: str) -> Any:
        """Filter records by uuid, org id null and default true."""
        return db.session.query(cls).filter(
            and_(cls.uuid == uuid, cls.org_id == None, cls.is_default.isnot(False))).first()  # type: ignore  # noqa: FKA100

    @classmethod
    def get_null_uuid(cls) -> list:
        """Return records whose uuid is None."""
        return db.session.query(cls).filter(cls.uuid.is_(None)).all()

    @classmethod
    def get_by_uuid(cls, uuid: str) -> Any:
        """Filter record by uuid. """
        return db.session.query(cls).filter(cls.uuid == uuid).first()

    @classmethod
    def get_active_by_uuid(cls, uuid: str) -> Any:
        """Filter record by uuid. """
        return db.session.query(cls).filter(and_(cls.uuid == uuid, cls.deactivated_at.is_(None))).first()  # type: ignore  # noqa: FKA100

    @classmethod
    def get_by_slug(cls, slug: str) -> Any:
        """Filter record by slug."""
        return db.session.query(cls).filter(cls.slug == slug).first()

    def save(self) -> Any:
        """Save object in DB."""
        db.session.add(self)
        db.session.commit()
        return self

    @classmethod
    def store_brand_category_by_sku(cls, account_id: str, seller_sku: str, asp_id: Optional[str] = None, category: Optional[str] = None, brand: Optional[str] = None) -> Any:
        """Store brand category data for a particular sku"""
        logger.info('*' * 200)
        logger.info(f'Table Name: {cls.__table__.name}')
        logger.info(f'Seller Sku: {seller_sku}')
        logger.info(f'Category: {category}')
        logger.info(f'Brand: {brand}')

        sku_attributes = ['seller_sku', 'sku', 'msku', 'advertised_sku']

        for sku_attr in sku_attributes:
            if hasattr(cls, sku_attr):
                logger.info('Base Model line 119 -> %s', sku_attr)

                stmt = (
                    update(cls)
                    # .where(getattr(cls, sku_attr) == seller_sku)
                    .where(and_(getattr(cls, 'account_id') == account_id, getattr(cls, sku_attr) == seller_sku))   # type: ignore  # noqa: FKA100
                    .values(
                        brand=brand if brand is not None and hasattr(
                            cls, 'brand') else cls.brand,
                        category=category if category is not None and hasattr(
                            cls, 'category') else cls.category
                    )
                )
                db.session.execute(stmt)
                db.session.commit()
                return None

        return None

    @classmethod
    def store_brand_category_by_asin(cls, account_id: str, asin: str, asp_id: Optional[str] = None, category: Optional[str] = None, brand: Optional[str] = None) -> Any:
        """Store brand category data for a particular asin"""
        logger.info('*' * 200)
        logger.info(f'Table Name: {cls.__table__.name}')
        logger.info(f'Asin: {asin}')
        logger.info(f'Category: {category}')
        logger.info(f'Brand: {brand}')

        asin_attributes = ['asin', 'child_asin']

        for asin_attr in asin_attributes:
            if hasattr(cls, asin_attr):
                logger.info('Base Model line 119 -> %s', asin_attr)

                stmt = (
                    update(cls)
                    # .where(getattr(cls, asin_attr) == asin)
                    .where(and_(getattr(cls, 'account_id') == account_id, getattr(cls, asin_attr) == asin))   # type: ignore  # noqa: FKA100
                    .values(
                        brand=brand if brand is not None and hasattr(
                            cls, 'brand') else cls.brand,
                        category=category if category is not None and hasattr(
                            cls, 'category') else cls.category
                    )
                )
                db.session.execute(stmt)
                db.session.commit()
                return None

        return None
