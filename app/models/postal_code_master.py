"""Contains Postal code table definitions."""
from __future__ import annotations

import time
from typing import Any

from app import db
from app.models.base import Base


class PostalCodeMaster(Base):
    """Stores details related to Pincode, State Name, District, Zone."""
    __tablename__ = 'postal_code_master'
    id = db.Column(db.BigInteger, primary_key=True)
    pincode = db.Column(db.BigInteger)
    district = db.Column(db.String)
    state_name = db.Column(db.String)
    zone = db.Column(db.String)
    created_at = db.Column(db.BigInteger, nullable=False,
                           default=int(time.time()))
    updated_at = db.Column(db.BigInteger, nullable=True)
    deactivated_at = db.Column(db.BigInteger, nullable=True)

    @classmethod
    def get_all_pincode(cls) -> Any:
        """Get all Pincode"""
        return db.session.query(cls.pincode).all()
