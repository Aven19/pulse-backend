"""
    Database model for storing item master data in database is written in this File along with its methods.
"""
import time

from app import db
from app.models.base import Base


class Attachment(Base):
    """
        Attachment model to store attachment data
    """
    __tablename__ = 'attachment'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    entity_type = db.Column(db.BigInteger, nullable=False)
    sub_entity_type = db.Column(db.BigInteger, nullable=True)
    entity_id = db.Column(db.BigInteger, nullable=True)
    sub_entity_id = db.Column(db.BigInteger, nullable=True)
    name = db.Column(db.String(100), nullable=False)
    path = db.Column(db.Text, nullable=False)
    size = db.Column(db.Text, nullable=False)
    description = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.BigInteger, nullable=False)
    updated_at = db.Column(db.BigInteger, nullable=False)

    @classmethod
    def add(cls, entity_type: int, sub_entity_type: None, entity_id: None, sub_entity_id: None, name: str, path: str, size: str, description: str) -> 'Attachment':
        """Create a new attachment"""
        attachment = cls(
            entity_type=entity_type,
            sub_entity_type=sub_entity_type,
            entity_id=entity_id,
            sub_entity_id=sub_entity_id,
            name=name,
            path=path,
            size=size,
            description=description,
            created_at=int(time.time()),
            updated_at=int(time.time())
        )

        db.session.add(attachment)
        db.session.commit()

        return attachment
