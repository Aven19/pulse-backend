"""
    Database model for storing Queue data in database is written in this File along with its methods.
"""
import time

from app import db
from app.models.base import Base


class QueueTask(Base):
    """
        Attachment model to store queue task
    """

    __tablename__ = 'queue_task'
    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    owner_id = db.Column(db.BigInteger, nullable=True)
    account_id = db.Column(db.String(36), nullable=True)
    queue_name = db.Column(db.Text, nullable=False)
    status = db.Column(db.Integer, nullable=False)
    input_attachment_id = db.Column(db.BigInteger, nullable=True)
    output_attachment_id = db.Column(db.BigInteger, nullable=True)
    created_at = db.Column(db.BigInteger, nullable=False)
    entity_type = db.Column(db.BigInteger, nullable=True)
    param = db.Column(db.Text, nullable=True)

    @classmethod
    def add_queue_task(cls, queue_name: str, status: str, entity_type: int, param: str,
                       input_attachment_id: None, output_attachment_id: None, account_id=None, owner_id=None,):
        """Create a new queue"""
        new_queue = cls(queue_name=queue_name, account_id=account_id, owner_id=owner_id, status=status, entity_type=entity_type, param=param,
                        input_attachment_id=input_attachment_id, output_attachment_id=output_attachment_id, created_at=int(time.time()))
        new_queue.save()
        return new_queue
