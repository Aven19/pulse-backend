"""
    Database model for storing Report Status in database is written in this File along with its methods.
"""
from datetime import datetime
from datetime import timedelta
import time
from typing import Any
from typing import Optional

from app import db
from app.helpers.constants import ASpReportProcessingStatus
from app.models.base import Base
from sqlalchemy import exists
from sqlalchemy import func


class AzReport(Base):
    """
        Report model to store reports in database
    """
    __tablename__ = 'az_report'

    id = db.Column(db.BigInteger, primary_key=True)
    account_id = db.Column(db.String(36), nullable=False)
    asp_id = db.Column(db.String(255))
    az_ads_profile_id = db.Column(db.String(255))
    seller_partner_id = db.Column(db.String(255))
    reference_id = db.Column(db.String(255))
    type = db.Column(db.String(255))
    status = db.Column(db.String(255))
    status_updated_at = db.Column(db.BigInteger, nullable=True)
    request_start_time = db.Column(db.DateTime)
    request_end_time = db.Column(db.DateTime)
    document_id = db.Column(db.String(255), nullable=True)
    document_id_updated_at = db.Column(db.BigInteger, nullable=True)
    queue_id = db.Column(db.BigInteger, nullable=True)
    created_at = db.Column(db.BigInteger)
    updated_at = db.Column(db.BigInteger)
    deactivated_at = db.Column(db.BigInteger, nullable=True)

    @classmethod
    def get_by_ref_id(cls, account_id: str, reference_id: int) -> Any:
        """Filter record by report id"""
        return db.session.query(cls).filter(cls.account_id == account_id, cls.reference_id == reference_id).first()

    @classmethod
    def get_by_report_status(cls, status: str) -> Any:
        """Filter record by report status"""
        return db.session.query(cls).filter(cls.status == status).all()

    @classmethod
    def get_processed_reports(cls, type: str) -> Any:
        """Filter record by report status pending, failed or fatal"""
        return db.session.query(cls).filter(cls.type == type, cls.status == ASpReportProcessingStatus.DONE.value).all()

    @classmethod
    def get_last_report(cls, account_id: str, type: str) -> Any:
        """Get the report with the maximum request_end_time for a specific seller and account_id"""
        subquery = db.session.query(func.max(cls.request_end_time).label('max_end_time')).filter(
            cls.account_id == account_id, cls.type == type, cls.status == ASpReportProcessingStatus.COMPLETED.value
        ).subquery()

        return db.session.query(cls).filter(
            cls.request_end_time == subquery.c.max_end_time,
            cls.account_id == account_id,
            cls.type == type,
            cls.status == ASpReportProcessingStatus.COMPLETED.value
        ).first()

    @classmethod
    def get_pending_reports(cls, account_id: Optional[str] = None, seller_partner_id: Optional[str] = None, type: Optional[str] = None) -> Any:
        """Get all the pending reports for a specific seller, account_id, and optional report type"""
        query = db.session.query(cls).filter(
            (cls.queue_id.is_(None))
            & (
                (cls.status == ASpReportProcessingStatus.DONE.value)
                | (cls.status == ASpReportProcessingStatus.NEW.value)
                | (cls.status == ASpReportProcessingStatus.IN_QUEUE.value)
                | (cls.status == ASpReportProcessingStatus.IN_PROGRESS.value)
                | (cls.status.is_(None))
            )
        )

        if account_id:
            query = query.filter(cls.account_id == account_id)

        if seller_partner_id:
            query = query.filter(cls.seller_partner_id == seller_partner_id)

        if type:
            query = query.filter(cls.type == type)

        return query.all()

    @classmethod
    def get_last_ads_report(cls, account_id: str, type: str, az_ads_profile_id: str) -> Any:
        """Get all the pending reports for a specific seller and account_id"""
        return db.session.query(cls).filter(cls.account_id == account_id, cls.type == type, cls.az_ads_profile_id == az_ads_profile_id, cls.status == ASpReportProcessingStatus.COMPLETED.value).order_by(cls.created_at.desc()).first()

    @classmethod
    def get_pending_ads_reports(cls, account_id: str, seller_partner_id: str, az_ads_profile_id: str) -> Any:
        """Get all the pending sponsored ads reports for a specific seller and account_id"""
        return db.session.query(cls).filter(
            (cls.account_id == account_id)
            & (cls.seller_partner_id == seller_partner_id)
            & (cls.az_ads_profile_id == az_ads_profile_id)
            & (cls.queue_id.is_(None))
            & (
                (cls.status == ASpReportProcessingStatus.SUCCESS.value)
                | (cls.status == ASpReportProcessingStatus.IN_PROGRESS.value)
                | (cls.status == ASpReportProcessingStatus.PROCESSING.value)
                | (cls.status == ASpReportProcessingStatus.IN_QUEUE.value)
                | (cls.status == ASpReportProcessingStatus.PENDING.value)
                | (cls.status.is_(None))
            )
        ).all()

    @classmethod
    def add(cls, account_id: str, seller_partner_id: str, type: str, reference_id: Any = None, request_start_time: Any = None, request_end_time: Any = None, status: Any = None, document_id: Any = None, queue_id: Any = None, az_ads_profile_id: Any = None) -> str:
        """Create reports status fetched by Amazon"""
        new_report = cls(account_id=account_id, type=type, reference_id=reference_id,
                         created_at=int(time.time()))

        if seller_partner_id is not None:
            new_report.seller_partner_id = seller_partner_id

        if request_start_time is not None:
            new_report.request_start_time = request_start_time

        if request_end_time is not None:
            new_report.request_end_time = request_end_time

        if status is not None:
            new_report.status = status

        if document_id is not None:
            new_report.document_id = document_id

        if queue_id is not None:
            new_report.queue_id = queue_id

        if az_ads_profile_id is not None:
            new_report.az_ads_profile_id = az_ads_profile_id

        db.session.add(new_report)
        db.session.commit()

        return new_report

    @classmethod
    def update_status(cls, reference_id: int, status: str, document_id: Any = None) -> Any:
        """Update report status by report ID"""
        report = db.session.query(cls).filter(
            cls.reference_id == reference_id).first()

        if report:
            report.status = status
            report.status_updated_at = int(time.time())

            if document_id is not None:
                report.document_id = document_id
                report.document_id_updated_at = int(time.time())
            db.session.commit()

        return report

    @classmethod
    def update_by_id(cls, id: int, status, document_id=None) -> Any:
        """Update report status by report ID"""
        report = db.session.query(cls).filter(
            cls.id == id).first()

        if report:
            report.status = status
            report.status_updated_at = int(time.time())

            if document_id is not None:
                report.document_id = document_id
                report.document_id_updated_at = int(time.time())
            db.session.commit()

        return report

    @classmethod
    def get_pending_sales_progress(cls, type: str) -> Any:
        """Filter record by report status pending, failed or fatal"""
        # return db.session.query(cls).filter(cls.account_id == account_id, cls.type == type, cls.status == status).all()
        return db.session.query(cls).filter(
            # (cls.account_id == account_id)
            # & (cls.seller_partner_id == asp_id) &
            (cls.type == type)
            # & (cls.queue_id.is_(None))
            & (
                (cls.status == ASpReportProcessingStatus.SUCCESS.value)
                | (cls.status == ASpReportProcessingStatus.NEW.value)
                | (cls.status == ASpReportProcessingStatus.IN_PROGRESS.value)
                | (cls.status == ASpReportProcessingStatus.PROCESSING.value)
                | (cls.status == ASpReportProcessingStatus.IN_QUEUE.value)
                | (cls.status == ASpReportProcessingStatus.PENDING.value)
                | (cls.status.is_(None))
            )
        ).all()

    @classmethod
    def check_last_report(cls, account_id: str, asp_id: str, type: str) -> Any:
        """Check if any row has non-None values in the hourly_sales column"""
        return db.session.query(exists().where(cls.account_id == account_id, cls.seller_partner_id == asp_id, cls.type == type)).scalar()  # type: ignore  # noqa: FKA100

    @classmethod
    def cancel_old_reports(cls, account_id: Optional[str] = None, asp_id: Optional[str] = None, max_age_minutes: Optional[int] = 45, report_type: Optional[str] = None):
        """Cancel reports created more than specified minutes ago and matching the specified type"""

        max_age_timestamp = int(
            (datetime.now() - timedelta(minutes=max_age_minutes)).timestamp())

        query = db.session.query(cls).filter(
            cls.created_at < max_age_timestamp,
            (
                (cls.status == ASpReportProcessingStatus.NEW.value)
                | (cls.status == ASpReportProcessingStatus.IN_PROGRESS.value)
                | (cls.status == ASpReportProcessingStatus.IN_QUEUE.value)
                | (cls.status == ASpReportProcessingStatus.PENDING.value)
                | (cls.status.is_(None))
            )
        )

        if account_id:
            query = query.filter(cls.account_id == account_id)

        if asp_id:
            query = query.filter(cls.seller_partner_id == asp_id)

        if report_type:
            query = query.filter(cls.type == report_type)

        old_reports = query.all()

        for report in old_reports:
            report.status = ASpReportProcessingStatus.CANCELLED.value

        db.session.commit()

        return old_reports
