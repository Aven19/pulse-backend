"""
    Database model for storing Fba Returns Report in database is written in this File along with its methods.
"""
import time

from app import db
from app.helpers.constants import DbAnomalies
from app.models.az_item_master import AzItemMaster
from app.models.base import Base


class AzFbaReturns(Base):
    """
        Fba Returns Report model to store returns report in database
    """
    __tablename__ = 'az_fba_returns'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    account_id = db.Column(db.String(36), nullable=False)
    asp_id = db.Column(db.String(255))
    category = db.Column(db.String(250), nullable=True)
    brand = db.Column(db.String(250), nullable=True)
    return_date = db.Column(db.String(255))
    order_id = db.Column(db.String(255))
    sku = db.Column(db.String(255))
    asin = db.Column(db.String(255))
    fnsku = db.Column(db.String(255))
    product_name = db.Column(db.String(255))
    quantity = db.Column(db.Integer)
    fulfillment_center_id = db.Column(db.String(255))
    detailed_disposition = db.Column(db.String(255))
    reason = db.Column(db.String(255))
    license_plate_number = db.Column(db.String(255))
    customer_comments = db.Column(db.Text)
    created_at = db.Column(db.BigInteger)
    updated_at = db.Column(db.BigInteger)

    @classmethod
    def get_by_order_id(cls, order_id: str):
        """Filter records by Order Id."""
        return db.session.query(cls).filter(cls.order_id == order_id).first()

    @classmethod
    def add_or_update(cls, account_id: str, asp_id: str, return_date: str, order_id: str, sku: str, asin: str, fnsku: str,
                      product_name: str, quantity: int, fulfillment_center_id: str, detailed_disposition: str, reason: str, license_plate_number: str, customer_comments: str):
        """Insert or update a FBA Return report"""

        category, brand = None, None

        if sku:
            category, brand = AzItemMaster.get_category_brand_by_sku(
                account_id=account_id, asp_id=asp_id, seller_sku=sku)

        fba_returns_report = db.session.query(cls).filter(cls.account_id == account_id, cls.asp_id == asp_id, cls.return_date == return_date,
                                                          cls.order_id == order_id, cls.sku == sku, cls.detailed_disposition == detailed_disposition, cls.license_plate_number == license_plate_number).first()
        current_time = int(time.time())
        record = DbAnomalies.UPDATE.value

        if fba_returns_report == None:
            record = DbAnomalies.INSERTION.value
            fba_returns_report = cls(account_id=account_id, asp_id=asp_id,
                                     return_date=return_date, order_id=order_id, sku=sku, detailed_disposition=detailed_disposition,
                                     license_plate_number=license_plate_number,
                                     created_at=current_time)

        fba_returns_report.category = category
        fba_returns_report.brand = brand
        fba_returns_report.asin = asin
        fba_returns_report.fnsku = fnsku
        fba_returns_report.product_name = product_name
        fba_returns_report.quantity = quantity
        fba_returns_report.fulfillment_center_id = fulfillment_center_id
        fba_returns_report.reason = reason
        fba_returns_report.customer_comments = customer_comments

        if record == DbAnomalies.INSERTION.value:
            fba_returns_report.save()
        else:
            fba_returns_report.updated_at = current_time
            db.session.commit()

        return fba_returns_report
