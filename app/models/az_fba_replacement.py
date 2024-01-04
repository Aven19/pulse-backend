"""
    Database model for storing Fba Replacements Report in database is written in this File along with its methods.
"""
import time

from app import db
from app import logger
from app.models.base import Base


class AzFbaReplacement(Base):
    """
        Fba Replacements Report model to store replacements report in database
    """
    __tablename__ = 'az_fba_replacement'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    account_id = db.Column(db.String(36), nullable=False)
    asp_id = db.Column(db.String(255))
    category = db.Column(db.String(250), nullable=True)
    brand = db.Column(db.String(250), nullable=True)
    selling_partner_id = db.Column(db.String(255))
    shipment_date = db.Column(db.BigInteger)
    sku = db.Column(db.String(255))
    asin = db.Column(db.String(255))
    fulfillment_center_id = db.Column(db.String(255))
    original_fulfillment_center_id = db.Column(db.String(255))
    quantity = db.Column(db.Integer)
    replacement_reason_code = db.Column(db.String(255))
    replacement_amazon_order_id = db.Column(db.String(255))
    original_amazon_order_id = db.Column(db.String(255))
    created_at = db.Column(db.BigInteger)
    updated_at = db.Column(db.BigInteger)

    @classmethod
    def add(cls, selling_partner_id: str, shipment_date: int, sku: str, asin: str, fulfillment_center_id: str,
            original_fulfillment_center_id: str, quantity: int, replacement_reason_code: str,
            replacement_amazon_order_id: str, original_amazon_order_id: str) -> 'AzFbaReplacement':
        """Create a new FBA Return report"""
        fba_replacements_report = cls()
        fba_replacements_report.selling_partner_id = selling_partner_id
        fba_replacements_report.shipment_date = shipment_date,
        fba_replacements_report.sku = sku,
        fba_replacements_report.asin = asin,
        fba_replacements_report.fulfillment_center_id = fulfillment_center_id,
        fba_replacements_report.original_fulfillment_center_id = original_fulfillment_center_id,
        fba_replacements_report.quantity = quantity,
        fba_replacements_report.replacement_reason_code = replacement_reason_code,
        fba_replacements_report.replacement_amazon_order_id = replacement_amazon_order_id,
        fba_replacements_report.original_amazon_order_id = original_amazon_order_id
        fba_replacements_report.created_at = int(time.time())
        fba_replacements_report.updated_at = int(time.time())
        fba_replacements_report.save()

        return fba_replacements_report

    @classmethod
    def add_or_update(cls, selling_partner_id: str, return_date: int, order_id: str, sku: str, asin: str, fnsku: str,
                      product_name: str, quantity: int, fulfillment_center_id: str, detailed_disposition: str, reason: str, license_plate_number: str, customer_comments: str):
        """Insert or update a FBA Return report"""
        try:
            fba_replacements_report = cls.query.filter_by(
                order_id=order_id).first()
            if fba_replacements_report:
                fba_replacements_report.selling_partner_id = selling_partner_id
                fba_replacements_report.return_date = return_date
                fba_replacements_report.order_id = order_id
                fba_replacements_report.sku = sku
                fba_replacements_report.asin = asin
                fba_replacements_report.fnsku = fnsku
                fba_replacements_report.product_name = product_name
                fba_replacements_report.quantity = quantity
                fba_replacements_report.fulfillment_center_id = fulfillment_center_id
                fba_replacements_report.detailed_disposition = detailed_disposition
                fba_replacements_report.reason = reason
                fba_replacements_report.license_plate_number = license_plate_number
                fba_replacements_report.customer_comments = customer_comments
                fba_replacements_report.updated_at = int(time.time())
                db.session.commit()
            else:
                fba_replacements_report = cls(selling_partner_id=selling_partner_id, return_date=return_date, order_id=order_id, sku=sku, asin=asin, fnsku=fnsku, product_name=product_name, quantity=quantity, fulfillment_center_id=fulfillment_center_id,
                                              detailed_disposition=detailed_disposition, reason=reason, license_plate_number=license_plate_number, customer_comments=customer_comments, created_at=int(time.time()), updated_at=int(time.time()))
                fba_replacements_report.save()

            return fba_replacements_report
        except Exception as e:
            logger.warning(
                'Error while inserting or updating FBA returns report: ' + e)
            db.session.rollback()
