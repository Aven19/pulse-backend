"""
    Database model for storing Settlement Report v2 in database is written in this File along with its methods.
"""
import time

from app import db
from app.models.base import Base


class AzStockTransferAdhoc(Base):
    """
        Model for storing Settlement Report v2 data in database
    """
    __tablename__ = 'az_stock_transfer_adhoc'

    id = db.Column(db.Integer, primary_key=True)
    account_id = db.Column(db.String(36), nullable=False)
    asp_id = db.Column(db.String(255))
    selling_partner_id = db.Column(db.String(255))
    category = db.Column(db.String(250), nullable=True)
    brand = db.Column(db.String(250), nullable=True)
    gstin_of_receiver = db.Column(db.String(255), nullable=False)
    transaction_type = db.Column(db.String(255), nullable=True)
    transaction_id = db.Column(db.String(255), nullable=True)
    order_id = db.Column(db.String(255), nullable=True)
    ship_from_fc = db.Column(db.String(255), nullable=True)
    ship_from_city = db.Column(db.String(255), nullable=True)
    ship_from_state = db.Column(db.String(255), nullable=True)
    ship_from_country = db.Column(db.String(255), nullable=True)
    ship_from_postal_code = db.Column(db.String(255), nullable=True)
    ship_to_fc = db.Column(db.String(255), nullable=True)
    ship_to_city = db.Column(db.String(255), nullable=True)
    ship_to_state = db.Column(db.String(255), nullable=True)
    ship_to_country = db.Column(db.String(255), nullable=True)
    ship_to_postal_code = db.Column(db.String(255), nullable=True)
    invoice_number = db.Column(db.String(255), nullable=True)
    invoice_date = db.Column(db.Date, nullable=True)
    invoice_value = db.Column(db.Float, nullable=True)
    asin = db.Column(db.String(255), nullable=True)
    sku = db.Column(db.String(255), nullable=True)
    quantity = db.Column(db.Integer, nullable=True)
    hsn_code = db.Column(db.String(255), nullable=True)
    taxable_value = db.Column(db.Float, nullable=True)
    igst_rate = db.Column(db.Float, nullable=True)
    igst_amount = db.Column(db.Float, nullable=True)
    sgst_rate = db.Column(db.Float, nullable=True)
    sgst_amount = db.Column(db.Float, nullable=True)
    utgst_rate = db.Column(db.Float, nullable=True)
    utgst_amount = db.Column(db.Float, nullable=True)
    cgst_rate = db.Column(db.Float, nullable=True)
    cgst_amount = db.Column(db.Float, nullable=True)
    compensatory_cess_rate = db.Column(db.Float, nullable=True)
    compensatory_cess_amount = db.Column(db.Float, nullable=True)
    gstin_of_supplier = db.Column(db.String(255), nullable=True)
    irn_number = db.Column(db.String(255), nullable=True)
    irn_filing_status = db.Column(db.String(255), nullable=True)
    irn_date = db.Column(db.Date, nullable=True)
    irn_error_code = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.BigInteger)
    updated_at = db.Column(db.BigInteger)

    def __repr__(self) -> str:
        """
            Object Representation Method for custom object representation on console or log
        """
        return '<id {}>'.format(self.id)

    @classmethod
    def add(cls, gstin_of_receiver: str, transaction_type: str, transaction_id: str, order_id: str,
            ship_from_fc: str, ship_from_city: str, ship_from_state: str, ship_from_country: str,
            ship_from_postal_code: str, ship_to_fc: str, ship_to_city: str, ship_to_state: str,
            ship_to_country: str, ship_to_postal_code: str, invoice_number: str, invoice_date: str,
            invoice_value: float, asin: str, sku: str, quantity: int, hsn_code: str, taxable_value: float,
            igst_rate: float, igst_amount: float, sgst_rate: float, sgst_amount: float, utgst_rate: float,
            utgst_amount: float, cgst_rate: float, cgst_amount: float, compensatory_cess_rate: float,
            compensatory_cess_amount: float, gstin_of_supplier: str, irn_number: str, irn_filing_status: str,
            irn_date: str, irn_error_code: str):
        new_stock_transfer = cls()
        new_stock_transfer.gstin_of_receiver = gstin_of_receiver,
        new_stock_transfer.transaction_type = transaction_type,
        new_stock_transfer.transaction_id = transaction_id,
        new_stock_transfer.order_id = order_id,
        new_stock_transfer.ship_from_fc = ship_from_fc,
        new_stock_transfer.ship_from_city = ship_from_city,
        new_stock_transfer.ship_from_state = ship_from_state,
        new_stock_transfer.ship_from_country = ship_from_country,
        new_stock_transfer.ship_from_postal_code = ship_from_postal_code,
        new_stock_transfer.ship_to_fc = ship_to_fc,
        new_stock_transfer.ship_to_city = ship_to_city,
        new_stock_transfer.ship_to_state = ship_to_state,
        new_stock_transfer.ship_to_country = ship_to_country,
        new_stock_transfer.ship_to_postal_code = ship_to_postal_code,
        new_stock_transfer.invoice_number = invoice_number,
        new_stock_transfer.invoice_date = invoice_date,
        new_stock_transfer.invoice_value = invoice_value,
        new_stock_transfer.asin = asin,
        new_stock_transfer.sku = sku,
        new_stock_transfer.quantity = quantity,
        new_stock_transfer.hsn_code = hsn_code,
        new_stock_transfer.taxable_value = taxable_value,
        new_stock_transfer.igst_rate = igst_rate,
        new_stock_transfer.igst_amount = igst_amount,
        new_stock_transfer.sgst_rate = sgst_rate,
        new_stock_transfer.sgst_amount = sgst_amount,
        new_stock_transfer.utgst_rate = utgst_rate,
        new_stock_transfer.utgst_amount = utgst_amount,
        new_stock_transfer.cgst_rate = cgst_rate,
        new_stock_transfer.cgst_amount = cgst_amount,
        new_stock_transfer.compensatory_cess_rate = compensatory_cess_rate,
        new_stock_transfer.compensatory_cess_amount = compensatory_cess_amount,
        new_stock_transfer.gstin_of_supplier = gstin_of_supplier,
        new_stock_transfer.irn_number = irn_number,
        new_stock_transfer.irn_filing_status = irn_filing_status,
        new_stock_transfer.irn_date = irn_date,
        new_stock_transfer.irn_error_code = irn_error_code,
        new_stock_transfer.created_at = int(time.time())
        new_stock_transfer.save()

        return new_stock_transfer
