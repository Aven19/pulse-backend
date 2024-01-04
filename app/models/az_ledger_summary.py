"""
    Database model for storing ledger summary data in database
"""
import time

from app import db
from app.helpers.constants import DbAnomalies
from app.models.az_item_master import AzItemMaster
from app.models.base import Base
from sqlalchemy import func


class AzLedgerSummary(Base):
    """
        ledger summary model to store ledger summary data
    """
    __tablename__ = 'az_ledger_summary'

    id = db.Column(db.BigInteger, primary_key=True)
    account_id = db.Column(db.String(36), nullable=False)
    asp_id = db.Column(db.String(255))
    category = db.Column(db.String(250), nullable=True)
    brand = db.Column(db.String(250), nullable=True)
    date = db.Column(db.Date)
    fnsku = db.Column(db.String(100), nullable=False)
    asin = db.Column(db.String(100), nullable=False)
    msku = db.Column(db.String(100), nullable=False)
    title = db.Column(db.Text)
    disposition = db.Column(db.String(100), nullable=False)
    starting_warehouse_balance = db.Column(db.Integer, nullable=False)
    in_transit_btw_warehouse = db.Column(db.Integer, nullable=False)
    receipts = db.Column(db.Integer, nullable=False)
    customer_shipments = db.Column(db.Integer, nullable=False)
    customer_returns = db.Column(db.Integer, nullable=False)
    vendor_returns = db.Column(db.Integer, nullable=False)
    warehouse_transfer = db.Column(db.Integer, nullable=False)
    found = db.Column(db.Integer, nullable=False)
    lost = db.Column(db.Integer, nullable=False)
    damaged = db.Column(db.Integer, nullable=False)
    disposed = db.Column(db.Integer, nullable=False)
    other_events = db.Column(db.Integer, nullable=False)
    ending_warehouse_balance = db.Column(db.Integer, nullable=False)
    unknown_events = db.Column(db.Integer, nullable=False)
    location = db.Column(db.String(20), nullable=False)
    created_at = db.Column(db.BigInteger)
    updated_at = db.Column(db.BigInteger)

    @classmethod
    def add_update(cls, account_id: str, asp_id: str, date: str, fnsku: str, asin: str,
                   msku: str, title: str, disposition: str, starting_warehouse_balance: int,
                   in_transit_btw_warehouse: int, receipts: int, customer_shipments: int, customer_returns: int, vendor_returns: int, warehouse_transfer: int,
                   found: int, lost: int, damaged: int, disposed: int, other_events: int, ending_warehouse_balance: int, unknown_events: int, location: str):
        """Insert if new ledger record according to date or update it."""

        category, brand = None, None

        if msku:
            category, brand = AzItemMaster.get_category_brand_by_sku(
                account_id=account_id, asp_id=asp_id, seller_sku=msku)

        ledger_summary = db.session.query(cls).filter(
            cls.account_id == account_id, cls.asp_id == asp_id, cls.date == date, cls.asin == asin, cls.disposition == disposition, cls.location == location).first()

        current_time = int(time.time())
        record = DbAnomalies.UPDATE.value

        if ledger_summary == None:
            record = DbAnomalies.INSERTION.value
            ledger_summary = cls(account_id=account_id, asp_id=asp_id,
                                 date=date, asin=asin, disposition=disposition, created_at=current_time)

        ledger_summary.category = category
        ledger_summary.brand = brand
        ledger_summary.fnsku = fnsku
        ledger_summary.msku = msku
        ledger_summary.title = title
        ledger_summary.starting_warehouse_balance = starting_warehouse_balance
        ledger_summary.in_transit_btw_warehouse = in_transit_btw_warehouse
        ledger_summary.receipts = receipts
        ledger_summary.customer_shipments = customer_shipments
        ledger_summary.customer_returns = customer_returns
        ledger_summary.vendor_returns = vendor_returns
        ledger_summary.warehouse_transfer = warehouse_transfer
        ledger_summary.found = found
        ledger_summary.lost = lost
        ledger_summary.damaged = damaged
        ledger_summary.disposed = disposed
        ledger_summary.other_events = other_events
        ledger_summary.ending_warehouse_balance = ending_warehouse_balance
        ledger_summary.unknown_events = unknown_events
        ledger_summary.location = location
        ledger_summary.updated_at = current_time

        if record == DbAnomalies.INSERTION.value:
            ledger_summary.save()
        else:
            db.session.commit()

        return ledger_summary

    @classmethod
    def get_inventory_by_location(cls, account_id: str, asp_id: str):
        """get inventory by location , groupyby and count from ledger table"""

        inventory_by_location = db.session.query(cls.location, func.count(cls.location)).filter(cls.account_id == account_id, cls.asp_id == asp_id).group_by(cls.location).all()        # type: ignore  # noqa: FKA100

        return inventory_by_location
