"""
    Database model for storing Fba Reimbursements Report in database is written in this File along with its methods.
"""
import time

from app import db
from app.helpers.constants import DbAnomalies
from app.models.az_item_master import AzItemMaster
from app.models.base import Base


class AzFbaReimbursements(Base):
    """
        Fba Reimbursements Report model to store returns report in database
    """
    __tablename__ = 'az_fba_reimbursements'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    account_id = db.Column(db.String(36), nullable=False)
    asp_id = db.Column(db.String(255))
    category = db.Column(db.String(250), nullable=True)
    brand = db.Column(db.String(250), nullable=True)
    approval_date = db.Column(db.String(255))
    reimbursement_id = db.Column(db.BigInteger)
    case_id = db.Column(db.String(255))
    az_order_id = db.Column(db.String(255))
    reason = db.Column(db.String(255))
    sku = db.Column(db.String(255))
    fnsku = db.Column(db.String(255))
    asin = db.Column(db.String(255))
    product_name = db.Column(db.String(255))
    condition = db.Column(db.String(255))
    currency_unit = db.Column(db.String(255))
    amount_per_unit = db.Column(db.Numeric(10, 2))  # type: ignore  # noqa: FKA100
    amount_total = db.Column(db.Numeric(10, 2))  # type: ignore  # noqa: FKA100
    quantity_reimbursed_cash = db.Column(db.BigInteger)
    quantity_reimbursed_inventory = db.Column(db.BigInteger)
    quantity_reimbursed_total = db.Column(db.BigInteger)
    original_reimbursement_id = db.Column(db.BigInteger)
    original_reimbursement_type = db.Column(db.String(255))
    created_at = db.Column(db.BigInteger)
    updated_at = db.Column(db.BigInteger)

    @classmethod
    def get_by_order_id(cls, order_id: str):
        """Filter records by Order Id."""
        return db.session.query(cls).filter(cls.order_id == order_id).first()

    @classmethod
    def add_or_update(cls, account_id: str, asp_id: str,
                      approval_date: str,
                      reimbursement_id: int,
                      case_id: str,
                      az_order_id: str,
                      reason: str,
                      sku: str,
                      fnsku: str,
                      asin: str,
                      product_name: str,
                      condition: str,
                      currency_unit: str,
                      amount_per_unit: float,
                      amount_total: float,
                      quantity_reimbursed_cash: int,
                      quantity_reimbursed_inventory: int,
                      quantity_reimbursed_total: int,
                      original_reimbursement_id: int,
                      original_reimbursement_type: str
                      ):
        """Insert or update a FBA Reimbursements report"""
        category, brand = None, None

        if sku:
            category, brand = AzItemMaster.get_category_brand_by_sku(
                account_id=account_id, asp_id=asp_id, seller_sku=sku)

        fba_reimbursements_report = db.session.query(cls).filter(cls.account_id == account_id, cls.asp_id == asp_id,
                                                                 cls.approval_date == approval_date, cls.reimbursement_id == reimbursement_id).first()
        current_time = int(time.time())
        record = DbAnomalies.UPDATE.value

        if fba_reimbursements_report == None:
            record = DbAnomalies.INSERTION.value
            fba_reimbursements_report = cls(account_id=account_id, asp_id=asp_id, approval_date=approval_date,
                                            reimbursement_id=reimbursement_id, created_at=current_time)

        fba_reimbursements_report.category = category
        fba_reimbursements_report.brand = brand
        fba_reimbursements_report.case_id = case_id
        fba_reimbursements_report.az_order_id = az_order_id
        fba_reimbursements_report.reason = reason
        fba_reimbursements_report.sku = sku
        fba_reimbursements_report.fnsku = fnsku
        fba_reimbursements_report.asin = asin
        fba_reimbursements_report.product_name = product_name
        fba_reimbursements_report.condition = condition
        fba_reimbursements_report.currency_unit = currency_unit
        fba_reimbursements_report.amount_per_unit = amount_per_unit
        fba_reimbursements_report.amount_total = amount_total
        fba_reimbursements_report.quantity_reimbursed_cash = quantity_reimbursed_cash
        fba_reimbursements_report.quantity_reimbursed_inventory = quantity_reimbursed_inventory
        fba_reimbursements_report.quantity_reimbursed_total = quantity_reimbursed_total
        fba_reimbursements_report.original_reimbursement_id = original_reimbursement_id
        fba_reimbursements_report.original_reimbursement_type = original_reimbursement_type

        if record == DbAnomalies.INSERTION.value:
            fba_reimbursements_report.save()
        else:
            fba_reimbursements_report.updated_at = current_time
            db.session.commit()

        return fba_reimbursements_report
