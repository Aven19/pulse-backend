"""Queue task related helper file"""
from typing import Any
from typing import Optional

from app import az_performance_zone_q
from app import az_sponsored_brand_q
from app import az_sponsored_display_q
from app import az_sponsored_product_q
from app import config_data
from app import fba_customer_shipment_sales_q
from app import fba_reimbursements_q
from app import fba_returns_q
from app import finance_event_q
from app import item_master_q
from app import ledger_summary_report_q
from app import logger
from app import order_report_q
from app import sales_traffic_report_q
from app import subscription_check_q
from app.helpers.constants import EntityType
from app.helpers.constants import QueueName
from app.helpers.constants import QueueTaskStatus
from app.models.queue_task import QueueTask
from workers.fba_concessions_report_worker import FbaConcessionsReportWorker
from workers.fba_payments_report_worker import FbaPaymentsReportWorker
from workers.fba_sales_report_worker import FbaSalesReportWorker
from workers.financial_event_worker import FinanceEventWorker
from workers.item_master_worker import ItemMasterWorker
from workers.ledger_summary_worker import LedgerSummaryWorker
from workers.order_report_worker import OrderReportWorker
from workers.performance_by_zone_worker import PerformanceByZoneWorker
from workers.sales_traffic_report_worker import SalesTrafficReportWorker
from workers.sponsored_brand_worker import SponsoredBrandWorker
from workers.sponsored_display_worker import SponsoredDisplayWorker
from workers.sponsored_product_worker import SponsoredProductWorker
from workers.subscription_check_worker import SubscriptionCheckWorker


def add_queue_task_and_enqueue(account_id: Optional[str], queue_name: str, logged_in_user: Optional[int], entity_type: int, data: dict, time_delta: Any = None):                # type: ignore  # noqa: C901
    """Add a queue task and enqueue a worker function.

    Args:
        account_id (str): ID of the account associated with the task.
        queue_name (str): Name of the queue for the task.
        logged_in_user (int): ID of the owner of the task.
        entity_type (int): Type of the entity for the task.
        data (dict): Data to be passed to the worker function.
        worker_func (function): Worker function to be executed for the task.

    Returns:
        None
    """
    try:
        queue_task = QueueTask.add_queue_task(
            queue_name=queue_name,
            account_id=account_id,
            owner_id=logged_in_user,
            status=QueueTaskStatus.NEW.value,
            entity_type=entity_type,
            param=str(data),
            input_attachment_id=None,
            output_attachment_id=None
        )

        if queue_task:
            queue_task_dict = {
                'job_id': queue_task.id,
                'queue_name': queue_task.queue_name,
                'status': QueueTaskStatus.get_status(queue_task.status),
                'entity_type': EntityType.get_type(queue_task.entity_type),
                'user_id': logged_in_user,
                'account_id': account_id,
            }
            data.update(queue_task_dict)
            queue_task.param = str(data)
            queue_task.save()
            if queue_name == QueueName.ITEM_MASTER_REPORT:
                if time_delta:
                    item_master_q.enqueue_in(time_delta=time_delta, func=ItemMasterWorker.create_item_master_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'), ttl=200, result_ttl=300, failure_ttl=300)  # type: ignore  # noqa: FKA100
                else:
                    item_master_q.enqueue(ItemMasterWorker.create_item_master_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'), ttl=200, result_ttl=300, failure_ttl=300)  # type: ignore  # noqa: FKA100
            elif queue_name == QueueName.ORDER_REPORT:
                if time_delta:
                    order_report_q.enqueue_in(time_delta=time_delta, func=OrderReportWorker.create_order_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'), ttl=200, result_ttl=300, failure_ttl=300)  # type: ignore  # noqa: FKA100
                else:
                    order_report_q.enqueue(OrderReportWorker.create_order_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'), ttl=200, result_ttl=300, failure_ttl=300)  # type: ignore  # noqa: FKA100
            elif queue_name == QueueName.SALES_TRAFFIC_REPORT:
                if time_delta:
                    sales_traffic_report_q.enqueue_in(time_delta=time_delta, func=SalesTrafficReportWorker.create_sales_traffic_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'), ttl=200, result_ttl=300, failure_ttl=300)  # type: ignore  # noqa: FKA100
                else:
                    sales_traffic_report_q.enqueue(SalesTrafficReportWorker.create_sales_traffic_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'), ttl=200, result_ttl=300, failure_ttl=300)  # type: ignore  # noqa: FKA100
            # elif queue_name == QueueName.SETTLEMENT_REPORT_V2:
            #     if time_delta:
            #         settlement_report_v2_q.enqueue_in(time_delta=time_delta, func=SettlementReportV2Worker.create_settlement_report_v2, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'), ttl=200, result_ttl=300, failure_ttl=300)  # type: ignore  # noqa: FKA100
            #     else:
            #         settlement_report_v2_q.enqueue(SettlementReportV2Worker.create_settlement_report_v2, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'), ttl=200, result_ttl=300, failure_ttl=300)  # type: ignore  # noqa: FKA100
            elif queue_name == QueueName.LEDGER_SUMMARY_REPORT:
                if time_delta:
                    ledger_summary_report_q.enqueue_in(time_delta=time_delta, func=LedgerSummaryWorker.create_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'), ttl=200, result_ttl=300, failure_ttl=300)  # type: ignore  # noqa: FKA100
                else:
                    ledger_summary_report_q.enqueue(LedgerSummaryWorker.create_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'), ttl=200, result_ttl=300, failure_ttl=300)  # type: ignore  # noqa: FKA100
            elif queue_name == QueueName.FINANCE_EVENT_LIST:
                if time_delta:
                    finance_event_q.enqueue_in(time_delta=time_delta, func=FinanceEventWorker.create_finance_event_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'), ttl=200, result_ttl=300, failure_ttl=300)  # type: ignore  # noqa: FKA100
                else:
                    finance_event_q.enqueue(FinanceEventWorker.create_finance_event_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'), ttl=200, result_ttl=300, failure_ttl=300)  # type: ignore  # noqa: FKA100
            elif queue_name == QueueName.AZ_SPONSORED_BRAND:
                if time_delta:
                    az_sponsored_brand_q.enqueue_in(time_delta=time_delta, func=SponsoredBrandWorker.create_sponsored_brand_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'), ttl=200, result_ttl=300, failure_ttl=300)  # type: ignore  # noqa: FKA100
                else:
                    az_sponsored_brand_q.enqueue(SponsoredBrandWorker.create_sponsored_brand_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'), ttl=200, result_ttl=300, failure_ttl=300)  # type: ignore  # noqa: FKA100
            elif queue_name == QueueName.AZ_SPONSORED_DISPLAY:
                if time_delta:
                    az_sponsored_display_q.enqueue_in(time_delta=time_delta, func=SponsoredDisplayWorker.create_sponsored_display_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'), ttl=200, result_ttl=300, failure_ttl=300)  # type: ignore  # noqa: FKA100
                else:
                    az_sponsored_display_q.enqueue(SponsoredDisplayWorker.create_sponsored_display_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'), ttl=200, result_ttl=300, failure_ttl=300)  # type: ignore  # noqa: FKA100
            elif queue_name == QueueName.AZ_SPONSORED_PRODUCT:
                if time_delta:
                    az_sponsored_product_q.enqueue_in(time_delta=time_delta, func=SponsoredProductWorker.create_sponsored_product_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'), ttl=200, result_ttl=300, failure_ttl=300)  # type: ignore  # noqa: FKA100
                else:
                    az_sponsored_product_q.enqueue(SponsoredProductWorker.create_sponsored_product_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'), ttl=200, result_ttl=300, failure_ttl=300)  # type: ignore  # noqa: FKA100
            elif queue_name == QueueName.FBA_RETURNS_REPORT:
                if time_delta:
                    fba_returns_q.enqueue_in(time_delta=time_delta, func=FbaConcessionsReportWorker.create_fba_returns_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'), ttl=200, result_ttl=300, failure_ttl=300)  # type: ignore  # noqa: FKA100
                else:
                    fba_returns_q.enqueue(FbaConcessionsReportWorker.create_fba_returns_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'), ttl=200, result_ttl=300, failure_ttl=300)  # type: ignore  # noqa: FKA100
            elif queue_name == QueueName.FBA_REIMBURSEMENTS_REPORT:
                if time_delta:
                    fba_reimbursements_q.enqueue_in(time_delta=time_delta, func=FbaPaymentsReportWorker.create_fba_reimbursements_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'), ttl=200, result_ttl=300, failure_ttl=300)  # type: ignore  # noqa: FKA100
                else:
                    fba_reimbursements_q.enqueue(FbaPaymentsReportWorker.create_fba_reimbursements_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'), ttl=200, result_ttl=300, failure_ttl=300)  # type: ignore  # noqa: FKA100
            elif queue_name == QueueName.FBA_CUSTOMER_SHIPMENT_SALES_REPORT:
                if time_delta:
                    fba_customer_shipment_sales_q.enqueue_in(time_delta=time_delta, func=FbaSalesReportWorker.create_customer_shipment_sales_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'), ttl=200, result_ttl=300, failure_ttl=300)  # type: ignore  # noqa: FKA100
                else:
                    fba_customer_shipment_sales_q.enqueue(FbaSalesReportWorker.create_customer_shipment_sales_report, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'), ttl=200, result_ttl=300, failure_ttl=300)  # type: ignore  # noqa: FKA100

            # wait for ads data to sync, then generate performance zone for now kept 60 seconds
            elif queue_name == QueueName.AZ_PERFORMANCE_ZONE:
                if time_delta:
                    az_performance_zone_q.enqueue(time_delta=time_delta, func=PerformanceByZoneWorker.get_performance_zone, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'), ttl=200, result_ttl=300, failure_ttl=300)  # type: ignore  # noqa: FKA100
                else:
                    az_performance_zone_q.enqueue(PerformanceByZoneWorker.get_performance_zone, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'), ttl=200, result_ttl=300, failure_ttl=300)  # type: ignore  # noqa: FKA100

            elif queue_name == QueueName.SUBSCRIPTION_CHECK:
                subscription_check_q.enqueue(SubscriptionCheckWorker.subscription_check, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'), ttl=200, result_ttl=300, failure_ttl=300)  # type: ignore  # noqa: FKA100

    except Exception as e:
        logger.exception(
            'An error occurred while adding a queue task in queue_helper Line 100: ' + str(e))
        return None
