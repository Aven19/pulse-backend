from datetime import datetime
from datetime import timedelta
import gzip
import json
import time
import traceback

from app import app
from app import az_sponsored_product_q
from app import config_data
from app import db
from app import logger
from app.helpers.constants import ASpReportProcessingStatus
from app.helpers.constants import ASpReportType
from app.helpers.constants import AzSponsoredAdMetrics
from app.helpers.constants import AzSponsoredAdPayloadData
from app.helpers.constants import EntityType
from app.helpers.constants import QueueName
from app.helpers.constants import QueueTaskStatus
from app.helpers.constants import TimePeriod
from app.helpers.utility import get_from_to_date_by_timestamp
from app.models.account import Account
from app.models.az_report import AzReport
from app.models.az_sponsored_product import AzSponsoredProduct
from app.models.queue_task import QueueTask
import numpy as np
import pandas as pd
from providers.amazon_ads_api import AmazonAdsReportEU
from providers.mail import send_error_notification
import requests


class SponsoredProductWorker():
    """Class to create, verify, store ads sponsored product from amazon API's"""

    @classmethod
    def create_sponsored_product_report(cls, data):
        with app.app_context():
            job_id = data.get('job_id')
            if job_id is None:
                logger.error(
                    "Queue Task with job_id '{}' not found".format(job_id))
                return

            queue_task = QueueTask.get_by_id(job_id)

            if not queue_task:
                logger.error(
                    "Queue Task with job_id '{}' not found".format(job_id))
                return

            az_ads_profile_id = data.get('az_ads_profile_id')

            if az_ads_profile_id is None:
                queue_task.status = QueueTaskStatus.ERROR.value
                queue_task.save()
                logger.error(
                    "Queue Task with job_id '{}' was failed due to az_ads_profile_id: {} not found".format(job_id, az_ads_profile_id))
                return

            queue_task.status = QueueTaskStatus.RUNNING.value
            queue_task.save()

            account_id = queue_task.account_id
            # user_id = queue_task.owner_id
            default_sync = data.get('default_sync', False)  # type: ignore  # noqa: FKA100

            account = Account.get_by_uuid(uuid=account_id)

            if not account:
                logger.error(
                    "Queue Task with job_id '{}' failed. Account : {} not found".format(job_id, account_id))
                raise Exception

            try:
                asp_id = account.asp_id
                credentials = account.retrieve_az_ads_credentials(account, az_ads_profile_id)[0]  # type: ignore  # noqa: FKA100

                if default_sync:
                    default_time_period = data.get('default_time_period', TimePeriod.LAST_30_DAYS.value)  # type: ignore  # noqa: FKA100
                    start_datetime, end_datetime = get_from_to_date_by_timestamp(
                        default_time_period)
                else:
                    start_datetime = data.get('start_datetime')
                    end_datetime = data.get('end_datetime')
                    if start_datetime is None or end_datetime is None:
                        logger.error(
                            "Queue Task with job_id '{}' failed. start_datetime : {}, end_datetime : {}".format(job_id, start_datetime, end_datetime))
                        raise Exception

                parsed_start_datetime = datetime.strptime(                  # type: ignore  # noqa: FKA100
                    start_datetime, '%Y-%m-%dT%H:%M:%S.000Z')
                parsed_end_datetime = datetime.strptime(                    # type: ignore  # noqa: FKA100
                    end_datetime, '%Y-%m-%dT%H:%M:%S.000Z')

                formatted_start_datetime = parsed_start_datetime.strftime(
                    '%Y-%m-%d')
                formatted_end_datetime = parsed_end_datetime.strftime(
                    '%Y-%m-%d')

                start_datetime = datetime.strptime(                         # type: ignore  # noqa: FKA100
                    formatted_start_datetime, '%Y-%m-%d')
                end_datetime = datetime.strptime(                           # type: ignore  # noqa: FKA100
                    formatted_end_datetime, '%Y-%m-%d')
                report = AmazonAdsReportEU(credentials=credentials)

                # format =2023-06-01
                while start_datetime <= end_datetime:
                    payload = {
                        'name': AzSponsoredAdPayloadData.SP_NAME.value,
                        'startDate': formatted_start_datetime,
                        'endDate': formatted_start_datetime,
                        'configuration': {
                            'adProduct': AzSponsoredAdPayloadData.SP_AD_PRODUCT.value,
                            'groupBy': AzSponsoredAdPayloadData.SP_GROUP_BY.value,
                            'columns': AzSponsoredAdMetrics.SP_COLUMNS.value,
                            'reportTypeId': AzSponsoredAdPayloadData.SP_REPORT_TYPE_ID.value,
                            'timeUnit': AzSponsoredAdPayloadData.SP_TIME_UNIT.value,
                            'format': AzSponsoredAdPayloadData.SP_FORMAT.value
                        }
                    }

                    response = report.create_report(payload=payload)

                    # adding the report_type, report_id to the report table
                    AzReport.add(account_id=account_id, seller_partner_id=asp_id, request_start_time=start_datetime,
                                 request_end_time=start_datetime, type=ASpReportType.AZ_SPONSORED_PRODUCT.value, reference_id=response['reportId'], az_ads_profile_id=str(az_ads_profile_id))

                    start_datetime = start_datetime + timedelta(days=1)
                    formatted_start_datetime = start_datetime.strftime(
                        '%Y-%m-%d')

                if queue_task:
                    queue_task.status = QueueTaskStatus.COMPLETED.value
                    queue_task.save()

            except Exception as e:
                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                error_message = 'Error while creating report in SponsoredProductWorker.create_sponsored_product_report(): ' + \
                    str(e)
                logger.error(error_message)
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='SponsoredProductWorker (AZ SPONSORED PRODUCT REPORT) Create Report Failure',
                                        template='emails/slack_email.html', data={}, error_message=error_message, traceback_info=traceback.format_exc())

    @classmethod
    def verify_sponsored_product_report(cls, data):
        """This should queue every minute to verify report"""
        with app.app_context():
            job_id = data.get('job_id')
            user_id = data.get('user_id')
            account_id = data.get('account_id')
            report_id = data.get('reference_id')

            if job_id is None:
                logger.error(
                    "Queue Task with job_id '{}' not found".format(job_id))
                return

            queue_task = QueueTask.get_by_id(job_id)

            if not queue_task:
                logger.error(
                    "Queue Task with job_id '{}' not found".format(job_id))
                return
            else:
                if account_id is None and account_id is None:
                    raise Exception

            account = Account.get_by_uuid(uuid=account_id)

            if not account:
                logger.error(
                    "Queue Task with job_id '{}' failed. Account : {} not found".format(job_id, account_id))
                raise Exception

            try:

                asp_id = account.asp_id
                credentials = account.retrieve_az_ads_credentials(account)[0]

                # querying Report table to get the entry for particular report_id
                get_report = AzReport.get_by_ref_id(reference_id=report_id)

                if not get_report:
                    raise Exception

                # creating AmazonReportEU object and passing the credentials
                report = AmazonAdsReportEU(credentials=credentials)

                # calling verify report function of report object and passing the payload
                response = report.verify_report(report_id=report_id)

                # checking the processing status of the report. if complete, we update status in the table entry for that particular report_id
                if response['status'] != ASpReportProcessingStatus.COMPLETED.value:
                    AzReport.update_status(
                        reference_id=report_id, status=response['status'], document_id=None)
                else:
                    AzReport.update_status(
                        reference_id=report_id, status=response['status'], document_id=response['reportId'])

                if queue_task:
                    queue_task.status = QueueTaskStatus.COMPLETED.value
                    queue_task.save()

                    queue_task_retrieve_verify = QueueTask.add_queue_task(queue_name=QueueName.AZ_SPONSORED_PRODUCT,
                                                                          owner_id=user_id,
                                                                          status=QueueTaskStatus.NEW.value,
                                                                          entity_type=EntityType.AZ_SPONSORED_PRODUCT.value,
                                                                          param=str(data), input_attachment_id=None, output_attachment_id=None)

                    if queue_task_retrieve_verify:
                        queue_task_dict = {
                            'job_id': queue_task_retrieve_verify.id,
                            'queue_name': queue_task_retrieve_verify.queue_name,
                            'status': QueueTaskStatus.get_status(queue_task_retrieve_verify.status),
                            'entity_type': EntityType.get_type(queue_task_retrieve_verify.entity_type),
                            'seller_partner_id': asp_id,
                            'account_id': account_id,
                            'reference_id': report_id
                        }

                    az_sponsored_product_q.enqueue(SponsoredProductWorker.get_sponsored_product_report, data=queue_task_dict, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100

                else:
                    raise Exception

            except Exception as e:
                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                error_message = 'Error while verifying report in SponsoredProductWorker.verify_sponsored_product_report(): ' + \
                    str(e)
                logger.error(error_message)
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='SponsoredProductWorker (AZ SPONSORED PRODUCT REPORT) Verify Report Failure',
                                        template='emails/slack_email.html', data={}, error_message=error_message, traceback_info=traceback.format_exc())

    @classmethod
    def get_sponsored_product_report(cls, data):
        """This method gets the sponsored product reports from amazon and inserts into databse"""

        with app.app_context():
            job_id = data.get('job_id')
            account_id = data.get('account_id')
            report_id = data.get('reference_id')

            if job_id is None:
                logger.error(
                    "Queue Task with job_id '{}' not found".format(job_id))
                return

            queue_task = QueueTask.get_by_id(job_id)

            if not queue_task:
                logger.error(
                    "Queue Task with job_id '{}' not found".format(job_id))
                return
            else:
                if report_id is None and account_id is None:
                    raise Exception

            az_ads_profile_id = data.get('az_ads_profile_id')

            if az_ads_profile_id is None:
                queue_task.status = QueueTaskStatus.ERROR.value
                queue_task.save()
                logger.error(
                    "Queue Task with job_id '{}' was failed due to az_ads_profile_id: {} not found".format(job_id, az_ads_profile_id))
                return

            account = Account.get_by_uuid(uuid=account_id)

            if not account:
                logger.error(
                    "Queue Task with job_id '{}' failed. Account : {} not found".format(job_id, account_id))
                raise Exception

            get_report_document_id = AzReport.get_by_ref_id(account_id=account_id,
                                                            reference_id=report_id)

            if not get_report_document_id:
                logger.error(
                    "Queue Task with job_id '{}' failed. Reference id : {} not found".format(job_id, report_id))
                raise Exception

            try:

                asp_id = account.asp_id
                credentials = account.retrieve_az_ads_credentials(account, az_ads_profile_id)[0]  # type: ignore  # noqa: FKA100

                report_document_id = get_report_document_id.document_id

                # creating AmazonAdsReportEU object and passing the credentials
                report = AmazonAdsReportEU(credentials=credentials)

                response = report.retrieve_report_download(
                    report_id=report_document_id)

                if 'url' in response:
                    file_url = response.get('url')
                else:
                    raise Exception

                # get file data by accessing the url
                file_data = requests.get(file_url)

                # decompressing the data
                content = file_data.content

                content = gzip.decompress(content)
                content = content.decode('utf-8')

                json_dict = json.loads(content)
                payload_date = get_report_document_id.request_start_time

                formatted_payload_date = payload_date.strftime(
                    '%Y-%m-%d')

                for product in json_dict:
                    asp_id = asp_id
                    account_id = account_id
                    payload_date = formatted_payload_date
                    attributed_sales_same_sku_1d = product.get(
                        'attributedSalesSameSku1d')
                    roas_clicks_14d = product.get('roasClicks14d')
                    end_date = product.get('endDate')
                    units_sold_clicks_1d = product.get('unitsSoldClicks1d')
                    attributed_sales_same_sku_14d = product.get(
                        'attributedSalesSameSku14d')
                    sales_7d = product.get('sales7d')
                    attributed_sales_same_sku_30d = product.get(
                        'attributedSalesSameSku30d')
                    kindle_edition_normalized_pages_royalties_14d = product.get(
                        'kindleEditionNormalizedPagesRoyalties14d')
                    units_sold_same_sku_1d = product.get('unitsSoldSameSku1d')
                    campaign_status = product.get('campaignStatus')
                    advertised_sku = product.get('advertisedSku')
                    sales_other_sku_7d = product.get('salesOtherSku7d')
                    purchases_same_sku_7d = product.get('purchasesSameSku7d')
                    campaign_budget_amount = product.get(
                        'campaignBudgetAmount')
                    purchases_7d = product.get('purchases7d')
                    units_sold_same_sku_30d = product.get(
                        'unitsSoldSameSku30d')
                    cost_per_click = product.get('costPerClick')
                    units_sold_clicks_14d = product.get('unitsSoldClicks14d')
                    ad_group_name = product.get('adGroupName')
                    campaign_id = product.get('campaignId')
                    click_through_rate = product.get('clickThroughRate')
                    kindle_edition_normalized_pages_read_14d = product.get(
                        'kindleEditionNormalizedPagesRead14d')
                    acos_clicks_14d = product.get('acosClicks14d')
                    units_sold_clicks_30d = product.get('unitsSoldClicks30d')
                    portfolio_id = product.get('portfolioId')
                    ad_id = product.get('adId')
                    campaign_budget_currency_code = product.get(
                        'campaignBudgetCurrencyCode')
                    start_date = product.get('startDate')
                    roas_clicks_7d = product.get('roasClicks7d')
                    units_sold_same_sku_14d = product.get(
                        'unitsSoldSameSku14d')
                    units_sold_clicks_7d = product.get('unitsSoldClicks7d')
                    attributed_sales_same_sku_7d = product.get(
                        'attributedSalesSameSku7d')
                    sales_1d = product.get('sales1d')
                    ad_group_id = product.get('adGroupId')
                    purchases_same_sku_14d = product.get('purchasesSameSku14d')
                    units_sold_other_sku_7d = product.get(
                        'unitsSoldOtherSku7d')
                    spend = product.get('spend')
                    purchases_same_sku_1d = product.get('purchasesSameSku1d')
                    campaign_budget_type = product.get('campaignBudgetType')
                    advertised_asin = product.get('advertisedAsin')
                    purchases_1d = product.get('purchases1d')
                    units_sold_same_sku_7d = product.get('unitsSoldSameSku7d')
                    cost = product.get('cost')
                    sales_14d = product.get('sales14d')
                    acos_clicks_7d = product.get('acosClicks7d')
                    sales_30d = product.get('sales30d')
                    impressions = product.get('impressions')
                    purchases_same_sku_30d = product.get('purchasesSameSku30d')
                    purchases_14d = product.get('purchases14d')
                    purchases_30d = product.get('purchases30d')
                    clicks = product.get('clicks')
                    campaign_name = product.get('campaignName')

                    AzSponsoredProduct.add_update(asp_id=asp_id, account_id=account_id, az_ads_profile_id=str(az_ads_profile_id), payload_date=payload_date, clicks=clicks, campaign_name=campaign_name, attributed_sales_same_sku_1d=attributed_sales_same_sku_1d,
                                                  roas_clicks_14d=roas_clicks_14d, end_date=end_date, units_sold_clicks_1d=units_sold_clicks_1d,
                                                  attributed_sales_same_sku_14d=attributed_sales_same_sku_14d, sales_7d=sales_7d,
                                                  attributed_sales_same_sku_30d=attributed_sales_same_sku_30d,
                                                  kindle_edition_normalized_pages_royalties_14d=kindle_edition_normalized_pages_royalties_14d,
                                                  units_sold_same_sku_1d=units_sold_same_sku_1d, campaign_status=campaign_status,
                                                  advertised_sku=advertised_sku, sales_other_sku_7d=sales_other_sku_7d,
                                                  purchases_same_sku_7d=purchases_same_sku_7d, campaign_budget_amount=campaign_budget_amount,
                                                  purchases_7d=purchases_7d, units_sold_same_sku_30d=units_sold_same_sku_30d,
                                                  cost_per_click=cost_per_click, units_sold_clicks_14d=units_sold_clicks_14d,
                                                  ad_group_name=ad_group_name, campaign_id=campaign_id, click_through_rate=click_through_rate,
                                                  kindle_edition_normalized_pages_read_14d=kindle_edition_normalized_pages_read_14d,
                                                  acos_clicks_14d=acos_clicks_14d, units_sold_clicks_30d=units_sold_clicks_30d,
                                                  portfolio_id=portfolio_id, ad_id=ad_id, campaign_budget_currency_code=campaign_budget_currency_code,
                                                  start_date=start_date, roas_clicks_7d=roas_clicks_7d, units_sold_same_sku_14d=units_sold_same_sku_14d,
                                                  units_sold_clicks_7d=units_sold_clicks_7d, attributed_sales_same_sku_7d=attributed_sales_same_sku_7d,
                                                  sales_1d=sales_1d, ad_group_id=ad_group_id, purchases_same_sku_14d=purchases_same_sku_14d,
                                                  units_sold_other_sku_7d=units_sold_other_sku_7d, spend=spend, purchases_same_sku_1d=purchases_same_sku_1d,
                                                  campaign_budget_type=campaign_budget_type, advertised_asin=advertised_asin,
                                                  purchases_1d=purchases_1d, units_sold_same_sku_7d=units_sold_same_sku_7d, cost=cost, sales_14d=sales_14d,
                                                  acos_clicks_7d=acos_clicks_7d, sales_30d=sales_30d, impressions=impressions,
                                                  purchases_same_sku_30d=purchases_same_sku_30d, purchases_14d=purchases_14d, purchases_30d=purchases_30d)
                if get_report_document_id:
                    get_report_document_id.status = ASpReportProcessingStatus.COMPLETED.value
                    get_report_document_id.status_updated_at = int(time.time())
                    db.session.commit()

                if queue_task:
                    queue_task.status = QueueTaskStatus.COMPLETED.value
                    queue_task.save()
                else:
                    raise Exception

            except Exception as e:

                if get_report_document_id:
                    get_report_document_id.status = ASpReportProcessingStatus.ERROR.value
                    get_report_document_id.status_updated_at = int(time.time())
                    db.session.commit()

                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                error_message = 'Error while retrieving report in SponsoredProductWorker.get_sponsored_product_report(): ' + \
                    str(e)
                logger.error(error_message)
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='SponsoredProductWorker (Sponsored Product Report) Download Report Failure',
                                        template='emails/slack_email.html', data={}, error_message=error_message, traceback_info=traceback.format_exc())
