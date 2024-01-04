from datetime import datetime
from datetime import timedelta
import gzip
import json
import time
import traceback

from app import app
from app import az_sponsored_display_q
from app import config_data
from app import db
from app import logger
from app.helpers.constants import AdsApiURL
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
from app.models.az_sponsored_display import AzSponsoredDisplay
from app.models.queue_task import QueueTask
import numpy as np
import pandas as pd
from providers.amazon_ads_api import AmazonAdsReportEU
from providers.mail import send_error_notification


class SponsoredDisplayWorker():
    """Class to create, verify, store ads sponsored display from amazon API's"""

    @classmethod
    def create_sponsored_display_report(cls, data):
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
                # credentials.update({'az_ads_profile_id': az_ads_profile_id})

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
                    '%Y%m%d')
                formatted_end_datetime = parsed_end_datetime.strftime('%Y%m%d')

                start_datetime = datetime.strptime(                         # type: ignore  # noqa: FKA100
                    formatted_start_datetime, '%Y%m%d')
                end_datetime = datetime.strptime(                           # type: ignore  # noqa: FKA100
                    formatted_end_datetime, '%Y%m%d')
                report = AmazonAdsReportEU(credentials=credentials)

                # format =20230601
                while start_datetime <= end_datetime:
                    payload = {
                        'reportDate': formatted_start_datetime,
                        'metrics': AzSponsoredAdMetrics.SD_METRICS.value,
                        'tactic': AzSponsoredAdPayloadData.SD_TACTIC.value
                    }
                    response = report.create_report_v2(
                        payload=payload, url=AdsApiURL.SPONSORED_DISPLAY_V2.value)
                    # adding the report_type, report_id to the report table
                    AzReport.add(account_id=account_id, seller_partner_id=asp_id, request_start_time=start_datetime,
                                 request_end_time=start_datetime, type=ASpReportType.AZ_SPONSORED_DISPLAY.value, reference_id=response['reportId'], az_ads_profile_id=str(az_ads_profile_id))

                    start_datetime = start_datetime + timedelta(days=1)
                    formatted_start_datetime = start_datetime.strftime(
                        '%Y%m%d')
                    time.sleep(10)

                if queue_task:
                    queue_task.status = QueueTaskStatus.COMPLETED.value
                    queue_task.save()

            except Exception as e:
                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                error_message = 'Error while creating report in SponsoredDisplayWorker.create_sponsored_display_report(): ' + \
                    str(e)
                logger.error(error_message)
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='SponsoredDisplayWorker (AZ SPONSORED DISPLAY REPORT) Create Report Failure',
                                        template='emails/slack_email.html', data={}, error_message=error_message, traceback_info=traceback.format_exc())

    @classmethod
    def verify_sponsored_display_report(cls, data):
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
                response = report.verify_report_v2(document_id=report_id)

                # checking the processing status of the report. if complete, we update status in the table entry for that particular report_id
                if response['status'] != 'SUCCESS':
                    AzReport.update_status(
                        reference_id=report_id, status=response['status'], document_id=None)
                else:
                    AzReport.update_status(
                        reference_id=report_id, status=response['status'], document_id=response['reportId'])

                if queue_task:
                    queue_task.status = QueueTaskStatus.COMPLETED.value
                    queue_task.save()

                    queue_task_retrieve_verify = QueueTask.add_queue_task(queue_name=QueueName.AZ_SPONSORED_DISPLAY,
                                                                          owner_id=user_id,
                                                                          status=QueueTaskStatus.NEW.value,
                                                                          entity_type=EntityType.AZ_SPONSORED_DISPLAY.value,
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

                    az_sponsored_display_q.enqueue(SponsoredDisplayWorker.get_sponsored_display_report, data=queue_task_dict, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100

                else:
                    raise Exception

            except Exception as e:
                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                error_message = 'Error while verifying report in SponsoredDisplayWorker.verify_sponsored_display_report(): ' + \
                    str(e)
                logger.error(error_message)
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='SponsoredDisplayWorker (AZ SPONSORED DISPLAY REPORT) Verify Report Failure',
                                        template='emails/slack_email.html', data={}, error_message=error_message, traceback_info=traceback.format_exc())

    @classmethod
    def get_sponsored_display_report(cls, data):
        """This method gets the sponsored display reports from amazon and inserts into databse"""

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

                response = report.retrieve_report_download_v2(
                    document_id=report_document_id)
                content = gzip.decompress(response.content)
                content = content.decode('utf-8')

                json_dict = json.loads(content)
                payload_date = get_report_document_id.request_start_time

                formatted_payload_date = payload_date.strftime(
                    '%Y-%m-%d')

                for display in json_dict:
                    payload_date = formatted_payload_date
                    ad_group_id = display.get('adGroupId')
                    ad_group_name = display.get('adGroupName')
                    ad_id = display.get('adId')
                    asin = display.get('asin')
                    attributed_conversions_14d = display.get(
                        'attributedConversions14d')
                    attributed_conversions_14d_same_sku = display.get(
                        'attributedConversions14dSameSKU')
                    attributed_conversions_1d = display.get(
                        'attributedConversions1d')
                    attributed_conversions_1d_same_sku = display.get(
                        'attributedConversions1dSameSKU')
                    attributed_conversions_30d = display.get(
                        'attributedConversions30d')
                    attributed_conversions_30d_same_sku = display.get(
                        'attributedConversions30dSameSKU')
                    attributed_conversions_7d = display.get(
                        'attributedConversions7d')
                    attributed_conversions_7d_same_sku = display.get(
                        'attributedConversions7dSameSKU')
                    attributed_detail_page_view_14d = display.get(
                        'attributedDetailPageView14d')
                    attributed_orders_new_to_brand_14d = display.get(
                        'attributedOrdersNewToBrand14d')
                    attributed_sales_14d = display.get('attributedSales14d')
                    attributed_sales_14d_same_sku = display.get(
                        'attributedSales14dSameSKU')
                    attributed_sales_1d = display.get('attributedSales1d')
                    attributed_sales_1d_same_sku = display.get(
                        'attributedSales1dSameSKU')
                    attributed_sales_30d = display.get('attributedSales30d')
                    attributed_sales_30d_same_sku = display.get(
                        'attributedSales30dSameSKU')
                    attributed_sales_7d = display.get('attributedSales7d')
                    attributed_sales_7d_same_sku = display.get(
                        'attributedSales7dSameSKU')
                    attributed_sales_new_to_brand_14d = display.get(
                        'attributedSalesNewToBrand14d')
                    attributed_units_ordered_14d = display.get(
                        'attributedUnitsOrdered14d')
                    attributed_units_ordered_1d = display.get(
                        'attributedUnitsOrdered1d')
                    attributed_units_ordered_30d = display.get(
                        'attributedUnitsOrdered30d')
                    attributed_units_ordered_7d = display.get(
                        'attributedUnitsOrdered7d')
                    attributed_units_ordered_new_to_brand_14d = display.get(
                        'attributedUnitsOrderedNewToBrand14d')
                    campaign_id = display.get('campaignId')
                    campaign_name = display.get('campaignName')
                    clicks = display.get('clicks')
                    cost = display.get('cost')
                    currency = display.get('currency')
                    impressions = display.get('impressions')
                    sku = display.get('sku')
                    view_attributed_conversions_14d = display.get(
                        'viewAttributedConversions14d')
                    view_impressions = display.get('viewImpressions')
                    view_attributed_detail_page_view_14d = display.get(
                        'viewAttributedDetailPageView14d')
                    view_attributed_sales_14d = display.get(
                        'viewAttributedSales14d')
                    view_attributed_units_ordered_14d = display.get(
                        'viewAttributedUnitsOrdered14d')
                    view_attributed_orders_new_to_brand_14d = display.get(
                        'viewAttributedOrdersNewToBrand14d')
                    view_attributed_sales_new_to_brand_14d = display.get(
                        'viewAttributedSalesNewToBrand14d')
                    view_attributed_units_ordered_new_to_brand_14d = display.get(
                        'viewAttributedUnitsOrderedNewToBrand14d')
                    attributed_branded_searches_14d = display.get(
                        'attributedBrandedSearches14d')
                    view_attributed_branded_searches_14d = display.get(
                        'viewAttributedBrandedSearches14d')
                    video_complete_views = display.get('videoCompleteViews')
                    video_first_quartile_views = display.get(
                        'videoFirstQuartileViews')
                    video_midpoint_views = display.get('videoMidpointViews')
                    video_third_quartile_views = display.get(
                        'videoThirdQuartileViews')
                    video_unmutes = display.get('videoUnmutes')
                    vtr = display.get('vtr')
                    vctr = display.get('vctr')
                    avg_impressions_frequency = display.get(
                        'avgImpressionsFrequency')
                    cumulative_reach = display.get('cumulativeReach')

                    AzSponsoredDisplay.add_update(asp_id=asp_id, az_ads_profile_id=str(az_ads_profile_id), account_id=account_id, payload_date=payload_date, ad_group_id=ad_group_id, ad_group_name=ad_group_name, ad_id=ad_id, asin=asin, attributed_conversions_14d=attributed_conversions_14d,
                                                  attributed_conversions_14d_same_sku=attributed_conversions_14d_same_sku, attributed_conversions_1d=attributed_conversions_1d, attributed_conversions_1d_same_sku=attributed_conversions_1d_same_sku, attributed_conversions_30d=attributed_conversions_30d,
                                                  attributed_conversions_30d_same_sku=attributed_conversions_30d_same_sku, attributed_conversions_7d=attributed_conversions_7d, attributed_conversions_7d_same_sku=attributed_conversions_7d_same_sku,
                                                  attributed_detail_page_view_14d=attributed_detail_page_view_14d, attributed_orders_new_to_brand_14d=attributed_orders_new_to_brand_14d, attributed_sales_14d=attributed_sales_14d,
                                                  attributed_sales_14d_same_sku=attributed_sales_14d_same_sku, attributed_sales_1d=attributed_sales_1d, attributed_sales_1d_same_sku=attributed_sales_1d_same_sku, attributed_sales_30d=attributed_sales_30d,
                                                  attributed_sales_30d_same_sku=attributed_sales_30d_same_sku, attributed_sales_7d=attributed_sales_7d, attributed_sales_7d_same_sku=attributed_sales_7d_same_sku, attributed_sales_new_to_brand_14d=attributed_sales_new_to_brand_14d,
                                                  attributed_units_ordered_14d=attributed_units_ordered_14d, attributed_units_ordered_1d=attributed_units_ordered_1d, attributed_units_ordered_30d=attributed_units_ordered_30d, attributed_units_ordered_7d=attributed_units_ordered_7d,
                                                  attributed_units_ordered_new_to_brand_14d=attributed_units_ordered_new_to_brand_14d, campaign_id=campaign_id, campaign_name=campaign_name, clicks=clicks, cost=cost, currency=currency, impressions=impressions, sku=sku,
                                                  view_attributed_conversions_14d=view_attributed_conversions_14d, view_impressions=view_impressions, view_attributed_detail_page_view_14d=view_attributed_detail_page_view_14d, view_attributed_sales_14d=view_attributed_sales_14d,
                                                  view_attributed_units_ordered_14d=view_attributed_units_ordered_14d, view_attributed_orders_new_to_brand_14d=view_attributed_orders_new_to_brand_14d, view_attributed_sales_new_to_brand_14d=view_attributed_sales_new_to_brand_14d,
                                                  view_attributed_units_ordered_new_to_brand_14d=view_attributed_units_ordered_new_to_brand_14d, attributed_branded_searches_14d=attributed_branded_searches_14d, view_attributed_branded_searches_14d=view_attributed_branded_searches_14d, video_complete_views=video_complete_views,
                                                  video_first_quartile_views=video_first_quartile_views, video_midpoint_views=video_midpoint_views, video_third_quartile_views=video_third_quartile_views, video_unmutes=video_unmutes, vtr=vtr, vctr=vctr,
                                                  avg_impressions_frequency=avg_impressions_frequency, cumulative_reach=cumulative_reach)
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

                error_message = 'Error while retrieving report in SponsoredDisplayWorker.get_sponsored_display_report(): ' + \
                    str(e)
                logger.error(error_message)
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='SponsoredDisplayWorker (Sponsored Display Report) Download Report Failure',
                                        template='emails/slack_email.html', data={}, error_message=error_message, traceback_info=traceback.format_exc())
