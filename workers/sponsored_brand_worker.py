from datetime import datetime
from datetime import timedelta
import gzip
import json
import time
import traceback

from app import app
from app import az_sponsored_brand_q
from app import config_data
from app import db
from app import logger
from app.helpers.constants import AdsApiURL
from app.helpers.constants import ASpReportProcessingStatus
from app.helpers.constants import ASpReportType
from app.helpers.constants import AzSponsoredAdMetrics
from app.helpers.constants import EntityType
from app.helpers.constants import QueueName
from app.helpers.constants import QueueTaskStatus
from app.helpers.constants import SponsoredBrandCreativeType
from app.helpers.constants import TimePeriod
from app.helpers.utility import get_from_to_date_by_timestamp
from app.models.account import Account
from app.models.az_report import AzReport
from app.models.az_sponsored_brand import AzSponsoredBrand
from app.models.queue_task import QueueTask
import numpy as np
import pandas as pd
from providers.amazon_ads_api import AmazonAdsReportEU
from providers.mail import send_error_notification


class SponsoredBrandWorker():
    """Class to create, verify, store ads sponsored brand from amazon API's"""

    @classmethod
    def create_sponsored_brand_report(cls, data):
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

                formatted_start_datetime = parsed_start_datetime.strftime(      # type: ignore  # noqa: FKA100
                    '%Y%m%d')
                formatted_end_datetime = parsed_end_datetime.strftime('%Y%m%d')         # type: ignore  # noqa: FKA100
                start_datetime = datetime.strptime(                                     # type: ignore  # noqa: FKA100
                    formatted_start_datetime, '%Y%m%d')
                end_datetime = datetime.strptime(                                       # type: ignore  # noqa: FKA100
                    formatted_end_datetime, '%Y%m%d')
                report = AmazonAdsReportEU(credentials=credentials)

                # format =20230601
                # YYYY-MM-DD handle
                report_type = ASpReportType.AZ_SPONSORED_BRAND_BANNER.value
                creative_type = SponsoredBrandCreativeType.ALL.value
                metrics = AzSponsoredAdMetrics.SB_ALL_METRICS.value

                if queue_task.entity_type == EntityType.AZ_SPONSORED_BRAND_VIDEO.value:
                    report_type = ASpReportType.AZ_SPONSORED_BRAND_VIDEO.value
                    creative_type = SponsoredBrandCreativeType.VIDEO.value
                    metrics = AzSponsoredAdMetrics.SB_VIDEO_METRICS.value

                while start_datetime <= end_datetime:

                    creative_type = creative_type
                    payload = {
                        'creativeType': creative_type,
                        'reportDate': formatted_start_datetime,
                        'metrics': metrics
                    }

                    response = report.create_report_v2(
                        payload=payload, url=AdsApiURL.SPONSORED_BRAND_V2.value)

                    # adding the report_type, report_id to the report table
                    AzReport.add(account_id=account_id, seller_partner_id=asp_id, request_start_time=start_datetime,
                                 request_end_time=start_datetime, type=report_type, reference_id=response['reportId'], az_ads_profile_id=str(az_ads_profile_id))

                    start_datetime = start_datetime + timedelta(days=1)
                    formatted_start_datetime = start_datetime.strftime(
                        '%Y%m%d')

                if queue_task:
                    queue_task.status = QueueTaskStatus.COMPLETED.value
                    queue_task.save()

            except Exception as e:
                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                error_message = 'Error while creating report in SponsoredBrandWorker.create_sponsored_brand_report(): ' + \
                    str(e)
                logger.error(error_message)
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='SponsoredBrandWorker (AZ SPONSORED BRAND REPORT) Create Report Failure',
                                        template='emails/slack_email.html', data={}, error_message=error_message, traceback_info=traceback.format_exc())

    @classmethod
    def verify_sponsored_brand_report(cls, data):
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
                    entity_type = queue_task.entity_type
                    queue_task.status = QueueTaskStatus.COMPLETED.value
                    queue_task.save()

                    queue_task_retrieve_verify = QueueTask.add_queue_task(queue_name=QueueName.AZ_SPONSORED_BRAND,
                                                                          owner_id=user_id,
                                                                          status=QueueTaskStatus.NEW.value,
                                                                          entity_type=entity_type,
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

                    az_sponsored_brand_q.enqueue(SponsoredBrandWorker.get_sponsored_brand_report, data=queue_task_dict, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100

                else:
                    raise Exception

            except Exception as e:
                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                error_message = 'Error while verifying report in SponsoredBrandWorker.verify_sponsored_brand_report(): ' + \
                    str(e)
                logger.error(error_message)
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='SponsoredBrandWorker (AZ SPONSORED BRAND REPORT) Verify Report Failure',
                                        template='emails/slack_email.html', data={}, error_message=error_message, traceback_info=traceback.format_exc())

    @classmethod
    def get_sponsored_brand_report(cls, data):
        """This method gets the sponsored reports from amazon and inserts into databse"""

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
                sb_type = SponsoredBrandCreativeType.ALL.value
                if queue_task.entity_type == EntityType.AZ_SPONSORED_BRAND_VIDEO.value:
                    sb_type = SponsoredBrandCreativeType.VIDEO.value
                for brand in json_dict:                                                # type: ignore  # noqa: FKA100
                    payload_date = formatted_payload_date
                    sb_type = sb_type
                    ad_group_name = brand.get('adGroupName')
                    attributed_conversions_14d = brand.get(
                        'attributedConversions14d')
                    attributed_conversions_14d_same_sku = brand.get(
                        'attributedConversions14dSameSKU')
                    attributed_sales_14d = brand.get('attributedSales14d')
                    attributed_sales_14d_same_sku = brand.get(
                        'attributedSales14dSameSKU')
                    campaign_budget = brand.get('campaignBudget')
                    campaign_budget_type = brand.get('campaignBudgetType')
                    campaign_id = brand.get('campaignId')
                    campaign_name = brand.get('campaignName')
                    campaign_status = brand.get('campaignStatus')
                    clicks = brand.get('clicks')
                    cost = brand.get('cost')
                    impressions = brand.get('impressions')
                    keyword_bid = brand.get('keywordBid')
                    keyword_id = brand.get('keywordId')
                    keyword_status = brand.get('keywordStatus')
                    keyword_text = brand.get('keywordText')
                    match_type = brand.get('matchType')
                    sbv_vctr = brand.get('vctr')
                    sbv_video_5_second_view_rate = brand.get(
                        'video5SecondViewRate')
                    sbv_video_5_second_views = brand.get('video5SecondViews')
                    sbv_video_complete_views = brand.get('videoCompleteViews')
                    sbv_video_first_quartile_views = brand.get(
                        'videoFirstQuartileViews')
                    sbv_video_midpoint_views = brand.get('videoMidpointViews')
                    sbv_video_third_quartile_views = brand.get(
                        'videoThirdQuartileViews')
                    sbv_video_unmutes = brand.get('videoUnmutes')
                    sbv_viewable_impressions = brand.get('viewableImpressions')
                    sbv_vtr = brand.get('vtr')
                    dpv_14d = brand.get('dpv14d')
                    attributed_detail_page_views_clicks_14d = brand.get(
                        'attributedDetailPageViewsClicks14d')
                    attributed_order_rate_new_to_brand_14d = brand.get(
                        'attributedOrderRateNewToBrand14d')
                    attributed_orders_new_to_brand_14d = brand.get(
                        'attributedOrdersNewToBrand14d')
                    attributed_orders_new_to_brand_percentage_14d = brand.get(
                        'attributedOrdersNewToBrandPercentage14d')
                    attributed_sales_new_to_brand_14d = brand.get(
                        'attributedSalesNewToBrand14d')
                    attributed_sales_new_to_brand_percentage_14d = brand.get(
                        'attributedSalesNewToBrandPercentage14d')
                    attributed_units_ordered_new_to_brand_14d = brand.get(
                        'attributedUnitsOrderedNewToBrand14d')
                    attributed_units_ordered_new_to_brand_percentage_14d = brand.get(
                        'attributedUnitsOrderedNewToBrandPercentage14d')
                    attributed_branded_searches_14d = brand.get(
                        'attributedBrandedSearches14d')
                    sbv_currency = brand.get('currency')
                    top_of_search_impression_share = brand.get(
                        'topOfSearchImpressionShare')
                    sbb_applicable_budget_rule_id = brand.get(
                        'applicableBudgetRuleId')
                    sbb_applicable_budget_rule_name = brand.get(
                        'applicableBudgetRuleName')
                    sbb_campaign_rule_based_budget = brand.get(
                        'campaignRuleBasedBudget')
                    sbb_search_term_impression_rank = brand.get(
                        'searchTermImpressionRank')
                    sbb_search_term_impression_share = brand.get(
                        'searchTermImpressionShare')
                    sbb_units_sold_14d = brand.get('unitsSold14d')

                    AzSponsoredBrand.add_update(asp_id=asp_id, az_ads_profile_id=str(az_ads_profile_id), account_id=account_id, payload_date=payload_date, sb_type=sb_type, ad_group_name=ad_group_name, attributed_conversions_14d=attributed_conversions_14d, attributed_conversions_14d_same_sku=attributed_conversions_14d_same_sku, attributed_sales_14d=attributed_sales_14d,
                                                attributed_sales_14d_same_sku=attributed_sales_14d_same_sku, campaign_budget=campaign_budget, campaign_budget_type=campaign_budget_type, campaign_id=campaign_id, campaign_name=campaign_name, campaign_status=campaign_status,
                                                clicks=clicks, cost=cost, impressions=impressions, keyword_bid=keyword_bid, keyword_id=keyword_id, keyword_status=keyword_status, keyword_text=keyword_text, match_type=match_type,
                                                sbv_vctr=sbv_vctr, sbv_video_5_second_view_rate=sbv_video_5_second_view_rate, sbv_video_5_second_views=sbv_video_5_second_views, sbv_video_complete_views=sbv_video_complete_views, sbv_video_first_quartile_views=sbv_video_first_quartile_views,
                                                sbv_video_midpoint_views=sbv_video_midpoint_views, sbv_video_third_quartile_views=sbv_video_third_quartile_views, sbv_video_unmutes=sbv_video_unmutes, sbv_viewable_impressions=sbv_viewable_impressions, sbv_vtr=sbv_vtr,
                                                dpv_14d=dpv_14d, attributed_detail_page_views_clicks_14d=attributed_detail_page_views_clicks_14d, attributed_order_rate_new_to_brand_14d=attributed_order_rate_new_to_brand_14d, attributed_orders_new_to_brand_14d=attributed_orders_new_to_brand_14d,
                                                attributed_orders_new_to_brand_percentage_14d=attributed_orders_new_to_brand_percentage_14d, attributed_sales_new_to_brand_14d=attributed_sales_new_to_brand_14d, attributed_sales_new_to_brand_percentage_14d=attributed_sales_new_to_brand_percentage_14d, attributed_units_ordered_new_to_brand_14d=attributed_units_ordered_new_to_brand_14d,
                                                attributed_units_ordered_new_to_brand_percentage_14d=attributed_units_ordered_new_to_brand_percentage_14d, attributed_branded_searches_14d=attributed_branded_searches_14d, sbv_currency=sbv_currency, top_of_search_impression_share=top_of_search_impression_share, sbb_applicable_budget_rule_id=sbb_applicable_budget_rule_id,
                                                sbb_applicable_budget_rule_name=sbb_applicable_budget_rule_name, sbb_campaign_rule_based_budget=sbb_campaign_rule_based_budget, sbb_search_term_impression_rank=sbb_search_term_impression_rank, sbb_search_term_impression_share=sbb_search_term_impression_share, sbb_units_sold_14d=sbb_units_sold_14d)

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

                error_message = 'Error while retrieving report in SponsoredBrandWorker.get_sponsored_brand_report(): ' + \
                    str(e)
                logger.error(error_message)
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='SponsoredBrandWorker (Sponsored Brand Report) Download Report Failure',
                                        template='emails/slack_email.html', data={}, error_message=error_message, traceback_info=traceback.format_exc())
