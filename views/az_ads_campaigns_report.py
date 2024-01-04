"""Amazon Ad's api view"""

from datetime import datetime
from datetime import timedelta
import gzip
import json
import os

from app import config_data
from app import logger
from app.helpers.constants import AdsApiURL
from app.helpers.constants import ASpReportType
from app.helpers.constants import AzSponsoredAdMetrics
from app.helpers.constants import AzSponsoredAdPayloadData
from app.helpers.constants import EntityType
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import QueueName
from app.helpers.constants import ResponseMessageKeys
from app.helpers.constants import SponsoredBrandCreativeType
from app.helpers.decorators import api_time_logger
from app.helpers.decorators import token_required
from app.helpers.queue_helper import add_queue_task_and_enqueue
from app.helpers.utility import field_type_validator
from app.helpers.utility import required_validator
from app.helpers.utility import send_json_response
from app.models.az_report import AzReport
from app.models.az_sponsored_brand import AzSponsoredBrand
from app.models.az_sponsored_display import AzSponsoredDisplay
from app.models.az_sponsored_product import AzSponsoredProduct
from flask import request
from providers.amazon_ads_api import AmazonAdsReportEU
import requests
from werkzeug.exceptions import BadRequest


class AzAdsReport:
    """Class for Amazon af's api"""

    @staticmethod
    @api_time_logger
    @token_required
    def create_product_advertised_report(user_object, account_object):
        """This method requests an product advertised report"""
        try:

            asp_id = account_object.asp_id
            account_id = account_object.uuid

            field_types = {'from_date': str, 'to_date': str}

            required_fields = ['from_date', 'to_date']

            data = field_type_validator(
                request_data=request.args, field_types=field_types)

            if data['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=data['data'])

            is_valid = required_validator(
                request_data=request.args, required_fields=required_fields)

            if is_valid['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=is_valid['data'])

            formatted_start_datetime = request.args.get('from_date')
            formatted_end_datetime = request.args.get('to_date')

            credentials = account_object.retrieve_az_ads_credentials(account_object)[
                0]

            report = AmazonAdsReportEU(credentials=credentials)

            start_datetime = datetime.strptime(                         # type: ignore  # noqa: FKA100
                formatted_start_datetime, '%Y-%m-%d')
            end_datetime = datetime.strptime(                           # type: ignore  # noqa: FKA100
                formatted_end_datetime, '%Y-%m-%d')

            # Sponsored Product
            while start_datetime <= end_datetime:
                payload = {
                    'name': 'SP advertised product report 7/5-7/10',
                    'startDate': formatted_start_datetime,
                    'endDate': formatted_end_datetime,
                    'configuration': {
                        'adProduct': 'SPONSORED_PRODUCTS',
                        'groupBy': ['advertiser'],
                        'columns': AzSponsoredAdMetrics.SP_COLUMNS.value,
                        'reportTypeId': 'spAdvertisedProduct',
                        'timeUnit': 'SUMMARY',
                        'format': 'GZIP_JSON'
                    }
                }

                response = report.create_report(payload=payload)

                # adding the report_type, report_id to the report table
                AzReport.add(account_id=account_id, seller_partner_id=asp_id, request_start_time=start_datetime,
                             request_end_time=start_datetime, type=ASpReportType.AZ_SPONSORED_PRODUCT.value, reference_id=response['reportId'])

                start_datetime = start_datetime + timedelta(days=1)
                formatted_start_datetime = start_datetime.strftime(
                    '%Y-%m-%d')

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response, error=None)

        except Exception as exception_error:
            logger.error(
                f'Exception occured while creating Sponsored Product report : {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

    @staticmethod
    @api_time_logger
    @token_required
    def get_advertised_report_verified(user_object, account_object):
        """This method checks if the report processing is completed"""

        try:

            data = request.get_json(force=True)

            # asp_id = account_object.asp_id
            # account_id = account_object.uuid

            credentials = account_object.retrieve_az_ads_credentials(account_object)[
                0]

            # Data Validation
            field_types = {'report_id': str}
            required_fields = ['report_id']

            post_data = field_type_validator(
                request_data=data, field_types=field_types)

            if post_data['is_error']:
                return send_json_response(
                    http_status=HttpStatusCode.BAD_REQUEST.value,
                    response_status=False,
                    message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
                    error=post_data['data'],
                )

            # Check Required Field
            is_valid = required_validator(
                request_data=data, required_fields=required_fields
            )

            if is_valid['is_error']:
                return send_json_response(
                    http_status=HttpStatusCode.BAD_REQUEST.value,
                    response_status=False,
                    message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
                    error=is_valid['data'],
                )

            report_id = data.get('report_id')

            # creating AmazonAdsReportEU object and passing the credentials
            report = AmazonAdsReportEU(credentials=credentials)

            # calling create report function of report object and passing the payload
            response = report.verify_report(report_id=report_id)

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response, error=None)

        except Exception as exception_error:
            logger.error(
                f'Exception occured while getting Sponsored Product data : {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

    @staticmethod
    @api_time_logger
    @token_required
    def retrieve_advertised_report(user_object, account_object):
        """This method retrieves and stores report data"""

        try:
            data = request.get_json(force=True)
            # Data Validation
            asp_id = account_object.asp_id
            account_id = account_object.uuid

            credentials = account_object.retrieve_az_ads_credentials(account_object)[
                0]
            field_types = {'report_id': str}
            required_fields = ['report_id']

            post_data = field_type_validator(
                request_data=data, field_types=field_types)

            if post_data['is_error']:
                return send_json_response(
                    http_status=HttpStatusCode.BAD_REQUEST.value,
                    response_status=False,
                    message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
                    error=post_data['data'],
                )

            # Check Required Field
            is_valid = required_validator(
                request_data=data, required_fields=required_fields
            )

            if is_valid['is_error']:
                return send_json_response(
                    http_status=HttpStatusCode.BAD_REQUEST.value,
                    response_status=False,
                    message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
                    error=is_valid['data'],
                )

            report_id = data.get('report_id')

            # creating AmazonAdsReportEU object and passing the credentials
            report = AmazonAdsReportEU(credentials=credentials)

            response = report.retrieve_report_download(
                report_id=report_id)
            # get url
            if 'url' in response:
                file_url = response.get('url')
            else:
                logger.error(
                    'file url not found for sponsored product report')
                return send_json_response(
                    http_status=500,
                    response_status=False,
                    message_key=ResponseMessageKeys.FAILED.value
                )

            # get file data by accessing the url
            file_data = requests.get(file_url)

            # decompressing the data
            content = file_data.content

            if response['configuration']['format'] == 'GZIP_JSON':
                content = gzip.decompress(content)

            content = content.decode('utf-8')

            file_path = config_data.get(
                'UPLOAD_FOLDER') + 'sponsored_product' + '/{}.txt'.format(report_id)
            directory = os.path.dirname(file_path)

            # Create the necessary directories if they don't exist
            os.makedirs(directory, exist_ok=True)

            with open(file_path, 'w') as f:                # type: ignore  # noqa: FKA100
                f.write(content)
            f.close()

            json_dict = json.loads(content)                # type: ignore  # noqa: FKA100

            for product in json_dict:
                asp_id = asp_id
                account_id = account_id
                payload_date = '2023-08-02'
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
                campaign_budget_amount = product.get('campaignBudgetAmount')
                purchases_7d = product.get('purchases7d')
                units_sold_same_sku_30d = product.get('unitsSoldSameSku30d')
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
                units_sold_same_sku_14d = product.get('unitsSoldSameSku14d')
                units_sold_clicks_7d = product.get('unitsSoldClicks7d')
                attributed_sales_same_sku_7d = product.get(
                    'attributedSalesSameSku7d')
                sales_1d = product.get('sales1d')
                ad_group_id = product.get('adGroupId')
                purchases_same_sku_14d = product.get('purchasesSameSku14d')
                units_sold_other_sku_7d = product.get('unitsSoldOtherSku7d')
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

                AzSponsoredProduct.add(asp_id=asp_id, account_id=account_id, payload_date=payload_date, clicks=clicks, campaign_name=campaign_name, attributed_sales_same_sku_1d=attributed_sales_same_sku_1d,
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

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response, error=None)

        except Exception as exception_error:
            logger.error(
                f'Exception occured while getting Sponsored brand data : {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

    @staticmethod
    @api_time_logger
    @token_required
    def create_brand_report(user_object, account_object):
        """This method requests an advertised report"""
        try:

            asp_id = account_object.asp_id
            account_id = account_object.uuid

            field_types = {'from_date': str, 'to_date': str}

            required_fields = ['from_date', 'to_date']

            data = field_type_validator(
                request_data=request.args, field_types=field_types)

            if data['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=data['data'])

            is_valid = required_validator(
                request_data=request.args, required_fields=required_fields)

            if is_valid['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=is_valid['data'])

            from_date = request.args.get('from_date')
            to_date = request.args.get('to_date')

            parsed_start_datetime = datetime.strptime(                                     # type: ignore  # noqa: FKA100
                from_date, '%Y-%m-%d')
            parsed_end_datetime = datetime.strptime(                                       # type: ignore  # noqa: FKA100
                to_date, '%Y-%m-%d')

            credentials = account_object.retrieve_az_ads_credentials(account_object)[
                0]

            report = AmazonAdsReportEU(credentials=credentials)

            report_type_list = [ASpReportType.AZ_SPONSORED_BRAND_BANNER.value,
                                ASpReportType.AZ_SPONSORED_BRAND_VIDEO.value]

            for report_type in report_type_list:

                formatted_start_datetime = parsed_start_datetime.strftime(
                    '%Y%m%d')
                formatted_end_datetime = parsed_end_datetime.strftime('%Y%m%d')

                start_datetime = datetime.strptime(                                     # type: ignore  # noqa: FKA100
                    formatted_start_datetime, '%Y%m%d')
                end_datetime = datetime.strptime(                                       # type: ignore  # noqa: FKA100
                    formatted_end_datetime, '%Y%m%d')

                if report_type == ASpReportType.AZ_SPONSORED_BRAND_BANNER.value:
                    creative_type = SponsoredBrandCreativeType.ALL.value
                    metrics = AzSponsoredAdMetrics.SB_ALL_METRICS.value
                elif report_type == EntityType.AZ_SPONSORED_BRAND_VIDEO.value:
                    creative_type = SponsoredBrandCreativeType.VIDEO.value
                    metrics = AzSponsoredAdMetrics.SB_VIDEO_METRICS.value

                while start_datetime <= end_datetime:

                    payload = {
                        'creativeType': creative_type,
                        'reportDate': formatted_start_datetime,
                        'metrics': metrics
                    }

                    response = report.create_report_v2(
                        payload=payload, url=AdsApiURL.SPONSORED_BRAND_V2.value)

                    # adding the report_type, report_id to the report table
                    AzReport.add(account_id=account_id, seller_partner_id=asp_id, request_start_time=start_datetime,
                                 request_end_time=start_datetime, type=report_type, reference_id=response['reportId'])

                    start_datetime = start_datetime + timedelta(days=1)
                    formatted_start_datetime = start_datetime.strftime(
                        '%Y%m%d')

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response, error=None)

        except Exception as exception_error:
            logger.error(
                f'Exception occured while creating Sponsored brand report : {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

    @staticmethod
    @api_time_logger
    @token_required
    def get_brand_report_verified(user_object, account_object):
        """This method checks if the report processing is completed"""

        try:

            data = request.get_json(force=True)

            credentials = account_object.retrieve_az_ads_credentials(account_object)[
                0]

            # Data Validation
            field_types = {'report_id': str}
            required_fields = ['report_id']

            post_data = field_type_validator(
                request_data=data, field_types=field_types)

            if post_data['is_error']:
                return send_json_response(
                    http_status=HttpStatusCode.BAD_REQUEST.value,
                    response_status=False,
                    message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
                    error=post_data['data'],
                )

            # Check Required Field
            is_valid = required_validator(
                request_data=data, required_fields=required_fields
            )

            if is_valid['is_error']:
                return send_json_response(
                    http_status=HttpStatusCode.BAD_REQUEST.value,
                    response_status=False,
                    message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
                    error=is_valid['data'],
                )

            report_id = data.get('report_id')

            # creating AmazonAdsReportEU object and passing the credentials
            report = AmazonAdsReportEU(credentials=credentials)

            # calling create report function of report object and passing the payload
            response = report.verify_report_v2(document_id=report_id)

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response, error=None)

        except Exception as exception_error:
            logger.error(
                f'Exception occured while getting Sponsored brand data : {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

    @staticmethod
    @api_time_logger
    @token_required
    def retrieve_brand_report(user_object, account_object):
        """This method retrieves and stores brand report data"""

        try:
            data = request.get_json(force=True)
            # Data Validation
            field_types = {'report_id': str}
            required_fields = ['report_id']

            asp_id = account_object.asp_id
            account_id = account_object.uuid

            credentials = account_object.retrieve_az_ads_credentials(account_object)[
                0]

            post_data = field_type_validator(
                request_data=data, field_types=field_types)

            if post_data['is_error']:
                return send_json_response(
                    http_status=HttpStatusCode.BAD_REQUEST.value,
                    response_status=False,
                    message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
                    error=post_data['data'],
                )

            # Check Required Field
            is_valid = required_validator(
                request_data=data, required_fields=required_fields
            )

            if is_valid['is_error']:
                return send_json_response(
                    http_status=HttpStatusCode.BAD_REQUEST.value,
                    response_status=False,
                    message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
                    error=is_valid['data'],
                )

            report_id = data.get('report_id')

            # creating AmazonAdsReportEU object and passing the credentials
            report = AmazonAdsReportEU(credentials=credentials)

            response = report.retrieve_report_download_v2(
                document_id=report_id)
            # get url
            # if 'url' in response:
            #     file_url = response.get('url')
            # else:
            #     logger.error(
            #         'file url not found for sponsored product report')
            #     return send_json_response(
            #         http_status=500,
            #         response_status=False,
            #         message_key=ResponseMessageKeys.FAILED.value
            #     )

            # get file data by accessing the url
            # file_data = requests.get(file_url)

            # decompressing the data
            # content = file_data.content

            # if response['configuration']['format'] == 'GZIP_JSON':
            #     content = gzip.decompress(content)

            # content = content.decode('utf-8')

            content = gzip.decompress(response.content)

            content = content.decode('utf-8')

            file_path = config_data.get(
                'UPLOAD_FOLDER') + 'sponsored_brand' + '/{}.txt'.format(report_id)
            directory = os.path.dirname(file_path)

            # Create the necessary directories if they don't exist
            os.makedirs(directory, exist_ok=True)

            with open(file_path, 'w') as f:                    # type: ignore  # noqa: FKA100
                f.write(content)
            f.close()

            json_dict = json.loads(content)

            for brand in json_dict:                                                # type: ignore  # noqa: FKA100
                payload_date = '2023-08-01'
                sb_type = 'banner'
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

                AzSponsoredBrand.add(asp_id=asp_id, account_id=account_id, payload_date=payload_date, sb_type=sb_type, ad_group_name=ad_group_name, attributed_conversions_14d=attributed_conversions_14d, attributed_conversions_14d_same_sku=attributed_conversions_14d_same_sku, attributed_sales_14d=attributed_sales_14d,
                                     attributed_sales_14d_same_sku=attributed_sales_14d_same_sku, campaign_budget=campaign_budget, campaign_budget_type=campaign_budget_type, campaign_id=campaign_id, campaign_name=campaign_name, campaign_status=campaign_status,
                                     clicks=clicks, cost=cost, impressions=impressions, keyword_bid=keyword_bid, keyword_id=keyword_id, keyword_status=keyword_status, keyword_text=keyword_text, match_type=match_type,
                                     sbv_vctr=sbv_vctr, sbv_video_5_second_view_rate=sbv_video_5_second_view_rate, sbv_video_5_second_views=sbv_video_5_second_views, sbv_video_complete_views=sbv_video_complete_views, sbv_video_first_quartile_views=sbv_video_first_quartile_views,
                                     sbv_video_midpoint_views=sbv_video_midpoint_views, sbv_video_third_quartile_views=sbv_video_third_quartile_views, sbv_video_unmutes=sbv_video_unmutes, sbv_viewable_impressions=sbv_viewable_impressions, sbv_vtr=sbv_vtr,
                                     dpv_14d=dpv_14d, attributed_detail_page_views_clicks_14d=attributed_detail_page_views_clicks_14d, attributed_order_rate_new_to_brand_14d=attributed_order_rate_new_to_brand_14d, attributed_orders_new_to_brand_14d=attributed_orders_new_to_brand_14d,
                                     attributed_orders_new_to_brand_percentage_14d=attributed_orders_new_to_brand_percentage_14d, attributed_sales_new_to_brand_14d=attributed_sales_new_to_brand_14d, attributed_sales_new_to_brand_percentage_14d=attributed_sales_new_to_brand_percentage_14d, attributed_units_ordered_new_to_brand_14d=attributed_units_ordered_new_to_brand_14d,
                                     attributed_units_ordered_new_to_brand_percentage_14d=attributed_units_ordered_new_to_brand_percentage_14d, attributed_branded_searches_14d=attributed_branded_searches_14d, sbv_currency=sbv_currency, top_of_search_impression_share=top_of_search_impression_share, sbb_applicable_budget_rule_id=sbb_applicable_budget_rule_id,
                                     sbb_applicable_budget_rule_name=sbb_applicable_budget_rule_name, sbb_campaign_rule_based_budget=sbb_campaign_rule_based_budget, sbb_search_term_impression_rank=sbb_search_term_impression_rank, sbb_search_term_impression_share=sbb_search_term_impression_share, sbb_units_sold_14d=sbb_units_sold_14d)

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=None, error=None)

        except Exception as exception_error:
            logger.error(
                f'Exception occured while getting Sponsored brand data : {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

    @staticmethod
    @api_time_logger
    @token_required
    def create_display_report(user_object, account_object):
        """This method requests an advertised report"""
        try:

            asp_id = account_object.asp_id
            account_id = account_object.uuid

            credentials = account_object.retrieve_az_ads_credentials(account_object)[
                0]

            field_types = {'from_date': str, 'to_date': str}

            required_fields = ['from_date', 'to_date']

            data = field_type_validator(
                request_data=request.args, field_types=field_types)

            if data['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=data['data'])

            is_valid = required_validator(
                request_data=request.args, required_fields=required_fields)

            if is_valid['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=is_valid['data'])

            from_date = request.args.get('from_date')
            to_date = request.args.get('to_date')

            parsed_start_datetime = datetime.strptime(                                     # type: ignore  # noqa: FKA100
                from_date, '%Y-%m-%d')
            parsed_end_datetime = datetime.strptime(                                       # type: ignore  # noqa: FKA100
                to_date, '%Y-%m-%d')

            formatted_start_datetime = parsed_start_datetime.strftime('%Y%m%d')
            formatted_end_datetime = parsed_end_datetime.strftime('%Y%m%d')

            start_datetime = datetime.strptime(                                     # type: ignore  # noqa: FKA100
                formatted_start_datetime, '%Y%m%d')
            end_datetime = datetime.strptime(                                       # type: ignore  # noqa: FKA100
                formatted_end_datetime, '%Y%m%d')

            report = AmazonAdsReportEU(credentials=credentials)

            # Sponsored Display
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
                             request_end_time=start_datetime, type=ASpReportType.AZ_SPONSORED_DISPLAY.value, reference_id=response['reportId'])

                start_datetime = start_datetime + timedelta(days=1)
                formatted_start_datetime = start_datetime.strftime(
                    '%Y%m%d')

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response, error=None)

        except Exception as exception_error:
            logger.error(
                f'Exception occured while creating Sponsored display report : {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

    @staticmethod
    @api_time_logger
    @token_required
    def get_display_report_verified(user_object, account_object):
        """This method checks if the report processing is completed"""

        try:

            # asp_id = account_object.asp_id
            # account_id = account_object.uuid

            credentials = account_object.retrieve_az_ads_credentials(account_object)[
                0]

            data = request.get_json(force=True)

            # Data Validation
            field_types = {'report_id': str}
            required_fields = ['report_id']

            post_data = field_type_validator(
                request_data=data, field_types=field_types)

            if post_data['is_error']:
                return send_json_response(
                    http_status=HttpStatusCode.BAD_REQUEST.value,
                    response_status=False,
                    message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
                    error=post_data['data'],
                )

            # Check Required Field
            is_valid = required_validator(
                request_data=data, required_fields=required_fields
            )

            if is_valid['is_error']:
                return send_json_response(
                    http_status=HttpStatusCode.BAD_REQUEST.value,
                    response_status=False,
                    message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
                    error=is_valid['data'],
                )

            report_id = data.get('report_id')

            # creating AmazonAdsReportEU object and passing the credentials
            report = AmazonAdsReportEU(credentials=credentials)

            # calling create report function of report object and passing the payload
            response = report.verify_report_v2(document_id=report_id)

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response, error=None)

        except Exception as exception_error:
            logger.error(
                f'Exception occured while getting sponsered dsiplay data : {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

    @staticmethod
    @api_time_logger
    @token_required
    def retrieve_display_report(user_object, account_object):
        """This method retrieves and stores display report data"""

        try:
            data = request.get_json(force=True)
            # Data Validation
            field_types = {'report_id': str}
            required_fields = ['report_id']

            asp_id = account_object.asp_id
            account_id = account_object.uuid

            credentials = account_object.retrieve_az_ads_credentials(account_object)[
                0]

            post_data = field_type_validator(
                request_data=data, field_types=field_types)

            if post_data['is_error']:
                return send_json_response(
                    http_status=HttpStatusCode.BAD_REQUEST.value,
                    response_status=False,
                    message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
                    error=post_data['data'],
                )

            # Check Required Field
            is_valid = required_validator(
                request_data=data, required_fields=required_fields
            )

            if is_valid['is_error']:
                return send_json_response(
                    http_status=HttpStatusCode.BAD_REQUEST.value,
                    response_status=False,
                    message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
                    error=is_valid['data'],
                )

            report_id = data.get('report_id')

            # creating AmazonAdsReportEU object and passing the credentials
            report = AmazonAdsReportEU(credentials=credentials)

            response = report.retrieve_report_download_v2(
                document_id=report_id)

            content = gzip.decompress(response.content)

            content = content.decode('utf-8')

            file_path = config_data.get(
                'UPLOAD_FOLDER') + 'sponsored_display' + '/{}.txt'.format(report_id)
            directory = os.path.dirname(file_path)

            # Create the necessary directories if they don't exist
            os.makedirs(directory, exist_ok=True)

            with open(file_path, 'w') as f:                                                # type: ignore  # noqa: FKA100
                f.write(content)
            f.close()

            json_dict = json.loads(content)

            for display in json_dict:
                asp_id = asp_id
                account_id = account_id
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

                AzSponsoredDisplay.add(asp_id=asp_id, account_id=account_id, payload_date='20230801', ad_group_id=ad_group_id, ad_group_name=ad_group_name, ad_id=ad_id, asin=asin, attributed_conversions_14d=attributed_conversions_14d,
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

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=None, error=None)

        except Exception as exception_error:
            logger.error(
                f'Exception occured while getting Sponsored display data : {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

    @api_time_logger
    @token_required
    def az_ad_data_sync(user_object, account_object):
        """Sync Ad's Report"""
        try:

            logged_in_user = user_object.id
            account_id = account_object.uuid

            data = request.get_json(force=True)

            # Data Validation
            field_types = {'az_ads_profile_ids': list,
                           'default_time_period': str, 'requested_queue': str}

            required_fields = ['az_ads_profile_ids',
                               'default_time_period']

            az_ads_profile_ids = data.get('az_ads_profile_ids')
            default_time_period = data.get('default_time_period')
            queue_name = data.get('requested_queue')

            post_data = field_type_validator(
                request_data=data, field_types=field_types)

            if post_data['is_error']:
                return send_json_response(
                    http_status=HttpStatusCode.BAD_REQUEST.value,
                    response_status=False,
                    message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
                    error=post_data['data'],
                )

            # Check Required Field
            is_valid = required_validator(
                request_data=data, required_fields=required_fields
            )

            if is_valid['is_error']:
                return send_json_response(
                    http_status=HttpStatusCode.BAD_REQUEST.value,
                    response_status=False,
                    message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
                    error=is_valid['data'],
                )

            if 'az_ads_profile_ids' in data:
                del data['az_ads_profile_ids']

            __ad_profile_ids = account_object.az_ads_profile_ids if account_object.az_ads_profile_ids is not None else None

            if __ad_profile_ids:

                for az_ads_profile_id in az_ads_profile_ids:
                    if az_ads_profile_id in __ad_profile_ids:

                        if queue_name is None:

                            # Send default as true to sync default dates.
                            data.update(
                                {'default_sync': True, 'az_ads_profile_id': str(az_ads_profile_id), 'default_time_period': default_time_period})

                            """Define timing in minutes"""
                            queue_az_sponsored_brand_video = 240
                            queue_az_sponsored_display = 480
                            queue_az_sponsored_product = 720
                            queue_mr_performance_zone = 1440

                            """ queuing Sponsored Brand Banner and Video Report"""
                            add_queue_task_and_enqueue(queue_name=QueueName.AZ_SPONSORED_BRAND, account_id=account_id,
                                                       logged_in_user=user_object.id, entity_type=EntityType.AZ_SPONSORED_BRAND_BANNER.value, data=data)
                            add_queue_task_and_enqueue(queue_name=QueueName.AZ_SPONSORED_BRAND, account_id=account_id,
                                                       logged_in_user=user_object.id, entity_type=EntityType.AZ_SPONSORED_BRAND_VIDEO.value, data=data, time_delta=timedelta(minutes=queue_az_sponsored_brand_video))
                            """ queuing Sponsored Display report """
                            add_queue_task_and_enqueue(queue_name=QueueName.AZ_SPONSORED_DISPLAY, account_id=account_id,
                                                       logged_in_user=user_object.id, entity_type=EntityType.AZ_SPONSORED_DISPLAY.value, data=data, time_delta=timedelta(minutes=queue_az_sponsored_display))
                            """ queuing Sponsored Product report """
                            add_queue_task_and_enqueue(queue_name=QueueName.AZ_SPONSORED_PRODUCT, account_id=account_id,
                                                       logged_in_user=user_object.id, entity_type=EntityType.AZ_SPONSORED_PRODUCT.value, data=data, time_delta=timedelta(minutes=queue_az_sponsored_product))
                            """ queuing Performance Zone """
                            add_queue_task_and_enqueue(queue_name=QueueName.AZ_PERFORMANCE_ZONE, account_id=account_id,
                                                       logged_in_user=logged_in_user, entity_type=EntityType.MR_PERFORMANCE_ZONE.value, data=data, time_delta=timedelta(minutes=queue_mr_performance_zone))

                        else:

                            data.update(
                                {'default_sync': True, 'az_ads_profile_id': az_ads_profile_id, 'default_time_period': default_time_period})

                            if queue_name == QueueName.AZ_SPONSORED_BRAND:
                                """ queuing Sponsored Brand Banner and Video Report"""
                                add_queue_task_and_enqueue(queue_name=QueueName.AZ_SPONSORED_BRAND, account_id=account_object.uuid,
                                                           logged_in_user=user_object.id, entity_type=EntityType.AZ_SPONSORED_BRAND_BANNER.value, data=data)
                                add_queue_task_and_enqueue(queue_name=QueueName.AZ_SPONSORED_BRAND, account_id=account_object.uuid,
                                                           logged_in_user=user_object.id, entity_type=EntityType.AZ_SPONSORED_BRAND_VIDEO.value, data=data)
                            elif queue_name == QueueName.AZ_SPONSORED_DISPLAY:
                                """ queuing Sponsored Display report """
                                add_queue_task_and_enqueue(queue_name=QueueName.AZ_SPONSORED_DISPLAY, account_id=account_object.uuid,
                                                           logged_in_user=user_object.id, entity_type=EntityType.AZ_SPONSORED_DISPLAY.value, data=data)
                            elif queue_name == QueueName.AZ_SPONSORED_PRODUCT:
                                """ queuing Sponsored Product report """
                                add_queue_task_and_enqueue(queue_name=QueueName.AZ_SPONSORED_PRODUCT, account_id=account_object.uuid,
                                                           logged_in_user=user_object.id, entity_type=EntityType.AZ_SPONSORED_PRODUCT.value, data=data)
                            elif queue_name == QueueName.AZ_PERFORMANCE_ZONE:
                                """ queuing Performance Zone """
                                add_queue_task_and_enqueue(queue_name=QueueName.AZ_PERFORMANCE_ZONE, account_id=account_id,
                                                           logged_in_user=logged_in_user, entity_type=EntityType.MR_PERFORMANCE_ZONE.value, data=data)

                result = {
                    'queued_item': queue_name if queue_name is not None else 'ALL'
                }

                return send_json_response(
                    http_status=200,
                    response_status=True,
                    message_key=ResponseMessageKeys.AZ_AD_SYNC_SUCCESS.value,
                    data=result,
                )

            return send_json_response(
                http_status=HttpStatusCode.UNAUTHORIZED.value,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

        except BadRequest as exception_error:
            logger.error(f'POST -> User Login Failed: {exception_error}')
            return send_json_response(
                http_status=HttpStatusCode.BAD_REQUEST.value,
                response_status=False,
                message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
            )

        except Exception as exception_error:
            logger.error(
                f"Exception occured while syncing Ad's report: {exception_error}")
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )
