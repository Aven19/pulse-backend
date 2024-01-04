"""Contains Amazon Seller Settlement Report related API definitions."""
import io

from app import config_data
from app import logger
from app.helpers.constants import ASpReportType
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import PAGE_LIMIT
from app.helpers.constants import ResponseMessageKeys
from app.helpers.decorators import api_time_logger
from app.helpers.utility import get_asp_market_place_ids
from app.helpers.utility import get_created_since
from app.helpers.utility import get_created_until
from app.helpers.utility import send_json_response
from app.models.az_report import AzReport
from app.models.az_settlement import AzSettlement
from flask import request
import numpy as np
import pandas as pd
from providers.amazon_sp_client import AmazonReportEU
import requests


@api_time_logger
def get_report():
    """get settlement report using sp-apis"""

    try:

        created_since = request.args.get('created_since')
        created_until = request.args.get('created_until')

        credentials = {
            'refresh_token': config_data.get('SP_REFRESH_TOKEN'),
            'client_id': config_data.get('SP_LWA_APP_ID'),
            'client_secret': config_data.get('SP_LWA_CLIENT_SECRET')
        }

        payload = {
            'reportTypes': ASpReportType.SETTLEMENT_REPORT_DATA_FLAT.value,
            'marketplaceIds': get_asp_market_place_ids(),
            'createdSince': get_created_since(created_since=created_since),
            'createdUntil': get_created_until(created_until=created_until),
            'pageSize': PAGE_LIMIT,
        }
        # creating AmazonReportEU object and passing the credentials
        report = AmazonReportEU(credentials=credentials)

        # calling create report function of report object and passing the payload
        response = report.get_scheduled_report(params=payload)
        if not response.get('nextToken'):

            for each_report in response['reports']:
                report_type = each_report.get('reportType')
                report_id = each_report.get('reportId')
                report_status = each_report.get('processingStatus')
                report_document_id = each_report.get('reportDocumentId')

                AzReport.add(seller_partner_id=config_data.get('SP_ID'), type=report_type, reference_id=report_id,
                             status=report_status, document_id=report_document_id)

                # getting the report document url from the response
                report = AmazonReportEU(credentials=credentials)

                get_report = report.retrieve_report(report_document_id)

                if 'url' in get_report:
                    file_url = get_report['url']
                else:
                    logger.error(
                        'file url not found for settlement report')
                    return send_json_response(
                        http_status=500,
                        response_status=False,
                        message_key=ResponseMessageKeys.FAILED.value
                    )

                # making request to the report document url to get the file data
                file_data = requests.get(file_url, stream=True)

                # reading text in file_data using pandas and converting into dataframe
                data_frame = pd.read_csv(io.StringIO(file_data.text),
                                         sep='\t', index_col=False, header=None, skiprows=1)

                # transforming the data
                data_frame.columns = ['settlement_id', 'settlement_start_date', 'settlement_end_date', 'deposit_date',
                                      'total_amount', 'currency', 'transaction_type', 'order_id', 'merchant_order_id', 'adjustment_id',
                                      'shipment_id', 'marketplace_name', 'shipment_fee_type', 'shipment_fee_amount',
                                      'order_fee_type', 'order_fee_amount', 'fulfillment_id', 'posted_date', 'order_item_code',
                                      'merchant_order_item_id', 'merchant_adjustment_item_id', 'sku', 'quantity_purchased',
                                      'price_type', 'price_amount', 'item_related_fee_type', 'item_related_fee_amount', 'misc_fee_amount',
                                      'other_fee_amount', 'other_fee_reason_description', 'promotion_id', 'promotion_type',
                                      'promotion_amount', 'direct_payment_type', 'direct_payment_amount', 'other_amount'
                                      ]

                data_frame['posted_date'] = pd.to_datetime(
                    data_frame['posted_date'], format='%Y-%m-%dT%H:%M:%S%z').dt.date

                # data_frame.to_csv(config_data.get('UPLOAD_FOLDER') + ASpReportType.SETTLEMENT_REPORT_DATA_FLAT.value.lower(
                # ) + '/{}.csv'.format(each_report['reportDocumentId']), index=False)

                # transformation
                data_frame = data_frame.fillna(np.nan).replace([np.nan], [None])   # type: ignore  # noqa: FKA100

                for index, row in data_frame[1:].iterrows():
                    AzSettlement.add(
                        selling_partner_id=config_data['SELLER_ID'],
                        settlement_id=row['settlement_id'],
                        settlement_start_date=row['settlement_start_date'],
                        settlement_end_date=row['settlement_end_date'],
                        deposit_date=row['deposit_date'],
                        total_amount=row['total_amount'],
                        currency=row['currency'],
                        transaction_type=row['transaction_type'],
                        order_id=row['order_id'],
                        merchant_order_id=row['merchant_order_id'],
                        adjustment_id=row['adjustment_id'],
                        shipment_id=row['shipment_id'],
                        marketplace_name=row['marketplace_name'],
                        shipment_fee_type=row['shipment_fee_type'],
                        shipment_fee_amount=row['shipment_fee_amount'],
                        order_fee_type=row['order_fee_type'],
                        order_fee_amount=row['order_fee_amount'],
                        fulfillment_id=row['fulfillment_id'],
                        posted_date=row['posted_date'],
                        order_item_code=row['order_item_code'],
                        merchant_order_item_id=row['merchant_order_item_id'],
                        merchant_adjustment_item_id=row['merchant_adjustment_item_id'],
                        sku=row['sku'],
                        quantity_purchased=row['quantity_purchased'],
                        price_type=row['price_type'],
                        price_amount=row['price_amount'],
                        item_related_fee_type=row['item_related_fee_type'],
                        item_related_fee_amount=row['item_related_fee_amount'],
                        misc_fee_amount=row['misc_fee_amount'],
                        other_fee_amount=row['other_fee_amount'],
                        other_fee_reason_description=row['other_fee_reason_description'],
                        promotion_id=row['promotion_id'],
                        promotion_type=row['promotion_type'],
                        promotion_amount=row['promotion_amount'],
                        direct_payment_type=row['direct_payment_type'],
                        direct_payment_amount=row['direct_payment_amount'],
                        other_amount=row['other_amount'])

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response, error=None)

        else:
            # main_response = {"reports": []}
            response = report.get_report_operation(payload=payload)

            # print(response)
            # while True:
            #     next_token = urllib.parse.quote_plus(response['nextToken'])
            #     payload = {'nextToken': next_token}
            #     print(payload)
            #     response = report.get_report_operation(payload=payload)
            #     print(response)
            #     for each in response['reports']:
            #         main_response['reports'].append(each)

            #     if not response.get('nextToken'):
            #         break

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response, error=None)

    except Exception as exception_error:
        logger.error(
            f'Exception occured while getting settlement report: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value
        )
