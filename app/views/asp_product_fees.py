"""Contains Amazon Seller Orders Report related API definitions."""
from app import logger
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import ResponseMessageKeys
from app.helpers.decorators import api_time_logger
from app.helpers.decorators import token_required
from app.helpers.utility import ASpMarketplaceId
from app.helpers.utility import field_type_validator
from app.helpers.utility import required_validator
from app.helpers.utility import send_json_response
from flask import request
from providers.amazon_sp_client import AmazonReportEU
from werkzeug.exceptions import BadRequest


class AspProductFees:
    """API to fetch fees estimates related ro product"""

    @staticmethod
    @api_time_logger
    @token_required
    def get_fees_estimate_by_asin(user_object, account_object):
        """To get fees estimate"""
        try:

            data = request.get_json(force=True)

            # Data Validation
            field_types = {'asin': str, 'price': int}
            required_fields = ['asin']

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

            asin = data.get('asin').strip()
            price = data.get('price')

            credentials = account_object.retrieve_asp_credentials(account_object)[
                0]

            payload = {
                'FeesEstimateRequest': {
                    'MarketplaceId': ASpMarketplaceId.IN.value,
                    'PriceToEstimateFees': {
                        'ListingPrice': {
                            'CurrencyCode': 'INR',
                            'Amount': price
                        },
                        'Shipping': {
                            'CurrencyCode': 'INR',
                            'Amount': 29662703.480306223
                        },
                        'Points': {
                            'PointsNumber': -45403595,
                            'PointsMonetaryValue': {
                                'CurrencyCode': 'do',
                                'Amount': 54821163.63579109
                            }
                        }
                    },
                    'Identifier': asin,
                    'IsAmazonFulfilled': 'false',
                    'OptionalFulfillmentProgram': 'FBA_CORE'
                }
            }

            report = AmazonReportEU(credentials=credentials)

            response = report.get_fees_estimates(payload=payload, asin=asin)
            return response

        except BadRequest as exception_error:
            logger.error(
                f'POST -> GET Fees estimate by Asin: {exception_error}')
            return send_json_response(
                http_status=HttpStatusCode.BAD_REQUEST.value,
                response_status=False,
                message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
            )

        except Exception as exception_error:
            """Exception while getting fees estimate by Asin"""
            logger.error(
                f'GET -> Order Report creation Failed: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    def get_fees_estimate_by_sku(user_object, account_object):
        """Returns the estimated fees for the item indicated by the specified seller SKU in the marketplace specified in the request body"""

        try:
            data = request.get_json(force=True)

            # Data Validation
            field_types = {'seller_sku': str, 'price': int}
            required_fields = ['seller_sku', 'price']

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

            seller_sku = data.get('seller_sku').strip()
            price = data.get('price')

            credentials = account_object.retrieve_asp_credentials(account_object)[
                0]

            payload = {
                'FeesEstimateRequest': {
                    'Identifier': seller_sku,
                    # 'Identifier': identifier or str(price),
                    'PriceToEstimateFees': {
                        'ListingPrice': {
                            'Amount': price,
                            'CurrencyCode': 'INR'
                        },
                        # 'Shipping': {
                        #     'Amount': shipping_price,
                        #     'CurrencyCode': 'INR'
                        # } if shipping_price else None,
                        # 'Points': points or None
                    },
                    'IsAmazonFulfilled': False,
                    'MarketplaceId': ASpMarketplaceId.IN.value
                }
            }

            # payload = {
            #     "IdType":  'SellerSKU', # Enum - (ASIN, SellerSKU)
            #     "IdValue": seller_sku,
            #     'FeesEstimateRequest': {
            #         'MarketplaceId': get_asp_market_place_ids(),
            #         'PriceToEstimateFees': {
            #             'ListingPrice': {
            #                 'CurrencyCode': 'INR',
            #                 'Amount': '1300'
            #             },
            #             'Shipping': {
            #                 'CurrencyCode': 'INR',
            #                 'Amount': '200'
            #             },
            #             'Points': {
            #                 'PointsNumber': '100',
            #                 'PointsMonetaryValue': {
            #                     'CurrencyCode': 'INR',
            #                     'Amount': '100'
            #                 }
            #             }
            #         },
            #         'Identifier': str(uuid4()),
            #         "IsAmazonFulfilled": False,
            #         "OptionalFulfillmentProgram": "FBA_CORE"
            #     }
            # }

            report = AmazonReportEU(credentials=credentials)

            response = report.get_my_fees_estimate_for_sku(
                payload=payload, seller_sku=seller_sku)

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response, error=None)

        except BadRequest as exception_error:
            logger.error(
                f'POST -> GET Fees estimate by SKU: {exception_error}')
            return send_json_response(
                http_status=HttpStatusCode.BAD_REQUEST.value,
                response_status=False,
                message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
            )

        except Exception as exception_error:
            """Exception while getting fees estimate by SKU"""
            logger.error(
                f'GET -> Order Report creation Failed: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )
