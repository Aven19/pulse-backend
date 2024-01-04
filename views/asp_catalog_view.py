"""Contains Amazon Catalog related API definitions."""

from app import logger
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import ResponseMessageKeys
from app.helpers.decorators import api_time_logger
from app.helpers.decorators import token_required
from app.helpers.utility import field_type_validator
from app.helpers.utility import get_asp_market_place_ids
from app.helpers.utility import required_validator
from app.helpers.utility import send_json_response
from flask import request
from providers.amazon_sp_client import AmazonReportEU
# from app.helpers.constants import ASpReportType
# from app.models.az_report import AzReport


class AspCatalogView:
    """class for Catalog Item view"""

    @staticmethod
    @api_time_logger
    @token_required
    def get_item(user_object, account_object):
        """Retrieves details for an item in the Amazon catalog."""
        try:

            data = request.get_json(force=True)

            # Data Validation
            field_types = {'asin': str}
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

            # account_id = account_object.uuid
            # asp_id = account_object.asp_id
            credentials = account_object.retrieve_asp_credentials(account_object)[
                0]

            asin = data.get('asin').strip()

            params = {
                'includedData': 'attributes,images,salesRanks,summaries',
                'marketplaceIds': get_asp_market_place_ids()
            }

            # creating AmazonReportEU object and passing the credentials
            report = AmazonReportEU(credentials=credentials)

            # calling create report function of report object and passing the payload
            response = report.get_catalog_item(params=params, asin=asin)

            # adding the report_type, report_id to the report table
            # AzReport.add(account_id=account_id,
            #             seller_partner_id=asp_id, type=ASpReportType.GET_CATALOG_ITEM.value, reference_id=response['reportId'])

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response, error=None)

        except Exception as exception_error:
            logger.error(
                f'Exception occured while Fetching Catalog Item: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )
