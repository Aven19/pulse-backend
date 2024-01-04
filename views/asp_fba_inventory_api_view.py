"""Contains Amazon Seller FBA Inventory related API definitions."""

import time

from app import logger
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import ResponseMessageKeys
from app.helpers.decorators import api_time_logger
from app.helpers.decorators import token_required
from app.helpers.utility import get_asp_market_place_ids
from app.helpers.utility import send_json_response
from app.models.az_item_master import AzItemMaster
from flask import request
from providers.amazon_sp_client import AmazonReportEU


class AspFbaInventoryApiView:
    """class for getting fba returns report from sp-api"""

    # FBA Concessions Reports
    @staticmethod
    @api_time_logger
    @token_required
    def get_fba_inventory_levels(user_object, account_object):
        """Create FBA Returns Report"""

        try:

            asp_id = account_object.asp_id
            account_id = account_object.uuid

            credentials = account_object.retrieve_asp_credentials(account_object)[
                0]

            data = request.get_json(force=True)

            seller_sku = data.get('seller_sku', None)   # type: ignore  # noqa: FKA100

            if seller_sku:

                params = {
                    'sellerSku': seller_sku,
                    'details': True,
                    'startDateTime': '2023-12-17T00:00:00Z',
                    'granularityType': 'Marketplace',
                    'granularityId': get_asp_market_place_ids(),
                    'marketplaceIds': get_asp_market_place_ids()
                }

                all_inventory_summaries = []

                while True:

                    report = AmazonReportEU(credentials=credentials)

                    response = report.get_fba_inventory(params=params)

                    # Process the current page of results
                    inventory_summaries = response.get('payload', {}).get('inventorySummaries', [])   # type: ignore  # noqa: FKA100
                    all_inventory_summaries.extend(inventory_summaries)

                    params['nextToken'] = response.get('pagination', {}).get('nextToken')   # type: ignore  # noqa: FKA100

                    # Break the loop if there are no more pages
                    if not params['nextToken']:
                        break

                    # Add sleep time to ensure 2 requests per second
                    time.sleep(0.5)

                prepare_fba_json = []
                for inventory in all_inventory_summaries:
                    # fba_inventory_json = json.dumps(inventory)
                    prepare_fba_json.append({
                        'fba_inventory_json': inventory,
                        'account_id': account_id,
                        'selling_partner_id': asp_id,
                        'seller_sku': inventory.get('sellerSku'),
                        'asin': inventory.get('asin'),
                        'item_name': inventory.get('productName'),
                        'created_at': int(time.time())
                    })

                logger.info(len(all_inventory_summaries))
                logger.info(len(prepare_fba_json))
                logger.info(prepare_fba_json)

                AzItemMaster.upsert_fba_inventory(prepare_fba_json)

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.UPDATED.value, error=None)

        except Exception as exception_error:
            """ Exception while fetching FBA Inventory API """
            logger.error(
                f'GET -> FBA Inventory API fetch Failed: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )
