"""order api view"""

from app import logger
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import ResponseMessageKeys
from app.helpers.decorators import api_time_logger
from app.helpers.decorators import asp_credentials_required
from app.helpers.decorators import token_required
from app.helpers.utility import flatten_json
from app.helpers.utility import get_asp_market_place_ids
from app.helpers.utility import get_created_since
from app.helpers.utility import send_json_response
from app.helpers.utility import string_to_bool
from app.models.az_order_items import AzOrderItems
from app.models.az_orders import AzOrders
from flask import request
from providers.amazon_sp_client import AmazonReportEU


class AZOrdersView:
    """class for amazon orders api"""

    @api_time_logger
    @token_required
    def get_orders(user_object, account_object):
        """get orders from amazon getOrders api """

        start_date = request.args.get('CreatedAfter')
        # end_date = request.args.get('CreatedBefore')
        # convert_date_string(request.args.get('CreatedBefore'))
        # request.args.get('CreatedBefore')

        try:

            credentials = account_object.retrieve_asp_credentials(account_object)[
                0]

            params = {
                'CreatedAfter': get_created_since(start_date),
                # 'CreatedBefore': get_created_until(end_date),
                'MarketplaceIds': get_asp_market_place_ids()
            }

            # creating AmazonReportEU object and passing the credentials
            report = AmazonReportEU(credentials=credentials)

            # calling create report function of report object and passing the payload
            response = report.get_orders(params=params)
            logger.info(response)

            while True:

                orders_data = response['payload']['Orders']

                for orders in orders_data:
                    orders_flatten = flatten_json(orders)
                    buyer_email = orders_flatten.get('BuyerInfo_BuyerEmail')
                    amazon_order_id = orders_flatten.get('AmazonOrderId')
                    earliest_ship_date = orders_flatten.get('EarliestShipDate')
                    sales_channel = orders_flatten.get('SalesChannel')
                    order_status = orders_flatten.get('OrderStatus')
                    number_of_items_shipped = orders_flatten.get(
                        'NumberOfItemsShipped')
                    order_type = orders_flatten.get('StandardOrder')
                    is_premium_order = orders_flatten.get('IsPremiumOrder')
                    is_prime = orders_flatten.get('IsPrime')
                    fulfillment_channel = orders_flatten.get(
                        'FulfillmentChannel')
                    number_of_items_unshipped = orders_flatten.get(
                        'NumberOfItemsUnshipped')
                    has_regulated_items = orders_flatten.get(
                        'HasRegulatedItems')
                    is_replacement_order = orders_flatten.get(
                        'IsReplacementOrder')
                    is_sold_by_ab = orders_flatten.get('IsSoldByAB')
                    latest_ship_date = orders_flatten.get('LatestShipDate')
                    ship_service_level = orders_flatten.get('ShipServiceLevel')
                    is_ispu = orders_flatten.get('IsISPU')
                    marketplace_id = orders_flatten.get('MarketplaceId')
                    purchase_date = orders_flatten.get('PurchaseDate')
                    shipping_address_state_or_region = orders_flatten.get(
                        'ShippingAddress_StateOrRegion')
                    shipping_address_postal_code = orders_flatten.get(
                        'ShippingAddress_PostalCode')
                    shipping_address_city = orders_flatten.get(
                        'ShippingAddress_City')
                    shipping_address_country_code = orders_flatten.get(
                        'ShippingAddress_CountryCode')
                    is_access_point_order = orders_flatten.get(
                        'IsAccessPointOrder')
                    seller_order_id = orders_flatten.get('SellerOrderId')
                    payment_method = orders_flatten.get('PaymentMethod')
                    is_business_order = orders_flatten.get('IsBusinessOrder')
                    order_total_currency_code = orders_flatten.get(
                        'OrderTotal_CurrencyCode')
                    order_total_amount = orders_flatten.get(
                        'OrderTotal_Amount')
                    payment_method_details = orders_flatten.get(
                        'PaymentMethodDetails_0')
                    is_global_express_enabled = orders_flatten.get(
                        'IsGlobalExpressEnabled')
                    last_update_date = orders_flatten.get('LastUpdateDate')
                    shipment_service_level_category = orders_flatten.get(
                        'ShipmentServiceLevelCategory')

                    # transform string to bool
                    is_premium_order = string_to_bool(is_premium_order)
                    is_prime = string_to_bool(is_prime)
                    has_regulated_items = string_to_bool(has_regulated_items)
                    is_replacement_order = string_to_bool(is_replacement_order)
                    is_sold_by_ab = string_to_bool(is_sold_by_ab)
                    is_ispu = string_to_bool(is_ispu)
                    is_access_point_order = string_to_bool(
                        is_access_point_order)
                    is_business_order = string_to_bool(is_business_order)
                    is_global_express_enabled = string_to_bool(
                        is_global_express_enabled)

                    AzOrders.insert_or_update(selling_partner_id=account_object.asp_id, account_id=account_object.uuid, buyer_email=buyer_email, amazon_order_id=amazon_order_id, earliest_ship_date=earliest_ship_date, sales_channel=sales_channel,
                                              order_status=order_status, number_of_items_shipped=number_of_items_shipped, order_type=order_type, is_premium_order=is_premium_order, is_prime=is_prime, fulfillment_channel=fulfillment_channel,
                                              number_of_items_unshipped=number_of_items_unshipped, has_regulated_items=has_regulated_items, is_replacement_order=is_replacement_order, is_sold_by_ab=is_sold_by_ab, latest_ship_date=latest_ship_date,
                                              ship_service_level=ship_service_level, is_ispu=is_ispu, marketplace_id=marketplace_id, purchase_date=purchase_date, shipping_address_state_or_region=shipping_address_state_or_region, shipping_address_postal_code=shipping_address_postal_code,
                                              shipping_address_city=shipping_address_city, shipping_address_country_code=shipping_address_country_code, is_access_point_order=is_access_point_order, seller_order_id=seller_order_id, payment_method=payment_method,
                                              is_business_order=is_business_order, order_total_currency_code=order_total_currency_code, order_total_amount=order_total_amount, payment_method_details=payment_method_details, is_global_express_enabled=is_global_express_enabled, last_update_date=last_update_date, shipment_service_level_category=shipment_service_level_category)

                if 'NextToken' not in response['payload']:
                    break

                next_token = response['payload']['NextToken']
                params = {'NextToken': next_token}
                response = report.get_next_page_result(params=params)

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=None, error=None)

        except Exception as exception_error:
            logger.error(
                f'Exception occured while getting getOrder data : {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

    @api_time_logger
    @asp_credentials_required
    def get_order_items(credentials):
        """get order items from amazon getOrderitems api """

        order_id = request.args.get('order_id')

        try:

            credentials = {
                'seller_partner_id': credentials['seller_partner_id'],
                'refresh_token': credentials['refresh_token'],
                'client_id': credentials['client_id'],
                'client_secret': credentials['client_secret']
            }

            # creating AmazonReportEU object and passing the credentials
            report = AmazonReportEU(credentials=credentials)

            # calling create report function of report object and passing the payload
            response = report.get_order_items(order_id=order_id)

            amazon_order_id = response['payload'].get('AmazonOrderId')

            if not amazon_order_id:
                return send_json_response(
                    http_status=500,
                    response_status=False,
                    message_key=ResponseMessageKeys.FAILED.value
                )

            order_items_data = response['payload'].get('OrderItems')

            if order_items_data:
                for order_item in order_items_data:
                    order_item_flatten = flatten_json(order_item)
                    selling_partner_id = credentials['seller_partner_id']
                    product_info_number_of_items = order_item_flatten.get(
                        'ProductInfo_NumberOfItems')
                    item_tax_currency_code = order_item_flatten.get(
                        'ItemTax_CurrencyCode')
                    item_tax_amount = order_item_flatten.get('ItemTax_Amount')
                    quantity_shipped = order_item_flatten.get(
                        'QuantityShipped')
                    item_price_currency_code = order_item_flatten.get(
                        'ItemPrice_CurrencyCode')
                    item_price_amount = order_item_flatten.get(
                        'ItemPrice_Amount')
                    asin = order_item_flatten.get('ASIN')
                    seller_sku = order_item_flatten.get('SellerSKU')
                    title = order_item_flatten.get('Title')
                    is_gift = order_item_flatten.get('IsGift')
                    is_transparency = order_item_flatten.get('IsTransparency')
                    quantity_ordered = order_item_flatten.get(
                        'QuantityOrdered')
                    promotion_discount_tax_currency_code = order_item_flatten.get(
                        'PromotionDiscountTax_CurrencyCode')
                    promotion_discount_tax_amount = order_item_flatten.get(
                        'PromotionDiscountTax_Amount')
                    promotion_ids = order_item_flatten.get('PromotionIds_0')
                    promotion_discount_currency_code = order_item_flatten.get(
                        'PromotionDiscount_CurrencyCode')
                    promotion_discount_amount = order_item_flatten.get(
                        'PromotionDiscount_Amount')
                    order_item_id = order_item_flatten.get('OrderItemId')

                    # transform string to bool
                    is_gift = string_to_bool(is_gift)
                    is_transparency = string_to_bool(is_transparency)

                    AzOrderItems.insert_or_update(selling_partner_id=selling_partner_id, amazon_order_id=amazon_order_id, product_info_number_of_items=product_info_number_of_items, item_tax_currency_code=item_tax_currency_code, item_tax_amount=item_tax_amount,
                                                  quantity_shipped=quantity_shipped, item_price_currency_code=item_price_currency_code, item_price_amount=item_price_amount,
                                                  asin=asin, seller_sku=seller_sku, title=title, is_gift=is_gift, is_transparency=is_transparency, quantity_ordered=quantity_ordered,
                                                  promotion_discount_tax_currency_code=promotion_discount_tax_currency_code, promotion_discount_tax_amount=promotion_discount_tax_amount, promotion_ids=promotion_ids, promotion_discount_currency_code=promotion_discount_currency_code,
                                                  promotion_discount_amount=promotion_discount_amount, order_item_id=order_item_id)

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=None, error=None)

        except Exception as exception_error:
            logger.error(
                f'Exception occured while getting order items data : {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )
