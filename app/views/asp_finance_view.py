"""view for finance apis"""

import time

from app import db
from app import logger
from app.helpers.constants import AspFinanceEventList
from app.helpers.constants import DbAnomalies
from app.helpers.constants import EntityType
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import QueueName
from app.helpers.constants import ResponseMessageKeys
from app.helpers.decorators import api_time_logger
from app.helpers.decorators import token_required
from app.helpers.queue_helper import add_queue_task_and_enqueue
from app.helpers.utility import convert_text_to_snake_case
from app.helpers.utility import field_type_validator
from app.helpers.utility import required_validator
from app.helpers.utility import send_json_response
from app.models.az_financial_event import AzFinancialEvent
from app.models.az_item_master import AzItemMaster
from app.models.az_product_performance import AzProductPerformance
from flask import request
from werkzeug.exceptions import BadRequest


class AspFinanceView():
    """Class containing views for amazon finance apis"""

    @staticmethod
    def create(
            account_id: str, asp_id: str, az_order_id=None, seller_order_id=None, market_place=None, posted_date=None,
            event_type=None, event_json=None, finance_type=None, finance_value=None, seller_sku=None):
        """ Create a new Finance event with an even type """

        category, brand = None, None

        if seller_sku:
            category, brand = AzItemMaster.get_category_brand_by_sku(
                account_id=account_id, asp_id=asp_id, seller_sku=seller_sku)

        finance_event_obj = AzFinancialEvent(
            account_id=account_id,
            asp_id=asp_id,
            category=category,
            brand=brand,
            az_order_id=az_order_id,
            seller_order_id=seller_order_id,
            market_place=market_place,
            posted_date=posted_date,
            event_type=event_type,
            event_json=event_json,
            finance_type=finance_type,
            finance_value=finance_value,
            seller_sku=seller_sku,
            created_at=int(time.time())
        )

        db.session.add(finance_event_obj)
        return finance_event_obj.save()

    @staticmethod
    def get_by_az_order_id(account_id: str, asp_id: str, az_order_id: str, event_type: str):
        """Filter record by Amazon order id and event type"""
        return db.session.query(AzFinancialEvent).filter(AzFinancialEvent.account_id == account_id, AzFinancialEvent.asp_id == asp_id, AzFinancialEvent.az_order_id == az_order_id,
                                                         AzFinancialEvent.event_type == event_type).first()

    @staticmethod
    def add_update(account_id: str, asp_id: str, az_order_id=None, seller_order_id=None, event_type=None, event_json=None,
                   market_place=None, posted_date=None, finance_type=None, finance_value=None, seller_sku=None):
        """ Add or update an existing Finance event with an even type """

        # logger.info('DB Insert Start')
        # logger.info(az_order_id)
        category, brand = None, None

        if seller_sku:
            category, brand = AzItemMaster.get_category_brand_by_sku(
                account_id=account_id, asp_id=asp_id, seller_sku=seller_sku)

        current_time = int(time.time())
        record = DbAnomalies.UPDATE.value

        # logger.info('Find AZ order Id')
        add_update_finance_event_ob = db.session.query(AzFinancialEvent).filter(
            AzFinancialEvent.account_id == account_id,
            AzFinancialEvent.asp_id == asp_id,
            AzFinancialEvent.az_order_id == az_order_id,
            AzFinancialEvent.event_type == event_type,
            AzFinancialEvent.posted_date == posted_date
        ).first()

        if add_update_finance_event_ob == None:
            # logger.info("Az order Id not found")
            add_update_finance_event_ob = db.session.query(AzFinancialEvent).filter(
                AzFinancialEvent.account_id == account_id,
                AzFinancialEvent.asp_id == asp_id,
                AzFinancialEvent.seller_sku == seller_sku,
                AzFinancialEvent.event_type == event_type,
                AzFinancialEvent.posted_date == posted_date
            ).first()
            if add_update_finance_event_ob is not None:
                # logger.info("Az order Id found")
                # logger.info(az_order_id)
                # logger.info(add_update_finance_event_ob.az_order_id)
                if az_order_id != add_update_finance_event_ob.az_order_id:
                    record = DbAnomalies.INSERTION.value
                    add_update_finance_event_ob = AzFinancialEvent(
                        account_id=account_id,
                        asp_id=asp_id,
                        az_order_id=az_order_id,
                        event_type=event_type,
                        # event_json = event_json,
                        created_at=current_time
                    )

        if add_update_finance_event_ob is None:
            record = DbAnomalies.INSERTION.value
            add_update_finance_event_ob = AzFinancialEvent(
                account_id=account_id,
                asp_id=asp_id,
                az_order_id=az_order_id,
                event_type=event_type,
                # event_json = event_json,
                created_at=current_time
            )

        add_update_finance_event_ob.category = category
        add_update_finance_event_ob.brand = brand
        add_update_finance_event_ob.seller_order_id = seller_order_id
        add_update_finance_event_ob.seller_sku = seller_sku
        add_update_finance_event_ob.market_place = market_place
        add_update_finance_event_ob.posted_date = posted_date
        add_update_finance_event_ob.event_json = [event_json]
        # if isinstance(event_json, dict):
        #     add_update_finance_event_ob.event_json = event_json
        # else:
        #     add_update_finance_event_ob.event_json = {}
        add_update_finance_event_ob.finance_type = finance_type
        add_update_finance_event_ob.finance_value = finance_value

        if record == DbAnomalies.INSERTION.value:
            logger.info(f'Az order to be inserted {az_order_id}')
            db.session.add(add_update_finance_event_ob)
            add_update_finance_event_ob.save()
        else:
            add_update_finance_event_ob.updated_at = current_time
            db.session.commit()

        # logger.info('DB Insert END')

        return add_update_finance_event_ob

    @api_time_logger
    @token_required
    def get_financial_events(user_object, account_object):
        """create finanace event list using sp-apis"""
        try:
            field_types = {'marketplace': str,
                           'posted_after': str, 'posted_before': str, 'max_results_per_page': int}

            required_fields = ['posted_after',
                               'posted_before', 'max_results_per_page']

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

            data = {}

            data.update({'start_datetime': request.args.get(
                'posted_after'), 'end_datetime': request.args.get('posted_before'), 'max_results_per_page': request.args.get('max_results_per_page')})

            # queuing Finance Event report
            add_queue_task_and_enqueue(queue_name=QueueName.FINANCE_EVENT_LIST, account_id=account_object.uuid,
                                       logged_in_user=user_object.id, entity_type=EntityType.FINANCE_EVENT_LIST.value, data=data)

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=None, error=None)

        except Exception as exception_error:
            logger.error(
                f'Exception occured while creating Financial Event report: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

    @api_time_logger
    @token_required
    def get_order_fees(user_object, account_object):
        """Get order related fees"""
        try:
            data = request.get_json(force=True)
            # Data Validation
            field_types = {'az_order_id': str}

            required_fields = ['az_order_id']

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

            az_order_id = data.get('az_order_id').strip()

            response = AspFinanceView.get_price_action(
                account_id=account_object.uuid, asp_id=account_object.asp_id, az_order_id=az_order_id)

            data = {
                'result': response
            }

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=data, error=None)

        except BadRequest as exception_error:
            logger.error(f'POST -> User Login Failed: {exception_error}')
            return send_json_response(
                http_status=HttpStatusCode.BAD_REQUEST.value,
                response_status=False,
                message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
            )

        except Exception as exception_error:
            logger.error(
                f'Exception occured while creating Financial Event report: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

    @staticmethod
    def get_price_action(account_id, asp_id, az_order_id):  # type: ignore  # noqa: C901
        """ Calculate Total Expense, Sales and it's Price break"""

        # Get the financial event data from the database.
        finance_data_event = AzFinancialEvent.get_by_az_order_id(
            account_id=account_id, asp_id=asp_id, az_order_id=az_order_id)

        _az_order_id_dict = {}
        _refund_event_az_order_id_dict = []

        # first_refund_event_processed = False
        service_fee_reverse_fba_fee = 0
        service_fee_total_expense_breakup = {}
        service_fee_market_place_fee = 0
        service_fee_market_place_fee_breakdown = {}

        for finance_data in finance_data_event:
            finance_event = finance_data.event_type
            finance_json = finance_data.event_json

            if finance_event == AspFinanceEventList.SHIPMENT.value:
                for shipped_item in finance_json:

                    _shipment_event_dict = AspFinanceView.shipment_event_list(
                        shipped_item)

                    for _shipment_event_dict_key, _shipment_event_dict_value in _shipment_event_dict.items():  # type: ignore  # noqa: FKA100
                        if _shipment_event_dict_key not in _az_order_id_dict:
                            _az_order_id_dict[_shipment_event_dict_key] = {
                                '_az_order_id': _shipment_event_dict_value.get('az_order_id'),
                                'marketplace_name': _shipment_event_dict_value.get('marketplace_name'),
                                '_seller_order_id': _shipment_event_dict_value.get('seller_order_id'),
                                '_seller_sku': _shipment_event_dict_key,
                                '_units_sold': _shipment_event_dict_value.get('quantity'),
                                '_gross_sales': _shipment_event_dict_value.get('total_sales_amount'),
                                '_gross_sales_breakdown': _shipment_event_dict_value.get('sales_breakup'),
                                'shipment_date': _shipment_event_dict_value.get('posted_date'),
                                '_fba_fee_forward': _shipment_event_dict_value.get('total_expense_amount'),
                                '_market_place_fee': _shipment_event_dict_value.get('total_expense_amount'),
                                '_market_place_fee_breakdown': _shipment_event_dict_value.get('expense_breakup'),
                                '_other_fee': _shipment_event_dict_value.get('other_fees_amount'),
                                '_other_fee_breakdown': _shipment_event_dict_value.get('other_fees_breakup'),
                            }
                        else:
                            _az_order_id_dict[_shipment_event_dict_key]['_units_sold'] += _shipment_event_dict_value.get('quantity', 0)  # type: ignore  # noqa: FKA100
                            _az_order_id_dict[_shipment_event_dict_key]['_gross_sales'] += _shipment_event_dict_value.get('total_sales_amount', 0)  # type: ignore  # noqa: FKA100
                            _az_order_id_dict[_shipment_event_dict_key]['_fba_fee_forward'] += _shipment_event_dict_value.get('total_expense_amount', 0)  # type: ignore  # noqa: FKA100
                            _az_order_id_dict[_shipment_event_dict_key]['_market_place_fee'] += _shipment_event_dict_value.get('total_expense_amount', 0)  # type: ignore  # noqa: FKA100
                            _az_order_id_dict[_shipment_event_dict_key]['_other_fee'] += _shipment_event_dict_value.get('_other_fee_breakdown', 0)  # type: ignore  # noqa: FKA100

                            for _gross_sales_bd_shipment_key, _gross_sales_bd_shipment_value in _shipment_event_dict_value.get('_gross_sales_breakdown', {}).items():  # type: ignore  # noqa: FKA100
                                _az_order_id_dict[_shipment_event_dict_key]['_market_place_fee_breakdown'][_gross_sales_bd_shipment_key] = _az_order_id_dict.get(_shipment_event_dict_key).get('_market_place_fee_breakdown').get(_gross_sales_bd_shipment_key, 0) + _gross_sales_bd_shipment_value  # type: ignore  # noqa: FKA100

                            for _expense_breakup_shipment_key, _expense_breakup_shipment_value in _shipment_event_dict_value.get('expense_breakup', {}).items():  # type: ignore  # noqa: FKA100
                                _az_order_id_dict[_shipment_event_dict_key]['_market_place_fee_breakdown'][_expense_breakup_shipment_key] = _az_order_id_dict.get(_shipment_event_dict_key).get('_market_place_fee_breakdown').get(_expense_breakup_shipment_key, 0) + _expense_breakup_shipment_value  # type: ignore  # noqa: FKA100

                            for _other_fee_shipment_key, _other_fee_shipment_value in _shipment_event_dict_value.get('other_fees_breakup', {}).items():  # type: ignore  # noqa: FKA100
                                _az_order_id_dict[_shipment_event_dict_key]['_other_fee_breakdown'][_other_fee_shipment_key] = _az_order_id_dict.get(_shipment_event_dict_key).get('_other_fee_breakdown').get(_other_fee_shipment_key, 0) + _other_fee_shipment_value   # type: ignore  # noqa: FKA100

            if finance_event == AspFinanceEventList.SERVICE_FEE.value:
                for service_fee in finance_json:
                    az_order_id, total_expense_amount, expense_breakup = AspFinanceView.service_fee_event_list(
                        service_fee)

                    service_fee_reverse_fba_fee += expense_breakup.get(
                        'total_expense_amount')

                    service_fee_total_expense_breakup = expense_breakup.get(
                        'refused_delivery')

                    service_fee_market_place_fee += expense_breakup.get(
                        'total_expense_amount')

                    if service_fee_total_expense_breakup:
                        for key, value in service_fee_total_expense_breakup.items():  # type: ignore  # noqa: FKA100
                            service_fee_market_place_fee_breakdown[key] = service_fee_market_place_fee_breakdown.get(key, 0) + value  # type: ignore  # noqa: FKA100

            if finance_event == AspFinanceEventList.REFUND.value:
                for shipped_item in finance_json:
                    _refund_event_dict = AspFinanceView.refund_event_list(
                        shipped_item)

                    for _refund_event_dict_key, _refund_event_dict_value in _refund_event_dict.items():

                        if _refund_event_dict_key not in _az_order_id_dict:
                            _az_order_id_dict[_refund_event_dict_key] = {
                                '_az_order_id': _refund_event_dict_value.get('az_order_id'),
                                'marketplace_name': _refund_event_dict_value.get('marketplace_name'),
                                'refund_date': _refund_event_dict_value.get('posted_date'),
                                '_seller_order_id': _refund_event_dict_value.get('seller_order_id'),
                                '_seller_sku': _refund_event_dict_key,
                                'order_item_ids': _refund_event_dict_value.get('order_item_ids'),
                                '_units_returned': _refund_event_dict_value.get('quantity'),
                                '_refunds': _refund_event_dict_value.get('total_sales_amount'),
                                '_fba_fee_reverse': _refund_event_dict_value.get('total_expense_amount'),
                                '_market_place_fee': _refund_event_dict_value.get('total_expense_amount'),
                                '_market_place_fee_breakdown': _refund_event_dict_value.get('expense_breakup'),
                                '_other_fee': _refund_event_dict_value.get('other_fees_amount'),
                                '_other_fee_breakdown': _refund_event_dict_value.get('other_fees_breakup')
                            }

                        else:
                            _az_order_id_dict[_refund_event_dict_key]['refund_date'] = _refund_event_dict_value.get(
                                'posted_date')
                            # _az_order_id_dict[_refund_event_dict_key]['order_item_ids'] = _refund_event_dict_value.get('order_item_ids')
                            _az_order_id_dict[_refund_event_dict_key]['_units_returned'] = _refund_event_dict_value.get(
                                'quantity')
                            _az_order_id_dict[_refund_event_dict_key]['_refunds'] = _refund_event_dict_value.get(
                                'total_sales_amount')
                            _az_order_id_dict[_refund_event_dict_key]['_other_fee'] += _refund_event_dict_value.get(
                                'other_fees_amount')
                            _az_order_id_dict[_refund_event_dict_key]['_market_place_fee'] += _refund_event_dict_value.get(
                                'total_expense_amount')
                            _az_order_id_dict[_refund_event_dict_key]['_fba_fee_reverse'] = _refund_event_dict_value.get(
                                'total_expense_amount')

                            for _expense_breakup_key, _expense_breakup_value in _refund_event_dict_value.get('expense_breakup', {}).items():  # type: ignore  # noqa: FKA100
                                _az_order_id_dict[_refund_event_dict_key]['_market_place_fee_breakdown'][_expense_breakup_key] = _az_order_id_dict.get(_refund_event_dict_key).get('_market_place_fee_breakdown').get(_expense_breakup_key, 0) + _expense_breakup_value  # type: ignore  # noqa: FKA100

                            for _other_fee_key, _other_fee_key_value in _refund_event_dict_value.get('other_fees_breakup', {}).items():  # type: ignore  # noqa: FKA100
                                _az_order_id_dict[_refund_event_dict_key]['_other_fee_breakdown'][_other_fee_key] = _az_order_id_dict.get(_refund_event_dict_key).get('_other_fee_breakdown').get(_other_fee_key, 0) + _other_fee_key_value   # type: ignore  # noqa: FKA100

                        if _refund_event_dict_key not in _refund_event_az_order_id_dict:
                            _refund_event_az_order_id_dict.append(
                                _refund_event_dict_key)

        """Distribute fees amongst all refund order ids"""
        total_skus = len(_refund_event_az_order_id_dict)

        for refund_order_sku in _refund_event_az_order_id_dict:
            # Calculate the share of fees for each SKU
            share_of_fba_fee = service_fee_reverse_fba_fee / total_skus
            share_of_market_place_fee = service_fee_market_place_fee / total_skus

            # Update fee fields for each SKU
            _az_order_id_dict[refund_order_sku]['_fba_fee_reverse'] += share_of_fba_fee
            _az_order_id_dict[refund_order_sku]['_market_place_fee'] += share_of_market_place_fee

            for _service_market_place_key, _service_market_place_value in service_fee_market_place_fee_breakdown.items():
                _az_order_id_dict[_refund_event_dict_key]['_market_place_fee_breakdown'][_service_market_place_key] = _az_order_id_dict[_refund_event_dict_key]['_market_place_fee_breakdown'].get(_service_market_place_key, 0) + (_service_market_place_value / total_skus)  # type: ignore  # noqa: FKA100

        # for refund_order_sku in _refund_event_az_order_id_dict:
        #     if not first_refund_event_processed:
        #         _az_order_id_dict[refund_order_sku]['_fba_fee_reverse'] += service_fee_reverse_fba_fee
        #         _az_order_id_dict[refund_order_sku]['_market_place_fee'] += service_fee_market_place_fee

        #         for _service_market_place_key, _service_market_place_value in service_fee_market_place_fee_breakdown.items():
        #             _az_order_id_dict[_refund_event_dict_key]['_market_place_fee_breakdown'][_service_market_place_key] = _az_order_id_dict[_refund_event_dict_key]['_market_place_fee_breakdown'].get(_service_market_place_key, 0) + _service_market_place_value  # type: ignore  # noqa: FKA100

        #         first_refund_event_processed = True

        return _az_order_id_dict

    @staticmethod
    def calculate_event_charges(event_type, item_list, fee_list, promotion_list):
        """ Calculate charges for different type of list """
        total_expense_amount = 0
        total_sales_amount = 0
        promotion_amount = 0
        other_fees_amount = 0
        data = {}

        # Exclude the type of fee list while calculating sales value i.e. sales_breakup
        other_fees_list = ['TCS-CGST', 'TCS-SGST', 'TCS-IGST']
        other_fees = {}
        sales_breakup = {}
        for item in item_list:
            charge_type = item.get('ChargeType')
            charge_amount = item.get('ChargeAmount').get('CurrencyAmount')
            charge_price_list = {
                convert_text_to_snake_case(charge_type): charge_amount
            }
            if charge_type not in other_fees_list:
                sales_breakup.update(charge_price_list)
                total_sales_amount += charge_amount
            else:
                other_fees_amount += charge_amount
                other_fees.update(charge_price_list)

        expense_breakup = {}
        if event_type == AspFinanceEventList.SERVICE_FEE.value:
            fee_reason = fee_list.get('FeeReason')
            if fee_reason:
                fees = {}
                service_fee_list = fee_list.get('FeeList')
                for service_fee in service_fee_list:
                    type = service_fee.get('FeeType')
                    amount = service_fee.get('FeeAmount').get('CurrencyAmount')
                    total_expense_amount += amount
                    fees.update({
                        f'{convert_text_to_snake_case(type)}': amount
                    })
                data.update({
                    convert_text_to_snake_case(fee_reason): fees,
                    'total_expense_amount': total_expense_amount
                })

            return data
        else:
            for item_fee in fee_list:
                fee_type = item_fee.get('FeeType')
                fee_amount = item_fee.get('FeeAmount').get('CurrencyAmount')
                fee_price_list = {
                    convert_text_to_snake_case(fee_type): fee_amount
                }
                expense_breakup.update(fee_price_list)
                total_expense_amount += fee_amount

        for promo in promotion_list:
            promo_amount = promo.get('PromotionAmount').get('CurrencyAmount')
            promotion_amount += promo_amount

        sales_breakup.update({'promotion_amount': promotion_amount})

        data.update({
            'total_expense_amount': total_expense_amount,
            'total_expense_breakup': expense_breakup,
            'total_sales_amount': total_sales_amount + promotion_amount,
            'total_sales_breakup': sales_breakup,
            'other_fees_amount': other_fees_amount,
            'other_fees': other_fees
        })

        return data

    @staticmethod
    def add_finance_events(account_id: str, asp_id: str, event_type: str, event_list: list, service_event_list=None):  # type: ignore  # noqa: C901
        """ Add event to database """

        all_az_order_id = []

        refund_event_orders = []

        for obj in event_list:

            az_order_id = None
            seller_order_id = None
            seller_sku = None
            finance_type = None
            finance_value = None
            posted_date = None
            marketplace_name = None

            if event_type == AspFinanceEventList.SHIPMENT.value:

                az_order_id = obj.get('AmazonOrderId')
                marketplace_name = obj.get('MarketplaceName')
                posted_date = obj.get('PostedDate')
                seller_order_id = obj.get('SellerOrderId')

                if az_order_id not in all_az_order_id:
                    all_az_order_id.append(az_order_id)

                AspFinanceView.add_update(account_id=account_id, asp_id=asp_id, az_order_id=az_order_id, seller_order_id=seller_order_id, market_place=marketplace_name,
                                          posted_date=posted_date, event_type=event_type, event_json=obj, finance_type=finance_type, finance_value=finance_value)

            elif event_type == AspFinanceEventList.REFUND.value:

                az_order_id = obj.get('AmazonOrderId')
                marketplace_name = obj.get('MarketplaceName')
                posted_date = obj.get('PostedDate')
                seller_order_id = obj.get('SellerOrderId')

                if az_order_id not in all_az_order_id:
                    all_az_order_id.append(az_order_id)

                refund_event = {
                    'az_order_id': az_order_id,
                    'posted_date': posted_date
                }

                refund_event_orders.append(refund_event)

                AspFinanceView.add_update(account_id=account_id, asp_id=asp_id, az_order_id=az_order_id, seller_order_id=seller_order_id, market_place=marketplace_name,
                                          posted_date=posted_date, event_type=event_type, event_json=obj, finance_type=finance_type, finance_value=finance_value)

            elif event_type == AspFinanceEventList.ADJUSTMENT.value:

                adjustment_event_dict = AspFinanceView.adjustment_event_list(
                    obj)

                for adjustment_event_sku, adjustment_event_value in adjustment_event_dict.items():
                    AspFinanceView.add_update(account_id=account_id, asp_id=asp_id, az_order_id=az_order_id,
                                              posted_date=adjustment_event_value.get('posted_date'), event_type=event_type, event_json=obj, finance_type=adjustment_event_value.get('finance_type'), finance_value=adjustment_event_value.get('finance_value'), seller_sku=adjustment_event_sku)

            elif event_type == AspFinanceEventList.PRODUCT_ADS_PAYMENT.value:
                finance_type, finance_value, posted_date = AspFinanceView.product_ads_payment_event_list(
                    obj)
                AspFinanceView.add_update(account_id=account_id, asp_id=asp_id, az_order_id=az_order_id, seller_order_id=seller_order_id, market_place=marketplace_name,
                                          posted_date=posted_date, event_type=AspFinanceEventList.PRODUCT_ADS_PAYMENT.value, event_json=obj, finance_type=finance_type, finance_value=finance_value, seller_sku=seller_sku)

        serivce_fee_list = []
        if event_type == AspFinanceEventList.REFUND.value and service_event_list is not None:
            if refund_event_orders:
                for service_event_ob in service_event_list:
                    az_order_id = service_event_ob.get('AmazonOrderId')
                    serivce_fee = {
                        'az_order_id': az_order_id,
                        'service_event_ob': service_event_ob
                    }
                    serivce_fee_list.append(serivce_fee)

                posted_date_dict = {}
                for item in refund_event_orders:
                    az_order_id = item['az_order_id']
                    posted_date = item['posted_date']
                    posted_date_dict.setdefault(az_order_id, []).append(posted_date)  # type: ignore  # noqa: FKA100

                for item in serivce_fee_list:
                    az_order_id = item['az_order_id']
                    if az_order_id in posted_date_dict and posted_date_dict[az_order_id]:
                        item['posted_date'] = posted_date_dict[az_order_id].pop(
                            0)

        if serivce_fee_list:
            for fees in serivce_fee_list:
                az_order_id = fees.get('az_order_id')
                posted_date = fees.get('posted_date')
                service_ob = fees.get('service_event_ob')
                if posted_date is not None:
                    AspFinanceView.add_update(account_id=account_id, asp_id=asp_id, az_order_id=az_order_id,
                                              posted_date=posted_date, event_type=AspFinanceEventList.SERVICE_FEE.value, event_json=service_ob)
                else:
                    refund_az_order_id = AspFinanceView.get_by_az_order_id(
                        account_id=account_id, asp_id=asp_id, az_order_id=az_order_id, event_type=AspFinanceEventList.REFUND.value)
                    if refund_az_order_id:
                        AspFinanceView.add_update(account_id=account_id, asp_id=asp_id, az_order_id=az_order_id,
                                                  posted_date=refund_az_order_id.posted_date, event_type=AspFinanceEventList.SERVICE_FEE.value, event_json=service_ob)

        all_event_types = [
            AspFinanceEventList.PRODUCT_ADS_PAYMENT.value, AspFinanceEventList.ADJUSTMENT.value]

        if event_type not in all_event_types:
            for _az_order_id in all_az_order_id:
                _data = AspFinanceView.get_price_action(
                    account_id=account_id, asp_id=asp_id, az_order_id=_az_order_id)

                for _data_seller_sku, _data_value in _data.items():
                    product = AzProductPerformance.get_by_az_order_id(
                        account_id=account_id, asp_id=asp_id, az_order_id=_az_order_id, seller_sku=_data_seller_sku)
                    _gross_sales = _data_value.get('_gross_sales', 0.0)   # type: ignore  # noqa: FKA100
                    _market_place_fee = _data_value.get('_market_place_fee', 0.0)  # type: ignore  # noqa: FKA100
                    _forward_fba_fee = _data_value.get('_fba_fee_forward', 0.0)   # type: ignore  # noqa: FKA100
                    _reverse_fba_fee = _data_value.get('_fba_fee_reverse', 0.0)  # type: ignore  # noqa: FKA100
                    _units_sold = _data_value.get('_units_sold', 0)  # type: ignore  # noqa: FKA100
                    _units_returned = _data_value.get('_units_returned', 0)  # type: ignore  # noqa: FKA100
                    _returns = _data_value.get('_refunds', 0)  # type: ignore  # noqa: FKA100
                    _shipment_date = _data_value.get('shipment_date')  # type: ignore  # noqa: FKA100
                    _refund_date = _data_value.get('refund_date')  # type: ignore  # noqa: FKA100
                    _calculate_total_expense = _data_value
                    _seller_order_id = _data_value.get('_seller_order_id')  # type: ignore  # noqa: FKA100

                    if product:
                        product.update(account_id=account_id, asp_id=asp_id, az_order_id=_az_order_id, seller_sku=_data_seller_sku, gross_sales=_gross_sales, market_place_fee=_market_place_fee,
                                       forward_fba_fee=_forward_fba_fee, reverse_fba_fee=_reverse_fba_fee, units_sold=_units_sold, units_returned=_units_returned, returns=_returns, shipment_date=_shipment_date, refund_date=_refund_date,
                                       summary_analysis=_calculate_total_expense)
                    else:
                        AzProductPerformance.create(account_id=account_id, asp_id=asp_id, az_order_id=_az_order_id, seller_sku=_data_seller_sku, gross_sales=_gross_sales,
                                                    seller_order_id=_seller_order_id, market_place_fee=_market_place_fee, forward_fba_fee=_forward_fba_fee, reverse_fba_fee=_reverse_fba_fee,
                                                    units_sold=_units_sold, units_returned=_units_returned, returns=_returns, shipment_date=_shipment_date, refund_date=_refund_date,
                                                    summary_analysis=_calculate_total_expense)

    @staticmethod
    def shipment_event_list(obj_item):
        """Shipment Event List From Finance Data"""
        az_order_id = obj_item.get('AmazonOrderId')
        marketplace_name = obj_item.get('MarketplaceName')
        posted_date = obj_item.get('PostedDate')
        seller_order_id = obj_item.get('SellerOrderId')
        shipment_item_list = obj_item.get('ShipmentItemList')

        _seller_sku_dict = {}

        other_fees_list = ['TCS-CGST', 'TCS-SGST', 'TCS-IGST']

        if shipment_item_list:
            for shipped_item in shipment_item_list:
                seller_sku = shipped_item.get('SellerSKU')

                if seller_sku not in _seller_sku_dict:
                    _seller_sku_dict[seller_sku] = {
                        'az_order_id': az_order_id,
                        'marketplace_name': marketplace_name,
                        'posted_date': posted_date,
                        'seller_order_id': seller_order_id,
                        'sku': seller_sku,
                        'quantity': 0,
                        # 'order_item_ids': set(),
                        'sales_breakup': {},
                        'other_fees_breakup': {},
                        'expense_breakup': {},
                        'promotion_amount': 0,
                        'total_sales_amount': 0,
                        'other_fees_amount': 0,
                        'total_expense_amount': 0
                    }

                _seller_sku_dict[seller_sku]['quantity'] += shipped_item.get(
                    'QuantityShipped')
                # _seller_sku_dict[seller_sku]['order_item_ids'].add(shipped_item.get('OrderItemId'))

                item_charge_list = shipped_item.get('ItemChargeList')
                if item_charge_list:
                    for item_charge in item_charge_list:
                        item_charge_type = item_charge.get('ChargeType')
                        item_charge_amount = item_charge.get(
                            'ChargeAmount').get('CurrencyAmount')

                        # Ecomm Calculation
                        charge_price_list = {
                            convert_text_to_snake_case(item_charge_type): item_charge_amount
                        }

                        if item_charge_type not in other_fees_list:
                            for _key, _value in charge_price_list.items():
                                _seller_sku_dict[seller_sku]['sales_breakup'][_key] = _seller_sku_dict[seller_sku]['sales_breakup'].get(_key, 0) + _value   # type: ignore  # noqa: FKA100
                            _seller_sku_dict[seller_sku]['total_sales_amount'] += item_charge_amount
                        else:
                            for __key, __value in charge_price_list.items():
                                _seller_sku_dict[seller_sku]['other_fees_breakup'][__key] = _seller_sku_dict[seller_sku]['other_fees_breakup'].get(__key, 0) + __value   # type: ignore  # noqa: FKA100
                            _seller_sku_dict[seller_sku]['other_fees_amount'] += item_charge_amount

                # expense part
                item_fee_list = shipped_item.get('ItemFeeList')
                if item_fee_list:
                    for item_fee in item_fee_list:
                        item_fee_type = item_fee.get('FeeType')
                        item_fee_amount = item_fee.get(
                            'FeeAmount').get('CurrencyAmount')
                        # item_fee_currency = item_fee.get(
                        #     'FeeAmount').get('CurrencyCode')

                        # Ecomm Calculation
                        fee_price_list = {
                            convert_text_to_snake_case(item_fee_type): item_fee_amount
                        }

                        for ___key, ___value in fee_price_list.items():
                            _seller_sku_dict[seller_sku]['expense_breakup'][___key] = _seller_sku_dict[seller_sku]['expense_breakup'].get(___key, 0) + ___value   # type: ignore  # noqa: FKA100

                        _seller_sku_dict[seller_sku]['total_expense_amount'] += item_fee_amount

                promotion_list = shipped_item.get('PromotionList')
                if promotion_list:
                    for promotion in promotion_list:
                        promotion_amount_info = promotion.get(
                            'PromotionAmount')
                        if promotion_amount_info:
                            promotion_fee_amount = promotion_amount_info.get(
                                'CurrencyAmount')
                            _seller_sku_dict[seller_sku]['promotion_amount'] += promotion_fee_amount
                            _seller_sku_dict[seller_sku]['total_sales_amount'] += promotion_fee_amount
                            _seller_sku_dict[seller_sku]['sales_breakup']['promotion_amount'] = _seller_sku_dict[seller_sku]['sales_breakup'].get('promotion_amount', 0) + promotion_fee_amount   # type: ignore  # noqa: FKA100

        return _seller_sku_dict

    @staticmethod
    def refund_event_list(obj_item):
        """Refund Event List From Finance Data"""
        az_order_id = obj_item.get('AmazonOrderId')
        marketplace_name = obj_item.get('MarketplaceName')
        posted_date = obj_item.get('PostedDate')
        seller_order_id = obj_item.get('SellerOrderId')
        shipment_item_list = obj_item.get('ShipmentItemAdjustmentList')

        _seller_sku_dict = {}
        other_fees_list = ['TCS-CGST', 'TCS-SGST', 'TCS-IGST']

        if shipment_item_list:
            for shipped_item in shipment_item_list:
                seller_sku = shipped_item.get('SellerSKU')
                if seller_sku not in _seller_sku_dict:
                    _seller_sku_dict[seller_sku] = {
                        'az_order_id': az_order_id,
                        'marketplace_name': marketplace_name,
                        'posted_date': posted_date,
                        'seller_order_id': seller_order_id,
                        'sku': seller_sku,
                        'quantity': 0,
                        # 'order_item_ids': set(),
                        'sales_breakup': {},
                        'other_fees_breakup': {},
                        'expense_breakup': {},
                        'promotion_amount': 0,
                        'total_sales_amount': 0,
                        'other_fees_amount': 0,
                        'total_expense_amount': 0
                    }

                _seller_sku_dict[seller_sku]['quantity'] += shipped_item.get(
                    'QuantityShipped')
                # _seller_sku_dict[seller_sku]['order_item_ids'].add(shipped_item.get('OrderItemId'))

                item_charge_adjustment_list = shipped_item.get(
                    'ItemChargeAdjustmentList')
                if item_charge_adjustment_list:
                    for item_charge_adjustment in item_charge_adjustment_list:
                        item_charge_adjustment_type = item_charge_adjustment.get(
                            'ChargeType')
                        item_charge_adjustment_amount = item_charge_adjustment.get(
                            'ChargeAmount').get('CurrencyAmount')
                        # item_charge_adjustment_currency = item_charge_adjustment.get(
                        #     'ChargeAmount').get('CurrencyCode')

                        # Ecomm Calculation
                        charge_price_list = {
                            convert_text_to_snake_case(item_charge_adjustment_type): item_charge_adjustment_amount
                        }

                        if item_charge_adjustment_type not in other_fees_list:
                            for _key, _value in charge_price_list.items():
                                _seller_sku_dict[seller_sku]['sales_breakup'][_key] = _seller_sku_dict[seller_sku]['sales_breakup'].get(_key, 0) + _value   # type: ignore  # noqa: FKA100
                            _seller_sku_dict[seller_sku]['total_sales_amount'] += item_charge_adjustment_amount
                        else:
                            for __key, __value in charge_price_list.items():
                                _seller_sku_dict[seller_sku]['other_fees_breakup'][__key] = _seller_sku_dict[seller_sku]['other_fees_breakup'].get(__key, 0) + __value   # type: ignore  # noqa: FKA100
                            _seller_sku_dict[seller_sku]['other_fees_amount'] += item_charge_adjustment_amount

                # expense part
                item_fee_adjustment_list = shipped_item.get(
                    'ItemFeeAdjustmentList')
                if item_fee_adjustment_list:
                    for item_fee_adjustment in item_fee_adjustment_list:
                        item_fee_adjustment_type = item_fee_adjustment.get(
                            'FeeType')
                        item_fee_adjustment_amount = item_fee_adjustment.get(
                            'FeeAmount').get('CurrencyAmount')
                        # item_fee_adjustment_currency = item_fee_adjustment.get(
                        #     'FeeAmount').get('CurrencyCode')

                        # Ecomm Calculation
                        fee_price_list = {
                            convert_text_to_snake_case(item_fee_adjustment_type): item_fee_adjustment_amount
                        }

                        for ___key, ___value in fee_price_list.items():
                            _seller_sku_dict[seller_sku]['expense_breakup'][___key] = _seller_sku_dict[seller_sku]['expense_breakup'].get(___key, 0) + ___value   # type: ignore  # noqa: FKA100

                        _seller_sku_dict[seller_sku]['total_expense_amount'] += item_fee_adjustment_amount

                promotion_adjustment_list = shipped_item.get(
                    'PromotionAdjustmentList')
                if promotion_adjustment_list:
                    for promotion_adjustment in promotion_adjustment_list:
                        # promotion_adjustment_id = promotion_adjustment.get(
                        #     'PromotionId')
                        # promotion_adjustment_type = promotion_adjustment.get(
                        #     'PromotionType')
                        promotion_adjustment_amount_info = promotion_adjustment.get(
                            'PromotionAmount')
                        if promotion_adjustment_amount_info:
                            promotion_adjustment_fee_amount = promotion_adjustment_amount_info.get(
                                'CurrencyAmount')
                            # promotion_adjustment_currency = promotion_adjustment_amount_info.get(
                            #     'CurrencyCode')

                            _seller_sku_dict[seller_sku]['promotion_amount'] += promotion_adjustment_fee_amount
                            _seller_sku_dict[seller_sku]['total_sales_amount'] += promotion_adjustment_fee_amount
                            _seller_sku_dict[seller_sku]['sales_breakup']['promotion_amount'] = _seller_sku_dict[seller_sku]['sales_breakup'].get('promotion_amount', 0) + promotion_adjustment_fee_amount   # type: ignore  # noqa: FKA100

        return _seller_sku_dict

    @staticmethod
    def service_fee_event_list(obj_item):
        """Service Event List From Finance Data"""
        az_order_id = obj_item.get('AmazonOrderId')
        # marketplace_name = obj_item.get('MarketplaceName', None)  # Not in API  # type: ignore  # noqa: FKA100
        # posted_date = obj_item.get('PostedDate', None)  # Not in API   # type: ignore  # noqa: FKA100
        # seller_order_id = obj_item.get('SellerOrderId', None)  # Not in API   # type: ignore  # noqa: FKA100
        # seller_sku = None  # Not in API
        # order_item_id = None  # Not in API
        # quantity_shipped = 0  # Not in API
        total_expense_amount = 0
        expense_breakup = {}

        fee_reason = obj_item.get('FeeReason')
        fee_list = obj_item.get('FeeList')

        fees = {}
        for fee in fee_list:
            fee_type = fee.get('FeeType')
            fee_amount = fee.get('FeeAmount').get('CurrencyAmount')
            total_expense_amount += fee_amount
            fees.update({
                f'{convert_text_to_snake_case(fee_type)}': fee_amount
            })

        expense_breakup.update({
            convert_text_to_snake_case(fee_reason): fees,
            'total_expense_amount': total_expense_amount
        })

        return az_order_id, total_expense_amount, expense_breakup

    @staticmethod
    def adjustment_event_list(obj_item):
        """Adjustment Event List From Finance Data"""
        finance_type = obj_item.get('AdjustmentType')
        posted_date = obj_item.get('PostedDate')

        adjustment_event_dict = {}

        adjustment_item_list = obj_item.get('AdjustmentItemList')
        if adjustment_item_list:
            for adjustment_item in adjustment_item_list:
                seller_sku = adjustment_item.get('SellerSKU')
                if seller_sku not in adjustment_event_dict:
                    adjustment_event_dict[seller_sku] = {
                        'finance_type': finance_type,
                        'posted_date': posted_date,
                        'sku': adjustment_item.get('SellerSKU'),
                        'quantity': 0,
                        'finance_value': 0
                    }

                adjustment_event_dict[seller_sku]['finance_value'] += adjustment_item.get(
                    'TotalAmount').get('CurrencyAmount')
                adjustment_event_dict[seller_sku]['quantity'] += int(adjustment_item.get(
                    'Quantity')) if adjustment_item.get('Quantity') is not None else 0

        return adjustment_event_dict

    @staticmethod
    def product_ads_payment_event_list(obj_item):
        """Product Ad's Payment Event List From Finance Data"""
        finance_value = 0
        finance_type = obj_item.get('transactionType')
        posted_date = obj_item.get('postedDate')
        finance_value = obj_item.get('transactionValue').get('CurrencyAmount')

        return finance_type, finance_value, posted_date
