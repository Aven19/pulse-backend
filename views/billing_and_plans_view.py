"""endpoints and class for billing and members page"""

import json

from app import db
from app import logger
from app import RAZORPAY_CLIENT
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import PlanInterval
from app.helpers.constants import ResponseMessageKeys
from app.helpers.decorators import api_time_logger
from app.helpers.decorators import token_required
from app.helpers.utility import convert_to_numeric
from app.helpers.utility import field_type_validator
from app.helpers.utility import required_validator
from app.helpers.utility import send_json_response
from app.models.plan import Plan
from flask import request


class BillingPlansView:
    """class containing methods for billing and page apis """

    # function to get plans
    @api_time_logger
    @token_required
    def get_plans(user_object, account_object):
        """endpoint for viewing all the available subscription plans"""
        try:
            data_list = []

            with open('app/static/files/plan.json') as fc_file:
                data_list = json.load(fc_file)

            if data_list:

                for data in data_list:
                    name = data.get('name')
                    period = data.get('period')
                    status = data.get('status')
                    amount = data.get('amount')
                    currency = data.get('currency')
                    features = data.get('features')
                    discount = data.get('discount')
                    reference_plan_id = data.get('reference_plan_id')
                    description = data.get('description')

                    plan = Plan.add_update(name=name, period=period, status=status, amount=amount, currency=currency,
                                           feature=features, reference_plan_id=reference_plan_id, description=description, discount=discount)

                plans = Plan.get_all_plans()

                result = {'result': []}

                for plan in plans:
                    result_dict = {
                        'name': plan.name,
                        'reference_plan_id': plan.reference_plan_id,
                        'period': plan.period,
                        'status': plan.status,
                        'amount': convert_to_numeric(plan.amount) / 100,
                        'currency': plan.currency,
                        'discount': plan.discount,
                        'feature': plan.feature,
                        'description': plan.description
                    }
                    result['result'].append(result_dict)

                return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=result, error=None)

            return send_json_response(
                http_status=404,
                response_status=False,
                message_key=ResponseMessageKeys.NO_DATA_FOUND.value,
                error=None
            )

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed getting plans: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    # function to add plans
    @api_time_logger
    @token_required
    def add_plans(user_object, account_object):
        """endpoint for adding plans , for now it is get methdo with dummy data"""
        try:

            data = request.get_json(force=True)

            field_types = {'name': str, 'period': str, 'status': str, 'amount': float, 'currency': str,
                           'feature': dict, 'discount': float, 'description': str}

            required_fields = ['name', 'period', 'status', 'amount',
                               'currency', 'feature', 'discount', 'description']

            post_data = field_type_validator(
                request_data=data, field_types=field_types)

            if post_data['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=post_data['data'])

            is_valid = required_validator(
                request_data=data, required_fields=required_fields)

            if is_valid['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=is_valid['data'])

            name = data.get('name')
            period = data.get('period')
            status = data.get('status')
            amount = data.get('amount')
            currency = data.get('currency')
            feature = data.get('feature')
            discount = data.get('discount')
            description = data.get('description')

            plan = Plan.add_update(name=name, period=period, status=status, amount=amount,
                                   currency=currency, feature=feature, description=description, discount=discount)

            try:
                response = RAZORPAY_CLIENT.plan.create({
                    'period': period,
                    'interval': PlanInterval.get_interval(period),
                    'item': {
                        'name': name,
                        'amount': amount,
                        'currency': currency,
                        'description': description
                    },
                })

                plan.reference_plan_id = response.get('id')
                db.session.commit()

            except Exception as exception_error:
                logger.error(
                    f'GET -> Failed while adding plan on razorpay: {exception_error}')
                return send_json_response(
                    http_status=500,
                    response_status=False,
                    message_key=ResponseMessageKeys.FAILED.value,
                )

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.ADDED.value, data=None, error=None)

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed adding plans: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )
