"""endpoints for members page"""

from datetime import datetime
from datetime import timedelta
import time

from app import db
from app import logger
from app import RAZORPAY_CLIENT
from app.helpers.constants import APP_NAME
from app.helpers.constants import BillingInterval
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import ResponseMessageKeys
from app.helpers.constants import SubscriptionStates
from app.helpers.constants import VerificationStatus
from app.helpers.constants import WebhookEvent
from app.helpers.decorators import api_time_logger
from app.helpers.decorators import token_required
from app.helpers.utility import field_type_validator
from app.helpers.utility import generate_uuid
from app.helpers.utility import required_validator
from app.helpers.utility import send_json_response
from app.models.account import Account
from app.models.payment import Payment
from app.models.plan import Plan
from app.models.subscription import Subscription
from app.models.user import User
from flask import request
from providers.mail import send_mail
import razorpay


class PaymentView():
    """Payment view class containing methods related to payment and webhooks"""
    # @api_time_logger
    # @token_required
    # def create_plan(user_object, account_object):

    #     try:

    #         response = RAZORPAY_CLIENT.plan.create({
    #             'period': 'monthly',
    #             'interval': 1,
    #             'item': {
    #                 'name': 'Silver plan - Monthly',
    #                 'amount': 240000,
    #                 'currency': 'INR',
    #                 'description': 'Description for the Silver Plan - Yearly',
    #                 },
    #             })

    #         print(response)

    #         return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response, error=None)

    #     except Exception as exception_error:
    #         logger.error(
    #             f'GET -> Failed while creating plan: {exception_error}')
    #         return send_json_response(
    #             http_status=500,
    #             response_status=False,
    #             message_key=ResponseMessageKeys.FAILED.value,
    #         )

    @api_time_logger
    @token_required
    def get_all_plans(user_object, account_object):
        """retrieves all plan from razorpay"""
        try:

            response = RAZORPAY_CLIENT.plan.all()

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response, error=None)

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed while getting all plans: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @api_time_logger
    @token_required
    def get_all_subscriptions(user_object, account_object):
        """retrieves all subscription"""
        try:

            response = RAZORPAY_CLIENT.subscription.all()

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response, error=None)

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed while getting all status: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @api_time_logger
    @token_required
    def create_subscription(user_object, account_object):
        """creates a subcsription on razorpay"""
        try:

            account_id = account_object.uuid
            user_id = user_object.id

            is_primary_user = False
            if account_object.primary_user_id == user_id:
                is_primary_user = True

            # check if subscription is ACTIVE
            current_subscription = Subscription.get_status_by_account_id(
                account_id=account_id)

            if current_subscription:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=True, message_key=ResponseMessageKeys.SUBSCRIPTION_ALREADY_ACTIVE.value, data=None, error=None)

            if not is_primary_user:
                return send_json_response(http_status=HttpStatusCode.UNAUTHORIZED.value, response_status=True, message_key=ResponseMessageKeys.ACCESS_DENIED.value, data=None, error=None)

            data = request.get_json(force=True)

            field_types = {
                'reference_plan_id': str}

            required_fields = ['reference_plan_id']

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

            reference_plan_id = data.get('reference_plan_id')

            plan = Plan.get_by_reference_plan_id(
                reference_plan_id=reference_plan_id)

            expire_at = datetime.utcnow() + timedelta(days=2)

            payload = {
                'plan_id': reference_plan_id,
                'total_count': int(BillingInterval.get_interval(plan.period)),
                'expire_by': int(expire_at.timestamp()),
                'customer_notify': 1,
                'addons': [],
                'notes': {}
            }

            try:
                subscription = RAZORPAY_CLIENT.subscription.create(payload)
            except Exception as e:
                logger.error(f'failed at creating subscription: {e}')
                return send_json_response(http_status=HttpStatusCode.INTERNAL_SERVER_ERROR.value, response_status=True, message_key=ResponseMessageKeys.FAILED.value, data=None, error=None)

            if subscription and subscription.get('status') == SubscriptionStates.CREATED.value:

                # create an order
                order_payload = {
                    'amount': int(plan.amount),
                    'currency': plan.currency,
                    'receipt': generate_uuid()
                }
                try:
                    order = RAZORPAY_CLIENT.order.create(data=order_payload)
                except Exception as e:
                    logger.error(f'failed at creating order: {e}')
                    return send_json_response(http_status=HttpStatusCode.INTERNAL_SERVER_ERROR.value, response_status=True, message_key=ResponseMessageKeys.FAILED.value, data=None, error=None)

                if order:

                    result_dict = {
                        'subscription_entity': subscription,
                        'order_entity': order
                    }

                    return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUBSCRIPTION_ORDER_CREATED.value, data=result_dict, error=None)

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed while creating subcription and order: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @api_time_logger
    @token_required
    def cancel_subscription(user_object, account_object):
        """cancels a subcsription on razorpay"""
        try:

            account_id = account_object.uuid
            user_id = user_object.id

            is_primary_user = False
            if account_object.primary_user_id == user_id:
                is_primary_user = True

            if not is_primary_user:
                return send_json_response(http_status=HttpStatusCode.UNAUTHORIZED.value, response_status=True, message_key=ResponseMessageKeys.ACCESS_DENIED.value, data=None, error=None)

            subscription = Subscription.get_by_account_id(
                account_id=account_id)

            if not subscription:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=True, message_key=ResponseMessageKeys.SUBSCRIPTION_INACTIVE.value, data=None, error=None)

            reference_subscription_id = subscription.reference_subscription_id

            if reference_subscription_id:

                try:
                    response = RAZORPAY_CLIENT.subscription.cancel(
                        reference_subscription_id)

                    # send mail > subscription is cancelled
                    email_subject = 'Your Subscription has been cancelled'

                    email_data = {
                        'email_to': user_object.email,
                        'app_name': APP_NAME,
                        'first_name': user_object.first_name,
                        'account_legal_name': account_object.legal_name
                    }

                    send_mail(email_to=user_object.email, subject=email_subject,
                              template='emails/subscription_cancelled.html', data=email_data)

                    return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUBSCRIPTION_CANCELLED.value, data=response, error=None)
                except Exception as e:
                    logger.error(
                        f'exception while calling cancelling method -----------{e}')
                    return send_json_response(http_status=HttpStatusCode.INTERNAL_SERVER_ERROR.value, response_status=True, message_key=ResponseMessageKeys.SUBSCRIPTION_INACTIVE.value, data=None, error=None)

            logger.error('--------Reference subcription id not found------')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed while cancelling a subcription: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @api_time_logger
    @token_required
    def verify_payment_checkout(user_object, account_object):
        """method to accept checkout callback response"""
        try:

            account_id = account_object.uuid
            user_id = user_object.id

            data = request.get_json(force=True)
            logger.info(f'verify data: {data}')
            field_types = {
                'razorpay_payment_id': str,
                'razorpay_signature': str,
                'razorpay_subscription_id': str,
                'subscription_id': str
            }

            required_fields = ['razorpay_payment_id',
                               'razorpay_signature', 'razorpay_subscription_id', 'subscription_id']

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

            razorpay_payment_id = data.get('razorpay_payment_id')
            razorpay_signature = data.get('razorpay_signature')
            razorpay_subscription_id = data.get('razorpay_subscription_id')
            subscription_id = data.get('subscription_id')

            try:
                verification = RAZORPAY_CLIENT.utility.verify_subscription_payment_signature(
                    {
                        'razorpay_subscription_id': subscription_id,
                        'razorpay_payment_id': razorpay_payment_id,
                        'razorpay_signature': razorpay_signature
                    }
                )

                if verification:
                    payment = Payment(user_id=user_id, account_id=account_id, reference_payment_id=razorpay_payment_id, reference_subscription_id=razorpay_subscription_id,
                                      verified=VerificationStatus.VERIFIED.value, request=data, created_at=int(time.time()))

                    db.session.add(payment)
                    db.session.commit()

                    return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data={'result': verification}, error=None)

            except razorpay.errors.SignatureVerificationError as e:
                logger.error(f'Failed verifying payment signature: {e}')
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=True, message_key=ResponseMessageKeys.PAYMENT_SIGNATURE_INVALID.value, data=None, error=None)

            except Exception as e:

                logger.error(f'Failed verifying payment signature: {e}')
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=True, message_key=ResponseMessageKeys.FAILED.value, data=None, error=None)

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data={'result': verification}, error=None)

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed while checkout callback: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @api_time_logger
    def standard_callback():
        """method to accept webhook responses"""
        try:

            data = request.get_json(force=True)

            if not data:
                return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.FAILED.value, data=None, error=None)

            event = data.get('event')

            if event and event == WebhookEvent.SUBSCRIPTION_CHARGED.value:
                logger.info('*' * 200)
                logger.info('Webhook Called')
                logger.info(
                    f'--------------{event}---------------------------')
                logger.info(f'---------------{data}------------')

                subscription_entity = data.get('payload').get(
                    'subscription').get('entity')

                payment_entity = data.get('payload').get(
                    'payment').get('entity')

                reference_payment_id = payment_entity.get('id')
                reference_order_id = payment_entity.get('order_id')
                status = payment_entity.get('status')
                method = payment_entity.get('method')
                amount = payment_entity.get('amount')
                currency = payment_entity.get('currency')

                logger.info(f'status: {status}')
                logger.info(f'method: {method}')
                logger.info(f'amount: {amount}')
                logger.info(f'currency: {currency}')

                sub_plan_id = subscription_entity.get('plan_id')
                sub_reference_subscription_id = subscription_entity.get('id')
                sub_status = subscription_entity.get('status')
                sub_start_date = datetime.utcfromtimestamp(
                    subscription_entity.get('current_start'))
                sub_end_date = datetime.utcfromtimestamp(
                    subscription_entity.get('current_end'))

                # check if payment with payment_id exists. If it exists it means it is first payment since we are making a payment entry
                # during verification for first payment
                payment_by_ref_payment_id = Payment.get_by_reference_payment_id(
                    reference_payment_id=reference_payment_id)

                logger.info(
                    f'********* reference payment id : {payment_by_ref_payment_id}')

                if payment_by_ref_payment_id:
                    # first time payment
                    logger.info(
                        '*********** first payment (authorization payment)*************')

                    payment_by_ref_payment_id.amount = amount
                    payment_by_ref_payment_id.payment_mode = method
                    payment_by_ref_payment_id.status = status
                    payment_by_ref_payment_id.currency = currency
                    payment_by_ref_payment_id.response = data
                    payment_by_ref_payment_id.reference_order_id = reference_order_id
                    payment_by_ref_payment_id.reference_subscription_id = sub_reference_subscription_id
                    payment_by_ref_payment_id.updated_at = int(time.time())

                    db.session.commit()

                    user_id = payment_by_ref_payment_id.user_id
                    account_uuid = payment_by_ref_payment_id.account_id

                    user = User.get_by_id(id=user_id)
                    account = Account.get_by_uuid(uuid=account_uuid)

                    plan = Plan.get_by_reference_plan_id(
                        reference_plan_id=sub_plan_id)

                    plan_id = plan.id

                    if payment_by_ref_payment_id.verified == VerificationStatus.VERIFIED.value:
                        logger.info(
                            f'account_id: {payment_by_ref_payment_id.account_id}')
                        logger.info(f'plan_id: {plan_id}')
                        logger.info(
                            f'reference_subscription_id: {sub_reference_subscription_id}')
                        logger.info(
                            f'payment_id: {payment_by_ref_payment_id.id}')
                        logger.info(f'status: {sub_status}')

                        # make other active subscription of the account_id as inactive
                        Subscription.set_status_inactive(
                            account_id=account_uuid)

                        # make a new subscription entry for new recurring payment
                        Subscription.add(account_id=account_uuid, reference_subscription_id=sub_reference_subscription_id, plan_id=plan.id,
                                         payment_id=payment_by_ref_payment_id.id, status=sub_status, start_date=sub_start_date, end_date=sub_end_date)

                        # to do send mail > subscription is activated
                        email_subject = 'Your Subscription is now Active'

                        email_data = {
                            'email_to': user.email,
                            'app_name': APP_NAME,
                            'first_name': user.first_name,
                            'account_legal_name': account.legal_name
                        }

                        send_mail(email_to=user.email, subject=email_subject,
                                  template='emails/subscription_activated.html', data=email_data)

                # if it is not first payment, it means it is recurring payment. Then we make an entry in payments table.
                else:

                    # not first payment, it means subscription id already exists in payments table.
                    # We need the user_id and account_id from querying payments table using reference_subscription_id
                    payment_by_sub_id = Payment.get_by_reference_subscription_id(
                        reference_subscription_id=sub_reference_subscription_id)

                    if payment_by_sub_id:

                        user_id = payment_by_sub_id.user_id
                        account_uuid = payment_by_sub_id.account_id

                        # make a new payment entry for new recurring payment
                        payment = Payment.add(user_id=user_id, account_id=account_uuid, amount=amount, status=status, currency=currency,
                                              reference_order_id=reference_order_id, reference_payment_id=reference_payment_id, reference_subscription_id=sub_reference_subscription_id,
                                              payment_mode=method, response=data, verified=VerificationStatus.VERIFIED.value)

                        # get plan id
                        plan = Plan.get_by_reference_plan_id(
                            reference_plan_id=sub_plan_id)

                        # make other active subscription of the account_id as inactive
                        Subscription.set_status_inactive(
                            account_id=account_uuid)

                        # make a new subscription entry for new recurring payment
                        Subscription.add(account_id=account_uuid, reference_subscription_id=sub_reference_subscription_id, plan_id=plan.id,
                                         payment_id=payment.id, status=sub_status, start_date=sub_start_date, end_date=sub_end_date)

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=None, error=None)

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed while making standard callback: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    # @api_time_logger
    # @token_required
    # def create_payment_link(user_object, account_object):
    #     """call check status api to confirm payment status.
    #         this needs to be a scheduler worker process which checks payment status until it becomes success or failed."""
    #     try:

    #         response = RAZORPAY_CLIENT.payment_link.create({
    #             "amount": 500,
    #             "currency": "INR",
    #             "accept_partial": True,
    #             "first_min_partial_amount": 100,
    #             "description": "For XYZ purpose",
    #             "customer": {
    #                 "name": "Gaurav Kumar",
    #                 "email": "gaurav.kumar@example.com",
    #                 "contact": "+919000090000"
    #             },
    #             "notify": {
    #                 "sms": True,
    #                 "email": True
    #             },
    #             "reminder_enable": True,
    #             "notes": {
    #                 "policy_name": "Jeevan Bima"
    #             },
    #             "callback_url": "https://5953-14-142-38-68.ngrok-free.app/api/v1/payment/callback",
    #             "callback_method": "get"
    #             })

    #         return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response, error=None)

    #     except Exception as exception_error:
    #         logger.error(
    #             f'GET -> Failed while making status check call: {exception_error}')
    #         return send_json_response(
    #             http_status=500,
    #             response_status=False,
    #             message_key=ResponseMessageKeys.FAILED.value,
    #         )

        # @api_time_logger
    # @token_required
    # def create_subscription_link(user_object, account_object):

    #     try:

    #         account_id = account_object.uuid
    #         user_id = user_object.id
    #         user_email = user_object.email

    #         is_primary_user = Account.is_primary_user(
    #             user_id=user_id, account_id=account_id)

    #         if not is_primary_user:
    #             return send_json_response(http_status=HttpStatusCode.UNAUTHORIZED.value, response_status=True, message_key=ResponseMessageKeys.ACCESS_DENIED.value, data=None, error=None)

    #         data = request.get_json(force=True)

    #         field_types = {
    #             'reference_plan_id': str}

    #         required_fields = ['reference_plan_id']

    #         post_data = field_type_validator(
    #             request_data=data, field_types=field_types)

    #         if post_data['is_error']:
    #             return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
    #                                       message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
    #                                       error=post_data['data'])

    #         is_valid = required_validator(
    #             request_data=data, required_fields=required_fields)

    #         if is_valid['is_error']:
    #             return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
    #                                       message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
    #                                       error=is_valid['data'])

    #         reference_plan_id = data.get('reference_plan_id')

    #         plan = Plan.get_by_reference_plan_id(
    #             reference_plan_id=reference_plan_id)

    #         expire_at = datetime.utcnow() + timedelta(days=2)

    #         payload = {
    #             'plan_id': reference_plan_id,
    #             'total_count': BillingInterval.get_interval(plan.period),
    #             'expire_by': int(expire_at.timestamp()),
    #             'customer_notify': 1,
    #             'addons': [],
    #             'notes': {},
    #             'notify_info': {'notify_email': user_email}
    #         }

    #         response = RAZORPAY_CLIENT.subscription.create(payload)

    #         return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUBSCRIPTION_LINK_CREATED.value, data=response, error=None)

    #     except Exception as exception_error:
    #         logger.error(
    #             f'GET -> Failed while creating plan: {exception_error}')
    #         return send_json_response(
    #             http_status=500,
    #             response_status=False,
    #             message_key=ResponseMessageKeys.FAILED.value,
    #         )
