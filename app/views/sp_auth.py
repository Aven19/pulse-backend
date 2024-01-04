"""Contains user related API definitions."""
import base64
from datetime import datetime
from datetime import timedelta
import time
import urllib.parse

from app import config_data
from app import db
from app import logger
from app.helpers.constants import APP_NAME
from app.helpers.constants import ASpURL
from app.helpers.constants import EcommPulsePlanName
from app.helpers.constants import EntityType
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import QueueName
from app.helpers.constants import ResponseMessageKeys
from app.helpers.constants import SubscriptionStates
from app.helpers.decorators import api_time_logger
from app.helpers.decorators import token_required
from app.helpers.queue_helper import add_queue_task_and_enqueue
from app.helpers.utility import field_type_validator
from app.helpers.utility import get_asp_credentials_schema
from app.helpers.utility import get_current_timestamp
from app.helpers.utility import required_validator
from app.helpers.utility import send_json_response
from app.models.account import Account
from app.models.plan import Plan
from app.models.subscription import Subscription
from app.models.user import User
from flask import request
from providers.mail import send_mail
import requests
from werkzeug.exceptions import BadRequest


class SpAuth:
    """Class for Selling Partner Authorization."""
    @staticmethod
    @api_time_logger
    @token_required
    def asp_authorisation(user_object, account_object):
        """ Authorise Selling Partner APP """
        try:

            state = account_object.uuid

            # Set the required parameters
            seller_region = config_data.get('SP_DEFAULT_MARKETPLACE')
            client_id = config_data.get('SP_APP_AUTH_CLIENT_ID')

            redirect_uri = config_data.get(
                'WEB_BASE_URL') + config_data.get('WEB_AZ_CALLBACK_URI')

            # Construct the authorization URI
            if config_data.get('APP_ENV') == 'PROD':
                authorization_uri = f'{seller_region}/apps/authorize/consent?application_id={client_id}&state={state}&redirect_uri={urllib.parse.quote(redirect_uri)}'
            else:
                authorization_uri = f'{seller_region}/apps/authorize/consent?application_id={client_id}&state={state}&redirect_uri={urllib.parse.quote(redirect_uri)}&version=beta'

            logger.info(
                f'Initiate Amazon Seller Partner url: {authorization_uri}')
            # Redirect the user to the authorization URI

            return send_json_response(
                http_status=200,
                response_status=True,
                message_key=ResponseMessageKeys.ASP_URI.value,
                data={'authorization_uri': authorization_uri},
            )

        except Exception as exception_error:
            """ Exception while connecting Amazon Account """
            logger.error(f'GET -> Amazon connect failed: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    def get_az_callback_template():
        """ Front end redirect url"""
        params = request.args
        return params

    @staticmethod
    @api_time_logger
    @token_required
    def asp_authorisation_callback(user_object, account_object):
        """Information sent by Amazon while authorization"""

        try:

            logged_in_user = user_object.id
            user_email = user_object.email

            account_id = account_object.uuid

            data = request.get_json(force=True)

            # Data Validation
            field_types = {'state': str,
                           'seller_partner_id': str, 'spapi_oauth_code': str}
            required_fields = [
                'state', 'seller_partner_id', 'spapi_oauth_code']

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

            state = data.get('state')
            mws_auth_token = data.get('mws_auth_token')
            seller_partner_id = data.get('seller_partner_id')
            spapi_oauth_code = data.get('spapi_oauth_code')

            if logged_in_user and state:
                account_exists = Account.get_by_asp_id(
                    asp_id=seller_partner_id)
                # check if user id and account id exist in user account
                if account_exists and account_exists.primary_user_id != logged_in_user:
                    return send_json_response(http_status=HttpStatusCode.UNAUTHORIZED.value, response_status=False, message_key=ResponseMessageKeys.ACCOUNT_EXISTS.value, data=[], error=None)

                account = Account.get_by_uuid(uuid=state)

                _asp_cred = account.asp_credentials

                # Get Amazon credentials schema
                credentials_schema = get_asp_credentials_schema()

                # Update Amazon credentials schema
                credentials_schema.update({
                    'oauth_state': state,
                    'mws_auth_token': mws_auth_token,
                    'seller_partner_id': seller_partner_id,
                    'spapi_oauth_code': spapi_oauth_code,
                    'spapi_oauth_code_updated_at': get_current_timestamp()
                })

                if _asp_cred is None:
                    account.asp_id = seller_partner_id
                    credentials_schema['seller_partner_id'] = seller_partner_id
                    credentials_schema['created_at'] = get_current_timestamp()

                account.asp_credentials = credentials_schema
                db.session.commit()

                if seller_partner_id and spapi_oauth_code:
                    url = f'{ASpURL.AMAZON_API_BASE_URL.value}/auth/o2/token'
                    params = {
                        'grant_type': 'authorization_code',
                        'code': spapi_oauth_code,
                        # 'redirect_uri': config_data.get('APP_AUTH_REDIRECT_BASE_URL') + ASpURL.LWA_CALLBACK_URL.value,
                        'client_id': config_data.get('SP_LWA_APP_ID'),
                        'client_secret': config_data.get('SP_LWA_CLIENT_SECRET')
                    }

                    headers = {
                        'host': ASpURL.AMAZON_API_BASE_URL.value[8:],
                        'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'
                    }

                    response = requests.request('POST', url, headers=headers, params=params)  # type: ignore  # noqa: FKA100

                    results = response.json()

                    if response.status_code != HttpStatusCode.OK.value:
                        if 'error' in results:
                            logger.error(
                                f'Amazon seller partner app authorisation failed: Error fetching lwa refresh_token from Amazon: {results}')
                            return send_json_response(
                                http_status=HttpStatusCode.UNAUTHORIZED.value,
                                response_status=False,
                                message_key=ResponseMessageKeys.ASP_TOKEN_EXPIRED.value,
                                data=None,
                                error=results
                            )

                    access_token = None
                    if 'refresh_token' in results:
                        get_asp_cred = Account.get_by_user_id(
                            primary_user_id=logged_in_user)
                        if get_asp_cred:
                            _update_refresh_token = get_asp_cred.asp_credentials.copy()
                            access_token = results['access_token']

                            _update_refresh_token.update({
                                'access_token': access_token,
                                'token_type': results['token_type'],
                                'expires_in': results['expires_in'],
                                'refresh_token': results['refresh_token'],
                                'refresh_token_updated_at': get_current_timestamp()
                            })

                            current_time = int(time.time())
                            get_asp_cred.asp_credentials = _update_refresh_token

                            if get_asp_cred.asp_id_connected_at is None:

                                get_asp_cred.asp_id_connected_at = current_time
                                get_asp_cred.asp_sync_started_at = current_time
                                db.session.commit()

                                # """Send Email to User for Seller Account connected and it will take 48 hours for sync"""

                                connect_email_subject = f'Your Amazon Seller Account is Now Successfully Connected to {APP_NAME}'

                                email_data = {
                                    'email_to': user_email,
                                    'app_name': APP_NAME,
                                    'first_name': user_object.first_name
                                }

                                send_mail(email_to=user_email, subject=connect_email_subject,
                                          template='emails/az_sp_sync_email.html', data=email_data)

                                # Send default as true to sync default dates.
                                data.update({'default_sync': True})

                                """Define timing in minutes"""
                                queue_ledger_summary_report = 5
                                queue_fba_returns_report = 20
                                queue_fba_reimbursements_report = 40
                                queue_fba_customer_shipment_sales_report = 60
                                queue_finance_event_list = 120
                                queue_order_report = 240
                                queue_sales_traffic_report = 360

                                # queuing Item master report
                                add_queue_task_and_enqueue(queue_name=QueueName.ITEM_MASTER_REPORT, account_id=account_id,
                                                           logged_in_user=logged_in_user, entity_type=EntityType.ITEM_MASTER.value, data=data)

                                # queuing Ledger Summary report
                                add_queue_task_and_enqueue(queue_name=QueueName.LEDGER_SUMMARY_REPORT, account_id=account_object.uuid,
                                                           logged_in_user=user_object.id, entity_type=EntityType.LEDGER_SUMMARY_REPORT.value, data=data, time_delta=timedelta(minutes=queue_ledger_summary_report))

                                # FBA Returns
                                add_queue_task_and_enqueue(queue_name=QueueName.FBA_RETURNS_REPORT, account_id=account_object.uuid,
                                                           logged_in_user=user_object.id, entity_type=EntityType.FBA_RETURNS_REPORT.value, data=data, time_delta=timedelta(minutes=queue_fba_returns_report))

                                # FBA Reimbursements
                                add_queue_task_and_enqueue(queue_name=QueueName.FBA_REIMBURSEMENTS_REPORT, account_id=account_object.uuid,
                                                           logged_in_user=user_object.id, entity_type=EntityType.FBA_REIMBURSEMENTS_REPORT.value, data=data, time_delta=timedelta(minutes=queue_fba_reimbursements_report))

                                # # FBA Customer Shipment Sales Data
                                add_queue_task_and_enqueue(queue_name=QueueName.FBA_CUSTOMER_SHIPMENT_SALES_REPORT, account_id=account_object.uuid,
                                                           logged_in_user=user_object.id, entity_type=EntityType.FBA_CUSTOMER_SHIPMENT_SALES_REPORT.value, data=data, time_delta=timedelta(minutes=queue_fba_customer_shipment_sales_report))

                                # queuing Finance Event report
                                add_queue_task_and_enqueue(queue_name=QueueName.FINANCE_EVENT_LIST, account_id=account_object.uuid,
                                                           logged_in_user=user_object.id, entity_type=EntityType.FINANCE_EVENT_LIST.value, data=data, time_delta=timedelta(minutes=queue_finance_event_list))

                                # queuing Order report
                                add_queue_task_and_enqueue(queue_name=QueueName.ORDER_REPORT, account_id=account_id,
                                                           logged_in_user=logged_in_user, entity_type=EntityType.ORDER_REPORT.value, data=data, time_delta=timedelta(minutes=queue_order_report))

                                # # queuing sales and traffic report
                                add_queue_task_and_enqueue(queue_name=QueueName.SALES_TRAFFIC_REPORT, account_id=account_id,
                                                           logged_in_user=logged_in_user, entity_type=EntityType.SALES_TRAFFIC_REPORT.value, data=data, time_delta=timedelta(minutes=queue_sales_traffic_report))

                                # queuing settlement report v2
                                # add_queue_task_and_enqueue(queue_name=QueueName.SETTLEMENT_REPORT_V2, account_id=account_id,
                                #                            logged_in_user=logged_in_user, entity_type=EntityType.SETTLEMENT_V2_REPORT.value, data=data)

                                # # queuing performance zone
                                # add_queue_task_and_enqueue(queue_name=QueueName.AZ_PERFORMANCE_ZONE, account_id=account_id,
                                #                            logged_in_user=logged_in_user, entity_type=EntityType.MR_PERFORMANCE_ZONE.value, data=data)

                            else:
                                # 'Update refresh token if connected again'
                                logger.info(
                                    'Amazon seller partner API: Updated refresh token')
                                db.session.commit()

                    else:
                        return send_json_response(
                            http_status=500,
                            response_status=False,
                            message_key=ResponseMessageKeys.FAILED.value,
                        )

                trial_plan_value = EcommPulsePlanName.ECOMM_FREE_TRAIL.value

                free_subscription = Subscription.get_by_reference_subscription_id(
                    account_id=account_id, reference_subscription_id=trial_plan_value)

                if free_subscription is None:
                    """Check for Free Plan"""
                    get_free_plan = Plan.get_by_reference_plan_id(
                        reference_plan_id=trial_plan_value)
                    if get_free_plan:
                        start_date = datetime.now()
                        end_date = start_date + timedelta(days=10)
                        start_date_iso = start_date.isoformat()
                        end_date_iso = end_date.isoformat()
                        Subscription.add_update(account_id=account_id, reference_subscription_id=get_free_plan.reference_plan_id, plan_id=get_free_plan.id, payment_id=0,
                                                status=SubscriptionStates.ACTIVE.value, start_date=start_date_iso, end_date=end_date_iso)

                        # """Send Free trial Email to User"""

                        trail_email_subject = f'Your {APP_NAME} Free Trial Has Begun!'

                        email_data = {
                            'email_to': user_email,
                            'app_name': APP_NAME,
                            'start_date': start_date.date(),
                            'first_name': user_object.first_name
                        }

                        send_mail(email_to=user_email, subject=trail_email_subject,
                                  template='emails/free_trial_email.html', data=email_data)

                users_data, account_dict = User.serialize(user_object)

                data = {
                    'objects': {
                        'user': users_data[0]
                    }
                }

                data['result'] = account_dict

                return send_json_response(
                    http_status=200,
                    response_status=True,
                    message_key=ResponseMessageKeys.ASP_CONNECTED.value,
                    data=data
                )

            return send_json_response(
                http_status=HttpStatusCode.NOT_FOUND.value,
                response_status=False,
                message_key=ResponseMessageKeys.PLEASE_TRY_AFTER_SECONDS.value,
                data=None
            )

        except BadRequest as exception_error:
            logger.error(
                f'POST -> Amazon seller partner app Authorisation Callback Failed: {exception_error}')
            return send_json_response(
                http_status=HttpStatusCode.BAD_REQUEST.value,
                response_status=False,
                message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
            )

        except Exception as exception_error:
            logger.error(
                f'Amazon seller partner app Authorisation Callback Failed: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    def app_lwa_refresh_token():
        """To exchange an LWA authorization code for an LWA refresh token"""
        try:
            logged_in_user = request.user.id

            account = Account.get_by_user_id(primary_user_id=logged_in_user)
            _asp_cred = account.asp_credentials.copy()

            if account.asp_id and _asp_cred.get('spapi_oauth_code'):
                url = f'{ASpURL.AMAZON_API_BASE_URL.value}/auth/o2/token'
                params = {
                    'grant_type': 'authorization_code',
                    'code': _asp_cred.get('spapi_oauth_code'),
                    # 'redirect_uri': config_data.get('APP_AUTH_REDIRECT_BASE_URL') + ASpURL.LWA_CALLBACK_URL.value,
                    'client_id': config_data.get('SP_LWA_APP_ID'),
                    'client_secret': config_data.get('SP_LWA_CLIENT_SECRET')
                }

                headers = {
                    'host': ASpURL.AMAZON_API_BASE_URL.value[8:],
                    'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'
                }

                response = requests.request('POST', url, headers=headers, params=params)  # type: ignore  # noqa: FKA100

                results = response.json()

                if response.status_code != HttpStatusCode.OK.value:
                    if 'error' in results:
                        logger.error(
                            f'App authorisation failed: Error fetching lwa refresh_token from Amazon: {results}')
                        return send_json_response(
                            http_status=HttpStatusCode.UNAUTHORIZED.value,
                            response_status=False,
                            message_key=ResponseMessageKeys.TOKEN_EXPIRED.value,
                            data=None,
                            error=results
                        )

                if 'refresh_token' in results:
                    if account:
                        _asp_cred.update({
                            'access_token': results['access_token'],
                            'token_type': results['token_type'],
                            'expires_in': results['expires_in'],
                            'refresh_token': results['refresh_token'],
                            'refresh_token_updated_at': get_current_timestamp()
                        })
                        account.asp_credentials = _asp_cred
                        db.session.commit()
                else:
                    return send_json_response(http_status=HttpStatusCode.OK.value, response_status=False, message_key=ResponseMessageKeys.NO_DATA_FOUND.value, data=None, error=None)

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data='', error=None)

        except Exception as exception_error:
            logger.error(
                f'App Authorisation Callback Failed: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    def app_get_access_token():
        """To exchange an LWA authorization code for an LWA refresh token"""
        try:

            data = {}
            data['access_token'] = request.args.get('access_token')
            data['token_type'] = request.args.get('mws_auth_token')
            data['expires_in'] = request.args.get('selling_partner_id')
            data['refresh_token'] = request.args.get('spapi_oauth_code')

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=data, error=None)

        except Exception as exception_error:
            logger.error(
                f'App Authorisation Callback Failed: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    def fsp_authorisation(user_object, account_object):
        """Information sent by Amazon while authorization"""
        try:

            state = account_object.uuid

            url = 'https://api.flipkart.net/oauth-service/oauth/authorize'

            params = {
                'client_id': config_data.get('FSP_CLIENT_ID'),
                'redirect_uri': config_data.get('WEB_FSP_CALLBACK_URI'),
                'response_type': 'code',
                'scope': 'Seller_Api',
            }

            # Construct the authorization URI
            if config_data.get('APP_ENV') == 'PROD':
                authorization_uri = f'{url}?client_id={params.get("client_id")}&redirect_uri={urllib.parse.quote(params.get("redirect_uri"))}&response_type={params.get("response_type")}&scope={params.get("scope")}&state={state}'
            else:
                authorization_uri = f'{url}?client_id={params.get("client_id")}&redirect_uri={urllib.parse.quote(params.get("redirect_uri"))}&response_type={params.get("response_type")}&scope={params.get("scope")}&state={state}'

            logger.info(
                f'Initiate Flipkart Seller Partner url: {authorization_uri}')
            # Redirect the user to the authorization URI

            return send_json_response(
                http_status=200,
                response_status=True,
                message_key=ResponseMessageKeys.FSP_URI.value,
                data={'authorization_uri': authorization_uri},
            )

        except Exception as exception_error:
            logger.error(
                f'FSP App Authorisation Callback Failed: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    def get_fsp_callback_template():
        """ Front end redirect url"""
        params = request.args
        return params

    @staticmethod
    @api_time_logger
    @token_required
    def fsp_authorisation_callback(user_object, account_object):
        """Information sent by Flipkart while authorization"""
        try:

            logged_in_user = user_object.id
            account_id = account_object.uuid

            data = request.get_json(force=True)

            # Data Validation
            field_types = {'state': str, 'code': str}
            required_fields = ['state', 'code']

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

            state = data.get('state')
            code = data.get('code')

            if logged_in_user and state:

                if config_data.get('APP_ENV') == 'PROD':
                    url = 'https://api.flipkart.net/oauth-service/oauth/token'
                else:
                    url = 'https://api.flipkart.net/oauth-service/oauth/token'

                params = {
                    'redirect_uri': config_data.get('WEB_FSP_CALLBACK_URI'),
                    'grant_type': 'authorization_code',
                    'state': state,
                    'code': code
                }

                # Replace with your application id or client id
                client_id = config_data.get('FSP_CLIENT_ID')
                # Replace with your application secret or client secret
                client_secret = config_data.get('FSP_SECRET')

                credentials = client_id + ':' + client_secret

                encoded_credentials = base64.b64encode(
                    credentials.encode('utf-8')).decode('utf-8')

                headers = {
                    'Authorization': 'Basic ' + encoded_credentials
                }

                response_json = requests.request('GET', url, headers=headers, params=params).json()  # type: ignore  # noqa: FKA100

                access_token = response_json['access_token']

                result = {
                    'account_id': account_id,
                    'fsp_token': access_token
                }

                return send_json_response(
                    http_status=200,
                    response_status=True,
                    message_key=ResponseMessageKeys.FSP_CONNECTED.value,
                    data=result,
                )

        except Exception as exception_error:
            logger.error(
                f'App Authorisation Callback Failed: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    def asp_data_sync(user_object, account_object):
        """Sync Amazon related Api's"""

        try:

            logged_in_user = user_object.id
            account_id = account_object.uuid

            data = request.get_json(force=True)

            # Data Validation
            field_types = {'seller_partner_id': str,
                           'default_time_period': str, 'requested_queue': str}

            required_fields = ['seller_partner_id',
                               'default_time_period', 'requested_queue']

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

            if logged_in_user:
                # Send default as true to sync default dates.
                data.update(
                    {'default_sync': True, 'default_time_period': default_time_period})

                logger.info('*' * 200)
                logger.info(f'Queue Name: {queue_name}')
                if queue_name == QueueName.ITEM_MASTER_REPORT:
                    logger.info(f'Queueing {QueueName.ITEM_MASTER_REPORT}')
                    """ queuing Item master report """
                    add_queue_task_and_enqueue(queue_name=QueueName.ITEM_MASTER_REPORT, account_id=account_id,
                                               logged_in_user=logged_in_user, entity_type=EntityType.ITEM_MASTER.value, data=data)
                elif queue_name == QueueName.ORDER_REPORT:
                    logger.info(f'Queueing {QueueName.ORDER_REPORT}')
                    """ queuing Order report """
                    add_queue_task_and_enqueue(queue_name=QueueName.ORDER_REPORT, account_id=account_id,
                                               logged_in_user=logged_in_user, entity_type=EntityType.ORDER_REPORT.value, data=data)
                elif queue_name == QueueName.SALES_TRAFFIC_REPORT:
                    logger.info(f'Queueing {QueueName.SALES_TRAFFIC_REPORT}')
                    """ queuing sales and traffic report """
                    add_queue_task_and_enqueue(queue_name=QueueName.SALES_TRAFFIC_REPORT, account_id=account_id,
                                               logged_in_user=logged_in_user, entity_type=EntityType.SALES_TRAFFIC_REPORT.value, data=data)
                elif queue_name == QueueName.LEDGER_SUMMARY_REPORT:
                    logger.info(f'Queueing {QueueName.LEDGER_SUMMARY_REPORT}')
                    """ queuing Ledger Summary report """
                    add_queue_task_and_enqueue(queue_name=QueueName.LEDGER_SUMMARY_REPORT, account_id=account_object.uuid,
                                               logged_in_user=user_object.id, entity_type=EntityType.LEDGER_SUMMARY_REPORT.value, data=data)
                elif queue_name == QueueName.FINANCE_EVENT_LIST:
                    logger.info(f'Queueing {QueueName.FINANCE_EVENT_LIST}')
                    """ queuing Finance Event report """
                    add_queue_task_and_enqueue(queue_name=QueueName.FINANCE_EVENT_LIST, account_id=account_object.uuid,
                                               logged_in_user=user_object.id, entity_type=EntityType.FINANCE_EVENT_LIST.value, data=data)
                elif queue_name == QueueName.FBA_RETURNS_REPORT:
                    logger.info(f'Queueing {QueueName.FBA_RETURNS_REPORT}')
                    """ FBA Returns """
                    add_queue_task_and_enqueue(queue_name=QueueName.FBA_RETURNS_REPORT, account_id=account_object.uuid,
                                               logged_in_user=user_object.id, entity_type=EntityType.FBA_RETURNS_REPORT.value, data=data)
                elif queue_name == QueueName.FBA_REIMBURSEMENTS_REPORT:
                    logger.info(
                        f'Queueing {QueueName.FBA_REIMBURSEMENTS_REPORT}')
                    """ FBA Reimbursements """
                    add_queue_task_and_enqueue(queue_name=QueueName.FBA_REIMBURSEMENTS_REPORT, account_id=account_object.uuid,
                                               logged_in_user=user_object.id, entity_type=EntityType.FBA_REIMBURSEMENTS_REPORT.value, data=data)
                elif queue_name == QueueName.FBA_CUSTOMER_SHIPMENT_SALES_REPORT:
                    logger.info(
                        f'Queueing {QueueName.FBA_CUSTOMER_SHIPMENT_SALES_REPORT}')
                    """ FBA Customer Shipment Sales Data """
                    add_queue_task_and_enqueue(queue_name=QueueName.FBA_CUSTOMER_SHIPMENT_SALES_REPORT, account_id=account_object.uuid,
                                               logged_in_user=user_object.id, entity_type=EntityType.FBA_CUSTOMER_SHIPMENT_SALES_REPORT.value, data=data)

                # queuing performance zone
                # add_queue_task_and_enqueue(queue_name=QueueName.AZ_PERFORMANCE_ZONE, account_id=account_id,
                #                            logged_in_user=logged_in_user, entity_type=EntityType.MR_PERFORMANCE_ZONE.value, data=data)

                result = {
                    'queue_name': queue_name,
                    'default_time_period': default_time_period
                }

                return send_json_response(
                    http_status=200,
                    response_status=True,
                    message_key=ResponseMessageKeys.ASP_SYNC_SUCCESS.value,
                    data=result,
                )
            else:
                return send_json_response(
                    http_status=500,
                    response_status=False,
                    message_key=ResponseMessageKeys.FAILED.value,
                )

        except BadRequest as exception_error:
            logger.error(
                f'POST -> Amazon seller partner API Sync Failed: {exception_error}')
            return send_json_response(
                http_status=HttpStatusCode.BAD_REQUEST.value,
                response_status=False,
                message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
            )

        except Exception as exception_error:
            logger.error(
                f'Amazon seller partner API Sync Failed: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    def store_sp_info(user_object, account_object):
        """ Amazon API store Account Info """
        try:
            logged_in_user = user_object.id

            data = request.get_json(force=True)

            # Data Validation
            field_types = {'profile_name': str, 'marketplace_region': str}
            required_fields = ['profile_name', 'marketplace_region']

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

            marketplace_region = data.get('marketplace_region')
            profile_name = data.get('profile_name')

            account = Account.get_by_user_id(logged_in_user)

            if account is not None:
                account.asp_marketplace = marketplace_region
                account.display_name = profile_name
                db.session.commit()

                users_data, account_dict = User.serialize(user_object)

                data = {
                    'objects': {
                        'user': users_data[0]
                    }
                }

                data['result'] = account_dict

                return send_json_response(
                    http_status=200,
                    response_status=True,
                    message_key=ResponseMessageKeys.INFO_SAVED.value,
                    data=data,
                )

            return send_json_response(
                http_status=HttpStatusCode.UNAUTHORIZED.value,
                response_status=True,
                message_key=ResponseMessageKeys.ACCESS_DENIED.value,
            )

        except BadRequest as exception_error:
            logger.error(
                f'POST -> Amazon Sp API save account info failed: {exception_error}')
            return send_json_response(
                http_status=HttpStatusCode.BAD_REQUEST.value,
                response_status=False,
                message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
            )

        except Exception as exception_error:
            logger.error(
                f'Amazon Sp API save account info failed: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )
