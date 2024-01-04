"""All custom decorators are defined in this file."""
from datetime import datetime
from datetime import timedelta
from functools import wraps
import time
from typing import Callable

from app import COGNITO_CLIENT
from app import config_data
from app import logger
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import ResponseMessageKeys
from app.helpers.constants import SubscriptionOpenUrl
from app.helpers.utility import get_date_and_time_from_timestamp
from app.helpers.utility import send_json_response
from app.models.account import Account
from app.models.subscription import Subscription
from app.models.user import User
from app.models.user_account import UserAccount
from flask import g
from flask import request

# def token_required(f: Callable) -> Callable:  # type: ignore  # noqa: C901
#     """To check request contains valid token."""
#     @wraps(f)
#     def decorated(*args, **kwargs):
#         """This method validates token with DB token."""

#         x_account_value = config_data.get('ECOMM_X_ACCOUNT')  # type: ignore  # noqa: FKA100
#         user_object = None
#         account_object = None

#         try:

#             user_object = User.get_by_email(
#                 config_data.get('ECOMM_USER_EMAIL'))

#             if user_object is None:
#                 return send_json_response(http_status=401, response_status=False, message_key=ResponseMessageKeys.USER_DETAILS_NOT_FOUND.value, data=None, error=None)

#             if x_account_value:

#                 account_object = Account.get_by_uuid(x_account_value)

#                 kwargs = {}

#                 if account_object is None:
#                     return send_json_response(http_status=401, response_status=False,
#                                               message_key=ResponseMessageKeys.ASP_TOKEN_EXPIRED.value,
#                                               data=None, error=None)

#         except Exception as e:
#             logger.info('we got exception {0}'.format(e))
#             return send_json_response(http_status=HttpStatusCode.UNAUTHORIZED.value, response_status=False,
#                                       message_key=ResponseMessageKeys.INVALID_TOKEN.value,
#                                       data=None,
#                                       error=None)

#         return f(user_object, account_object, *args, **kwargs)  # type: ignore  # noqa: FKA100

#     return decorated


def token_required(f: Callable) -> Callable:  # type: ignore  # noqa: C901
    """To check request contains valid token."""
    @wraps(f)
    def decorated(*args, **kwargs):
        """This method validates token with DB token."""

        x_account_value = kwargs.get('x-account', False)  # type: ignore  # noqa: FKA100
        access_token = None
        user_object = None
        account_object = None

        # jwt is passed in the request header
        if 'x-authorization' in request.headers:
            access_token = request.headers['x-authorization']

        if not access_token:
            return send_json_response(http_status=HttpStatusCode.UNAUTHORIZED.value, response_status=False,
                                      message_key=ResponseMessageKeys.INVALID_TOKEN.value,
                                      data=None,
                                      error=None)

        try:

            user_info = COGNITO_CLIENT.get_user(
                AccessToken=request.headers.get('x-authorization')  # authtoken
            )

            user_email = [data.get('Value') for data in user_info.get(
                'UserAttributes') if data.get('Name') == 'email'][0]

            user_object = User.get_by_email(user_email)

            if user_object is None:
                return send_json_response(http_status=401, response_status=False, message_key=ResponseMessageKeys.USER_DETAILS_NOT_FOUND.value, data=None, error=None)

            if x_account_value:
                x_account = request.headers.get('x-account')
                if not x_account:
                    return send_json_response(http_status=HttpStatusCode.UNAUTHORIZED.value, response_status=False,
                                              message_key=ResponseMessageKeys.INVALID_ACCOUNT.value,
                                              data=None,
                                              error=None)

                account_object = Account.get_by_uuid(x_account)

                kwargs = {}

                if account_object is None:
                    return send_json_response(http_status=401, response_status=False,
                                              message_key=ResponseMessageKeys.ASP_TOKEN_EXPIRED.value,
                                              data=None, error=None)

                is_subscription_active = Subscription.get_status_by_account_id(
                    account_id=x_account)

                if account_object.asp_id is not None and account_object.asp_id_connected_at is not None:
                    asp_id_connected_at_datetime = get_date_and_time_from_timestamp(
                        account_object.asp_id_connected_at)

                    hours_48 = asp_id_connected_at_datetime + \
                        timedelta(hours=48)

                    if request.path not in SubscriptionOpenUrl.URLS.value and datetime.now() < hours_48:
                        """Show No content while we are syncing data"""
                        return send_json_response(http_status=204, response_status=False,
                                                  message_key=ResponseMessageKeys.ASP_SYNC_RUNNING.value)

                if request.path not in SubscriptionOpenUrl.URLS.value and not is_subscription_active:
                    """Return Inactive Subscription"""
                    return send_json_response(http_status=400, response_status=False,
                                              message_key=ResponseMessageKeys.SUBSCRIPTION_INACTIVE.value,
                                              data=None, error=None)

                # check if user-account is deactivated
                user_account_object = UserAccount.is_user_account_exists(
                    user_id=user_object.id, account_id=account_object.id)

                if user_account_object is None:
                    return send_json_response(http_status=401, response_status=False, message_key=ResponseMessageKeys.USER_ACCOUNT_NOT_LINKED.value, data=None, error=None)

                if user_account_object.deactivated_at:
                    return send_json_response(http_status=401, response_status=False,
                                              message_key=ResponseMessageKeys.INACTIVE_USER_ACCOUNT.value,
                                              data=None, error=None)

            # setattr(request, 'user', user_object)
            # setattr(request, 'account', account_object)

        except Exception as e:
            logger.info('we got exception {0}'.format(e))
            return send_json_response(http_status=HttpStatusCode.UNAUTHORIZED.value, response_status=False,
                                      message_key=ResponseMessageKeys.INVALID_TOKEN.value,
                                      data=None,
                                      error=None)
        # returns the current logged in users contex to the routes

        return f(user_object, account_object, *args, **kwargs)  # type: ignore  # noqa: FKA100

    return decorated


def brand_filter(f: Callable) -> Callable:
    """
    A decorator to filter brands based on user and account.

    This decorator retrieves the allowed brands for a logged-in user and account
    and passes them as an argument to the decorated function.

    :param f: The function to be decorated.
    :return: The decorated function.
    """
    @wraps(f)
    def decorated(user_object, account_object, *args, **kwargs):
        # Get the ID of the logged-in user and account
        logged_in_user = user_object.id
        account_id = account_object.id

        # Retrieve the allowed brands for the user and account
        allowed_brands = UserAccount.get_brand(
            user_id=logged_in_user, account_id=account_id)

        # Pass the allowed brands to the decorated function along with other arguments
        return f(user_object, account_object, allowed_brands, *args, **kwargs)  # type: ignore  # noqa: FKA100
    return decorated


def api_time_logger(method: Callable) -> Callable:
    """To return total request time.
    1. takes time at start
    2. performs 'method'
    3. takes time at end: after method returns a response
    4. Calculates the difference and inserts in the api log table to keep track of how long the method took to execute
    """
    @wraps(method)
    def wrapper(*args, **kwargs):
        """This method calculate time difference."""
        start = time.time()
        response = method(*args, **kwargs)
        end = time.time()
        g.time_log = round(end - start, 5)  # type: ignore  # noqa: FKA100
        return response
    return wrapper


def asp_credentials_required(f: Callable) -> Callable:  # type: ignore  # noqa: C901
    """To get amazon seller partner credentials from DB"""

    @wraps(f)
    def decorated(*args, **kwargs):
        """This method validates token with DB token."""
        try:

            account = request.account_object.uuid
            asp_cred = request.account_object.asp_credentials

            credentials = {}

            # account_exists = Account.get_by_uuid(
            #     uuid=account.uuid)

            # current_seller = Account.get_by_user_id(
            #     primary_user_id=logged_in_user)

            if account is not None and asp_cred is not None:
                credentials['seller_partner_id'] = asp_cred.get(
                    'seller_partner_id')
                credentials['refresh_token'] = asp_cred.get('refresh_token')
                credentials['client_id'] = config_data.get('SP_LWA_APP_ID')
                credentials['client_secret'] = config_data.get(
                    'SP_LWA_CLIENT_SECRET')
            else:
                return send_json_response(http_status=HttpStatusCode.UNAUTHORIZED.value, response_status=False,
                                          message_key=ResponseMessageKeys.ACCESS_DENIED.value,
                                          data=None,
                                          error=None)

        except Exception as e:
            logger.info('we got exception {0}'.format(e))
            return send_json_response(http_status=HttpStatusCode.UNAUTHORIZED.value, response_status=False,
                                      message_key=ResponseMessageKeys.ASP_INVALID_TOKEN.value,
                                      data=None,
                                      error=None)

        return f(credentials, *args, **kwargs)  # type: ignore  # noqa: FKA100

    return decorated


def az_ads_credentials_required(f: Callable) -> Callable:  # type: ignore  # noqa: C901
    """To get amazon ad's credentials from DB"""
    @wraps(f)
    def decorated(*args, **kwargs):
        """This method validates token with DB token."""
        try:

            account = request.account_object.uuid
            az_cred = request.account_object.az_ads_credentials

            credentials = {}

            if account is not None and az_cred is not None:
                a_ads_cred = az_cred
                credentials['refresh_token'] = a_ads_cred.get('refresh_token')
                credentials['client_id'] = config_data.get('AZ_AD_CLIENT_ID')
                credentials['client_secret'] = config_data.get(
                    'AZ_AD_CLIENT_SECRET')
            else:
                return send_json_response(http_status=HttpStatusCode.UNAUTHORIZED.value, response_status=False,
                                          message_key=ResponseMessageKeys.ADS_INVALID_TOKEN.value,
                                          data=None,
                                          error=None)

        except Exception as e:
            logger.info('we got exception {0}'.format(e))
            return send_json_response(http_status=HttpStatusCode.UNAUTHORIZED.value, response_status=False,
                                      message_key=ResponseMessageKeys.ADS_INVALID_TOKEN.value,
                                      data=None,
                                      error=None)

        return f(credentials, *args, **kwargs)  # type: ignore  # noqa: FKA100

    return decorated
