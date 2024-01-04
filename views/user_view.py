"""Contains user related API definitions."""
from datetime import datetime
from datetime import timedelta
import json
import time
import urllib.parse

from app import COGNITO_CLIENT
from app import config_data
from app import logger
from app.helpers.constants import APP_NAME
from app.helpers.constants import CognitoProviderType
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import InputValidation
from app.helpers.constants import ResponseMessageKeys
from app.helpers.constants import SubscriptionStates
from app.helpers.constants import UserInviteStatus
from app.helpers.decorators import token_required
from app.helpers.utility import field_type_validator
from app.helpers.utility import get_date_and_time_from_timestamp
from app.helpers.utility import object_as_dict
from app.helpers.utility import required_validator
from app.helpers.utility import send_json_response
from app.models.account import Account
from app.models.plan import Plan
from app.models.subscription import Subscription
from app.models.user import User
from app.models.user_account import UserAccount
from app.models.user_invite import UserInvite
from flask import render_template
from flask import request
from providers.mail import send_mail
from werkzeug.exceptions import BadRequest


def create_user():
    """ Creates a user and set password in cognito and add user in User table """
    try:
        """ Create User """
        # logged_in_user = request.user.id

        data = request.get_json(force=True)

        # Data Validation
        field_types = {'first_name': str, 'last_name': str,
                       'email': str, 'password': str}
        required_fields = ['first_name', 'last_name', 'email', 'password']

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

        first_name = data.get('first_name').strip()
        last_name = data.get('last_name').strip()
        email = data.get('email').strip()
        password = data.get('password').strip()

        # Check if valid Input
        if not InputValidation.is_valid(string=email, validation_type=InputValidation.EMAIL):
            return send_json_response(
                http_status=HttpStatusCode.BAD_REQUEST.value,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED_EMAIL_VALIDATION.value,
            )

        if not InputValidation.is_valid(
            string=password, validation_type=InputValidation.PASSWORD
        ):
            return send_json_response(
                http_status=HttpStatusCode.BAD_REQUEST.value,
                response_status=False,
                message_key=ResponseMessageKeys.INVALID_PASSWORD.value,
            )

        # To prevent user to register if already registered from Google/LoginWithAmazon IDP
        user_obj = User.get_by_email(email)

        if user_obj:

            is_primary_user = Account.is_account_and_primary_user(
                user_id=user_obj.id)

            if is_primary_user:

                return send_json_response(
                    http_status=HttpStatusCode.BAD_REQUEST.value,
                    response_status=False,
                    message_key=ResponseMessageKeys.ACCOUNT_ALREADY_EXISTS.value,
                )

            # if not primary user of any account -> create account
            create_account_details(user_obj.id)

            """Send Welcome Email to User"""

            email_subject = f'Welcome to {APP_NAME}!'

            email_data = {
                'email_to': user_obj.email,
                'app_name': APP_NAME,
                'first_name': user_obj.first_name
            }

            send_mail(email_to=user_obj.email, subject=email_subject,
                      template='emails/welcome_email.html', data=email_data)

            logger.info('---------email sent-------------------')

            return send_json_response(
                http_status=200,
                response_status=True,
                message_key=ResponseMessageKeys.USER_ADDED_SUCCESSFULLY.value,
            )

        # check if user exits in cognito
        # response = COGNITO_CLIENT.admin_get_user(
        #     UserPoolId=config_data.get('COGNITO_USER_POOL_ID'),
        #     Username=email
        # )
        # response = COGNITO_CLIENT.list_users(
        #     UserPoolId=config_data.get('COGNITO_USER_POOL_ID'),
        #     AttributesToGet=['email'],
        #     Filter=f'email="{email}"'
        # )

        """ Add User in Cognito User Pool """
        COGNITO_CLIENT.admin_create_user(
            UserPoolId=config_data.get('COGNITO_USER_POOL_ID'),
            Username=email,
            UserAttributes=[
                {'Name': 'email', 'Value': email},
                {'Name': 'name', 'Value': f'{first_name} {last_name}'},
                {'Name': 'email_verified', 'Value': 'true'}
            ],
            ForceAliasCreation=True,
            # Uncomment this if don't want to send email invite while creating user on cognito.
            MessageAction='SUPPRESS',
            TemporaryPassword=password,
            # DesiredDeliveryMediums=[
            #     'EMAIL',
            # ],
        )

        """ Set User Password """
        COGNITO_CLIENT.admin_set_user_password(
            UserPoolId=config_data.get('COGNITO_USER_POOL_ID'),
            Username=email,
            Password=password,
            # Permanent=True | False, # Uncomment this code if we want user status as CONFIRM. i.e. if we don't require user to change password at first time login.
            Permanent=True,
        )

        """ Add User to Database """
        user = User.add(email=email, first_name=first_name,
                        last_name=last_name, password=password)

        user.save()

        if user:
            create_account_details(user.id)

            """Send Welcome Email to User"""

            email_subject = f'Welcome to {APP_NAME}!'

            email_data = {
                'email_to': user.email,
                'app_name': APP_NAME,
                'first_name': user.first_name
            }

            send_mail(email_to=user.email, subject=email_subject,
                      template='emails/welcome_email.html', data=email_data)

        return send_json_response(
            http_status=200,
            response_status=True,
            message_key=ResponseMessageKeys.USER_ADDED_SUCCESSFULLY.value,
        )

    except COGNITO_CLIENT.exceptions.UsernameExistsException as exception_error:
        """ Exception in creating user at cognito """
        logger.error(f'POST -> User Registration Failed: {exception_error}')
        return send_json_response(
            http_status=HttpStatusCode.BAD_REQUEST.value,
            response_status=False,
            message_key=ResponseMessageKeys.USER_ALREADY_EXIST.value,
        )

    except BadRequest as exception_error:
        logger.error(f'POST -> User Login Failed: {exception_error}')
        return send_json_response(
            http_status=HttpStatusCode.BAD_REQUEST.value,
            response_status=False,
            message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
        )

    except Exception as exception_error:
        """ Exception in create user """
        logger.error(f'POST -> User Registration Failed: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value,
        )


def authenticate():
    """ Authenticate user and return token """
    try:
        data = request.get_json(force=True)

        # Data Validation
        field_types = {'email': str, 'password': str}
        required_fields = ['email', 'password']

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

        email = data.get('email')
        password = data.get('password')

        # Check if valid Input
        if not InputValidation.is_valid(string=email, validation_type=InputValidation.EMAIL):
            return send_json_response(
                http_status=HttpStatusCode.BAD_REQUEST.value,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED_EMAIL_VALIDATION.value,
            )

        if not InputValidation.is_valid(
            string=password, validation_type=InputValidation.PASSWORD
        ):
            return send_json_response(
                http_status=HttpStatusCode.BAD_REQUEST.value,
                response_status=False,
                message_key=ResponseMessageKeys.INVALID_PASSWORD.value,
            )

        user_obj = User.get_by_email(email)

        # check if user is a primary user in an account or is active in atleast one account
        if user_obj:
            is_primary_user = Account.is_account_and_primary_user(
                user_id=user_obj.id)

            is_active = UserAccount.is_active(user_id=user_obj.id)

            if not is_primary_user and not is_active:
                return send_json_response(
                    http_status=400,
                    response_status=True,
                    message_key=ResponseMessageKeys.ACCOUNT_DOES_NOT_EXIST.value,
                    data=None,
                )

        if not user_obj:
            """ Sync user in local db from cognito """
            # return send_json_response(http_status=401, response_status=False, message_key=ResponseMessageKeys.USER_DETAILS_NOT_FOUND.value, data=None, error=None)
            COGNITO_CLIENT.admin_get_user(
                UserPoolId=config_data.get('COGNITO_USER_POOL_ID'), Username=email)
            # name = [attribute['Value'] for attribute in user['UserAttributes']
            #         if attribute['Name'] == 'name'][0]
            user_obj = User(email=email, password=password, last_login_at=int(
                time.time()), created_at=int(time.time()), updated_at=int(time.time())).save()
        else:
            # Check if user is logged in via Google/LoginWithAmazon provider
            user_obj.last_login = int(time.time())

        # Check if user is invited from user_invite table and is accepted then do not create account details
        is_invited = UserInvite.get_by_email(email=user_obj.email)

        if is_invited and is_invited.status == UserInviteStatus.ACCEPTED.value:
            pass
        else:
            # Create an account
            create_account_details(user_obj.id)

        # """ Uncomment this to Confirm User - Set New Password """
        # COGNITO_CLIENT.admin_set_user_password(
        #     UserPoolId=os.environ.get('COGNITO_USER_POOL_ID'),
        #     Username=email,
        #     Password=password,
        #     Permanent=True,
        # )

        # Fetch Auth Token From Cognito
        auth_token = (
            COGNITO_CLIENT.initiate_auth(
                AuthFlow='USER_PASSWORD_AUTH',
                AuthParameters={
                    'USERNAME': email,
                    'PASSWORD': password,
                },
                ClientId=config_data.get('COGNITO_APP_CLIENT_ID'),
            )
            .get('AuthenticationResult')
            .get('AccessToken')
        )

        user_obj.save()

        users_data, account_dict = User.serialize(user_obj)

        subscription_obj = get_subscription_object(
            account_dict[0]['account_id'])

        data = {
            'objects': {
                'user': users_data[0],
                'subscription': subscription_obj
            }
        }

        data['result'] = account_dict
        data['objects']['user']['auth_token'] = auth_token

        return send_json_response(
            http_status=200,
            response_status=True,
            message_key=ResponseMessageKeys.LOGIN_SUCCESSFULLY.value,
            data=data,
        )

    except COGNITO_CLIENT.exceptions.UserNotFoundException as exception_error:
        logger.error(
            f'POST -> User Not Registered: {exception_error}')
        return send_json_response(
            http_status=HttpStatusCode.BAD_REQUEST.value,
            response_status=False,
            message_key=ResponseMessageKeys.USER_NOT_EXIST.value,
        )

    except COGNITO_CLIENT.exceptions.PasswordResetRequiredException as exception_error:
        logger.error(
            f'POST -> User Password Reset Required: {exception_error}')
        return send_json_response(
            http_status=HttpStatusCode.BAD_REQUEST.value,
            response_status=False,
            message_key=ResponseMessageKeys.USER_PASS_RESET_REQUIRED.value,
        )

    except COGNITO_CLIENT.exceptions.UserNotConfirmedException as exception_error:
        logger.error(
            f'POST -> User Not Confirmed: {exception_error}')
        return send_json_response(
            http_status=HttpStatusCode.BAD_REQUEST.value,
            response_status=False,
            message_key=ResponseMessageKeys.USER_NOT_CONFIRMED.value,
        )

    except AttributeError as exception_error:
        logger.error(
            f'POST -> AttributeError while processing the request: {exception_error}')
        return send_json_response(
            http_status=HttpStatusCode.BAD_REQUEST.value,
            response_status=False,
            message_key=ResponseMessageKeys.ERROR_IS_ON_US.value,
        )

    except COGNITO_CLIENT.exceptions.NotAuthorizedException as exception_error:
        logger.error(
            f'POST -> Auth Token Generation Failed: {exception_error}')
        return send_json_response(
            http_status=HttpStatusCode.BAD_REQUEST.value,
            response_status=False,
            message_key=ResponseMessageKeys.LOGIN_FAILED.value,
        )

    except BadRequest as exception_error:
        logger.error(f'POST -> User Login Failed: {exception_error}')
        return send_json_response(
            http_status=HttpStatusCode.BAD_REQUEST.value,
            response_status=False,
            message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
        )

    except Exception as exception_error:
        logger.error(f'POST -> User Login Failed: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value,
        )


@token_required
def get_user_account_list(user_object, account_object):
    """
        API to fetch/select the user details from the database.
    """
    try:

        users_data, account_dict = User.serialize(user_object)
        subscription_obj = get_subscription_object(account_object.uuid)

        data_sync_progress = False
        if account_object.asp_id is not None and account_object.asp_id_connected_at is not None:
            asp_id_connected_at_datetime = get_date_and_time_from_timestamp(
                account_object.asp_id_connected_at)
            hours_48 = asp_id_connected_at_datetime + \
                timedelta(hours=48)
            if datetime.now() < hours_48:
                data_sync_progress = True

        data = {
            'objects': {
                'user': users_data[0],
                'az_ads_profile_listing': [profile for profile in account_object.az_ads_account_info if profile.get('created_by') == user_object.id] if account_object.az_ads_account_info is not None else None,
                'subscription': subscription_obj,
                'data_sync_progress': data_sync_progress
            }
        }

        data['result'] = account_dict

        return send_json_response(
            http_status=200,
            response_status=True,
            message_key=ResponseMessageKeys.USER_DETAILS_FETCHED_SUCCESSFULLY.value,
            data=data
        )

    except Exception as exception_error:
        logger.error(f'GET -> User Info Failed: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value,
        )


def get_user_info():
    """
        API to fetch/select the user details from the database.
    """
    try:
        user_info = object_as_dict(request.user)
        return send_json_response(
            http_status=200,
            response_status=True,
            message_key=ResponseMessageKeys.USER_DETAILS_FETCHED_SUCCESSFULLY.value,
            data=user_info,
        )

    except Exception as exception_error:
        logger.error(f'GET -> User Info Failed: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value,
        )


def get_callback_template():
    """ Front end redirect url"""
    return render_template('google-callback.html')


def google_login():
    """ Initiate Google login url """
    try:
        # The OAuth response type, which can be code for code grant flow and token for implicit flow.
        response_type = 'token'
        client_id = config_data.get('COGNITO_APP_CLIENT_ID')
        auth_url = config_data.get('COGNITO_DOMAIN_URL')
        # Cognito will send details on this url
        redirect_uri = config_data.get('COGNITO_IDP_CALLBACK_URL')
        identity_provider = 'Google'
        scope = 'aws.cognito.signin.user.admin openid phone profile'
        authorization_uri = f'{auth_url}/oauth2/authorize?identity_provider={identity_provider}&redirect_uri={urllib.parse.quote(redirect_uri)}&response_type={response_type}&client_id={client_id}&scope={scope}'

        logger.info(f'Initiate Google login url: {authorization_uri}')

        return send_json_response(
            http_status=200,
            response_status=True,
            message_key=ResponseMessageKeys.GOOGLE_AUTH_URI.value,
            data={'authorization_uri': authorization_uri},
        )

    except Exception as exception_error:
        logger.error(f'Google app authorisation failed: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value,
        )


def amazon_login():
    """ Initiate Amazon login url """
    try:
        # The OAuth response type, which can be code for code grant flow and token for implicit flow.
        response_type = 'token'
        client_id = config_data.get('COGNITO_APP_CLIENT_ID')
        auth_url = config_data.get('COGNITO_DOMAIN_URL')
        redirect_uri = config_data.get('COGNITO_IDP_CALLBACK_URL')
        identity_provider = 'LoginWithAmazon'
        scope = 'aws.cognito.signin.user.admin email openid phone profile'
        authorization_uri = f'{auth_url}/oauth2/authorize?identity_provider={identity_provider}&redirect_uri={urllib.parse.quote(redirect_uri)}&response_type={response_type}&client_id={client_id}&scope={scope}'

        logger.info(f'Initiate Amazon login url: {authorization_uri}')

        return send_json_response(
            http_status=200,
            response_status=True,
            message_key=ResponseMessageKeys.AMAZON_AUTH_URI.value,
            data={'authorization_uri': authorization_uri},
        )

    except Exception as exception_error:
        logger.error(f'Amazon app authorisation failed: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value,
        )


def idp_callback():
    """ Fetch user details by token sent by amazon cognito for IDP """
    try:
        access_token = None

        if 'x-authorization' in request.headers:
            access_token = request.headers['x-authorization']

        if not access_token:
            return send_json_response(http_status=HttpStatusCode.UNAUTHORIZED.value, response_status=False,
                                      message_key=ResponseMessageKeys.INVALID_TOKEN.value,
                                      data=None,
                                      error=None)

        # data = request.get_json(force=True)

        # # Data Validation
        # field_types = {'access_token': str}
        # required_fields = ['access_token']

        # post_data = field_type_validator(
        #     request_data=data, field_types=field_types)

        # if post_data['is_error']:
        #     return send_json_response(
        #         http_status=HttpStatusCode.BAD_REQUEST.value,
        #         response_status=False,
        #         message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
        #         error=post_data['data'],
        #     )

        # # Check Required Field
        # is_valid = required_validator(
        #     request_data=data, required_fields=required_fields
        # )

        # if is_valid['is_error']:
        #     return send_json_response(
        #         http_status=HttpStatusCode.BAD_REQUEST.value,
        #         response_status=False,
        #         message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
        #         error=is_valid['data'],
        #     )

        # access_token = data.get('access_token')

        response = COGNITO_CLIENT.get_user(
            AccessToken=access_token
        )

        if response:
            # Extract the desired user attributes
            name = None
            email = None
            provider_type = None
            for attr in response['UserAttributes']:
                if attr['Name'] == 'name':
                    name = attr['Value']
                if attr['Name'] == 'email':
                    email = attr['Value']
                if attr['Name'] == 'identities':
                    identity = attr['Value']
                    provider = json.loads(identity)
                    if provider:
                        # Accessing the first element of the list
                        provider = provider[0]
                        # Access the "providerType" value
                        provider_type = provider['providerType']

            user_obj = User.get_by_email(email)

            if not user_obj:
                """ Sync user in local db from cognito """
                google_auth = None
                amazon_auth = None

                if provider_type == CognitoProviderType.GOOGLE.value:
                    google_auth = response
                elif provider_type == CognitoProviderType.LOGIN_WITH_AMZON.value:
                    amazon_auth = response

                user_obj = User(first_name=name, email=email, google_auth=google_auth, amazon_auth=amazon_auth, last_login_at=int(
                    time.time()), created_at=int(time.time()), updated_at=int(time.time())).save()

                """Send Welcome Email to User"""
                email_subject = f'Welcome to {APP_NAME}! Your 7-Day Free Trial Begins Now 🚀'

                email_data = {
                    'email_to': user_obj.email,
                    'app_name': APP_NAME,
                    'first_name': user_obj.first_name
                }

                send_mail(email_to=user_obj.email, subject=email_subject,
                          template='emails/welcome_email.html', data=email_data)

            else:
                user_obj.last_login_at = int(time.time())

            # Check if user is invited from user_invite table and is accepted then do not create account details
            is_invited = UserInvite.get_by_email(email=user_obj.email)

            if is_invited and is_invited.status == UserInviteStatus.ACCEPTED.value:
                pass
            else:
                # Create an account
                create_account_details(user_obj.id)

            users_data, account_dict = User.serialize(user_obj)

            subscription_obj = get_subscription_object(
                account_dict[0]['account_id'])

            data = {
                'objects': {
                    'user': users_data[0],
                    'subscription': subscription_obj
                }
            }

            data['result'] = account_dict
            data['objects']['user']['auth_token'] = access_token

            return send_json_response(
                http_status=200,
                response_status=True,
                message_key=ResponseMessageKeys.LOGIN_SUCCESSFULLY.value,
                data=data,
            )

    except Exception as exception_error:
        logger.error(
            f'Google app authorisation callback failed: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value,
        )


def idp_logout():
    """ Logout url for IDP """
    try:
        # The OAuth response type, which can be code for code grant flow and token for implicit flow.
        response_type = 'token'
        client_id = config_data.get('COGNITO_APP_CLIENT_ID')
        auth_url = config_data.get('COGNITO_DOMAIN_URL')
        # Cognito will send details on this url
        redirect_uri = config_data.get('COGNITO_IDP_CALLBACK_URL')

        scope = 'aws.cognito.signin.user.admin openid phone profile'

        logout_uri = f'{auth_url}/logout?response_type={response_type}&client_id={client_id}&redirect_uri={redirect_uri}&scope={scope}'
        return send_json_response(
            http_status=200,
            response_status=True,
            message_key=ResponseMessageKeys.LOGOUT.value,
            data={'logout_uri': logout_uri},
        )

    except Exception as exception_error:
        logger.error(f'Google app authorisation failed: {exception_error}')
        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value,
        )


def create_account_details(logged_in_user: int):
    """ Create Account for user"""
    account = None
    try:

        account = Account.get_by_user_id(primary_user_id=logged_in_user)

        if not account:
            account = Account.add(primary_user_id=logged_in_user)

            UserAccount.add(user_id=logged_in_user, account_id=account.id)

    except Exception as exception_error:
        logger.error(f'Unable to create account details: {exception_error}')

    return account


def get_subscription_object(account_id: str):
    """Get Subscription Data"""

    subscribed = SubscriptionStates.EXPIRED.value.upper()

    is_subscription = Subscription.get_by_account_id(
        account_id=account_id)

    plan_id, plan_name, plan_expiry, subscription_id = None, None, None, None

    if is_subscription:
        subscribed = SubscriptionStates.ACTIVE.value.upper()
        plan_expiry = is_subscription.end_date
        subscription_id = is_subscription.reference_subscription_id

        get_plan = Plan.get_by_id(id=is_subscription.plan_id)
        if get_plan is not None:
            plan_name = get_plan.name
            plan_id = get_plan.reference_plan_id

    subscription_obj = {
        'user': 'NEW' if is_subscription is None else '',
        'state': subscribed,
        'plan_expiry': plan_expiry,
        'plan_name': plan_name,
        'plan_id': plan_id,
        'subscription_id': subscription_id
    }

    return subscription_obj
