"""endpoints for members page"""

import time

from app import COGNITO_CLIENT
from app import config_data
from app import db
from app import logger
from app import ses_email_delivery_q
from app.helpers.constants import APP_NAME
from app.helpers.constants import ChangeUserStatus
from app.helpers.constants import EntityType
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import InputValidation
from app.helpers.constants import PAGE_DEFAULT
from app.helpers.constants import PAGE_LIMIT
from app.helpers.constants import QueueName
from app.helpers.constants import QueueTaskStatus
from app.helpers.constants import ResponseMessageKeys
from app.helpers.constants import SortingOrder
from app.helpers.constants import UserInviteStatus
from app.helpers.constants import UserStatus
from app.helpers.decorators import api_time_logger
from app.helpers.decorators import token_required
from app.helpers.utility import create_invite_auth_token
from app.helpers.utility import enum_validator
from app.helpers.utility import field_type_validator
from app.helpers.utility import generate_uuid
from app.helpers.utility import get_pagination_meta
from app.helpers.utility import is_valid_invite_auth_token
from app.helpers.utility import required_validator
from app.helpers.utility import send_json_response
from app.models.account import Account
from app.models.queue_task import QueueTask
from app.models.user import User
from app.models.user_account import UserAccount
from app.models.user_invite import UserInvite
from flask import request
from providers.mail import send_mail
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.orm import aliased
from workers.email_worker import EmailWorker

MAIL_REDIRECT_URL = config_data['MAIL'].get('MAIL_REDIRECT_URL')


class MembersView:
    """class for members view methods """

    # function to add user as members
    @api_time_logger
    @token_required
    def invite_user(user_object, account_object):
        """endpoint for inviting users as members"""
        try:

            data = request.get_json(force=True)

            field_types = {
                'first_name': str, 'last_name': str, 'email': str, 'brand': list}

            required_fields = ['first_name', 'email', 'brand']

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

            invited_by_user_id = user_object.id
            invited_by_account_id = account_object.id
            invited_by_account_uuid = account_object.uuid
            invited_by_legal_name = account_object.legal_name
            invited_by_first_name = user_object.first_name

            logged_in_user = user_object.id
            account_id = account_object.uuid
            asp_id = account_object.asp_id

            first_name = data.get('first_name')
            last_name = data.get('last_name')
            email_to = data.get('email')
            brand = data.get('brand', [])                   # type: ignore  # noqa: FKA100
            category = data.get('category', [])             # type: ignore  # noqa: FKA100

            # validate brand
            if not brand:
                return send_json_response(
                    http_status=HttpStatusCode.BAD_REQUEST.value,
                    response_status=False,
                    message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
                )

            if not InputValidation.is_valid(string=email_to, validation_type=InputValidation.EMAIL):
                return send_json_response(
                    http_status=HttpStatusCode.BAD_REQUEST.value,
                    response_status=False,
                    message_key=ResponseMessageKeys.FAILED_EMAIL_VALIDATION.value,
                )

            # check if invited by is always a primary user of the account
            is_primary_user = Account.is_primary_user(
                user_id=invited_by_user_id, account_id=invited_by_account_uuid)

            if not is_primary_user:
                return send_json_response(http_status=HttpStatusCode.FORBIDDEN.value, response_status=True, message_key=ResponseMessageKeys.ACCESS_DENIED.value, data=None, error=None)

            # check if user-account already exists
            invited_user = User.get_by_email(email=email_to)

            if invited_user:
                invited_user_id = invited_user.id
                user_account = UserAccount.is_user_account_exists(
                    user_id=invited_user_id, account_id=invited_by_account_id)

                if user_account:
                    return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=True, message_key=ResponseMessageKeys.USER_ALREADY_EXIST.value, data=None, error=None)

            # check invitation status
            invitation = UserInvite.get_user_invite(
                email=email_to, invited_by_user_id=invited_by_user_id, invited_by_account_id=invited_by_account_id)

            if not invitation:

                uuid = generate_uuid()

                UserInvite.add(uuid=uuid, first_name=first_name,
                               email=email_to, invited_by_user_id=invited_by_user_id, invited_by_account_id=invited_by_account_id, brand=brand, category=category, last_name=last_name)

                # send invite mail here
                token = create_invite_auth_token(uuid=uuid)

                mail_msg = 'You have been invited to be a part of the EcommPulse account'

                if invited_by_first_name:
                    mail_msg = f'{invited_by_first_name} has invited you to be a part of their EcommPulse account'

                email_data = {
                    'accept_url': f'{MAIL_REDIRECT_URL}/auth/set-password/?uuid={uuid}&token={token}',
                    'logged_in_user': logged_in_user,
                    'account_id': account_id,
                    'asp_id': asp_id,
                    'subject': 'Invitation to Join EcommPulse!',
                    'email_to': email_to,
                    'invited_user_first_name': first_name,
                    'invited_by_first_name': invited_by_first_name,
                    'invited_by_legal_name': invited_by_legal_name,
                    'mail_msg': mail_msg,
                    'app_name': APP_NAME
                }

                MembersView.__enqueue_email_invite(data=email_data)

                result_dict = {'uuid': uuid, 'token': token}

                return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.USER_INVITE_SUCCESSFUL.value, data=result_dict, error=None)

            # check if user has already accepted and set password
            if invitation.status == UserInviteStatus.ACCEPTED.value:
                return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.USER_ALREADY_EXIST.value, data=None, error=None)

            if invitation.status == UserInviteStatus.PENDING.value:

                # send invitation mail here
                token = create_invite_auth_token(uuid=invitation.uuid)

                mail_msg = 'You have been invited to be a part of the EcommPulse account'

                if invited_by_first_name:
                    mail_msg = f'{invited_by_first_name} has invited you to be a part of their EcommPulse account'

                email_data = {
                    'accept_url': f'{MAIL_REDIRECT_URL}/auth/set-password/?uuid={invitation.uuid}&token={token}',
                    'logged_in_user': logged_in_user,
                    'account_id': account_id,
                    'asp_id': asp_id,
                    'subject': 'Invitation to Join EcommPulse!',
                    'email_to': email_to,
                    'invited_user_first_name': first_name,
                    'invited_by_first_name': invited_by_first_name,
                    'invited_by_legal_name': invited_by_legal_name,
                    'mail_msg': mail_msg,
                    'app_name': APP_NAME
                }

                MembersView.__enqueue_email_invite(data=email_data)

                UserInvite.update_invite(
                    uuid=invitation.uuid, first_name=first_name, brand=brand, category=category, last_name=last_name)

                result_dict = {'uuid': invitation.uuid, 'token': token}

                return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.USER_INVITE_RESENT.value, data=result_dict, error=None)

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed inviting users: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    # function to set password
    @api_time_logger
    def set_password():
        """endpoint for setting up password for user"""
        try:

            data = request.get_json(force=True)

            field_types = {
                'password': str, 'uuid': str}

            required_fields = ['password', 'uuid']

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

            password = data.get('password')
            account_uuid = data.get('uuid')

            if not InputValidation.is_valid(string=password, validation_type=InputValidation.PASSWORD):
                return send_json_response(
                    http_status=HttpStatusCode.BAD_REQUEST.value,
                    response_status=False,
                    message_key=ResponseMessageKeys.INVALID_PASSWORD.value,
                )

            invite = UserInvite.get_by_uuid(uuid=account_uuid)

            # if invite does not exist
            if not invite:

                return send_json_response(
                    http_status=400,
                    response_status=True,
                    message_key=ResponseMessageKeys.USER_INVITE_DOES_NOT_EXIST.value,
                )

            invited_by_account_id = invite.invited_by_account_id
            brand = invite.brand
            category = invite.category

            # check if user-account already exists , reclick on same accept link
            invited_user = User.get_by_email(email=invite.email)

            if invited_user:
                invited_user_id = invited_user.id
                user_account = UserAccount.is_user_account_exists(
                    user_id=invited_user_id, account_id=invited_by_account_id)

                if user_account:
                    return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=True, message_key=ResponseMessageKeys.USER_ALREADY_EXIST.value, data=None, error=None)

            if invite.status == UserInviteStatus.ACCEPTED.value:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=True, message_key=ResponseMessageKeys.USER_ALREADY_EXIST.value, data=None, error=None)

            elif invite.status == UserInviteStatus.PENDING.value:
                # add user with email and password
                logger.info('*' * 200)
                logger.info(
                    'Add user and set password for pending invited user.')

                email = invite.email
                first_name = invite.first_name
                last_name = invite.last_name

                logger.info(
                    f'email="{email}", first_name="{first_name}", last_name="{last_name}"')

                new_user = User.add(
                    email=email, first_name=first_name, last_name=last_name, password=password)

                if new_user:

                    user_account = UserAccount.add(
                        user_id=new_user.id, account_id=invited_by_account_id, brand=brand, category=category)

                    invited_user = UserInvite.get_by_uuid(uuid=account_uuid)

                    if invited_user:
                        invited_user.status = UserInviteStatus.ACCEPTED.value
                        invited_user.updated_at = int(time.time())
                        db.session.commit()

                    name = first_name
                    if last_name:
                        name = first_name + ' ' + last_name

                    COGNITO_CLIENT.admin_create_user(
                        UserPoolId=config_data.get('COGNITO_USER_POOL_ID'),
                        Username=invite.email,
                        UserAttributes=[
                            {'Name': 'email', 'Value': invite.email},
                            {'Name': 'email_verified', 'Value': 'true'},
                            {'Name': 'name', 'Value': name}
                        ],
                        ForceAliasCreation=True,
                        MessageAction='SUPPRESS',
                        TemporaryPassword=password
                    )

                    """ Set User Password """
                    COGNITO_CLIENT.admin_set_user_password(
                        UserPoolId=config_data.get('COGNITO_USER_POOL_ID'),
                        Username=invite.email,
                        Password=data.get('password'),
                        # Permanent=True | False, # Uncomment this code if we want user status as CONFIRM. i.e. if we don't require user to change password at first time login.
                        Permanent=True,
                    )

                    return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.USER_ADDED_SUCCESSFULLY.value, data=None, error=None)

                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.FAILED.value, data=None, error=None)

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed setting password and adding user from email invitation: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    def get_users(user_object, account_object):                  # type: ignore  # noqa: C901
        """endpoint for getting all members details"""
        try:

            q = request.args.get(key='q', default=None)
            sort_key = request.args.get(key='sort_key', default='created_at')
            sort_order = request.args.get(
                key='sort_order', default=SortingOrder.DESC.value)
            page = request.args.get(key='page', default=PAGE_DEFAULT)
            size = request.args.get(key='size', default=PAGE_LIMIT)

            account_id = account_object.id

            # validation
            params = {}
            if q:
                params['q'] = q
            if sort_key:
                params['sort_key'] = sort_key
            if sort_order:
                params['sort_order'] = sort_order
            if page:
                params['page'] = page
            if size:
                params['size'] = size

            field_types = {'q': str,
                           'sort_key': str, 'sort_order': str, 'page': int, 'size': int}

            required_fields = []

            data = field_type_validator(
                request_data=params, field_types=field_types)

            if data['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=data['data'])

            is_valid = required_validator(
                request_data=params, required_fields=required_fields)

            if is_valid['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=is_valid['data'])

            members, total_count_result = User.get_member_details(account_id=account_id,
                                                                  page=int(page), size=int(size), q=q, sort_key=sort_key.lower(), sort_order=sort_order)
            if members:
                result_dict = {'result': [], }

                for member in members:

                    invited_by = User.get_by_id(id=member.invited_by_user_id)

                    status = UserStatus.ACTIVE.value

                    if member.deactivated_at:
                        status = UserStatus.DEACTIVATED.value

                    first_name = member.first_name
                    last_name = member.last_name
                    email = member.email
                    brand = member.brand
                    invited_by = invited_by.email if invited_by else None

                    result_dict['result'].append(
                        {'first_name': first_name, 'last_name': last_name, 'email': email, 'invited_by': invited_by, 'brand': brand, 'status': status})

                objects = {
                    'pagination_metadata': get_pagination_meta(current_page=1 if page is None else int(page), page_size=int(size), total_items=total_count_result)
                }

                result_dict['objects'] = objects

                return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=result_dict, error=None)

            else:
                return send_json_response(
                    http_status=200,
                    response_status=True,
                    message_key=ResponseMessageKeys.NO_DATA_FOUND.value,
                )

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed getting members details: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    def get_accounts(user_object, account_object):
        """endpoint for getting all members details"""
        try:

            user_id = user_object.id

            page = request.args.get('page', default=PAGE_DEFAULT)
            size = request.args.get('size', default=PAGE_LIMIT)

            # validation
            params = {}

            if page:
                params['page'] = page
            if size:
                params['size'] = size

            field_types = {'page': int, 'size': int}

            required_fields = []

            data = field_type_validator(
                request_data=params, field_types=field_types)

            if data['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=data['data'])

            is_valid = required_validator(
                request_data=params, required_fields=required_fields)

            if is_valid['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=is_valid['data'])

            accounts, total_count_result = MembersView.__get_user_accounts(
                user_id=user_id, page=int(page), size=int(size))

            if accounts:

                result_dict = {'result': []}

                for account in accounts:

                    result_dict['result'].append(
                        {'account_id': account.account_id, 'account_name': account.account_name})

                objects = {
                    'pagination_metadata': get_pagination_meta(current_page=1 if page is None else int(page), page_size=int(size), total_items=total_count_result)
                }

                result_dict['objects'] = objects

                return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=result_dict, error=None)

            else:
                return send_json_response(
                    http_status=200,
                    response_status=True,
                    message_key=ResponseMessageKeys.NO_DATA_FOUND.value,
                )

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed getting user accounts: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    def is_valid_invite():
        """endpoint for checking if invite link is valid before setting password"""
        try:

            uuid = request.args.get('uuid')
            token = request.args.get('token')

            # validation
            params = {}

            if uuid:
                params['uuid'] = uuid
            if token:
                params['token'] = token

            field_types = {'uuid': str, 'token': str}

            required_fields = ['uuid', 'token']

            data = field_type_validator(
                request_data=params, field_types=field_types)

            if data['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=data['data'])

            is_valid = required_validator(
                request_data=params, required_fields=required_fields)

            if is_valid['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=is_valid['data'])

            invite = UserInvite.get_by_uuid(uuid=uuid)

            # if invite does not exist
            if not invite:

                return send_json_response(
                    http_status=400,
                    response_status=True,
                    message_key=ResponseMessageKeys.USER_INVITE_DOES_NOT_EXIST.value,
                )

            if invite.status == UserInviteStatus.ACCEPTED.value:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=True, message_key=ResponseMessageKeys.USER_ALREADY_EXIST.value, data=None, error=None)

            # check if user-account already exists , reclick on same accept link
            invited_user = User.get_by_email(email=invite.email)

            is_user_account_added = False

            # check if token is valid and thereby link is valid
            is_valid_token = is_valid_invite_auth_token(token=token)

            result = {
                'is_valid_invite': is_valid_token,
                'is_user_account_added': is_user_account_added
            }

            if not is_valid_token:
                return send_json_response(http_status=HttpStatusCode.GONE.value, response_status=True, message_key=ResponseMessageKeys.USER_INVITE_LINK_EXPIRED.value, data=None, error=None)

            # if user link is valid , check if user already exists
            if invited_user:

                invited_user_id = invited_user.id
                user_account = UserAccount.is_user_account_exists(
                    user_id=invited_user_id, account_id=invite.invited_by_account_id)

                if user_account:
                    return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=True, message_key=ResponseMessageKeys.USER_ALREADY_EXIST.value, data=None, error=None)
                else:
                    # add user-account mapping , dont ask to set password
                    user_account = UserAccount(
                        user_id=invited_user_id, account_id=invite.invited_by_account_id, brand=invite.brand, category=invite.category)

                    db.session.add(user_account)

                    # update invite table
                    invite.status = UserInviteStatus.ACCEPTED.value
                    invite.updated_at = int(time.time())

                    db.session.commit()

                    result['is_user_account_added'] = True

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.USER_INVITE_VALID.value, data=result, error=None)

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed checking if valid invite: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    # function to activate/deactivate account
    @api_time_logger
    @token_required
    def change_user_status(user_object, account_object):
        """endpoint for activating / deactivating user for a particular account"""

        try:

            data = request.get_json(force=True)

            status = data.get('status')
            target_user_email = data.get('email')

            enum_fields = {
                'status': (status, 'ChangeUserStatus')
            }

            field_types = {
                'status': str, 'email': str}

            required_fields = ['status', 'email']

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

            valid_enum = enum_validator(enum_fields)

            if valid_enum['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=valid_enum['data'])

            account_id = account_object.id
            account_legal_name = account_object.legal_name

            target_user = User.get_by_email(email=target_user_email)
            target_user_id = target_user.id
            target_user_email = target_user.email
            target_user_first_name = target_user.first_name

            user_account = UserAccount.is_user_account_exists(
                user_id=target_user_id, account_id=account_id)

            if user_account:

                if status == ChangeUserStatus.ACTIVATE.value:

                    user_account.deactivated_at = None
                    db.session.commit()

                    # Send user access active mail"""

                    email_subject = f'Your {APP_NAME} User access is Now Activated!'

                    email_data = {
                        'email_to': target_user_email,
                        'app_name': APP_NAME,
                        'status': UserStatus.ACTIVE.value,
                        'account_legal_name': account_legal_name,
                        'target_user_first_name': target_user_first_name
                    }

                    send_mail(email_to=target_user_email, subject=email_subject,
                              template='emails/user_activated.html', data=email_data)

                    return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.USER_ACTIVATED.value, data=None, error=None)

                elif status == ChangeUserStatus.DEACTIVATE.value:

                    user_account.deactivated_at = int(time.time())
                    db.session.commit()

                    # Send user access deactived mail"""

                    email_subject = f'{APP_NAME} Account Deactivation Notification'

                    email_data = {
                        'email_to': target_user_email,
                        'app_name': APP_NAME,
                        'status': UserStatus.DEACTIVATED.value,
                        'account_legal_name': account_legal_name,
                        'target_user_first_name': target_user_first_name
                    }

                    send_mail(email_to=target_user_email, subject=email_subject,
                              template='emails/user_deactivated.html', data=email_data)

                    return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.USER_DEACTIVATED.value, data=None, error=None)

            else:

                return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.NO_DATA_FOUND.value, data=None, error=None)

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed activating/deactivating account: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @api_time_logger
    @token_required
    def update_member_details(user_object, account_object):
        """endpoint for updating member details"""

        try:

            data = request.get_json(force=True)
            field_types = {
                'first_name': str, 'last_name': str, 'email': str, 'brand': list}

            required_fields = ['first_name', 'email', 'brand']

            is_valid = required_validator(
                request_data=data, required_fields=required_fields)

            if is_valid['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=is_valid['data'])

            field_types = {'email': str}

            post_data = field_type_validator(
                request_data=data, field_types=field_types)

            if post_data['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=post_data['data'])

            first_name = data.get('first_name')
            last_name = data.get('last_name', None)  # type: ignore  # noqa: FKA100
            email = data.get('email')
            brand = data.get('brand', [])                   # type: ignore  # noqa: FKA100
            category = data.get('category', [])             # type: ignore  # noqa: FKA100

            # Primary user name cannot be changed
            primary_account_id = account_object.id
            primary_user_id = user_object.id

            user = User.get_by_email(email=email)

            if user:
                secondary_user_id = user.id

                shared_account = UserAccount.is_user_account_exists(
                    user_id=secondary_user_id, account_id=primary_account_id)

                if shared_account:
                    primary_user = Account.is_account_and_primary_user(user.id)

                    __first_name = user.first_name
                    __last_name = user.last_name

                    if not primary_user or primary_user_id == primary_user.id:
                        user.first_name = first_name
                        __first_name = first_name
                        if last_name:
                            user.last_name = last_name
                            __last_name = last_name
                        db.session.commit()

                    _brand = {}

                    if not primary_user or primary_user_id != secondary_user_id:
                        user_account = UserAccount.update(
                            user_id=user.id, account_id=primary_account_id, brand=brand, category=category)
                        _brand = user_account.brand

                    data = {
                        'result': {
                            'brand': [f'{brand_data}' for brand_data in _brand],
                            'first_name': __first_name,
                            'last_name': __last_name
                        }
                    }

                    return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.UPDATED.value, data=data, error=None)

            return send_json_response(
                http_status=HttpStatusCode.BAD_REQUEST.value,
                response_status=False,
                message_key=ResponseMessageKeys.ACCOUNT_DOES_NOT_EXIST.value,
            )

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed activating/deactivating account: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    def __get_user_accounts(user_id: int, page: int, size: int):
        """returns all accounts of a particular user"""

        user_acc = aliased(UserAccount)
        acc = aliased(Account)

        query = db.session.query(user_acc.account_id.label('account_id'), acc.display_name.label(                     # type: ignore  # noqa: FKA100
            'account_name')).join(acc, user_acc.account_id == acc.id).filter(user_acc.user_id == user_id)                   # type: ignore  # noqa: FKA100

        total_count_query = select(func.count()).select_from(query)

        total_count_result = db.session.execute(total_count_query).scalar()

        if page and size:
            page = int(page) - 1
            size = int(size)
            query = query.limit(size).offset(page * size)

        accounts = query.all()

        return accounts, total_count_result

    @staticmethod
    def __enqueue_email_invite(data: dict):
        """enqueues email invite in SES EMAIL DELIVERY queue"""

        queue_task = QueueTask.add_queue_task(queue_name=QueueName.SES_EMAIL_DELIVERY,
                                              account_id=data.get(
                                                  'account_id'),
                                              owner_id=data.get(
                                                  'logged_in_user'),
                                              status=QueueTaskStatus.NEW.value,
                                              entity_type=EntityType.SES_EMAIL_DELIVERY.value,
                                              param=str(data), input_attachment_id=None, output_attachment_id=None)

        if queue_task:
            queue_task_dict = {
                'job_id': queue_task.id,
                'queue_name': queue_task.queue_name,
                'status': QueueTaskStatus.get_status(queue_task.status),
                'entity_type': EntityType.get_type(queue_task.entity_type)
            }

            data.update(queue_task_dict)
            queue_task.param = str(data)
            queue_task.save()

            ses_email_delivery_q.enqueue(EmailWorker.send_invite_mail, data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100

    # @staticmethod
    # def __add_user_account(new_user_id: int, invited_by_account_id: int):
    #     """method to add user-account entry"""

    #     user_account = db.session.query(UserAccount).filter(
    #         UserAccount.user_id == new_user_id).first()

    #     if not user_account:
    #         created_at = int(time.time())
    #         new_user_account = UserAccount(
    #             user_id=new_user_id, account_id=invited_by_account_id, created_at=created_at, updated_at=created_at)
    #         db.session.add(new_user_account)

    #     return user_account

    # @staticmethod
    # def __add_user(email: str, first_name=None, last_name=None, password=None, google_auth=None, amazon_auth=None):
    #     """Create new user"""
    #     created_at = int(time.time())

    #     new_user = User(email=email, password=password, first_name=first_name, last_name=last_name,
    #                    google_auth=google_auth, amazon_auth=amazon_auth, last_login_at=created_at, created_at=created_at, updated_at=created_at)

    #     db.session.add(new_user)

    #     return new_user

    # @classmethod
    # def __update_status(cls, uuid: str, status: str):
    #     """method to update status of invitation"""

    #     invited_user = UserInvite.get_by_uuid(uuid=uuid)

    #     if invited_user:

    #         invited_user.status = status
    #         invited_user.updated_at = int(time.time())

    #     return invited_user
