"""endpoints for members page"""

import time

from app import db
from app import logger
from app.helpers.constants import AccountStatus
from app.helpers.constants import AttachmentType
from app.helpers.constants import EntityType
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import ResponseMessageKeys
from app.helpers.constants import SubEntityType
from app.helpers.decorators import api_time_logger
from app.helpers.decorators import token_required
from app.helpers.file_upload_helper import upload_file_s3
from app.helpers.utility import enum_validator
from app.helpers.utility import field_type_validator
from app.helpers.utility import get_profile_detail_schema
from app.helpers.utility import required_validator
from app.helpers.utility import send_json_response
from app.models.account import Account
from flask import request


class ProfileView:

    """class for profile view methods """

    # function to add user as members
    @api_time_logger
    @token_required
    def get_profile(user_object, account_object):
        """endpoint for getting account profile details"""

        try:
            account_id = account_object.uuid

            account = Account.get_by_uuid(uuid=account_id)

            if account:

                account_detail = account.detail

                return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=account_detail, error=None)

            else:

                return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.NO_DATA_FOUND.value, data=None, error=None)

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed getting account details: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    # function to add user as members
    @api_time_logger
    @token_required
    def add_update_profile(user_object, account_object):
        """endpoint for adding/updating account profile details"""

        try:

            form_data = request.form
            legal_name = form_data.get('legal_name')
            phone = form_data.get('phone')
            address = form_data.get('address')
            country = form_data.get('country')
            state = form_data.get('state')
            city = form_data.get('city')
            zip_code = form_data.get('zip_code')

            field_types = {'name': str, 'phone': str, 'address': str,
                           'country': str, 'state': str, 'city': str, 'zip_code': str}

            data = field_type_validator(
                request_data=form_data, field_types=field_types)

            if data['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=data['data'])

            required_fields = ['legal_name']

            is_valid = required_validator(
                request_data=form_data, required_fields=required_fields)

            if is_valid['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=is_valid['data'])

            account_id = account_object.uuid

            account = Account.get_by_uuid(uuid=account_id)

            if account:
                get_detail_json = account.detail
                account.detail = None
                db.session.commit()

                # Get Account profile schema
                profile_schema = get_profile_detail_schema()

                """Update previous profile picture details"""
                if get_detail_json and get_detail_json['file_attachment_id'] is not None:
                    profile_schema.update({
                        'file_attachment_id': get_detail_json['file_attachment_id'],
                        'file_name': get_detail_json['file_name'],
                        'file_path': get_detail_json['file_path'],
                        'file_size': get_detail_json['file_size']
                    })

                # Update Account profile schema with common data
                profile_schema.update({
                    'legal_name': legal_name,
                    'phone': phone,
                    'address': address,
                    'country': country,
                    'state': state,
                    'city': city,
                    'zip_code': zip_code
                })

                """ Add/Update legal name """
                if legal_name:
                    account.legal_name = legal_name

                """ Add/Update new profile icon if in request"""
                profile_photo = request.files.get('profile_photo')

                if profile_photo:
                    upload_photo = upload_file_s3(file_obj=profile_photo, obj_name=profile_photo.filename, entity_type=EntityType.ACCOUNT_PROFILE_PICTURE.value,
                                                  attachment_type=AttachmentType.ACCOUNT_PROFILE_PICTURE.value, sub_entity_type=SubEntityType.IMAGE.value, description='User Account Profile Picture')
                    profile_schema.update(upload_photo)

                """ Add/Update detail json """
                account.detail = profile_schema
                db.session.commit()

                return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.UPDATED.value, data={'result': profile_schema}, error=None)

            return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.NO_DATA_FOUND.value, data=None, error=None)

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed adding/updating account details: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    # function to activate/deactivate account
    @api_time_logger
    @token_required
    def change_account_status(user_object, account_object):
        """endpoint for activating / deactivating account"""

        try:

            data = request.get_json(force=True)

            status = data.get('status')

            enum_fields = {
                'status': (status, 'AccountStatus')
            }

            valid_enum = enum_validator(enum_fields)

            if valid_enum['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=valid_enum['data'])

            account_id = account_object.uuid

            account = Account.get_by_uuid(uuid=account_id)

            if account:

                if status == AccountStatus.ACTIVATE.value:
                    account.deactivated_at = None
                    db.session.commit()

                    return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.ACCOUNT_ACTIVATED.value, data=None, error=None)

                elif status == AccountStatus.DEACTIVATE.value:

                    account.deactivated_at = int(time.time())
                    db.session.commit()

                    return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.ACCOUNT_DEACTIVATED.value, data=None, error=None)

            else:

                return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.NO_DATA_FOUND.value, data=None, error=None)

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed deactivating account: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )
