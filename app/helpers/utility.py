"""Common methods is defined here."""
from datetime import date
from datetime import datetime
from datetime import timedelta
from datetime import timezone
import enum
import hashlib
import math
import random
from random import randint
import re
from re import sub
import string
import time
from typing import Any
from typing import Dict
from typing import Optional
import uuid

from app import AWS_BUCKET
from app import config_data
from app import ENVIRONMENT_LIST
from app import fernet_key
from app import logger
from app import S3_RESOURCE
from app.helpers.constants import ASpMarketplaceId
from app.helpers.constants import DateFormats
from app.helpers.constants import str_to_class
from app.helpers.constants import TimeFormats
from app.helpers.constants import TimePeriod
from app.helpers.constants import ValidationMessages
from app.helpers.sign_helper import AWSV4Auth
import boto3
from dateutil import parser
from dateutil.relativedelta import relativedelta
from flask import jsonify
from hashids import Hashids
import jwt
from requests_auth_aws_sigv4 import AWSSigV4
from sqlalchemy import inspect
# from app.models.attachments import Attachment
# from app.models.organization import Organization
# from app.models.user import User

hash_id = Hashids(min_length=7, salt=config_data.get('HASH_ID_SALT'))


def generate_uuid():
    """Generates a unique uuid"""
    uuid_value = str(uuid.uuid4())
    return uuid_value


def encrypt_data(data: str) -> str:
    """ Encrypt data """
    if data:
        encrypted_data = fernet_key.encrypt(data.encode('utf-8')).decode()
        return encrypted_data
    else:
        return data


def decrypt_data(data: str) -> str:
    """ Decrypt data """
    if data:
        decrypted_data = fernet_key.decrypt(data.encode('utf-8')).decode()
        return decrypted_data
    else:
        return data


def days_to_seconds(days: int) -> int:
    """converting days to seconds"""
    seconds = 86400 * days  # 24 * 60 * 60 * days
    return seconds


def generate_random_string(length: int) -> str:
    """generates a random string of length"""
    # initializing size of string
    random_string_length = length

    # using random.choices()
    # generating random strings
    res = ''.join(random.choices(
        string.ascii_lowercase, k=random_string_length))

    return res


def generate_random_number_string(length: int) -> str:
    """generates a random string of length"""
    # initializing size of string
    random_string_length = length

    # using random.choices()
    # generating random strings
    res = ''.join(random.choices('123456789', k=random_string_length))

    return res


def generate_password() -> str:
    """ Method to generate random password. """
    password = ''
    for _ in range(2):
        password += random.choice(string.ascii_lowercase)
    for _ in range(2):
        password += random.choice(string.ascii_uppercase)
    for _ in range(2):
        password += random.choice('@$!%*#?&')
    for _ in range(2):
        password += random.choice(string.digits)
    password = ''.join(random.sample(password, 8))  # type: ignore  # noqa: FKA100
    return password


class EmailStatus(enum.Enum):
    """Enum for various email status."""
    NEW = 'new'

    ACCEPTED = 'accepted'
    REJECTED = 'rejected'
    DELIVERED = 'delivered'
    FAILED = 'failed'

    @classmethod
    def get_name(cls, status: str) -> str:
        """This method returns key name of enum from value."""
        if status == cls.NEW.value:
            return 'New'
        elif status == cls.ACCEPTED.value:
            return 'Accepted'
        elif status == cls.REJECTED.value:
            return 'Rejected'
        elif status == cls.DELIVERED.value:
            return 'Delivered'
        elif status == cls.FAILED.value:
            return 'Failed'
        else:
            return 'N/A'

    @classmethod
    def get_status_message(cls, status: str) -> str:
        """This method returns message of enum from key."""
        if status == cls.ACCEPTED:
            return 'The email is in process of sending.'
        elif status == cls.REJECTED:
            return 'The email has been rejected by the email service.'
        elif status == cls.DELIVERED:
            return 'The email has been delivered to the email address.'
        elif status == cls.FAILED:
            return 'The email could not be sent to the email address.'
        else:
            return ''

    @classmethod
    def check_status(cls, status: str) -> bool:
        """This method check if status belongs to given status or not."""
        return status in [cls.ACCEPTED.value, cls.REJECTED.value, cls.DELIVERED.value, cls.FAILED.value]


TYPE_NAMES = {
    int: 'integer', float: 'float', bool: 'boolean', str: 'string', dict: 'dict', list: 'list'
}


def generate_email_token(id: int, org_id: Any = None) -> bytes:
    """Generates a unique jwt token with id and timestamp to be sent via email for one time use as login link:
    Encodes teh current timestamp and id in jwt"""
    secret = config_data.get('SECRET_KEY')
    current_time = datetime.now(timezone.utc)
    utc_time = current_time.replace(tzinfo=timezone.utc)
    utc_timestamp = utc_time.timestamp()
    data = {
        'timestamp': utc_timestamp,
        'id': id,
        'org_id': org_id
    }
    token = jwt.encode(payload=data, key=secret)
    # token_string = token.decode('utf-8')
    return token


def generate_otp_token(len: int, user_id: int) -> Any:
    """Generates a unique jwt token with otp and timestamp to be sent to admin panel So, that whenever user enters the
    otp received on email then admin panel call API (reset-password) with otp and otp_token after that back-end
    can validate otp.
    JWT token contains Encoded current timestamp, otp and user_id. """
    secret = config_data.get('SECRET_KEY')
    current_time = datetime.now(timezone.utc)
    utc_time = current_time.replace(tzinfo=timezone.utc)
    utc_timestamp = utc_time.timestamp()
    otp = random_with_n_digits(num=len)
    data = {
        'timestamp': utc_timestamp,
        'otp': str(otp),
        'user_id': user_id
    }
    token = jwt.encode(payload=data, key=secret)
    # token_string = token.decode('utf-8')
    return otp, token


def random_with_n_digits(num: int) -> int:
    """Generates a random number of length num"""
    range_start = 10 ** (num - 1)
    range_end = (10 ** num) - 1
    return randint(range_start, range_end)  # type: ignore  # noqa: FKA100


# def delete_invalid_tokens() -> None:
#     """manage command function to delete all old invalid access tokens from db"""
#     users_list = User.get_all()
#     for user in users_list:
#         if user.user_type == UserType.ADMIN.value:
#             continue
#         if user.device_tokens:
#             invalid_devices = []
#             for key, val in user.device_tokens.items():
#                 if user.device_tokens[key]['auth_token']:
#                     if type(user.device_tokens[key]['auth_token']) == str:
#                         device_tokens = user.device_tokens
#                         invalid_devices.append(key)
#             for dev in invalid_devices:
#                 device_tokens.pop(dev)
#                 user.device_tokens = device_tokens
#                 user.update_object()


# def insert_old_yacht_in_multi_yacht_table() -> None:
#     """manage command function to old users from org and user table into multi yacht functionality uacht_user table"""
#     old_yacht_users = db.session.query(Organization, User).filter(  # type: ignore  # noqa: FKA100
#         and_(Organization.id == User.org_id, User.user_type is not None))  # type: ignore  # noqa: FKA100
#     for old_relation in old_yacht_users:
#         if old_relation.User.user_type:
#             check = YachtUser.get_by_org_user_id(
#                 org_id=old_relation.Organization.id, user_id=old_relation.User.id).first()
#             if check is None:
#                 new_yacht_user = YachtUser(org_id=old_relation.Organization.id, user_type=old_relation.User.user_type,
#                                            user_id=old_relation.User.id,
#                                            deleted_at=old_relation.User.deleted_at,
#                                            position=old_relation.User.position,
#                                            deactivated_at=old_relation.User.deactivated_at)
#                 db.session.add(new_yacht_user)
#                 db.session.commit()
#                 new_yacht_user.update_preference(
#                     key=Preference.EMAIL_NOTIFICATIONS.value, value=True)
#                 new_yacht_user.update_preference(
#                     key=Preference.MOBILE_NOTIFICATIONS.value, value=True)


def is_token_valid(token: bytes) -> bool:
    """ Returns true if jwt token  is valid"""
    try:
        jwt.decode(jwt=token, key=config_data.get(
            'SECRET_KEY'), algorithms=['HS256'])
        return True
    except Exception as e:  # type: ignore  # noqa: F841
        return False


def field_type_validator(request_data: dict = {}, field_types: dict = {}, prefix: str = '') -> dict:
    """
    Validate given dict of fields and their types:
    Iterates over field_types keys and checks if the values received from the request match the values specified in the api function
    If one does not match it returns and error with the field name
    """
    cleaned_data = {}
    errors = {}
    is_error = False
    for field in field_types.keys():
        field_value = request_data.get(field)

        if field_value is not None:
            field_type = field_types[field]

            if field_type == float:
                try:
                    field_value = float(field_value)
                except Exception as e:  # type: ignore  # noqa: F841
                    pass
            if field_type == int:
                try:
                    field_value = int(field_value)
                except Exception as e:  # type: ignore  # noqa: F841
                    pass
            if type(field_value) != field_type:
                type_name = TYPE_NAMES.get(field_type, field_type.__name__)  # type: ignore  # noqa: FKA100

                if prefix:
                    message = f'{prefix} {field} should be {type_name} value.'
                else:
                    formatted_field = field.replace('_', ' ').title()  # type: ignore  # noqa: FKA100
                    message = f'{formatted_field} should be {type_name} value.'

                errors[field] = message

                if is_error is False:
                    is_error = True

        cleaned_data[field] = field_value

    return {'is_error': is_error, 'data': errors if is_error else cleaned_data}


def enum_validator(_data: dict) -> dict:
    """
    Validate given dict of fields with their enum values:
    Iterates over field_types keys and checks if the values received from the request match the values specified in the api function
    If one does not match it returns and error with the field name
    """
    is_error = False
    errors = {}

    for field, value in _data.items():
        data = value[0]
        _cls = str_to_class(value[1])
        try:
            if data:
                if isinstance(data, list):
                    # data = [_cls.get_name(value) for value in data]
                    data = [_cls.validate_name(name) for name in data]
                    if None in data:
                        is_error = True
                        message = f'value for {field} is not valid.'
                        errors[field] = message
                elif isinstance(data, dict):
                    # data = {k: _cls.get_name(v) for k, v in data.items()}
                    data = {k: _cls.validate_name(k) for k, v in data.items()}
                    if None in data.values():
                        is_error = True
                        message = f'value for {field} is not valid.'
                        errors[field] = message
                else:
                    # data = _cls.get_name(data)
                    data = _cls.validate_name(data)
                    if data is None:
                        is_error = True
                        message = f'value for {field} is not valid.'
                        errors[field] = message
            _data[field] = data
        except Exception:
            is_error = True
            message = f'value for {field} is not valid.'
            errors[field] = message

    return {'is_error': is_error, 'data': errors if is_error else _data}


def required_validator(request_data: dict = {}, required_fields: list = [], prefix: Any = None,
                       module_name: Any = None) -> dict:
    """
    Validate required fields of given dict of data:
    Iterates over required fields list and checks if that key is present in request
    If one also is not found it returns and error with the field name

    """
    errors = {}
    is_error = False
    for field in required_fields:
        if request_data.get(field) in [None, '']:
            try:
                message = ValidationMessages[field.upper()].value
                if module_name and field == ValidationMessages.NAME.name.lower():
                    message = module_name.replace(  # type: ignore  # noqa: FKA100
                        '_', ' ').capitalize() + ' ' + message
            except Exception:
                if prefix:
                    message = f'{prefix} {field} is required.'
                else:
                    formatted_field = re.sub(r'(_uuids)|(_ids)|(_uuid)|(_id)', '',  # type: ignore  # noqa: FKA100
                                             field)
                    formatted_field = formatted_field.replace('_', ' ').title()  # type: ignore  # noqa: FKA100
                    message = f'{formatted_field} is required.'

            errors[field] = message

            if is_error is False:
                is_error = True

    return {'is_error': is_error, 'data': errors}


def hash_password(string: str) -> str:
    """Converts simple pin or password string into a hashed string"""
    return hashlib.pbkdf2_hmac(hash_name='sha256', password=string.encode(),
                               salt=config_data.get('PASSWORD_SALT').encode(), iterations=200).hex()


def compare_password(password: str, hashed_password: str):
    """compares password field with hashed password in db"""

    password = hashlib.pbkdf2_hmac(hash_name='sha256', password=password.encode(),
                                   salt=config_data.get('PASSWORD_SALT').encode(), iterations=200).hex()

    if password == hashed_password:
        return {'is_error': False}

    return {'is_error': True}


def create_auth_token(user_id: int) -> bytes:
    """Generates jwt token with hashed user id and 2 hour expiry time """
    user_id_hash = hash_id.encode(user_id)
    expire_at = datetime.utcnow() + timedelta(minutes=120)
    token = jwt.encode({'user_id_hash': user_id_hash, 'expire_at': expire_at.timestamp()},
                       key=config_data.get('JWT_SALT'), algorithm='HS256')
    return token


def is_valid_aut_token(token: bytes) -> bool:
    """Returns true true if passed auth token is valid jwt token"""
    decoded = jwt.decode(token.encode(), key=config_data.get(
        'JWT_SALT'), algorithms=['HS256'])
    decoded['user_id'] = hash_id.decode(decoded['user_id_hash'])[0]
    if decoded['expire_at'] < datetime.utcnow().timestamp():
        return False
    return True


def update_auth_token(token: bytes) -> bytes:
    """Adds expiry of 2 hours to auth token"""
    decoded = jwt.decode(token.encode(), key=config_data.get(
        'JWT_SALT'), algorithms=['HS256'])
    decoded['expire_at'] = datetime.utcnow() + timedelta(minutes=120)
    decoded['expire_at'] = decoded['expire_at'].timestamp()
    return jwt.encode(decoded, key=config_data.get('JWT_SALT'), algorithm='HS256')


def decode_auth_token(token: bytes) -> dict:
    """Retuns decoded dict from jwt token"""
    decoded = jwt.decode(token.encode(), key=config_data.get(
        'JWT_SALT'), algorithms=['HS256'])
    decoded['user_id'] = hash_id.decode(decoded['user_id_hash'])[0]
    return decoded


def decrypt_hashid(hashval: str) -> Any:
    """Used to retrive value from hashed string"""
    if hashval:
        _hashids = Hashids(salt=config_data.get('HASH_ID_SALT'), min_length=10)
        decoded_val = _hashids.decode(hashval)
        if decoded_val:
            return decoded_val[0]
    return None


def encrypt_value(value: int) -> Any:
    """Encryts value into hash string"""
    if value:
        try:
            _hashids = Hashids(config_data.get('HASH_ID_SALT'), min_length=10)
            encoded_val = _hashids.encode(value)
            if encoded_val:
                return encoded_val
        except Exception as e:  # type: ignore  # noqa: F841
            pass
    return None


# def add_document_common(entity_id: Any, entity_type: str, title: str, file: Any, created_by: User, org_id: int,
#                         uuid: Any = None) -> Any:
#     """This method is used for adding document."""
#     try:
#         file_name, file_path, file_size = upload_file_and_get_object_details(file_obj=file,
#                                                                              obj_name=entity_type + '_' + str(
#                                                                                  entity_id), entity_type=entity_type,
#                                                                              attachment_type=AttachmentType.DOCUMENT.value)
#         doc = Attachment(entity_type=entity_type, entity_id=entity_id, file_size=str(file_size), file_name=file_name,
#                          attachment_type=AttachmentType.DOCUMENT.value, title=title, uuid=uuid,
#                          created_by_id=created_by.id, org_id=org_id, file_path=file_path)
#         db.session.add(doc)
#         db.session.commit()
#         return doc
#     except Exception as e:
#         logger.info(e)
#         return False


# def update_document_common(id: Any, title: str, updated_by: User, file: Any = None) -> bool:
#     """This method is used for updating document."""
#     try:
#         doc = Attachment.get_by_org_id_id(org_id=updated_by.org_id, id=id)
#         if doc:
#             if file:
#                 file_name, file_path, file_size = upload_file_and_get_object_details(
#                     file_obj=file, obj_name=doc.entity_type + '_' + str(doc.entity_id), entity_type=doc.entity_type, attachment_type=doc.attachment_type)
#                 delete_file_from_bucket(doc.file_path)
#                 doc.file_size = str(file_size)
#                 doc.file_name = file_name
#                 doc.file_path = file_path
#             doc.title = title
#             doc.updated_by_id = updated_by.id
#             db.session.commit()
#             return True
#     except Exception as e:
#         logger.info(e)
#         return False


def send_json_response(http_status: int, response_status: bool, message_key: str, data: Any = None,
                       error: Any = None) -> tuple:
    """This method used to send JSON response in custom dir structure. Here, status represents boolean value true/false
    and http_status is http response status code."""

    if data is None and error is None:
        return jsonify({'status': response_status, 'message': message_key}), http_status
    if response_status:
        return jsonify({'status': response_status, 'message': message_key, 'data': data}), http_status
    else:
        return jsonify({'status': response_status, 'message': message_key, 'error': error}), http_status


def get_display_date(date_time, date_format):
    """ converts the date time object and returns a string in the yacht user's selected date format"""
    return date_time.strftime(DateFormats.get_python_date_format(date_format))


def get_display_time(date_time, time_format):
    """ converts the date time object and returns a string in the yacht user's selected time format"""
    return date_time.strftime(TimeFormats.get_python_time_format(time_format))


# def export_excel(headings: list, data: list, entity_type: str, attachment_type: str, entity_id: Any, user: None) -> str:
#     """This method is used to export data into Excel file.
#     Here, we pass list of dictionary as data from which it will generate Excel file and store it in AWS bucket and
#      return file URL."""
#     file_name = entity_type + '_' + attachment_type + '_' + str(
#         datetime.now().strftime('%d_%m_%Y_%H_%M_%S')) + '.xls'
#     if not os.path.exists(config_data['UPLOAD_FOLDER'] + 'report/'):
#         os.mkdir(config_data['UPLOAD_FOLDER'] + 'report/')
#     path = config_data['UPLOAD_FOLDER'] + 'report/' + file_name
#     with open(path, 'wb') as file:  # type: ignore  # noqa: FKA100
#         work_book = xlwt.Workbook(encoding='utf-8')
#         work_sheet = work_book.add_sheet('Tests', cell_overwrite_ok=True)

#         # Sheet header, first row
#         row_num = 0
#         font_style = xlwt.XFStyle()
#         font_style.font.bold = True
#         columns = headings

#         for col_num in range(len(columns)):
#             # at 0 row 0 column
#             work_sheet.write(row_num, col_num, columns[col_num], font_style)  # type: ignore  # noqa: FKA100

#         # Sheet body, remaining rows
#         font_style = xlwt.XFStyle()
#         row_num += 1
#         for datum in data:
#             col = 0
#             for heading in headings:
#                 if heading == 'created_by':
#                     value = datum['crew_full_name']
#                 elif heading == 'type':
#                     value = get_transaction_type(datum['credit'])
#                 elif heading == 'address':
#                     if datum['location'] is not None:
#                         value = '{0}, {1}'.format(datum.get('location').get(  # type: ignore  # noqa: FKA100
#                             'city', None), datum.get('location').get('country', None))  # type: ignore  # noqa: FKA100
#                     else:
#                         value = None
#                 else:
#                     value = datum[heading]

#                 if isinstance(value, datetime) and yacht_user is not None:
#                     date_part = get_display_date(
#                         date_time=value, date_format=yacht_user.preference[Preference.DATE_FORMAT.value])
#                     time_part = get_display_time(
#                         date_time=value, time_format=yacht_user.preference[Preference.TIME_FORMAT.value])
#                     value = date_part + ' ' + time_part
#                 work_sheet.write(row_num, col, str(value) if value else '-', font_style)  # type: ignore  # noqa: FKA100
#                 col += 1
#             row_num += 1

#         work_book.save(file)
#     file.close()
#     name, path, size = upload_file_and_get_object_details(file_obj=path,
#                                                           obj_name=file_name, entity_type=entity_type,
#                                                           attachment_type=attachment_type, is_report=True)

#     attachment = Attachment(entity_type=entity_type, entity_id=entity_id,
#                             attachment_type=attachment_type, file_name=name, file_size=size,
#                             created_by_id=user.id, file_path=path)
#     db.session.add(attachment)
#     db.session.commit()

#     return get_object_url(path=attachment.file_path)


def get_transaction_type(type: Any) -> str:
    """This method returns valid transaction type from boolean value."""
    if isinstance(type, bool):
        if type:
            return 'Credit'
        else:
            return 'Debit'
    else:
        return '-'


def object_as_dict(obj: Any) -> Any:
    """This method returns sqlalchemy object as key value pair dictionary.
        If you want to remove some confidential keys from dictionary you can do it after generating
        dictionary with if condition.
    """
    if obj:
        if not isinstance(obj, list):
            obj_type = dict
            obj = [obj]
        else:
            obj_type = list
        object_dict_list = []
        for object in obj:
            # object_dict = {c.key: (getattr(object, c.key) if not isinstance(getattr(object, c.key), enum.Enum) else getattr(object, c.key).name)
            #                for c in inspect(object).mapper.column_attrs}
            # object_dict = lambda r: {c.name: str(getattr(r, c.name)) for c in r.__table__.columns}
            object_dict = {}
            for _obj in inspect(object).mapper.column_attrs:
                value = getattr(object, _obj.key)
                if isinstance(value, enum.Enum):
                    object_dict[_obj.key] = value.name
                elif isinstance(value, list) and len(value) and isinstance(value[0], enum.Enum):
                    object_dict[_obj.key] = [c1.name for c1 in value]
                else:
                    object_dict[_obj.key] = value

                if isinstance(value, dict) and 'file_path' in value.keys():
                    object_dict[_obj.key] = get_object_url(value['file_path'])
                elif value and isinstance(value, list) and isinstance(value[0], dict) and 'file_path' in value[0].keys():
                    object_dict[_obj.key] = [get_object_url(
                        v['file_path']) for v in value if 'file_path' in v.keys()]
            if 'password' in object_dict.keys():
                object_dict.pop('password')
            if 'temporary_password' in object_dict.keys():
                object_dict.pop('temporary_password')

            if hasattr(object, 'accounts'):
                object_dict['accounts'] = [account_as_dict(
                    account=account, user_id=object_dict['id']) for account in object.accounts]
            else:
                object_dict['accounts'] = []

            # object_dict['accounts'] = [account_as_dict(account, object_dict['id']) for account in object.accounts]

            object_dict_list.append(object_dict)
        if obj_type == list:
            return object_dict_list
        elif obj_type == dict:
            return object_dict_list[0]
    return None


def account_as_dict(account: Any, user_id: int) -> dict:
    """Converts an Account object to a dictionary."""
    is_primary = True if user_id == account.primary_user_id else False

    account_dict = {
        'id': account.id,
        'legal_name': account.legal_name,
        'display_name': account.display_name,
        'primary_user_id': account.primary_user_id,
        'asp_id': account.asp_id,
        'is_primary': is_primary
    }

    return account_dict


def get_current_timestamp() -> int:
    """Get the current Unix timestamp."""
    return int(time.time())


def get_asp_credentials_schema() -> dict:
    """ get Amazon Credentials object schema """

    schema = {
        'oauth_state': '',
        'mws_auth_token': '',
        'seller_partner_id': '',
        'spapi_oauth_code': '',
        'spapi_oauth_code_updated_at': '',
        'access_token': '',
        'token_type': '',
        'expires_in': '',
        'refresh_token': '',
        'refresh_token_updated_at': '',
        'created_at': '',
        'updated_at': ''
    }

    return schema


def get_az_ads_credentials_schema() -> dict:
    """ get Amazon Credentials object schema """

    schema = {
        'refresh_token': '',
        'refresh_token_updated_at': '',
        'token_type': '',
        'expires_in': '',
        'created_at': '',
    }

    return schema


def convert_date_string(date_str):
    """
    Converts a date string to a formatted string if necessary.

    Args:
        date_str (str or datetime): Date string or datetime object to be converted.

    Returns:
        str: Formatted date string in the format 'YYYY-MM-DD HH:MM:SS'.
    """
    if isinstance(date_str, datetime):
        # If the input is already a datetime object, format it and return
        return date_str.strftime('%Y-%m-%dT%H:%M:%S.000Z')

    # If the input is a string, parse it and format the resulting datetime object
    date_obj = parser.parse(date_str)
    formatted_date_str = date_obj.strftime('%Y-%m-%dT%H:%M:%S.000Z')

    return formatted_date_str


def convert_string_to_datetime(input_string):
    """Converts a string to a datetime.date object if possible."""
    # Check if the input is already a datetime object
    if isinstance(input_string, datetime):
        # Extract the date part from the datetime object
        date_only = input_string.date()
        return date_only

    if isinstance(input_string, str):
        try:
            # Try to convert with the first format '%Y-%m-%d'
            datetime_obj = datetime.strptime(input_string, '%Y-%m-%d')  # type: ignore  # noqa: FKA100
            date_only = datetime_obj.date()
            return date_only
        except ValueError:
            try:
                # If the first format fails, try the second format '%Y-%m-%dT%H:%M:%S.%fZ'
                datetime_obj = datetime.strptime(input_string, '%Y-%m-%dT%H:%M:%S.%fZ')  # type: ignore  # noqa: FKA100
                date_only = datetime_obj.date()
                return date_only
            except ValueError:
                # Handle the case where the input string is not in either format
                return None
    else:
        return None


def get_current_datetime():
    """
    Returns the current date and time as a formatted string.

    Returns:
        str: Current date and time in the format 'YYYY-MM-DD HH:MM:SS'
    """
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def get_current_date():
    """
    Returns the current date as a formatted string.

    Returns:
        str: Current date in the format 'YYYY-MM-DD'
    """
    return datetime.now().strftime('%Y-%m-%d')


def get_from_to_date_by_timestamp(time_period: datetime) -> tuple:
    """This function calculates the start and end date for the given time period and returns them as db.datetime objects."""
    current_time = datetime.now()
    start_date = datetime(year=current_time.year,
                          month=current_time.month, day=current_time.day)
    end_date = current_time

    if time_period == TimePeriod.LAST_3_DAYS.value:
        start_date -= timedelta(days=3)
    if time_period == TimePeriod.LAST_7_DAYS.value:
        start_date -= timedelta(days=7)
    elif time_period == TimePeriod.LAST_30_DAYS.value:
        start_date -= timedelta(days=30)
    elif time_period == TimePeriod.LAST_MONTH.value:
        end_date = start_date.replace(day=1) - timedelta(days=1)
        start_date = start_date.replace(day=1) - timedelta(days=end_date.day)
    elif time_period == TimePeriod.LAST_YEAR.value:
        start_date = datetime(year=current_time.year - 1,
                              month=current_time.month, day=current_time.day)
        end_date = start_date.replace(month=12, day=31)
        start_date = start_date.replace(month=1, day=1)
    elif time_period == TimePeriod.CURRENT_MONTH.value:
        start_date = start_date.replace(day=1)

    # Convert start and end dates to ISO 8601 format
    start_datetime = start_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    end_datetime = end_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')

    return start_datetime, end_datetime


def get_from_to_date_by_time_period(time_period: datetime) -> tuple:
    """This method calculates start and end date from time period."""
    current_time = datetime.now()
    start_date = datetime(
        year=current_time.year, month=current_time.month, day=current_time.day)

    if time_period == TimePeriod.LAST_3_DAYS.value:
        end_date = start_date - timedelta(days=3)
        start_date, end_date = end_date, start_date
    if time_period == TimePeriod.LAST_7_DAYS.value:
        end_date = start_date - timedelta(days=7)
        start_date, end_date = end_date, start_date
    elif time_period == TimePeriod.LAST_30_DAYS.value:
        end_date = start_date - timedelta(days=30)
        start_date, end_date = end_date, start_date
    elif time_period == TimePeriod.LAST_MONTH.value:
        end_date = start_date.replace(day=1) - timedelta(days=1)
        start_date = start_date.replace(day=1) - timedelta(days=end_date.day)
    elif time_period == TimePeriod.LAST_YEAR.value:
        start_date = datetime(year=current_time.year - 1,
                              month=current_time.month, day=current_time.day)
        end_date = start_date.replace(month=12, day=31)
        start_date = start_date.replace(month=1, day=1)
    elif time_period == TimePeriod.CURRENT_MONTH.value:
        end_date = start_date
        start_date = start_date.replace(day=1)
    end_date = end_date.replace(hour=23, minute=59, second=59)
    return start_date, end_date


def get_object_url(path: str) -> str:
    """This method returns s3 object full url from given path."""
    presigned_url = generate_presigned_url(path=path)
    if path is None:
        return ''
    return presigned_url


def get_bucket_name() -> str:
    """Returns Current Bucket name according to environment"""
    environment = config_data.get('APP_ENV')
    bucket = 'ep-backend-staging-config'
    if environment in ENVIRONMENT_LIST:
        bucket = AWS_BUCKET
    return bucket


def generate_presigned_url(path: str) -> str:
    """
        Generate a presigned URL for the uploaded file.
    """
    try:

        bucket = get_bucket_name()

        presigned_url = S3_RESOURCE.meta.client.generate_presigned_url(
            'get_object', Params={'Bucket': bucket, 'Key': path}, ExpiresIn=3600)

        return presigned_url

    except Exception as exception_error:
        logger.error(
            f'RESOURCE -> Presigned URL Generation Failed: {exception_error}')


def get_current_month_first_date():
    """Get the first date of the current month."""
    today = datetime.now().date()
    first_date = today.replace(day=1)
    return f'{first_date}T00:00:00.000Z'


def get_asp_data_start_time():
    """Get the data start time as yesterday's date."""
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    return f'{yesterday}T00:00:00.000Z'


def get_asp_data_end_time():
    """Get the data start time as yesterday's date."""
    today = datetime.now().date()
    return f'{today}T00:00:00.000Z'


def get_asp_market_place_ids():
    """
    Get the marketplace IDs.

    Returns:
        list: List of marketplace IDs.
    """
    # By default, the Amazon marketplace ID returned is for India (IN)
    return [ASpMarketplaceId.IN.value]


def get_created_since(created_since: str):
    """get created since"""
    created_since = datetime.strptime(created_since, '%Y-%m-%d').date()  # type: ignore  # noqa: FKA100
    return f'{created_since}T00:00:00.000Z'


def get_created_until(created_until: str):
    """get created until"""
    created_until = datetime.strptime(created_until, '%Y-%m-%d').date()  # type: ignore  # noqa: FKA100
    return f'{created_until}T23:59:59.999Z'


def get_date_from_string(date_string: str):
    """
        Convert a string to a date object.

        Args:
            date_string (str): Date string in the format 'YYYY-MM-DD'.

        Returns:
            datetime.date: Date object representing the input date string.
        """
    if date_string:
        date_obj = datetime.strptime(date_string, '%Y-%m-%d').date()    # type: ignore  # noqa: FKA100
        return date_obj
    else:
        return None


def get_datetime_from_string(date_string: str):
    """convert string to datetime obj"""
    datetime_obj = datetime.strptime(date_string, '"%Y-%m-%dT%H:%M:%S%z"')   # type: ignore  # noqa: FKA100
    return datetime_obj


def get_prior_to_from_date(from_date: str, to_date: str):
    """ calculate date difference and get prior to and from date equivalently

        #sample input
        from_date = 2023-06-16
        to_date = 2023-06-20

        #sample output
        prior_from_date = 2023-06-11
        prior_to_date = 2023-06-15

    """

    from_date = datetime.strptime(from_date, '%Y-%m-%d').date()        # type: ignore  # noqa: FKA100
    to_date = datetime.strptime(to_date, '%Y-%m-%d').date()               # type: ignore  # noqa: FKA100

    day_difference = (to_date - from_date).days + 1

    prior_from_date = from_date - relativedelta(days=day_difference)
    prior_to_date = from_date - relativedelta(days=1)

    return datetime.strftime(prior_from_date, '%Y-%m-%d'), datetime.strftime(prior_to_date, '%Y-%m-%d')       # type: ignore  # noqa: FKA100


def get_previous_year_to_from_date(from_date: str, to_date: str):
    """ calculate one previous year from_date and to_date

        #sample input
        from_date = 2023-06-16
        to_date = 2023-06-20

        #sample output
        previous_year_from_date = 2022-06-16
        previous_year_to_date = 2022-06-20

    """

    from_date = datetime.strptime(from_date, '%Y-%m-%d').date()        # type: ignore  # noqa: FKA100
    to_date = datetime.strptime(to_date, '%Y-%m-%d').date()               # type: ignore  # noqa: FKA100

    previous_year_from_date = from_date - relativedelta(years=1)
    previous_year_to_date = to_date - relativedelta(years=1)

    return datetime.strftime(previous_year_from_date, '%Y-%m-%d'), datetime.strftime(previous_year_to_date, '%Y-%m-%d')       # type: ignore  # noqa: FKA100


def flatten_json(unflatten_json):
    """flatten json"""
    out = {}

    def flatten(json_obj, name=''):

        # If the Nested key-value
        # pair is of dict type
        if type(json_obj) is dict:

            for item in json_obj:
                flatten(json_obj[item], name + item + '_')    # type: ignore  # noqa: FKA100

        # If the Nested key-value
        # pair is of list type
        elif type(json_obj) is list:

            index = 0

            for item in json_obj:
                flatten(item, name + str(index) + '_')     # type: ignore  # noqa: FKA100
                index += 1
        else:
            out[name[:-1]] = json_obj

    flatten(unflatten_json)
    return out


def is_valid_numeric_value(value):
    """Check if value is numeric"""
    try:
        if value.isdigit() or value.isdecimal():
            return True
        else:
            return False
    except ValueError:
        return False


def generate_seller_api_awssigv4():
    """Returns AWS Signature Version 4 for Seller API"""

    amw_client = boto3.client(
        'sts'
    )

    res = amw_client.assume_role(
        RoleArn=config_data.get('SP_ROLE_ARN'),
        RoleSessionName=config_data.get('SP_ROLE_SESSION_NAME')
    )

    credentials = res['Credentials']
    access_key_id = credentials['AccessKeyId']
    secret_access_key = credentials['SecretAccessKey']
    session_token = credentials['SessionToken']

    return AWSSigV4('execute-api',
                    aws_access_key_id=access_key_id,
                    aws_secret_access_key=secret_access_key,
                    aws_session_token=session_token,
                    region=config_data.get('AWS_REGION')
                    )


def generate_paapi_awssigv4(host: str, service: str, method_name: str, timestamp: Any, headers: Dict[str, str], resource_path: str, payload: Any) -> Any:
    """Returns AWS Signature Version 4 for Product adverting API"""

    access_key_id = config_data.get('PAAPI_KEY')
    secret_access_key = config_data.get('PAAPI_SECRET')
    region = config_data.get('PAAPI_REGION')

    logger.info('generate_paapi_awssigv4 Start')
    logger.info('*' * 100)
    logger.info(host)
    logger.info(region)
    logger.info(service)
    logger.info(method_name)
    logger.info(headers)
    logger.info(timestamp)
    logger.info(payload)
    logger.info(resource_path)
    logger.info('*' * 100)
    logger.info('generate_paapi_awssigv4 End')

    auth = AWSV4Auth(
        access_key=access_key_id,
        secret_key=secret_access_key,
        host=host,
        region=region,
        service=service,
        method_name=method_name,
        timestamp=timestamp,
        headers=headers,
        path=resource_path,
        payload=payload
    )

    return auth


def string_to_bool(string):
    """returns bool value """

    str_to_bool = {'True': True, 'False': False, 'true': True, 'false': False}

    if isinstance(string, str):
        if string in str_to_bool:
            return str_to_bool[string]
    else:
        return string


def amount_details_to_json(amount_details_dict: dict, row):
    """convert columns related to amount details to dict"""
    details_dict = {}
    if row.amount_type:
        details_dict['amount_type'] = row.amount_type
    if row.amount_description:
        details_dict['amount_description'] = row.amount_description
    if row.amount:
        details_dict['amount'] = row.amount

    amount_details_dict['amount_details'].append(details_dict)

    return amount_details_dict


def get_date_from_timestamp(timestamp: int):
    """converts unix timestamp to date object"""
    date = datetime.fromtimestamp(timestamp).date()

    return date


def get_date_and_time_from_timestamp(timestamp: int):
    """converts unix timestamp to datetime object"""
    date_time = datetime.fromtimestamp(timestamp)
    return date_time


def get_yesterday_date():
    """get yesterdays date object"""

    today = date.today()
    yesterday = today - timedelta(days=1)

    return yesterday


def convert_text_to_snake_case(text):
    """Converts text to snake case.
    Args:
    s (str): The text to convert.

    Returns:
    str: The text in snake case.

    This function converts text to snake case by replacing all occurrences of
    uppercase letters followed by lowercase letters with a single underscore, and
    then converting all letters to lowercase.
    """
    return '_'.join(
        sub('([A-Z][a-z]+)', r' \1',  # type: ignore  # noqa: FKA100
            sub('([A-Z]+)', r' \1',  # type: ignore  # noqa: FKA100
                text.replace('-', ' '))).split()).lower()  # type: ignore  # noqa: FKA100


def convert_to_numeric(value):
    """check if value is int or float and return. if value is string return appropriate numeric value """

    if isinstance(value, int):
        return value
    elif isinstance(value, float):
        return round(value, 5)              # type: ignore  # noqa: FKA100
    elif value is None:
        return 0
    else:
        if float(value).is_integer():
            return int(value)
        else:
            return round(float(value), 5)       # type: ignore  # noqa: FKA100


def calculate_percentage_growth(current_value, prior_value):
    """calculate percentage growth with prior value as baseline"""
    if prior_value:
        prior_percentage_growth = round(
            number=((current_value - prior_value) / prior_value) * 100, ndigits=2)
    elif not prior_value and not current_value:
        prior_percentage_growth = 0
    else:
        prior_percentage_growth = current_value

    return prior_percentage_growth


def get_pagination_meta(current_page: int, page_size: int, total_items: int) -> dict:
    """
        This method generates pagination metadata.
    """

    if page_size:
        total_pages = math.ceil(total_items / page_size)
        has_next_page = current_page < total_pages
        has_previous_page = current_page > 1
        next_page = current_page + 1 if has_next_page else None
        previous_page = current_page - 1 if has_previous_page else None
    else:
        total_pages = current_page
        has_next_page = None
        has_previous_page = None
        next_page = None
        previous_page = None
        page_size = None

    return {
        'current_page': current_page,
        'page_size': page_size,
        'total_items': total_items,
        'total_pages': total_pages,
        'has_next_page': has_next_page,
        'has_previous_page': has_previous_page,
        'next_page': next_page,
        'previous_page': previous_page
    }


def date_to_string(input_date):
    """Convert a datetime.date object to a string in 'YYYY-MM-DD' format."""

    if not isinstance(input_date, date):
        raise ValueError('Input must be a datetime.date object')

    return input_date.strftime('%Y-%m-%d')


def is_valid_numeric(value):
    """Check if value is numeric"""
    if isinstance(value, (int, float)):
        return True
    try:
        float(value)  # Try to convert the value to a float
        return True
    except ValueError:
        return False


def get_amz_date(utc_timestamp):
    """
    Convert a UTC timestamp to an Amazon-style date string.

    Args:
        utc_timestamp (datetime.datetime): A UTC timestamp to be converted.

    Returns:
        str: The Amazon-style date string in the format '%Y%m%dT%H%M%SZ'.
    """
    return utc_timestamp.strftime('%Y%m%dT%H%M%SZ')


def is_same_month_year(from_date: str, to_date: str):
    """validates if dates belong to same month"""

    from_date = datetime.strptime(from_date, '%Y-%m-%d')        # type: ignore  # noqa: FKA100
    to_date = datetime.strptime(to_date, '%Y-%m-%d')            # type: ignore  # noqa: FKA100

    # Check if they belong to the same month and year
    if from_date <= to_date and from_date.year == to_date.year and from_date.month == to_date.month:
        return True
    return False


def get_first_last_date(from_date: str, to_date: str):
    """returns first day and last day of month if valid"""

    from_date = datetime.strptime(from_date, '%Y-%m-%d')        # type: ignore  # noqa: FKA100
    to_date = datetime.strptime(to_date, '%Y-%m-%d')            # type: ignore  # noqa: FKA100

    current_date = from_date.replace(day=1)

    # Get the current month for comparison
    ongoing_month = datetime.now().replace(day=1)

    dates = []
    while current_date <= to_date:

        if current_date.month != ongoing_month.month:
            # Calculate the last day of the current month
            last_day = (current_date.replace(day=1)
                        + timedelta(days=32)).replace(day=1) - timedelta(days=1)
            dates.append({'first_day': current_date.strftime(
                '%Y-%m-%d'), 'last_day': last_day.strftime('%Y-%m-%d')})

        if current_date.month == 12:
            current_date = current_date.replace(
                year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)
    return dates


def get_previous_month_dates():
    """returns first and last date of previous month from current time"""

    current_date = datetime.now()
    first_day_of_current_month = current_date.replace(day=1)

    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    first_day_of_previous_month = last_day_of_previous_month.replace(day=1)

    return first_day_of_previous_month.strftime('%Y-%m-%d'), last_day_of_previous_month.strftime('%Y-%m-%d')


def format_iso_to_12_hour_format(input_datetime):
    try:
        # Parse the ISO format datetime string
        dt_obj = datetime.fromisoformat(input_datetime)

        # Format it as "Mon DD hh:mm am/pm"
        formatted_datetime = dt_obj.strftime('%b %d %I:%M %p')

        return formatted_datetime

    except ValueError:
        return 'Invalid input format'


def create_invite_auth_token(uuid: str):
    """Generates jwt token with data and expire_at key """

    expire_at = datetime.utcnow() + timedelta(days=2)

    token = jwt.encode({'uuid': uuid, 'expire_at': expire_at.timestamp()},
                       key=config_data.get('JWT_SALT'), algorithm='HS256')

    return token


def is_valid_invite_auth_token(token: str):
    """Returns true true if passed auth token is valid jwt token"""

    try:
        decoded = jwt.decode(token.encode(), key=config_data.get(
            'JWT_SALT'), algorithms=['HS256'])

        if decoded['expire_at'] < datetime.utcnow().timestamp():
            return False

        return True

    except Exception as e:              # type: ignore  # noqa: F841
        return False


def get_file_schema() -> dict:
    """ get file object schema """
    schema = {'file_attachment_id': '', 'file_name': '',
              'file_path': '', 'file_size': ''}
    return schema


def get_profile_detail_schema() -> dict:
    """ get Account profile object schema """

    schema = {
        'legal_name': '',
        'phone': '',
        'address': '',
        'country': '',
        'state': '',
        'city': '',
        'zip_code': '',
        'profile_photo': '',
        'file_name': '',
        'file_path': '',
        'file_size': '',
        'file_attachment_id': ''
    }

    return schema


def get_previous_day_date(date):
    """retrieve one day back date"""

    previous_day_date_obj = date - timedelta(days=1)
    previous_day_date_str = previous_day_date_obj.strftime('%Y-%m-%d')

    return previous_day_date_str


def generate_date_ranges(num_months=3, batch=30, start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Get last 3 months batch dates"""

    if not start_date:
        current_date = datetime.now()
        start_date = (current_date - timedelta(days=current_date.day - 1)
                      ).replace(month=current_date.month - num_months).strftime('%Y-%m-%d')
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')

    # First convert the string version of start and end dates into datetime

    start = datetime.strptime(start_date, '%Y-%m-%d')  # type: ignore  # noqa: FKA100
    end = datetime.strptime(end_date, '%Y-%m-%d')  # type: ignore  # noqa: FKA100

    # then set the timedelta to the batch - 1 day
    # end date is always calculated as 9 more days not 10 (hence -1)

    step = timedelta(days=(batch - 1))

    # iterate through the loop until start <= end

    result = []

    while start <= end:
        date_dict = {'start_date': '', 'end_date': ''}
        date_dict['start_date'] = start.strftime('%Y-%m-%d')
        start += step  # add the timedelta to start
        if start > end:
            start = end
        date_dict['end_date'] = start.strftime('%Y-%m-%d')
        result.append(date_dict)
        start += timedelta(days=1)  # now increment by 1 more to get start date

    return result


def format_float_values(data_dict):
    formatted_dict = {}

    for key, value in data_dict.items():
        if isinstance(value, dict):
            formatted_dict[key] = format_float_values(value)
        elif isinstance(value, float):
            formatted_dict[key] = round(value, 2)  # type: ignore  # noqa: FKA100
        else:
            formatted_dict[key] = value

    return formatted_dict
