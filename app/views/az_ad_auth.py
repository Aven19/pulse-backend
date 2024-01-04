"""Contains user related API definitions."""
from datetime import timedelta
import time

from app import config_data
from app import db
from app import logger
from app.helpers.constants import APP_NAME
from app.helpers.constants import EntityType
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import QueueName
from app.helpers.constants import ResponseMessageKeys
from app.helpers.decorators import api_time_logger
from app.helpers.decorators import token_required
from app.helpers.queue_helper import add_queue_task_and_enqueue
from app.helpers.utility import field_type_validator
from app.helpers.utility import get_az_ads_credentials_schema
from app.helpers.utility import get_current_timestamp
from app.helpers.utility import required_validator
from app.helpers.utility import send_json_response
from app.models.account import Account
from flask import request
from providers.mail import send_mail
import requests
from werkzeug.exceptions import BadRequest


class AzAdAuth:
    """ Class for Amazon ad's api authentication"""

    @staticmethod
    def get_az_ads_callback_template():
        """ Front end redirect url"""
        params = request.args
        return params

    @staticmethod
    @api_time_logger
    def az_ads():
        """ Initiate Amazon Ads API authorization code grant flow """

        try:
            # Configuration data
            client_id = config_data.get('AZ_AD_CLIENT_ID')
            scope = 'advertising::campaign_management'
            response_type = 'code'
            redirect_uri = config_data.get('AZ_AD_CALLBACK_URL')

            url = 'https://eu.account.amazon.com/ap/oa'
            authorization_uri = f'{url}?client_id={client_id}&scope={scope}&response_type={response_type}&redirect_uri={redirect_uri}'

            return send_json_response(
                http_status=200,
                response_status=True,
                message_key=ResponseMessageKeys.ADS_URI.value,
                data={'authorization_uri': authorization_uri},
            )

        except Exception as exception_error:
            # Handle any errors that occur during the authorization flow
            logger.error(
                f'Amazon Ads API authorization failed: {exception_error}')
            return 'Error'

    @staticmethod
    @api_time_logger
    @token_required
    def az_ads_callback(user_object, account_object):
        """ Amazon Ads API authorization code grant flow """
        try:
            logged_in_user = user_object.id
            account_id = account_object.uuid

            data = request.get_json(force=True)

            # Data Validation
            field_types = {'code': str}
            required_fields = ['code']

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

            code = data.get('code')
            # scope = data.get('scope')

            url = 'https://api.amazon.co.uk/auth/o2/token'

            headers = {
                'host': 'api.amazon.com',
                'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'
            }

            params = {
                'grant_type': 'authorization_code',
                'code': code,
                'client_id': config_data.get('AZ_AD_CLIENT_ID'),
                'client_secret': config_data.get('AZ_AD_CLIENT_SECRET'),
                'redirect_uri': config_data.get('AZ_AD_CALLBACK_URL')
            }

            response = requests.request(
                method='POST', url=url, headers=headers, params=params)

            result = response.json()

            if response.status_code != 200:
                return send_json_response(
                    http_status=HttpStatusCode.BAD_REQUEST.value,
                    response_status=True,
                    data={'result': result},
                    message_key=ResponseMessageKeys.ACCESS_DENIED.value,
                )

            """Get All Profiles associated with this account by access token"""

            access_token = result.get('access_token')
            refresh_token = result.get('refresh_token')
            logger.info('refresh_token')
            logger.info(refresh_token)
            token_type = result.get('token_type')
            expires_in = result.get('expires_in')

            url = 'https://advertising-api-eu.amazon.com/v2/profiles'
            headers = {
                'Amazon-Advertising-API-ClientId': config_data.get('AZ_AD_CLIENT_ID'),
                'Authorization': f'Bearer {access_token}'
            }

            get_profiles = requests.get(url, headers=headers)

            profile_response = get_profiles.json()

            """Get all az_ads_account_info"""
            all_az_ads_account_info = Account.get_all_az_ads_account_info()

            """Check if Ad's already connected with an account ID."""
            is_connected_user_id, is_connected_account_id = AzAdAuth.get_connected_ads_account_id(
                profile_response=profile_response, all_az_ads_account_info=all_az_ads_account_info)

            if is_connected_account_id:
                account_id = is_connected_account_id
                logged_in_user = is_connected_user_id

            """Store Credentians with Profile's"""
            # account = Account.get_by_user_id(primary_user_id=logged_in_user)
            account = Account.get_by_uuid(uuid=account_id)

            if account:

                """Get all activated Ad accounts"""
                az_ads_profile_ids = account.az_ads_profile_ids if account.az_ads_profile_ids is not None else []

                """Get profile json for current AD account user."""
                az_ad_profile_json = account.az_ads_account_info if account.az_ads_account_info is not None else []
                get_profile_response = AzAdAuth.add_created_by_profile_response(
                    created_by=logged_in_user, profile_response=profile_response, az_ad_profile_json=az_ad_profile_json)

                """List of Profile Id's"""
                profile_ids = [profile_from_db.get(
                    'profileId') for profile_from_db in az_ad_profile_json]
                logger.info(profile_ids)

                """Get all credentials"""
                cred_array = account.az_ads_credentials if account.az_ads_credentials is not None else []
                account.az_ads_credentials = None
                account.az_ads_account_info = None
                db.session.commit()

                for _profile in profile_response:
                    _profile_id = _profile.get('profileId')

                    credentials = get_az_ads_credentials_schema()

                    """Append credentials for the fist time"""
                    if _profile_id not in profile_ids:
                        az_ad_profile_json.append(_profile)
                        credentials.update({
                            'refresh_token': refresh_token,
                            'refresh_token_updated_at': get_current_timestamp(),
                            'token_type': token_type,
                            'expires_in': expires_in,
                            'az_ads_profile_id': str(_profile_id),
                            'created_at': get_current_timestamp()
                        })
                        cred_array.append(credentials)

                """Update credentials only for profile id in profile responses"""
                profile_response_ids = [profile_res.get(
                    'profileId') for profile_res in profile_response]

                for cred in cred_array:
                    cred_profile_id = int(cred.get('az_ads_profile_id'))

                    if cred_profile_id in profile_response_ids:
                        cred.update({
                            'refresh_token': refresh_token,
                            'refresh_token_updated_at': get_current_timestamp(),
                            'token_type': token_type,
                            'expires_in': expires_in,
                            'created_at': get_current_timestamp()
                        })

                """Update profiles json and credentials"""
                account.az_ads_credentials = cred_array
                account.az_ads_account_info = get_profile_response
                db.session.commit()

                """Send profile id remaining to be activated"""
                profile_data = []

                for az_ad_profile in get_profile_response:
                    __profile_id = str(az_ad_profile.get('profileId'))
                    if __profile_id not in az_ads_profile_ids and az_ad_profile.get('created_by') == user_object.id:
                        profile_item = {
                            'ad_account_name': az_ad_profile.get('accountInfo').get('name'),
                            'ad_profile_id': az_ad_profile.get('profileId'),
                            'created_by': az_ad_profile.get('created_by')
                        }
                        profile_data.append(profile_item)

                if is_connected_user_id and user_object.id != is_connected_user_id:
                    return send_json_response(
                        http_status=HttpStatusCode.BAD_REQUEST.value,
                        response_status=False,
                        message_key=ResponseMessageKeys.ADS_PROFILE_EXISTS.value,
                    )

                return send_json_response(
                    http_status=200,
                    response_status=True,
                    message_key=ResponseMessageKeys.ADS_API_CONNECTED.value,
                    data={'result': profile_data},
                )

            return send_json_response(
                http_status=HttpStatusCode.NOT_FOUND.value,
                response_status=False,
                message_key=ResponseMessageKeys.NO_DATA_FOUND.value,
            )

        except BadRequest as exception_error:
            logger.error(
                f'POST -> Amazon Ads API Authorisation Callback Failed: {exception_error}')
            return send_json_response(
                http_status=HttpStatusCode.BAD_REQUEST.value,
                response_status=False,
                message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
            )

        except Exception as exception_error:
            # Handle any errors that occur during the authorization flow
            logger.error(
                f'Amazon Ads API authorization failed: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    def add_created_by_profile_response(created_by: int, profile_response: list, az_ad_profile_json: list):
        """Add created by in profiles for which user has connected Ad's account"""

        unique_profile_ids = set()
        response = []

        for az_ad_profile in az_ad_profile_json:
            profile_id = az_ad_profile['profileId']
            if profile_id not in unique_profile_ids:
                # az_ad_profile['created_by'] = created_by
                if 'created_at' not in az_ad_profile:
                    az_ad_profile['created_at'] = int(time.time())

                unique_profile_ids.add(profile_id)
                response.append(az_ad_profile)

        for __profile in profile_response:
            __profile_id = __profile['profileId']
            if __profile_id not in unique_profile_ids:
                __profile['created_by'] = created_by
                __profile['created_at'] = int(time.time())

                unique_profile_ids.add(__profile_id)
                response.append(__profile)

        return response

    @staticmethod
    def get_connected_ads_account_id(profile_response: list, all_az_ads_account_info: list):
        """Get user ID and account ID for already connected Ad's account"""

        unique_profile_ids = set()

        for __profile in profile_response:
            __profile_id = __profile['profileId']
            if __profile_id not in unique_profile_ids:
                unique_profile_ids.add(__profile_id)

        is_connected_user_id = None
        is_connected_account_id = None

        for account_info in all_az_ads_account_info:

            az_ad_profile_json = account_info.az_ads_account_info if account_info.az_ads_account_info is not None else None

            if az_ad_profile_json:
                for az_ad_profile in az_ad_profile_json:
                    profile_id = az_ad_profile['profileId']
                    if profile_id in unique_profile_ids:
                        is_connected_user_id = account_info.primary_user_id
                        is_connected_account_id = account_info.uuid
                        break

        return is_connected_user_id, is_connected_account_id

    @staticmethod
    @api_time_logger
    @token_required
    def store_profile(user_object, account_object):
        """ Amazon Ads API store Ad's Account Info """
        try:
            logged_in_user = user_object.id
            account_id = account_object.uuid

            data = request.get_json(force=True)

            # Data Validation
            field_types = {'ad_profile_id': int}
            required_fields = ['ad_profile_id']

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

            ad_profile_id = data.get('ad_profile_id')

            """Check if profile is associated with any other account"""
            all_az_ads_profile_ids = Account.get_all_az_ads_profile_ids()
            if str(ad_profile_id) in all_az_ads_profile_ids:
                return send_json_response(
                    http_status=HttpStatusCode.BAD_REQUEST.value,
                    response_status=False,
                    message_key=ResponseMessageKeys.ADS_PROFILE_EXISTS.value
                )

            account = Account.get_by_uuid(uuid=account_id)

            found_profile = []

            if account is not None and account.az_ads_account_info is not None:
                ads_profile_json = account.az_ads_account_info
                """Get all activated Ad accounts"""
                ads_profile_ids = account.az_ads_profile_ids.copy(
                ) if account.az_ads_profile_ids is not None else []

                if isinstance(ads_profile_json, list):
                    for get_profile in ads_profile_json:
                        if 'profileId' in get_profile and get_profile['profileId'] == ad_profile_id:
                            if str(ad_profile_id) not in ads_profile_ids:
                                ads_profile_ids.append(ad_profile_id)
                                found_profile.append(get_profile)

                                # Send default as true to sync default dates.
                                data.update(
                                    {'default_sync': True, 'az_ads_profile_id': str(ad_profile_id)})

                                """Define timing in minutes"""
                                queue_az_sponsored_brand_video = 240
                                queue_az_sponsored_display = 480
                                queue_az_sponsored_product = 720
                                queue_mr_performance_zone = 1440

                                """ queuing Sponsored Brand Banner and Video Report"""
                                add_queue_task_and_enqueue(queue_name=QueueName.AZ_SPONSORED_BRAND, account_id=account_id,
                                                           logged_in_user=user_object.id, entity_type=EntityType.AZ_SPONSORED_BRAND_BANNER.value, data=data)
                                add_queue_task_and_enqueue(queue_name=QueueName.AZ_SPONSORED_BRAND, account_id=account_id,
                                                           logged_in_user=user_object.id, entity_type=EntityType.AZ_SPONSORED_BRAND_VIDEO.value, data=data, time_delta=timedelta(minutes=queue_az_sponsored_brand_video))
                                """ queuing Sponsored Display report """
                                add_queue_task_and_enqueue(queue_name=QueueName.AZ_SPONSORED_DISPLAY, account_id=account_id,
                                                           logged_in_user=user_object.id, entity_type=EntityType.AZ_SPONSORED_DISPLAY.value, data=data, time_delta=timedelta(minutes=queue_az_sponsored_display))
                                """ queuing Sponsored Product report """
                                add_queue_task_and_enqueue(queue_name=QueueName.AZ_SPONSORED_PRODUCT, account_id=account_id,
                                                           logged_in_user=user_object.id, entity_type=EntityType.AZ_SPONSORED_PRODUCT.value, data=data, time_delta=timedelta(minutes=queue_az_sponsored_product))
                                """ queuing Performance Zone """
                                add_queue_task_and_enqueue(queue_name=QueueName.AZ_PERFORMANCE_ZONE, account_id=account_id,
                                                           logged_in_user=logged_in_user, entity_type=EntityType.MR_PERFORMANCE_ZONE.value, data=data, time_delta=timedelta(minutes=queue_mr_performance_zone))

                                # send mail for ads connected
                                connect_email_subject = f'Your Amazon Ads Account is Now Successfully Connected to {APP_NAME}'

                                email_data = {
                                    'email_to': user_object.email,
                                    'app_name': APP_NAME,
                                    'first_name': user_object.first_name
                                }

                                send_mail(email_to=user_object.email, subject=connect_email_subject,
                                          template='emails/az_ads_sync_email.html', data=email_data)

                        if 'profileId' in get_profile and str(get_profile['profileId']) in ads_profile_ids and get_profile['created_by'] == logged_in_user:
                            found_profile.append(get_profile)

                    account.az_ads_profile_ids = ads_profile_ids
                    db.session.commit()

                    return send_json_response(
                        http_status=200,
                        response_status=True,
                        message_key=ResponseMessageKeys.ADS_INFO_SAVED.value,
                        data={'result': found_profile},
                    )

            return send_json_response(
                http_status=HttpStatusCode.UNAUTHORIZED.value,
                response_status=True,
                message_key=ResponseMessageKeys.ACCESS_DENIED.value,
            )

        except BadRequest as exception_error:
            logger.error(
                f'POST -> Amazon Ads API save account info failed: {exception_error}')
            return send_json_response(
                http_status=HttpStatusCode.BAD_REQUEST.value,
                response_status=False,
                message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value,
            )

        except Exception as exception_error:
            logger.error(
                f'Amazon Ads API save account info failed: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )
