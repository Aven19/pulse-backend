"""test cases for user module apis"""
import os

from app import COGNITO_CLIENT
from app import config_data
from tests.conftest import validate_response
from tests.conftest import validate_status_code

# user login test cases


def test_user_login_positive(test_client):
    """
        TEST CASE: This is a positive test case. Return a successful response if fields are validated and user login successfully.
    """

    expected_response = {
        'data': {
            'objects': {
                'subscription': {
                    'plan_expiry': '*',
                    'plan_id': '*',
                    'plan_name': '*',
                    'state': '*',
                    'subscription_id': '*',
                    'user': '*'
                },
                'user': {
                    'auth_token': '*',
                    'created_at': '*',
                    'email_id': 'testpurposephp@gmail.com',
                    'first_name': 'john',
                    'id': 1,
                    'last_name': 'doe'
                }
            },
            'result': [
                {
                    'account_id': '7e36ba8f-073a-4dee-9ba5-d7880c3c3ba9',
                    'asp_id': 'B3D3499BOYNHX',
                    'asp_id_connected_at': '*',
                    'asp_marketplace': '*',
                    'az_ads_account_name': '*',
                    'az_ads_profile_id': [

                    ],
                    'display_name': '*',
                    'is_primary': '*',
                    'legal_name': '*',
                    'profile_attachment_id': '*',
                    'selling_partner_id': 'B3D3499BOYNHX'
                }
            ]
        },
        'message': 'Hi, great to see you!',
        'status': True
    }

    body = {
        'email': 'testpurposephp@gmail.com',
        'password': 'qwerty@123Q'
    }

    api_response = test_client.post(
        '/api/v1/user/authenticate', json=body, content_type='application/json'
    )
    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_user_login_missing_password_negative(test_client):
    """
        TEST CASE: This is a negative test case. Return a failure response if password is missing from the body.
    """

    expected_response = {
        'error': {
            'password': 'Password is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    body = {
        'email': 'testpurposephp@gmail.com',
        'password': ''
    }

    api_response = test_client.post(
        '/api/v1/user/authenticate', json=body, content_type='application/json'
    )
    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_user_login_missing_email_negative(test_client):
    """
        TEST CASE: This is a negative test case. Return a failure response if email is missing from the body.
    """

    expected_response = {
        'error': {
            'email': 'Email is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    body = {
        'password': 'qwerty@123Q'
    }

    api_response = test_client.post(
        '/api/v1/user/authenticate', json=body, content_type='application/json'
    )
    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_user_login_incorrect_password_negative(test_client):
    """
        TEST CASE: This is a negative test case. Return a failure response if password is incorrect.
    """

    expected_response = {
        'message': 'Login Failed.',
        'status': False
    }

    body = {
        'email': 'testpurposephp@gmail.com',
        'password': 'abcd@123Q'
    }

    api_response = test_client.post(
        '/api/v1/user/authenticate', json=body, content_type='application/json'
    )
    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_user_login_incorrect_email_negative(test_client):
    """
        TEST CASE: This is a negative test case. Return a failure response if email/username is incorrect.
    """

    expected_response = {
        'message': 'Entered Email ID is not registered with us.',
        'status': False
    }

    body = {
        'email': 'testpose@gmail.com',
        'password': 'abcd@123Q'
    }

    api_response = test_client.post(
        '/api/v1/user/authenticate', json=body, content_type='application/json'
    )
    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


# get account list for user

def test_get_account_positive(test_client):
    """
        TEST CASE: This is a positive test case. Return a successful response if fields are validated and data exists.
    """

    expected_response = {
        'data': {
            'objects': {
                'az_ads_profile_listing': '*',
                'data_sync_progress': '*',
                'subscription': {
                    'plan_expiry': '*',
                    'plan_id': '*',
                    'plan_name': '*',
                    'state': '*',
                    'subscription_id': '*',
                    'user': '*'
                },
                'user': {
                    'created_at': '*',
                    'email_id': 'testpurposephp@gmail.com',
                    'first_name': 'john',
                    'id': '*',
                    'last_name': 'doe'
                }
            },
            'result': [
                {
                    'account_id': '7e36ba8f-073a-4dee-9ba5-d7880c3c3ba9',
                    'asp_id': 'B3D3499BOYNHX',
                    'asp_id_connected_at': '*',
                    'asp_marketplace': '*',
                    'az_ads_account_name': '*',
                    'az_ads_profile_id': [

                    ],
                    'display_name': '*',
                    'is_primary': '*',
                    'legal_name': '*',
                    'profile_attachment_id': '*',
                    'selling_partner_id': 'B3D3499BOYNHX'
                }
            ]
        },
        'message': 'User details fetched successfully.',
        'status': True
    }

    query_params = {}

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/user/account/list', query_string=query_params, headers=headers, content_type='application/json'
    )
    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)

# user register


def test_register_positive(test_client):
    """
        TEST CASE: This is a positive test case. Return a successful response if fields are validated and user registration successfully.
    """

    expected_response = {
        'message': 'User added successfully.',
        'status': True
    }

    body = {
        'email': 'registertest@gmail.com',
        'password': 'qwerty@123Q',
        'first_name': 'register',
        'last_name': 'test'
    }

    api_response = test_client.post(
        '/api/v1/user/register', json=body, content_type='application/json'
    )

    COGNITO_CLIENT.admin_delete_user(UserPoolId=config_data.get(
        'COGNITO_USER_POOL_ID'), Username=body.get('email'))

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_register_missing_email_negative(test_client):
    """
        TEST CASE: This is a negative test case. Return a failure response if email field is missing.
    """

    expected_response = {
        'error': {
            'email': 'Email is required.',
        },
        'message': 'Enter correct input.',
        'status': False
    }

    body = {
        'password': 'qwerty@123Q',
        'first_name': 'register',
        'last_name': 'test'
    }

    api_response = test_client.post(
        '/api/v1/user/register', json=body, content_type='application/json'
    )

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_register_missing_password_negative(test_client):
    """
        TEST CASE: This is a negative test case. Return a failure response if password field is missing.
    """

    expected_response = {
        'error': {
            'password': 'Password is required.',
        },
        'message': 'Enter correct input.',
        'status': False
    }

    body = {
        'email': 'registertest@gmail.com',
        'first_name': 'register',
        'last_name': 'test'
    }

    api_response = test_client.post(
        '/api/v1/user/register', json=body, content_type='application/json'
    )

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_register_missing_first_name_negative(test_client):
    """
        TEST CASE: This is a negative test case. Return a failure response if first_name field is missing.
    """

    expected_response = {
        'error': {
            'first_name': 'First Name is required.',
        },
        'message': 'Enter correct input.',
        'status': False
    }

    body = {
        'password': 'qwerty@123Q',
        'email': 'registertest@gmail.com',
        'last_name': 'test'
    }

    api_response = test_client.post(
        '/api/v1/user/register', json=body, content_type='application/json'
    )

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_register_missing_last_name_negative(test_client):
    """
        TEST CASE: This is a negative test case. Return a failure response if last_name field is missing.
    """

    expected_response = {
        'error': {
            'last_name': 'Last Name is required.',
        },
        'message': 'Enter correct input.',
        'status': False
    }

    body = {
        'password': 'qwerty@123Q',
        'email': 'registertest@gmail.com',
        'first_name': 'register'
    }

    api_response = test_client.post(
        '/api/v1/user/register', json=body, content_type='application/json'
    )

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_register_missing_last_first_name_negative(test_client):
    """
        TEST CASE: This is a negative test case. Return a failure response if first_name and last_name field is missing.
    """

    expected_response = {
        'error': {
            'first_name': 'First Name is required.',
            'last_name': 'Last Name is required.',
        },
        'message': 'Enter correct input.',
        'status': False
    }

    body = {
        'password': 'qwerty@123Q',
        'email': 'registertest@gmail.com'
    }

    api_response = test_client.post(
        '/api/v1/user/register', json=body, content_type='application/json'
    )

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_register_missing_email_pwd_negative(test_client):
    """
        TEST CASE: This is a negative test case. Return a failure response if email and password field is missing.
    """

    expected_response = {
        'error': {
            'email': 'Email is required.',
            'password': 'Password is required.',
        },
        'message': 'Enter correct input.',
        'status': False
    }

    body = {
        'first_name': 'register',
        'last_name': 'test'
    }

    api_response = test_client.post(
        '/api/v1/user/register', json=body, content_type='application/json'
    )

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


# ------------ test cases for google login-----------------#

def test_google_auth(test_client):
    """
        TEST CASE: This is a positive test case. Return a successful response if fields are validated and data exists.
    """

    expected_response = {
        'data': {
            'authorization_uri': '*'
        },
        'message': 'Google login URI generated successfully',
        'status': True
    }
    query_params = {}

    api_response = test_client.get(
        '/api/v1/user/auth/google', query_string=query_params, content_type='application/json'
    )
    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


# ------------ test cases for amazon auth ---------------#

def test_amazon_auth(test_client):
    """
        TEST CASE: This is a positive test case. Return a successful response if fields are validated and data exists.
    """

    expected_response = {
        'data': {
            'authorization_uri': '*'
        },
        'message': 'Amazon login URI generated successfully',
        'status': True
    }
    query_params = {}

    api_response = test_client.get(
        '/api/v1/user/auth/amazon', query_string=query_params, content_type='application/json'
    )
    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


# ------------ test cases for idp callback ---------------#

def test_idp_callback_positive(test_client):
    """
        TEST CASE: This is a positive test case. Return a successful response if fields are validated and data exists.
    """

    expected_response = {
        'data': {
            'objects': {
                'subscription': {
                    'plan_expiry': '*',
                    'plan_id': '*',
                    'plan_name': '*',
                    'state': 'ACTIVE',
                    'subscription_id': 'xyz',
                    'user': ''
                },
                'user': {
                    'auth_token': '*',
                    'created_at': '*',
                    'email_id': 'testpurposephp@gmail.com',
                    'first_name': 'john',
                    'id': '*',
                    'last_name': 'doe'
                }
            },
            'result': [
                {
                    'account_id': '7e36ba8f-073a-4dee-9ba5-d7880c3c3ba9',
                    'asp_id': 'B3D3499BOYNHX',
                    'asp_id_connected_at': '*',
                    'asp_marketplace': '*',
                    'az_ads_account_name': '*',
                    'az_ads_profile_id': [

                    ],
                    'display_name': '*',
                    'is_primary': '*',
                    'legal_name': '*',
                    'profile_attachment_id': '*',
                    'selling_partner_id': 'B3D3499BOYNHX'
                }
            ]
        },
        'message': 'Hi, great to see you!',
        'status': True
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.post(
        '/api/v1/user/auth/idp/callback', headers=headers, content_type='application/json'
    )

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_idp_callback_negative(test_client):
    """
        TEST CASE: This is a negative test case. Return a failure response if request is unauthorized.
    """

    expected_response = {
        'message': 'Invalid Token.',
        'status': False
    }

    headers = {}

    api_response = test_client.post(
        '/api/v1/user/auth/idp/callback', headers=headers, content_type='application/json'
    )

    assert validate_status_code(
        expected=401, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)

# -------------------------- user/account/list----------------------------------------------#


def test_account_list_positive_first(test_client):
    """
    TEST CASE: This is the first positive test case.
    It returns a successful response if request is valid and authorized.
    """

    expected_response = {
        'data': {
            'objects': {
                'az_ads_profile_listing': '*',
                'data_sync_progress': '*',
                'subscription': {
                    'plan_expiry': '*',
                    'plan_id': '*',
                    'plan_name': '*',
                    'state': '*',
                    'subscription_id': '*',
                    'user': '*'
                },
                'user': {
                    'created_at': '*',
                    'email_id': 'testpurposephp@gmail.com',
                    'first_name': 'john',
                    'id': '*',
                    'last_name': 'doe'
                }
            },
            'result': [
                {
                    'account_id': '7e36ba8f-073a-4dee-9ba5-d7880c3c3ba9',
                    'asp_id': 'B3D3499BOYNHX',
                    'asp_id_connected_at': '*',
                    'asp_marketplace': '*',
                    'az_ads_account_name': '*',
                    'az_ads_profile_id': [

                    ],
                    'display_name': '*',
                    'is_primary': '*',
                    'legal_name': '*',
                    'profile_attachment_id': '*',
                    'selling_partner_id': 'B3D3499BOYNHX'
                }
            ]
        },
        'message': 'User details fetched successfully.',
        'status': True
    }

    query_params = {
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/user/account/list',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_account_list_missing_token(test_client):
    """
    TEST CASE: This is the first negative test case.
    It returns a failure response if request is unauthorized due to missing/invalid token in request header.
    """

    expected_response = {'message': 'Invalid Token.', 'status': False}

    query_params = {
    }

    headers = {}

    api_response = test_client.get(
        '/api/v1/user/account/list',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=401, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)
