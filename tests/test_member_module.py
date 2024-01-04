"""test cases for member module apis"""
import os
import time

from app import COGNITO_CLIENT
from app import config_data
from tests.conftest import validate_response
from tests.conftest import validate_status_code

# ----------------test cases for members/invite-user-------------------------


def test_invite_user_positive(test_client):
    """
        TEST CASE: This is a positive test case. Return a successful response if fields are validated and invite sent successfully.
    """

    expected_response = {
        'data': {'uuid': '*', 'token': '*'},
        'message': 'User invite has been sent successfully',
        'status': True
    }

    body = {
        'email': 'invited_user@gmail.com',
        'first_name': 'donald',
        'last_name': 'trump',
        'brand': ['Cuticolor', 'Sterlomax']
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.post(
        '/api/v1/members/invite-user', json=body, headers=headers, content_type='application/json'
    )

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_invite_user_resend_positive(test_client):
    """
        TEST CASE: This is a positive test case. Return a successful response if fields are validated and invite resent successfully.
    """

    expected_response = {
        'data': {'uuid': '*', 'token': '*'},
        'message': 'Invitation has been resent',
        'status': True
    }

    body = {
        'email': 'invited_user@gmail.com',
        'first_name': 'donald',
        'last_name': 'trump',
        'brand': ['Cuticolor', 'Sterlomax'],
        'category': ['Beauty']
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.post(
        '/api/v1/members/invite-user', json=body, headers=headers, content_type='application/json'
    )

    os.environ['invite_uuid'] = api_response.json.get('data').get('uuid')
    os.environ['invite_token'] = api_response.json.get('data').get('token')

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_invite_user_already_exists_negative(test_client):
    """
        TEST CASE: This is a negative test case that returns bad requst if user already exists
    """

    expected_response = {
        'message': 'User already exists',
        'status': True
    }

    body = {
        'email': 'gopro@gmail.com',
        'first_name': 'donald',
        'last_name': 'trump',
        'brand': ['Cuticolor', 'Sterlomax']
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.post(
        '/api/v1/members/invite-user', json=body, headers=headers, content_type='application/json'
    )

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_invite_user_missing_email_negative(test_client):
    """
        TEST CASE: This is a negative test case for missing email in body
    """

    expected_response = {
        'error': {
            'email': 'Email is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    body = {
        'first_name': 'donald',
        'last_name': 'trump',
        'brand': ['Cuticolor', 'Sterlomax']
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.post(
        '/api/v1/members/invite-user', json=body, headers=headers, content_type='application/json'
    )

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_invite_user_missing_firstname_negative(test_client):
    """
        TEST CASE: This is a negative test case for missing first name in body
    """

    expected_response = {
        'error': {
            'first_name': 'First Name is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    body = {
        'email': 'invited_user@gmail.com',
        'last_name': 'trump',
        'brand': ['Cuticolor', 'Sterlomax']
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.post(
        '/api/v1/members/invite-user', json=body, headers=headers, content_type='application/json'
    )

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


# ------------------test cases for invite-link-verification--------------- #
def test_is_valid_invite_positive(test_client):
    """
       TEST CASE: This is the first positive test for checking invite link validity.
                  Return a sucessful response and the data if all required parameters are passed and the data for the conditions exist.
    """

    expected_response = {
        'data': {
            'is_user_account_added': False,
            'is_valid_invite': True
        },
        'message': 'The invite link is valid.',
        'status': True
    }

    query_params = {
        'uuid': os.environ['invite_uuid'],
        'token': os.environ['invite_token']
    }

    api_response = test_client.get(
        '/api/v1/members/check-invite-validity',
        query_string=query_params)

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_is_valid_invite_bad_token_negative(test_client):
    """
       TEST CASE: This is the first negative test for checking invite link validity.
                  Return a failure response if the token is invalid/tampered with.
    """

    expected_response = {
        'message': 'The invite link you are trying to use has expired. Please request a new invitation.',
        'status': True
    }

    query_params = {
        'uuid': os.environ['invite_uuid'],
        'token': os.environ['invite_token'] + 'cG6'
    }

    api_response = test_client.get(
        '/api/v1/members/check-invite-validity',
        query_string=query_params)

    assert validate_status_code(
        expected=410, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_is_valid_invite_bad_uuid_negative(test_client):
    """
       TEST CASE: This is the second negative test for checking invite link validity.
                  Return a failure response if the uuid is invalid/tampered with.
    """

    expected_response = {
        'message': 'Invite does not exist',
        'status': True
    }

    query_params = {
        'uuid': '7e36ba8f-073a-4dee-9ba5-d7880c3c3aba9',
        'token': os.environ['invite_token']
    }

    api_response = test_client.get(
        '/api/v1/members/check-invite-validity',
        query_string=query_params)

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)

# ----------------test cases for members/set-password -------------------------


def test_set_password_positive(test_client):
    """
        TEST CASE: This is a positive test case. Returns a successful response if password has been set and user added successfully.
    """

    expected_response = {
        'message': 'User added successfully.',
        'status': True
    }

    body = {
        'uuid': '11659bfa-d2c9-47e9-acc7-4bb8b94cddf4',
        'password': '1234@Jklasd'
    }

    api_response = test_client.post(
        '/api/v1/members/set-password', json=body, content_type='application/json'
    )

    COGNITO_CLIENT.admin_delete_user(UserPoolId=config_data.get(
        'COGNITO_USER_POOL_ID'), Username='point@xyz.com')

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_set_password_missing_uuid_negative(test_client):
    """
        TEST CASE: This is a negative test case. Returns a bad request if uuid field is missing in body.
    """

    expected_response = {
        'error': {
            'uuid': 'Enter valid UUID.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    body = {
        'password': '1234'
    }

    api_response = test_client.post(
        '/api/v1/members/set-password', json=body, content_type='application/json'
    )

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_set_password_missing_password_negative(test_client):
    """
        TEST CASE: This is a negative test case. Returns a bad request if password field is missing in body.
    """

    expected_response = {
        'error': {
            'password': 'Password is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    body = {
        'uuid': '11659bfa-d2c9-47e9-acc7-4bb8b94cddf4'
    }

    api_response = test_client.post(
        '/api/v1/members/set-password', json=body, content_type='application/json'
    )

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_set_password_user_exists_negative(test_client):
    """
        TEST CASE: This is a negative test case.
        Returns bad request if the user has already accepted the invite and has been added to the user table.
    """

    expected_response = {
        'message': 'User already exists',
        'status': True
    }

    body = {
        'uuid': '11659bfa-d2c9-47e9-acc7-4bb8b94cddf4',
        'password': '1234@Jklasd'
    }

    api_response = test_client.post(
        '/api/v1/members/set-password', json=body, content_type='application/json'
    )

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_set_password_wrong_uuid_negative(test_client):
    """
        TEST CASE: This is a negative test case.
        Returns bad request if the uuid i.e. the invite does not exist.
    """

    expected_response = {
        'message': 'Invite does not exist',
        'status': True
    }

    body = {
        'uuid': 'pppppp-59bfa-d2c9-47e9-acc7-4bb8b94cddf4',
        'password': '1234@Jklasd'
    }

    api_response = test_client.post(
        '/api/v1/members/set-password', json=body, content_type='application/json'
    )
    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_set_password_already_accepted_negative(test_client):
    """
        TEST CASE: This is a negative test case.
        Returns bad request if invite status is already accepted
    """

    expected_response = {
        'message': 'User already exists',
        'status': True
    }

    body = {
        'uuid': '239bfa-d2c9-47e9-acc7-4bb8b94cddf4',
        'password': '1234@Jklasd'
    }

    api_response = test_client.post(
        '/api/v1/members/set-password', json=body, content_type='application/json'
    )

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


# # ----------------test cases for members/get-users -------------------------
def test_get_user_positive(test_client):
    """
       TEST CASE: This is the first positive test for getting users data.
                  Return a sucessful response and the data if all required parameters are passed and the data for the conditions exist.
    """

    expected_response = {
        'data': {
            'objects': {
                'pagination_metadata': {
                    'current_page': 1,
                    'has_next_page': False,
                    'has_previous_page': False,
                    'next_page': None,
                    'page_size': 3,
                    'previous_page': None,
                    'total_items': 3,
                    'total_pages': 1
                }
            },
            'result': [
                {
                    'brand': [
                        'Cuticolor',
                        'Sterlomax'
                    ],
                    'email': 'gopro@gmail.com',
                    'first_name': 'go',
                    'invited_by': None,
                    'last_name': 'pro',
                    'status': 'ACTIVE'
                },
                {
                    'brand': [
                        'Cuticolor'
                    ],
                    'email': 'point@xyz.com',
                    'first_name': 'ricky',
                    'invited_by': 'testpurposephp@gmail.com',
                    'last_name': 'ponting',
                    'status': 'ACTIVE'
                },
                {
                    'brand': [
                        'Cuticolor',
                        'Sterlomax'
                    ],
                    'email': 'testpurposephp@gmail.com',
                    'first_name': 'john',
                    'invited_by': None,
                    'last_name': 'doe',
                    'status': 'ACTIVE'
                }
            ]
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    query_params = {
        'sort_key': 'email',
        'sort_order': 'asc',
        'page': 1,
        'size': 3
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/members/get-users', headers=headers,
        query_string=query_params)

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_get_user_second_positive(test_client):
    """
       TEST CASE: This is the first positive test for getting users data.
                  Return a sucessful response and the data if all required parameters are passed and the data for the conditions exist.
    """

    expected_response = {
        'data': {
            'objects': {
                'pagination_metadata': {
                    'current_page': 1,
                    'has_next_page': False,
                    'has_previous_page': False,
                    'next_page': None,
                    'page_size': 3,
                    'previous_page': None,
                    'total_items': 1,
                    'total_pages': 1
                }
            },
            'result': [
                {
                    'brand': [
                        'Cuticolor',
                        'Sterlomax'
                    ],
                    'email': 'testpurposephp@gmail.com',
                    'first_name': 'john',
                    'invited_by': None,
                    'last_name': 'doe',
                    'status': 'ACTIVE'
                }
            ]
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    query_params = {
        'q': 'john',
        'sort_key': 'email',
        'sort_order': 'asc',
        'page': 1,
        'size': 3
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/members/get-users', headers=headers,
        query_string=query_params)

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_get_user_missing_headers_negative(test_client):
    """
       TEST CASE: This is the first negative test for getting users data.
                  Return a failure response if request not authorized.
    """

    expected_response = {
        'message': 'Invalid Token.',
        'status': False
    }

    query_params = {
        'q': 'john',
        'sort_key': 'email',
        'sort_order': 'asc',
        'page': 1,
        'size': 3
    }

    api_response = test_client.get(
        '/api/v1/members/get-users',
        query_string=query_params)

    assert validate_status_code(
        expected=401, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


# # ----------------test cases for members/get-accounts -------------------------
def test_get_accounts_positive(test_client):
    """
       TEST CASE: This is the first positive test for getting account data.
                  Return a sucessful response and the data if all required parameters are passed and the data for the conditions exist.
    """

    expected_response = {
        'data': {
            'objects': {
                'pagination_metadata': {
                    'current_page': 1,
                    'has_next_page': False,
                    'has_previous_page': False,
                    'next_page': None,
                    'page_size': 10,
                    'previous_page': None,
                    'total_items': 1,
                    'total_pages': 1
                }
            },
            'result': [
                {
                    'account_id': 1,
                    'account_name': None
                }
            ]
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    query_params = {
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/members/get-accounts', headers=headers,
        query_string=query_params)

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_get_accounts_missing_headers_negative(test_client):
    """
       TEST CASE: This is the first negative test for getting accounts data.
                  Return a failure response if request not authorized.
    """

    expected_response = {'message': 'Invalid Token.', 'status': False}

    query_params = {
    }

    api_response = test_client.get(
        '/api/v1/members/get-accounts',
        query_string=query_params)

    assert validate_status_code(
        expected=401, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


# --------------- test cases for change-user-status ------------------#
def test_change_user_status_positive(test_client):
    """
        TEST CASE: This is a positive test case.
        Returns successful response if parameters are validated and user status changed successfully.
    """

    expected_response = {
        'message': 'User deactivated',
        'status': True
    }

    body = {
        'status': 'DEACTIVATE',
        'email': 'gopro@gmail.com'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.post(
        '/api/v1/members/change-user-status', json=body, headers=headers, content_type='application/json'
    )
    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_change_user_status_positive_second(test_client):
    """
        TEST CASE: This is second positive test case.
        Returns successful response if parameters are validated and user status changed successfully.
    """

    expected_response = {
        'message': 'User activated successfully',
        'status': True
    }

    body = {
        'status': 'ACTIVATE',
        'email': 'gopro@gmail.com'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.post(
        '/api/v1/members/change-user-status', json=body, headers=headers, content_type='application/json'
    )
    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_change_user_status_missing_email(test_client):
    """
        TEST CASE: This is first negative test case.
        Returns failure response if parameters are invalid.
    """

    expected_response = {
        'error': {
            'email': 'Email is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    body = {
        'status': 'ACTIVATE'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.post(
        '/api/v1/members/change-user-status', json=body, headers=headers, content_type='application/json'
    )
    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_change_user_status_missing_status(test_client):
    """
        TEST CASE: This is second negative test case.
        Returns failure response if parameters are invalid.
    """

    expected_response = {
        'error': {
            'status': 'Status is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    body = {
        'email': 'gopro@gmail.com'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.post(
        '/api/v1/members/change-user-status', json=body, headers=headers, content_type='application/json'
    )
    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_change_user_status_invalid_token(test_client):
    """
        TEST CASE: This is third negative test case.
        Returns failure response if request is unauthorized
    """

    expected_response = {
        'message': 'Invalid Token.',
        'status': False
    }

    body = {
        'status': 'ACTIVATE',
        'email': 'gopro@gmail.com'
    }

    headers = {}

    api_response = test_client.post(
        '/api/v1/members/change-user-status', json=body, headers=headers, content_type='application/json'
    )
    assert validate_status_code(
        expected=401, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)

    time.sleep(1)
# --------------- test cases for members/edit------------------#


def test_user_edit_positive_first(test_client):
    """
        TEST CASE: This is a positive test case.
        Returns successful response if parameters are validated and user status changed successfully.
    """

    expected_response = {
        'data': {
            'result': {
                'brand': [
                    'cosmoshine'
                ],
                'first_name': 'changed',
                'last_name': 'pro'
            }
        },
        'message': 'Details updated Successfully',
        'status': True
    }

    body = {
        'first_name': 'changed',
        'email': 'gopro@gmail.com',
        'brand': ['cosmoshine']
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.post(
        '/api/v1/members/edit', json=body, headers=headers, content_type='application/json'
    )
    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_user_edit_positive_second(test_client):
    """
        TEST CASE: This is second positive test case.
        Returns successful response if parameters are validated and user status changed successfully.
    """

    expected_response = {
        'data': {
            'result': {
                'brand': [
                    'cosmoshine'
                ],
                'first_name': 'hello',
                'last_name': 'moto'
            }
        },
        'message': 'Details updated Successfully',
        'status': True
    }

    body = {
        'first_name': 'hello',
        'last_name': 'moto',
        'email': 'gopro@gmail.com',
        'brand': ['cosmoshine']
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.post(
        '/api/v1/members/edit', json=body, headers=headers, content_type='application/json'
    )
    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_user_edit_missing_email(test_client):
    """
        TEST CASE: This is first negative test case.
        Returns failure response if email is missing from request parameter.
    """

    expected_response = {
        'error': {
            'email': 'Email is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    body = {
        'first_name': 'hello',
        'last_name': 'moto',
        'brand': ['cosmoshine']
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.post(
        '/api/v1/members/edit', json=body, headers=headers, content_type='application/json'
    )
    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_user_edit_missing_first_name(test_client):
    """
        TEST CASE: This is second negative test case.
        Returns failure response if first_name is missing from request parameter.
    """

    expected_response = {
        'error': {
            'first_name': 'First Name is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    body = {
        'email': 'gopro@gmail.com',
        'last_name': 'moto',
        'brand': ['cosmoshine']
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.post(
        '/api/v1/members/edit', json=body, headers=headers, content_type='application/json'
    )
    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_user_edit_missing_brand(test_client):
    """
        TEST CASE: This is third negative test case.
        Returns failure response if brand is missing from request parameter.
    """

    expected_response = {
        'error': {
            'brand': 'Brand is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    body = {
        'email': 'gopro@gmail.com',
        'first_name': 'hello',
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.post(
        '/api/v1/members/edit', json=body, headers=headers, content_type='application/json'
    )
    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_user_edit_bad_email(test_client):
    """
        TEST CASE: This is fourth negative test case.
        Returns failure response if brand is missing from request parameter.
    """

    expected_response = {'error': {'email': 'Email should be string value.'},
                         'message': 'Enter correct input.', 'status': False}

    body = {
        'email': 12,
        'first_name': 'hello',
        'brand': ['cosmoshine']
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.post(
        '/api/v1/members/edit', json=body, headers=headers, content_type='application/json'
    )
    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_user_edit_missing_header(test_client):
    """
        TEST CASE: This is fifth negative test case.
        Returns failure response if brand is missing from request parameter.
    """

    expected_response = {
        'message': 'Invalid Token.',
        'status': False
    }

    body = {
        'email': 'gopro@gmail.com',
        'first_name': 'hello',
        'brand': ['cosmoshine']
    }

    headers = {}

    api_response = test_client.post(
        '/api/v1/members/edit', json=body, headers=headers, content_type='application/json'
    )

    assert validate_status_code(
        expected=401, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)
