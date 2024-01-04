import os
import time

import pytest
from tests.conftest import validate_response
from tests.conftest import validate_status_code


@pytest.mark.order(1)
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
                    'asp_id_connected_at': None,
                    'asp_marketplace': None,
                    'az_ads_account_name': None,
                    'az_ads_profile_id': [

                    ],
                    'display_name': None,
                    'is_primary': True,
                    'legal_name': None,
                    'profile_attachment_id': None,
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

    os.environ['x_authorization'] = api_response.json.get(
        'data').get('objects').get('user').get('auth_token')
    os.environ['x_account'] = '7e36ba8f-073a-4dee-9ba5-d7880c3c3ba9'

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


# -------------------------- test cases for /category---------------------------#
def test_category_positive_first(test_client):
    """
    TEST CASE: This is the first positive test case. It returns a positive response if request is valid and authorized.
    """

    expected_response = {
        'data': {
            'objects': {
                'pagination_metadata': {
                    'current_page': '*',
                    'has_next_page': False,
                    'has_previous_page': False,
                    'next_page': None,
                    'page_size': '*',
                    'previous_page': None,
                    'total_items': '*',
                    'total_pages': '*'
                }
            },
            'result': ['*'
                       ]
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    query_params = {
        'marketplace': 'AMAZON'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/category',
        query_string=query_params, headers=headers)
    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_category_positive_second(test_client):
    """
    TEST CASE: This is the second positive test case. It returns a positive response if request is valid and authorized.
    """

    expected_response = {
        'data': {
            'objects': {
                'pagination_metadata': {
                    'current_page': '*',
                    'has_next_page': False,
                    'has_previous_page': False,
                    'next_page': None,
                    'page_size': '*',
                    'previous_page': None,
                    'total_items': '*',
                    'total_pages': '*'
                }
            },
            'result': ['*'
                       ]
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    query_params = {
        'marketplace': 'AMAZON',
        'q': 'beauty'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/category',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_category_missing_marketplace(test_client):
    """
    TEST CASE: This is the first negative test case.
    It returns a failure response if marketplace is missing from request parameter.
    """

    expected_response = {
        'error': {
            'marketplace': 'Marketplace is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    query_params = {
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/category',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_category_no_data_found(test_client):
    """
    TEST CASE: This is the second negative test case.
    It returns a failure response if search query does not match any result.
    """

    expected_response = {'message': 'No data found.', 'status': True}

    query_params = {
        'marketplace': 'AMAZON',
        'q': 'godzilla'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/category',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=404, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


# -------------------------- test cases for /brand---------------------------#

def test_brand_positive_first(test_client):
    """
    TEST CASE: This is the first positive test case. It returns a positive response if request is valid and authorized.
    """

    expected_response = {
        'data': {
            'objects': {
                'pagination_metadata': {
                    'current_page': '*',
                    'has_next_page': False,
                    'has_previous_page': False,
                    'next_page': None,
                    'page_size': '*',
                    'previous_page': None,
                    'total_items': '*',
                    'total_pages': '*'
                }
            },
            'result': ['*'
                       ]
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    query_params = {
        'marketplace': 'AMAZON'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/brand',
        query_string=query_params, headers=headers)
    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_brand_positive_second(test_client):
    """
    TEST CASE: This is the second positive test case. It returns a positive response if request is valid and authorized.
    """

    expected_response = {
        'data': {
            'objects': {
                'pagination_metadata': {
                    'current_page': '*',
                    'has_next_page': False,
                    'has_previous_page': False,
                    'next_page': None,
                    'page_size': '*',
                    'previous_page': None,
                    'total_items': '*',
                    'total_pages': '*'
                }
            },
            'result': ['*'
                       ]
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    query_params = {
        'marketplace': 'AMAZON',
        'q': 'cuti'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/brand',
        query_string=query_params, headers=headers)
    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_brand_missing_marketplace(test_client):
    """
    TEST CASE: This is the first negative test case.
    It returns a failure response if marketplace is missing from request parameter.
    """

    expected_response = {
        'error': {
            'marketplace': 'Marketplace is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    query_params = {
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/brand',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_brand_no_data_found(test_client):
    """
    TEST CASE: This is the second negative test case.
    It returns a failure response if search query does not match any result.
    """

    expected_response = {'message': 'No data found.', 'status': True}

    query_params = {
        'marketplace': 'AMAZON',
        'q': 'godzilla'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/brand',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=404, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


# -------------------------- test cases for /product---------------------------#

def test_product_positive_first(test_client):
    """
    TEST CASE: This is the first positive test case. It returns a positive response if request is valid and authorized.
    """

    expected_response = {
        'data': {
            'objects': {
                'pagination_metadata': {
                    'current_page': '*',
                    'has_next_page': True,
                    'has_previous_page': False,
                    'next_page': '*',
                    'page_size': '*',
                    'previous_page': None,
                    'total_items': '*',
                    'total_pages': '*'
                }
            },
            'result': ['*'
                       ]
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    query_params = {
        'marketplace': 'AMAZON'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/product',
        query_string=query_params, headers=headers)
    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_product_positive_second(test_client):
    """
    TEST CASE: This is the second positive test case. It returns a positive response if request is valid and authorized.
    """

    expected_response = {
        'data': {
            'objects': {
                'pagination_metadata': {
                    'current_page': '*',
                    'has_next_page': True,
                    'has_previous_page': False,
                    'next_page': '*',
                    'page_size': '*',
                    'previous_page': None,
                    'total_items': '*',
                    'total_pages': '*'
                }
            },
            'result': ['*'
                       ]
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    query_params = {
        'marketplace': 'AMAZON',
        'q': 'cuti'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/product',
        query_string=query_params, headers=headers)
    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_product_missing_marketplace(test_client):
    """
    TEST CASE: This is the first negative test case.
    It returns a failure response if marketplace is missing from request parameter.
    """

    expected_response = {
        'error': {
            'marketplace': 'Marketplace is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    query_params = {
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/product',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_product_no_data_found(test_client):
    """
    TEST CASE: This is the second negative test case.
    It returns a failure response if search query does not match any result.
    """

    expected_response = {'message': 'No data found.', 'status': True}

    query_params = {
        'marketplace': 'AMAZON',
        'q': 'godzilla'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/product',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=404, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)

    time.sleep(1)
