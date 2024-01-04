"""test cases for member module apis"""
import os

from tests.conftest import validate_response
from tests.conftest import validate_status_code

# ----------------------------test cases for refund insights---------------------------------#


def test_refund_insights_positive(test_client):
    """
        TEST CASE: This is a positive test case. Return a successful response if fields are validated and invite sent successfully.
    """

    expected_response = {
        'data': {
            'result': [
                {
                    'potential_refund': 255.0975,
                    'refundable_items': 1,
                    'report_date': '*',
                    'report_type': 'LOST',
                    'seller': 'B3D3499BOYNHX'
                }
            ]
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    params = {
        'marketplace': 'AMAZON'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/refund-insights', query_string=params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_refund_insights_missing_marketplace(test_client):
    """
        TEST CASE: This is a negative test case.
        Return a failure response if marketplace is missing from request parameters
    """

    expected_response = {
        'error': {
            'marketplace': 'Marketplace is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    params = {
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/refund-insights', query_string=params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_refund_insights_missing_headers(test_client):
    """
        TEST CASE: This is a negative test case.
        Return a failure response if headers is missing from request.
    """

    expected_response = {'message': 'Invalid Token.', 'status': False}

    params = {
    }

    headers = {}

    api_response = test_client.get(
        '/api/v1/refund-insights', query_string=params, headers=headers)

    assert validate_status_code(
        expected=401, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)
