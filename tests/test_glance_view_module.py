"""test cases for member module apis"""
from datetime import datetime
from datetime import timedelta
import os

from app import db
from app.models.az_sales_traffic_asin import AzSalesTrafficAsin
from sqlalchemy import update
from tests.conftest import validate_response
from tests.conftest import validate_status_code

today_date = datetime.now().date()
three_days_early = today_date - timedelta(days=3)
today_date_str = today_date.strftime('%Y-%m-%d')
three_days_early_str = three_days_early.strftime('%Y-%m-%d')
fifty_days_early = today_date - timedelta(days=50)
fifty_days_early_str = fifty_days_early.strftime('%Y-%m-%d')


# -----------------------test for glance view planner-------------------#

def test_glance_view_positive_first(test_client):
    """
        TEST CASE: This is a positive test case. Return a successful response if fields are validated and invite sent successfully.
    """

    db.session.execute(update(AzSalesTrafficAsin).values(
        payload_date=fifty_days_early_str))

    expected_response = {
        'data': {
            'result': [
                {
                    'asp': '*',
                    'conversion': '*',
                    'month': '*',
                    'total_gvs': '*',
                    'total_sales': '*',
                    'units': '*'
                }
            ]
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    params = {
        'from_date': datetime.now().date() - timedelta(days=50),
        'to_date': datetime.now().date(),
        'marketplace': 'AMAZON'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/glance-view-planner', query_string=params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_glance_view_positive_second(test_client):
    """
        TEST CASE: This is a positive test case. Return a successful response if fields are validated and invite sent successfully.
    """

    expected_response = {
        'data': {
            'result': [
                {
                    'asp': '*',
                    'conversion': '*',
                    'month': '*',
                    'total_gvs': '*',
                    'total_sales': '*',
                    'units': '*'
                }
            ]
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    params = {
        'from_date': datetime.now().date() - timedelta(days=50),
        'to_date': datetime.now().date(),
        'marketplace': 'AMAZON',
        'brand': 'Sterlomax'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/glance-view-planner', query_string=params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_glance_view_positive_third(test_client):
    """
        TEST CASE: This is a positive test case.
        Return a failure response if from_date is not passed in request parameter.
    """

    expected_response = {
        'data': {
            'result': [
                {
                    'asp': '*',
                    'conversion': '*',
                    'month': '*',
                    'total_gvs': '*',
                    'total_sales': '*',
                    'units': '*'
                }
            ]
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    params = {
        'to_date': datetime.now().date(),
        'marketplace': 'AMAZON'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/glance-view-planner', query_string=params, headers=headers)
    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_glance_view_no_data_found(test_client):
    """
        TEST CASE: This is a negative test case.
        Return a failure response if from_date is not passed in request parameter.
    """

    expected_response = {'message': 'No data found.', 'status': False}

    params = {
        'from_date': datetime.now().date(),
        'marketplace': 'AMAZON'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/glance-view-planner', query_string=params, headers=headers)
    assert validate_status_code(
        expected=404, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_glance_view_missing_marketplace(test_client):
    """
        TEST CASE: This is a negative test case.
        Return a failure response if marketplace is not passed in request parameter.
    """

    expected_response = {'error': {'marketplace': 'Marketplace is required.'},
                         'message': 'Enter correct input.', 'status': False}

    params = {
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/glance-view-planner', query_string=params, headers=headers)
    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_glance_view_missing_headers(test_client):
    """
        TEST CASE: This is a negative test case.
        Return a failure response if request is not authorized.
    """

    expected_response = {'message': 'Invalid Token.', 'status': False}

    params = {
        'from_date': datetime.now().date(),
        'marketplace': 'AMAZON'
    }

    headers = {}

    api_response = test_client.get(
        '/api/v1/glance-view-planner', query_string=params, headers=headers)

    assert validate_status_code(
        expected=401, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)
