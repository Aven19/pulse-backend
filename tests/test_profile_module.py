"""test cases for member module apis"""
import os
import time

from tests.conftest import validate_response
from tests.conftest import validate_status_code
from werkzeug.datastructures import FileStorage


# -------------------test cases for /profile/add-update---------------------#

def test_add_update_profile_positive_first(test_client):
    """
        TEST CASE: This is a positive test case. Return a successful response if fields are validated and invite sent successfully.
    """

    expected_response = {
        'data': {
            'result': {
                'address': '14-b, grove street',
                'city': 'san andreas',
                'country': 'Pandora',
                'file_attachment_id': 1,
                'file_name': '*',
                'file_path': 'media/200/70/',
                'file_size': 6022,
                'legal_name': 'test_legal_name',
                'phone': '1234567890',
                'profile_photo': '',
                'state': 'LA',
                'zip_code': None
            }
        },
        'message': 'Details updated Successfully',
        'status': True
    }

    data = {
        'legal_name': 'test_legal_name',
        'phone': '1234567890',
        'address': '14-b, grove street',
        'country': 'Pandora',
        'state': 'LA',
        'city': 'san andreas',
        'zipcode': '989009'
    }

    data['profile_photo'] = FileStorage(
        stream=open(file='tests/assets/avatar.jpeg', mode='rb'),
        filename='avatar.jpeg',
        content_type='image/*',
    )

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.post(
        '/api/v1/profile/add-update', data=data, headers=headers, content_type='multipart/form-data')

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_add_update_profile_positive_second(test_client):
    """
        TEST CASE: This is a positive test case. Return a successful response if fields are validated and invite sent successfully.
    """

    expected_response = {
        'data': {
            'result': {
                'address': '14-b, grove street',
                'city': '',
                'country': 'India',
                'file_attachment_id': 2,
                'file_name': '*',
                'file_path': 'media/200/70/',
                'file_size': 6022,
                'legal_name': 'test_legal_name',
                'phone': '4564564566',
                'profile_photo': '',
                'state': 'Delhi',
                'zip_code': None
            }
        },
        'message': 'Details updated Successfully',
        'status': True
    }

    data = {
        'legal_name': 'test_legal_name',
        'phone': '4564564566',
        'address': '14-b, grove street',
        'country': 'India',
        'state': 'Delhi',
        'city': '',
        'zipcode': '989009'
    }

    data['profile_photo'] = FileStorage(
        stream=open(file='tests/assets/avatar.jpeg', mode='rb'),
        filename='avatar.jpeg',
        content_type='image/*',
    )

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.post(
        '/api/v1/profile/add-update', data=data, headers=headers, content_type='multipart/form-data')
    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_profile_missing_legal_name(test_client):
    """
        TEST CASE: This is a negative test case.
        Return a failure response if legal_name is missing in form data
    """

    expected_response = {
        'error': {
            'legal_name': 'Legal Name is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    data = {
        'phone': '4564564566',
        'address': '14-b, grove street',
        'country': 'India',
        'state': 'Delhi',
        'city': '',
        'zipcode': '989009'
    }

    data['profile_photo'] = FileStorage(
        stream=open(file='tests/assets/avatar.jpeg', mode='rb'),
        filename='avatar.jpeg',
        content_type='image/*',
    )

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.post(
        '/api/v1/profile/add-update', data=data, headers=headers, content_type='multipart/form-data')

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_profile_missing_headers(test_client):
    """
        TEST CASE: This is a negative test case.
        Return a failure response if legal_name is missing in form data
    """

    expected_response = {
        'message': 'Invalid Token.',
        'status': False
    }

    data = {
        'legal_name': 'test_legal_name',
        'phone': '4564564566',
        'address': '14-b, grove street',
        'country': 'India',
        'state': 'Delhi',
        'city': '',
        'zipcode': '989009'
    }

    data['profile_photo'] = FileStorage(
        stream=open(file='tests/assets/avatar.jpeg', mode='rb'),
        filename='avatar.jpeg',
        content_type='image/*',
    )

    headers = {}

    api_response = test_client.post(
        '/api/v1/profile/add-update', data=data, headers=headers, content_type='multipart/form-data')

    assert validate_status_code(
        expected=401, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


# -------------------test cases for /profile/get---------------------#

def test_get_profile_positive(test_client):
    """
        TEST CASE: This is a positive test case. Return a successful response if fields are validated and invite sent successfully.
    """

    expected_response = {
        'data': {
            'address': '14-b, grove street',
            'city': '',
            'country': 'India',
            'file_attachment_id': 2,
            'file_name': '*',
            'file_path': 'media/200/70/',
            'file_size': 6022,
            'legal_name': 'test_legal_name',
            'phone': '4564564566',
            'profile_photo': '',
            'state': 'Delhi',
            'zip_code': None
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    params = {}

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/profile/get', query_string=params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_get_profile_missing_headers(test_client):
    """
        TEST CASE: This is a negative test case.
        Return a failure response if request is not authorized.
    """

    expected_response = {
        'message': 'Invalid Token.',
        'status': False
    }

    params = {}

    headers = {}

    api_response = test_client.get(
        '/api/v1/profile/get', query_string=params, headers=headers)

    assert validate_status_code(
        expected=401, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)

    time.sleep(1)
