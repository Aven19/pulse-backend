"""test cases for queue-task module apis"""
import json
import os

from app import db
from app.models.queue_task import QueueTask
from tests.conftest import validate_response
from tests.conftest import validate_status_code


# ------------------------- test case for queue-task-----------------------------#

def test_queue_task_positive_first(test_client):
    """
        TEST CASE: This is a positive test case. Return a successful response if fields are validated and invite sent successfully.
    """

    with open('tests/assets/queue_task.json', 'r') as queue_task_file:             # type: ignore  # noqa: FKA100
        queue_task = json.load(queue_task_file)
        db.session.bulk_insert_mappings(QueueTask, queue_task)                          # type: ignore  # noqa: FKA100
        db.session.commit()

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
            'result': [
                {
                    'created_at': '*',
                    'entity_type': 'Finance Event List',
                    'input_file': '*',
                    'output_file': '*',
                    'param': {
                        'account_id': '7e36ba8f-073a-4dee-9ba5-d7880c3c3ba9',
                        'end_datetime': '*',
                        'entity_type': 'Finance Event List',
                        'job_id': '*',
                        'max_results_per_page': '*',
                        'queue_name': 'EXPORT_CSV',
                        'start_datetime': '*',
                        'status': 'New',
                        'user_id': '*'
                    },
                    'queue_name': 'EXPORT_CSV',
                    'status': 'Completed',
                    'user': 'john'
                },
                {
                    'created_at': '*',
                    'entity_type': 'Finance Event List',
                    'input_file': '*',
                    'output_file': '*',
                    'param': {
                        'account_id': '7e36ba8f-073a-4dee-9ba5-d7880c3c3ba9',
                        'end_datetime': '*',
                        'entity_type': 'Finance Event List',
                        'job_id': '*',
                        'max_results_per_page': '*',
                        'queue_name': 'EXPORT_CSV',
                        'start_datetime': '*',
                        'status': 'New',
                        'user_id': '*'
                    },
                    'queue_name': 'EXPORT_CSV',
                    'status': 'Completed',
                    'user': 'john'
                }
            ]
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    params = {
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/queue-task', query_string=params, headers=headers)
    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_queue_task_positive_second(test_client):
    """
        TEST CASE: This is a positive test case. Return a successful response if fields are validated and invite sent successfully.
    """

    expected_response = {
        'data': {
            'objects': {
                'pagination_metadata': {
                    'current_page': '*',
                    'has_next_page': '*',
                    'has_previous_page': '*',
                    'next_page': '*',
                    'page_size': '*',
                    'previous_page': '*',
                    'total_items': '*',
                    'total_pages': '*'
                }
            },
            'result': [
                {
                    'created_at': '*',
                    'entity_type': 'Order Report',
                    'input_file': '*',
                    'output_file': '*',
                    'param': {
                        'account_id': '7e36ba8f-073a-4dee-9ba5-d7880c3c3ba9',
                        'entity_type': 'Order Report',
                        'job_id': '*',
                        'queue_name': 'ORDER_REPORT',
                        'reference_id': '316467019657',
                        'seller_partner_id': 'A3D3499BOYNHU',
                        'status': 'New',
                        'user_id': '*'
                    },
                    'queue_name': 'ORDER_REPORT',
                    'status': 'Completed',
                    'user': 'john'
                },
                {
                    'created_at': '*',
                    'entity_type': 'Order Report',
                    'input_file': '*',
                    'output_file': '*',
                    'param': {
                        'account_id': '7e36ba8f-073a-4dee-9ba5-d7880c3c3ba9',
                        'entity_type': 'Order Report',
                        'job_id': '*',
                        'queue_name': 'ORDER_REPORT',
                        'reference_id': '316461019657',
                        'seller_partner_id': 'A3D3499BOYNHU',
                        'status': 'New',
                        'user_id': '*'
                    },
                    'queue_name': 'ORDER_REPORT',
                    'status': 'Completed',
                    'user': 'john'
                }
            ]
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    params = {
        'q': 'order',
        'size': 2
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/queue-task', query_string=params, headers=headers)
    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_queue_task_no_data_found(test_client):
    """
        TEST CASE: This is a negative test case. Return a failure response if no data found.
    """

    expected_response = {'message': 'No data found.', 'status': False}

    params = {
        'q': 'finance',
        'size': 2
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/queue-task', query_string=params, headers=headers)

    assert validate_status_code(
        expected=404, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_queue_task_missing_header(test_client):
    """
        TEST CASE: This is a negative test case. Return a failure response if request is not authorized.
    """

    expected_response = {
        'message': 'Invalid Token.',
        'status': False
    }

    params = {
        'q': 'finance',
        'size': 2
    }

    headers = {
    }

    api_response = test_client.get(
        '/api/v1/queue-task', query_string=params, headers=headers)

    assert validate_status_code(
        expected=401, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


# --------------------------- test case for queue-task/status-list-----------------------------#

def test_queuetask_status_list_positive(test_client):
    """
        TEST CASE: This is a positive test case. Return a successful response if fields are validated and invite sent successfully.
    """

    expected_response = {
        'data': {
            'result': [
                {
                    'id': 10,
                    'name': 'New'
                },
                {
                    'id': 20,
                    'name': 'Running'
                },
                {
                    'id': 30,
                    'name': 'Error'
                },
                {
                    'id': 40,
                    'name': 'Completed'
                }
            ]
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    params = {
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/queue-task/status-list', query_string=params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_queuetask_status_list_negative(test_client):
    """
        TEST CASE: This is a negative test case. Return a failure response request is not authorized.
    """

    expected_response = {
        'message': 'Invalid Token.',
        'status': False
    }
    params = {
    }

    headers = {}

    api_response = test_client.get(
        '/api/v1/queue-task/status-list', query_string=params, headers=headers)

    assert validate_status_code(
        expected=401, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)
