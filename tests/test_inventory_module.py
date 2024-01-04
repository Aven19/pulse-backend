"""test cases for inventory module"""
from datetime import datetime
import os
import time

from tests.conftest import validate_response
from tests.conftest import validate_status_code

# ------------ get inventory---------------------------#


def test_get_inventory_positive_first(test_client):
    """
    TEST CASE: This is the first positive test case.
    It returns a successful response if request is valid and authorized.
    """

    expected_response = {
        'data': {
            'objects': {
                'from_date': '*',
                'pagination_metadata': {
                    'current_page': 1,
                    'has_next_page': True,
                    'has_previous_page': False,
                    'next_page': 2,
                    'page_size': 10,
                    'previous_page': None,
                    'total_items': 43,
                    'total_pages': 5
                },
                'stock_and_units_info': {
                }
            },
            'result': ['*']
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    query_params = {
        'from_date': datetime.now().date(),
        'to_date': datetime.now().date(),
        'marketplace': 'AMAZON'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/inventory',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_get_inventory_positive_second(test_client):
    """
    TEST CASE: This is the second positive test case.
    It returns a successful response if request is valid and authorized.
    """

    expected_response = {
        'data': {
            'objects': {
                'from_date': '*',
                'pagination_metadata': {
                    'current_page': 1,
                    'has_next_page': True,
                    'has_previous_page': False,
                    'next_page': 2,
                    'page_size': 10,
                    'previous_page': None,
                    'total_items': 15,
                    'total_pages': 2
                },
                'stock_and_units_info': {
                }
            },
            'result': ['*']
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    query_params = {
        'from_date': datetime.now().date(),
        'to_date': datetime.now().date(),
        'marketplace': 'AMAZON',
        'brand': 'Cuticolor'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/inventory',
        query_string=query_params, headers=headers)
    assert validate_status_code(
        expected=200, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_get_inventory_missing_from_date(test_client):
    """
    TEST CASE: This is the first negative test case.
    It returns a failure response if from_date is missing from request parameter
    """

    expected_response = {
        'error': {
            'from_date': 'From Date is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    query_params = {
        'to_date': datetime.now().date(),
        'marketplace': 'AMAZON'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/inventory',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_get_inventory_missing_to_date(test_client):
    """
    TEST CASE: This is the second negative test case.
    It returns a failure response if to_date is missing from request parameter
    """

    expected_response = {
        'error': {
            'to_date': 'To Date is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    query_params = {
        'from_date': datetime.now().date(),
        'marketplace': 'AMAZON'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/inventory',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_get_inventory_missing_marketplace(test_client):
    """
    TEST CASE: This is the third negative test case.
    It returns a failure response if marketplace is missing from request parameter
    """

    expected_response = {
        'error': {
            'marketplace': 'Marketplace is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    query_params = {
        'from_date': datetime.now().date(),
        'to_date': datetime.now().date()
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/inventory',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_get_inventory_missing_token(test_client):
    """
    TEST CASE: This is the fourth negative test case.
    It returns a failure response if marketplace is missing from request parameter
    """

    expected_response = {'message': 'Invalid Token.', 'status': False}

    query_params = {
        'from_date': datetime.now().date(),
        'to_date': datetime.now().date()
    }

    headers = {}

    api_response = test_client.get(
        '/api/v1/dashboard/inventory',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=401, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_get_inventory_no_data_found(test_client):
    """
    TEST CASE: This is the fifth negative test case.
    It returns a failure response if no data found as per the filters in the request parameter
    """

    expected_response = {'message': 'No data found.', 'status': False}

    query_params = {
        'from_date': datetime.now().date(),
        'to_date': datetime.now().date(),
        'marketplace': 'AMAZON',
        'brand': 'Pepsi'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/inventory',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=404, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)

    time.sleep(2)

# ------------ inventory product graph -----------------#


def test_get_inventory_graph_positive_first(test_client):
    """
    TEST CASE: This is the first positive test case.
    It returns a successful response if request is valid and authorized.
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
            'result': ['*']
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    query_params = {
        'from_date': datetime.now().date(),
        'to_date': datetime.now().date(),
        'marketplace': 'AMAZON',
        'product': 'B0BQWLZ5S1'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/inventory/product-graph',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_get_inventory_graph_positive_second(test_client):
    """
    TEST CASE: This is the second positive test case.
    It returns a successful response if request is valid and authorized.
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
            'result': ['*']
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    query_params = {
        'from_date': datetime.now().date(),
        'to_date': datetime.now().date(),
        'marketplace': 'AMAZON',
        'product': 'B0B3N612LH'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/inventory/product-graph',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_get_inventory_graph_positive_third(test_client):
    """
    TEST CASE: This is the third positive test case.
    It returns a successful response if request is valid and authorized.
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
            'result': ['*']
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    query_params = {
        'marketplace': 'AMAZON',
        'product': 'B0B3N612LH'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/inventory/product-graph',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_get_inventory_graph_missing_marketplace(test_client):
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
        'from_date': datetime.now().date(),
        'to_date': datetime.now().date(),
        'product': 'B0B3N612LH'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/inventory/product-graph',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_get_inventory_graph_missing_product(test_client):
    """
    TEST CASE: This is the second negative test case.
    It returns a failure response if product is missing from request parameter.
    """

    expected_response = {
        'error': {
            'product': 'Product is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    query_params = {
        'from_date': datetime.now().date(),
        'to_date': datetime.now().date(),
        'marketplace': 'AMAZON'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/inventory/product-graph',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


# ------------ fulfillment center map stats--------------#

def test_fulfillment_center_map_positive_first(test_client):
    """
    TEST CASE: This is the first positive test case.
    It returns a successful response if request is valid and authorized.
    """

    expected_response = {
        'data': {
            'objects': {
                'disposition_ratios_over_total_quantity': {
                    'in_transit': {
                        'percentage': '*',
                        'quantity': '*'
                    },
                    'sellable': {
                        'percentage': '*',
                        'quantity': '*'
                    },
                    'total_quantity': '*',
                    'unfulfillable': {
                        'percentage': '*',
                        'quantity': '*'
                    }
                },
                'zonal_statistics': ['*']
            },
            'result': ['*']
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    query_params = {
        'from_date': datetime.now().date(),
        'to_date': datetime.now().date(),
        'marketplace': 'AMAZON',
        'product': 'B0BQWLZ5S1'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/inventory/fulfillment-center-map-stats',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_fulfillment_center_map_positive_second(test_client):
    """
    TEST CASE: This is the second positive test case.
    It returns a successful response if request is valid and authorized.
    """

    expected_response = {
        'data': {
            'objects': {
                'disposition_ratios_over_total_quantity': {
                    'in_transit': {
                        'percentage': '*',
                        'quantity': '*'
                    },
                    'sellable': {
                        'percentage': '*',
                        'quantity': '*'
                    },
                    'total_quantity': '*',
                    'unfulfillable': {
                        'percentage': '*',
                        'quantity': '*'
                    }
                },
                'zonal_statistics': ['*']
            },
            'result': ['*']
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    query_params = {
        'from_date': datetime.now().date(),
        'to_date': datetime.now().date(),
        'marketplace': 'AMAZON',
        'product': 'B0B3N612LH'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/inventory/fulfillment-center-map-stats',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_fulfillment_center_map_missing_from_date(test_client):
    """
    TEST CASE: This is the first negative test case.
    It returns a failure response if from_date is missing from request parameter.
    """

    expected_response = {
        'error': {
            'from_date': 'From Date is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    query_params = {
        'to_date': datetime.now().date(),
        'marketplace': 'AMAZON',
        'product': 'B0B3N612LH'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/inventory/fulfillment-center-map-stats',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_fulfillment_center_map_missing_to_date(test_client):
    """
    TEST CASE: This is the second negative test case.
    It returns a failure response if to_date is missing from request parameter.
    """

    expected_response = {
        'error': {
            'to_date': 'To Date is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    query_params = {
        'from_date': datetime.now().date(),
        'marketplace': 'AMAZON',
        'product': 'B0B3N612LH'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/inventory/fulfillment-center-map-stats',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_fulfillment_center_map_missing_marketplace(test_client):
    """
    TEST CASE: This is the third negative test case.
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
        'to_date': datetime.now().date(),
        'from_date': datetime.now().date(),
        'product': 'B0B3N612LH'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/inventory/fulfillment-center-map-stats',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_fulfillment_center_map_missing_product(test_client):
    """
    TEST CASE: This is the fourth negative test case.
    It returns a failure response if product is missing from request parameter.
    """

    expected_response = {
        'error': {
            'product': 'Product is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    query_params = {
        'to_date': datetime.now().date(),
        'from_date': datetime.now().date(),
        'marketplace': 'AMAZON'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/inventory/fulfillment-center-map-stats',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_fulfillment_center_map_no_data(test_client):
    """
    TEST CASE: This is the fifth negative test case.
    It returns a failure response if no data found for provided filters.
    """

    expected_response = {'message': 'No data found.', 'status': False}

    query_params = {
        'to_date': datetime.now().date(),
        'from_date': datetime.now().date(),
        'marketplace': 'AMAZON',
        'product': 'ABCDEFG'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/inventory/fulfillment-center-map-stats',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=404, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_fulfillment_center_map_no_data_second(test_client):
    """
    TEST CASE: This is the sixth negative test case.
    It returns a failure response if no data found for provided filters.
    """

    expected_response = {'message': 'No data found.', 'status': False}

    query_params = {
        'to_date': '2021-06-01',
        'from_date': '2021-01-01',
        'marketplace': 'AMAZON',
        'product': 'B0B3N612LH'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/inventory/fulfillment-center-map-stats',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=404, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)

    time.sleep(1)

# ------------- update cogs------------------#


def test_update_cogs_positive_first(test_client):
    """
    TEST CASE: This is the first positive test case.
    It returns a successful response if request is valid and authorized.
    """

    expected_response = {
        'message': 'Details updated Successfully',
        'status': True
    }

    body = {
        'sku': 'Cuti_black_2',
        'cogs': '200'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.post(
        '/api/v1/dashboard/inventory/update-cogs',
        json=body, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_update_cogs_positive_second(test_client):
    """
    TEST CASE: This is the second positive test case.
    It returns a successful response if request is valid and authorized.
    """

    expected_response = {
        'message': 'Details updated Successfully',
        'status': True
    }

    body = {
        'sku': 'Cuti\\Brown',
        'cogs': '999'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.post(
        '/api/v1/dashboard/inventory/update-cogs',
        json=body, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_update_cogs_missing_sku(test_client):
    """
    TEST CASE: This is the first negative test case.
    It returns a negative response if sku is missing from request body.
    """

    expected_response = {
        'error': {
            'sku': 'Sku is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    body = {
        'cogs': '999'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.post(
        '/api/v1/dashboard/inventory/update-cogs',
        json=body, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_update_cogs_missing_cogs(test_client):
    """
    TEST CASE: This is the second negative test case.
    It returns a negative response if cogs is missing from request body.
    """

    expected_response = {
        'error': {
            'cogs': 'Cogs is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    body = {
        'sku': 'Cuti\\Brown'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.post(
        '/api/v1/dashboard/inventory/update-cogs',
        json=body, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_update_cogs_missing_body(test_client):
    """
    TEST CASE: This is the third negative test case.
    It returns a negative response if cogs is missing from request body.
    """

    expected_response = {
        'error': {
            'cogs': 'Cogs is required.',
            'sku': 'Sku is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    body = {
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.post(
        '/api/v1/dashboard/inventory/update-cogs',
        json=body, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_update_cogs_no_data_found(test_client):
    """
    TEST CASE: This is the fourth negative test case.
    It returns a negative response if given sku does not exist.
    """

    expected_response = {'message': 'SKU does not exist', 'status': False}

    body = {
        'sku': 'ABCD',
        'cogs': '999'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.post(
        '/api/v1/dashboard/inventory/update-cogs',
        json=body, headers=headers)

    assert validate_status_code(
        expected=404, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


# ----------------- inventory product attributes---------------#

# def test_product_attribute_positive_first(test_client):
#     """
#     TEST CASE: This is the first positive test case.
#     It returns a successful response if request is valid and authorized.
#     """

#     expected_response = {}

#     query_params = {
#         'marketplace': 'AMAZON'
#     }

#     headers = {
#         'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

#     api_response = test_client.get(
#         '/api/v1/dashboard/inventory/B0B3NDKBMQ/attributes',
#         query_string=query_params, headers=headers)

#     print("api_response---",api_response.json)
#     assert validate_status_code(
#         expected=400, received=api_response.status_code)

#     assert validate_response(
#         expected=expected_response, received=api_response.json)
