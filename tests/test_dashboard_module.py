from datetime import datetime
from datetime import timedelta
import os
import time

from tests.conftest import validate_response
from tests.conftest import validate_status_code


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


def test_sales_statistics_positive_first(test_client):
    """
    TEST CASE: This is the first positive test case. It returns a successful response.
    """

    expected_response = {
        'data': {
            'result': {
                'aov': {
                    'aov_difference': '*',
                    'aov_percentage_growth': '*',
                    'current_aov': '*',
                    'prior_aov': '*'
                },
                'gross_sales': {
                    'current_gross_sales': '*',
                    'gross_sales_difference': '*',
                    'gross_sales_percentage_growth': '*',
                    'prior_gross_sales': '*'
                },
                'total_orders': {
                    'current_total_orders': '*',
                    'prior_total_orders': '*',
                    'total_orders_difference': '*',
                    'total_orders_percentage_growth': '*'
                },
                'units_sold': {
                    'current_units_sold': '*',
                    'prior_units_sold': '*',
                    'units_sold_difference': '*',
                    'units_sold_percentage_growth': '*'
                }
            }
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    query_params = {
    }

    api_response = test_client.get(
        '/api/v1/dashboard/sales-statistics',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_sales_statistics_positive_second(test_client):
    """
    TEST CASE: This is the second positive test case. It returns a successful response.
    """

    expected_response = {
        'data': {
            'result': {
                'aov': {
                    'aov_difference': '*',
                    'aov_percentage_growth': '*',
                    'current_aov': '*',
                    'prior_aov': '*'
                },
                'gross_sales': {
                    'current_gross_sales': '*',
                    'gross_sales_difference': '*',
                    'gross_sales_percentage_growth': '*',
                    'prior_gross_sales': '*'
                },
                'total_orders': {
                    'current_total_orders': '*',
                    'prior_total_orders': '*',
                    'total_orders_difference': '*',
                    'total_orders_percentage_growth': '*'
                },
                'units_sold': {
                    'current_units_sold': '*',
                    'prior_units_sold': '*',
                    'units_sold_difference': '*',
                    'units_sold_percentage_growth': '*'
                }
            }
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    query_params = {
        'from_date': datetime.now().date() - timedelta(days=1),
        'to_date': datetime.now().date()
    }

    api_response = test_client.get(
        '/api/v1/dashboard/sales-statistics',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_sales_statistics_negative(test_client):
    """
    TEST CASE: This is the first negative test case. It returns a failure response.
    """

    expected_response = {'message': 'Invalid Token.', 'status': False}

    headers = {}

    query_params = {
        'from_date': datetime.now().date() - timedelta(days=2),
        'to_date': datetime.now().date()
    }

    api_response = test_client.get(
        '/api/v1/dashboard/sales-statistics',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=401, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


# ----------------------- sales statistics bar graph-------------------#

def test_sales_statistics_graph_positive(test_client):
    """
    TEST CASE: This is the first negative test case. It returns a successful response.
    """

    expected_response = {
        'data': {
            'result': {
                'graph': [
                    {
                        'date': '*',
                        'gross_sales': '*',
                        'units_sold': '*'
                    },
                    {
                        'date': '*',
                        'gross_sales': '*',
                        'units_sold': '*'
                    }
                ],
                'gross_sales': {
                    'current': '*',
                    'percentage_growth': '*',
                    'previous_year_percentage_growth': None
                },
                'units_sold': {
                    'current': '*',
                    'percentage_growth': '*',
                    'previous_year_percentage_growth': None
                }
            }
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    query_params = {
        'from_date': datetime.now().date() - timedelta(days=4),
        'to_date': datetime.now().date() + timedelta(days=6),
        'marketplace': 'AMAZON'
    }

    api_response = test_client.get(
        '/api/v1/dashboard/sales-statistics-bar-graph',
        query_string=query_params, headers=headers)
    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_sales_statistics_graph_positive_second(test_client):
    """
    TEST CASE: This is the second positive test case. It returns a successful response.
    """

    expected_response = {
        'data': {
            'result': {
                'graph': [
                    {
                        'date': '*',
                        'gross_sales': '*',
                        'units_sold': '*'
                    },
                    {
                        'date': '*',
                        'gross_sales': '*',
                        'units_sold': '*'
                    }
                ],
                'gross_sales': {
                    'current': '*',
                    'percentage_growth': '*',
                    'previous_year_percentage_growth': None
                },
                'units_sold': {
                    'current': '*',
                    'percentage_growth': '*',
                    'previous_year_percentage_growth': None
                }
            }
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    query_params = {
        'from_date': datetime.now().date() - timedelta(days=4),
        'to_date': datetime.now().date() + timedelta(days=6),
        'marketplace': 'AMAZON',
        'brand': 'Sterlomax'
    }

    api_response = test_client.get(
        '/api/v1/dashboard/sales-statistics-bar-graph',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_sales_statistics_graph_negative_first(test_client):
    """
    TEST CASE: This is the first negative test case. It returns a negative response if headers are missing and request is unauthorized.
    """

    expected_response = {'message': 'Invalid Token.', 'status': False}

    headers = {}

    query_params = {
        'from_date': datetime.now().date() - timedelta(days=4),
        'to_date': datetime.now().date() + timedelta(days=6),
        'marketplace': 'AMAZON'
    }

    api_response = test_client.get(
        '/api/v1/dashboard/sales-statistics-bar-graph',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=401, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_sales_statistics_graph_negative_second(test_client):
    """
    TEST CASE: This is the first negative test case. It returns a negative response if from_date is missing from request.
    """

    expected_response = {
        'error': {'from_date': 'From Date is required.'},
        'message': 'Enter correct input.', 'status': False
    }
    headers = {}

    query_params = {
        'to_date': datetime.now().date() + timedelta(days=6),
        'marketplace': 'AMAZON'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/sales-statistics-bar-graph',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_sales_statistics_graph_negative_third(test_client):
    """
    TEST CASE: This is the third negative test case. It returns a negative response if to_date is missing from request.
    """

    expected_response = {
        'error': {'to_date': 'To Date is required.'},
        'message': 'Enter correct input.', 'status': False
    }
    headers = {}

    query_params = {
        'from_date': datetime.now().date() + timedelta(days=6),
        'marketplace': 'AMAZON'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/sales-statistics-bar-graph',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


# ---------------- sales statistics hourly graph --------------------#

def test_hourly_graph_positive_first(test_client):
    """
    TEST CASE: This is the first positive test case. It returns a positive response if request is valid and authenticated.
    """

    expected_response = {
        'data': {
            'objects': {
                'date': '*',
                'hourly_sales': ['*'
                                 ],
                'pagination_metadata': {
                    'current_page': '*',
                    'has_next_page': False,
                    'has_previous_page': False,
                    'next_page': None,
                    'page_size': '*',
                    'previous_page': None,
                    'total_items': '*',
                    'total_pages': '*'
                },
                'top_selling_products': [
                    {
                        'asin': 'B0BQWLZ5S1',
                        'gross_sales': '3555.00',
                        'item_image': 'https://m.media-amazon.com/images/I/71PRszLrIoL.jpg',
                        'product_name': 'Cuticolor Hair Coloring Cream 120gm (New Pack) No PPD, No Ammonia (Black)',
                        'sku': 'Cuti|Black',
                        'units_sold': 3
                    },
                    {
                        'asin': 'B074SL2PHW',
                        'gross_sales': '1300.00',
                        'item_image': 'https://m.media-amazon.com/images/I/71GFaYuAE6L.jpg',
                        'product_name': 'Cuticolor Permanent Hair Color Cream (Black)120gm',
                        'sku': 'VZ-6F58-V5RY',
                        'units_sold': 1
                    },
                    {
                        'asin': 'B087N21PH1',
                        'gross_sales': '429.00',
                        'item_image': 'https://m.media-amazon.com/images/I/71-cdzGQGQL.jpg',
                        'product_name': 'SterloMax 80% Ethanol-based Hand Rub Sanitizer and Disinfectant Liquid Alcohol, 500 ml -Pack of 2',
                        'sku': 'P1-FWLF-D2ZN',
                        'units_sold': 1
                    },
                    {
                        'asin': 'B07ZVHHRQ3',
                        'gross_sales': '0.00',
                        'item_image': 'https://m.media-amazon.com/images/I/71WxrT0QNeL.jpg',
                        'product_name': 'Cuticolor Hair Coloring Cream, Hair Color, 60g + 60g - Dark Brown (Pack of 1)',
                        'sku': '0Y-116Y-MEEN',
                        'units_sold': 0
                    }
                ]
            },
            'result': [
                {
                    'asin': 'B0BQWLZ5S1',
                    'item_image': 'https://m.media-amazon.com/images/I/71PRszLrIoL.jpg',
                    'price': '2370.00',
                    'product_name': 'Cuticolor Hair Coloring Cream 120gm (New Pack) No PPD, No Ammonia (Black)',
                    'quantity': 2,
                    'sku': 'Cuti|Black',
                    'timing': '*'
                },
                {
                    'asin': 'B074SL2PHW',
                    'item_image': 'https://m.media-amazon.com/images/I/71GFaYuAE6L.jpg',
                    'price': '1300.00',
                    'product_name': 'Cuticolor Permanent Hair Color Cream (Black)120gm',
                    'quantity': 1,
                    'sku': 'VZ-6F58-V5RY',
                    'timing': '*'
                },
                {
                    'asin': 'B07ZVHHRQ3',
                    'item_image': 'https://m.media-amazon.com/images/I/71WxrT0QNeL.jpg',
                    'price': '0.00',
                    'product_name': 'Cuticolor Hair Coloring Cream, Hair Color, 60g + 60g - Dark Brown (Pack of 1)',
                    'quantity': 0,
                    'sku': '0Y-116Y-MEEN',
                    'timing': '*'
                },
                {
                    'asin': 'B087N21PH1',
                    'item_image': 'https://m.media-amazon.com/images/I/71-cdzGQGQL.jpg',
                    'price': '429.00',
                    'product_name': 'SterloMax 80% Ethanol-based Hand Rub Sanitizer and Disinfectant Liquid Alcohol, 500 ml -Pack of 2',
                    'quantity': 1,
                    'sku': 'P1-FWLF-D2ZN',
                    'timing': '*'
                },
                {
                    'asin': 'B0BQWLZ5S1',
                    'item_image': 'https://m.media-amazon.com/images/I/71PRszLrIoL.jpg',
                    'price': '1185.00',
                    'product_name': 'Cuticolor Hair Coloring Cream 120gm (New Pack) No PPD, No Ammonia (Black)',
                    'quantity': 1,
                    'sku': 'Cuti|Black',
                    'timing': '*'
                }
            ]
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
        '/api/v1/dashboard/sales-statistics/hourly-graph',
        query_string=query_params, headers=headers)
    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_hourly_graph_positive_second(test_client):
    """
    TEST CASE: This is the second positive test case. It returns a positive response if request is valid and authenticated.
    """

    expected_response = {
        'data': {
            'objects': {
                'date': '*',
                'hourly_sales': ['*'
                                 ],
                'pagination_metadata': {
                    'current_page': '*',
                    'has_next_page': False,
                    'has_previous_page': False,
                    'next_page': None,
                    'page_size': '*',
                    'previous_page': None,
                    'total_items': '*',
                    'total_pages': '*'
                },
                'top_selling_products': [
                    {
                        'asin': 'B0BQWLZ5S1',
                        'gross_sales': '3555.00',
                        'item_image': 'https://m.media-amazon.com/images/I/71PRszLrIoL.jpg',
                        'product_name': 'Cuticolor Hair Coloring Cream 120gm (New Pack) No PPD, No Ammonia (Black)',
                        'sku': 'Cuti|Black',
                        'units_sold': 3
                    },
                    {
                        'asin': 'B074SL2PHW',
                        'gross_sales': '1300.00',
                        'item_image': 'https://m.media-amazon.com/images/I/71GFaYuAE6L.jpg',
                        'product_name': 'Cuticolor Permanent Hair Color Cream (Black)120gm',
                        'sku': 'VZ-6F58-V5RY',
                        'units_sold': 1
                    },
                    {
                        'asin': 'B07ZVHHRQ3',
                        'gross_sales': '0.00',
                        'item_image': 'https://m.media-amazon.com/images/I/71WxrT0QNeL.jpg',
                        'product_name': 'Cuticolor Hair Coloring Cream, Hair Color, 60g + 60g - Dark Brown (Pack of 1)',
                        'sku': '0Y-116Y-MEEN',
                        'units_sold': 0
                    }
                ]
            },
            'result': [
                {
                    'asin': 'B0BQWLZ5S1',
                    'item_image': 'https://m.media-amazon.com/images/I/71PRszLrIoL.jpg',
                    'price': '2370.00',
                    'product_name': 'Cuticolor Hair Coloring Cream 120gm (New Pack) No PPD, No Ammonia (Black)',
                    'quantity': 2,
                    'sku': 'Cuti|Black',
                    'timing': '*'
                },
                {
                    'asin': 'B074SL2PHW',
                    'item_image': 'https://m.media-amazon.com/images/I/71GFaYuAE6L.jpg',
                    'price': '1300.00',
                    'product_name': 'Cuticolor Permanent Hair Color Cream (Black)120gm',
                    'quantity': 1,
                    'sku': 'VZ-6F58-V5RY',
                    'timing': '*'
                },
                {
                    'asin': 'B07ZVHHRQ3',
                    'item_image': 'https://m.media-amazon.com/images/I/71WxrT0QNeL.jpg',
                    'price': '0.00',
                    'product_name': 'Cuticolor Hair Coloring Cream, Hair Color, 60g + 60g - Dark Brown (Pack of 1)',
                    'quantity': 0,
                    'sku': '0Y-116Y-MEEN',
                    'timing': '*'
                },
                {
                    'asin': 'B0BQWLZ5S1',
                    'item_image': 'https://m.media-amazon.com/images/I/71PRszLrIoL.jpg',
                    'price': '1185.00',
                    'product_name': 'Cuticolor Hair Coloring Cream 120gm (New Pack) No PPD, No Ammonia (Black)',
                    'quantity': 1,
                    'sku': 'Cuti|Black',
                    'timing': '*'
                }
            ]
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
        '/api/v1/dashboard/sales-statistics/hourly-graph',
        query_string=query_params, headers=headers)
    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_hourly_graph_missing_from_date(test_client):
    """
    TEST CASE: This is the first negative test case. It returns a negative response if request parameter is missing.
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
        '/api/v1/dashboard/sales-statistics/hourly-graph',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_hourly_graph_missing_to_date(test_client):
    """
    TEST CASE: This is the second negative test case. It returns a negative response if request parameter is missing.
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
        '/api/v1/dashboard/sales-statistics/hourly-graph',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_hourly_graph_missing_marketplace(test_client):
    """
    TEST CASE: This is the third negative test case. It returns a negative response if request parameter is missing.
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
        '/api/v1/dashboard/sales-statistics/hourly-graph',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)

# -------------------- marketplace breakdown -------------------------------_#


def test_marketplace_breakdown_positive_first(test_client):
    """
    TEST CASE: This is the first positive test case. It returns a positive response if request is valid and authorized.
    """

    expected_response = {'data': {'result': {'all_marketplaces': {'aov': {'aov_percentage_growth': '*', 'current_aov': '*', 'prior_aov': '*'},
                                                                  'gross_sales': {'current_gross_sales': '*', 'gross_sales_percentage_growth': '*', 'gross_sales_percentage_growth_previous_year': '*', 'previous_year_gross_sales': '*', 'prior_gross_sales': '*'}, 'margin': {'current_margin': '*', 'margin_percentage_growth': '*', 'prior_margin': '*'},
                                                                  'marketplace_fee': {'current_marketplace_fee': '*', 'marketplace_fee_percentage_growth': '*', 'prior_marketplace_fee': '*'}, 'profit': {'current_profit': '*', 'prior_profit': '*', 'profit_percentage_growth': '*'},
                                                                  'return_rate': {'current_return_rate': '*', 'prior_return_rate': '*', 'return_rate_percentage_growth': '*'}, 'returns': {'current_returns': '*', 'prior_returns': '*', 'returns_percentage_growth': '*'},
                                                                  'roi': {'current_roi': '*', 'prior_roi': '*', 'roi_percentage_growth': '*'}, 'total_cogs': {'current_total_cogs': '*', 'prior_total_cogs': '*', 'total_cogs_percentage_growth': '*'}, 'units_returned': {'current_units_returned': '*', 'prior_units_returned': '*', 'units_returned_percentage_growth': '*'},
                                                                  'units_sold': {'current_units_sold': '*', 'previous_year_units_sold': '*', 'prior_units_sold': '*', 'units_sold_percentage_growth': '*', 'units_sold_percentage_growth_previous_year': '*'}}, 'amazon': {'aov': {'aov_percentage_growth': '*', 'current_aov': '*', 'prior_aov': '*'},
                                                                                                                                                                                                                                                                           'gross_sales': {'current_gross_sales': '*', 'gross_sales_percentage_growth': '*', 'gross_sales_percentage_growth_previous_year': '*', 'previous_year_gross_sales': '*', 'prior_gross_sales': '*'}, 'margin': {'current_margin': '*', 'margin_percentage_growth': '*', 'prior_margin': '*'}, 'marketplace_fee': {'current_marketplace_fee': '*', 'marketplace_fee_percentage_growth': '*', 'prior_marketplace_fee': '*'},
                                                                                                                                                                                                                                                                           'profit': {'current_profit': '*', 'prior_profit': '*', 'profit_percentage_growth': '*'}, 'return_rate': {'current_return_rate': '*', 'prior_return_rate': '*', 'return_rate_percentage_growth': '*'}, 'returns': {'current_returns': '*', 'prior_returns': '*', 'returns_percentage_growth': '*'},
                                                                                                                                                                                                                                                                           'roi': {'current_roi': '*', 'prior_roi': '*', 'roi_percentage_growth': '*'}, 'total_cogs': {'current_total_cogs': '*', 'prior_total_cogs': '*', 'total_cogs_percentage_growth': '*'}, 'units_returned': {'current_units_returned': '*', 'prior_units_returned': '*', 'units_returned_percentage_growth': '*'},
                                                                                                                                                                                                                                                                           'units_sold': {'current_units_sold': '*', 'previous_year_units_sold': '*', 'prior_units_sold': '*', 'units_sold_percentage_growth': '*', 'units_sold_percentage_growth_previous_year': '*'}}, 'graph': [{'date': '*', 'gross_sales': '*', 'units_sold': '*'}]}}, 'message': 'Details Fetched Successfully.', 'status': True}

    query_params = {
        'from_date': datetime.now().date(),
        'to_date': datetime.now().date(),
        'marketplace': 'AMAZON'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/marketplace-breakdown',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_marketplace_breakdown_positive_second(test_client):
    """
    TEST CASE: This is the second positive test case. It returns a positive response if request is valid and authorized.
    """

    expected_response = {'data': {'result': {'all_marketplaces': {'aov': {'aov_percentage_growth': '*', 'current_aov': '*', 'prior_aov': '*'},
                                                                  'gross_sales': {'current_gross_sales': '*', 'gross_sales_percentage_growth': '*', 'gross_sales_percentage_growth_previous_year': '*', 'previous_year_gross_sales': '*', 'prior_gross_sales': '*'}, 'margin': {'current_margin': '*', 'margin_percentage_growth': '*', 'prior_margin': '*'},
                                                                  'marketplace_fee': {'current_marketplace_fee': '*', 'marketplace_fee_percentage_growth': '*', 'prior_marketplace_fee': '*'}, 'profit': {'current_profit': '*', 'prior_profit': '*', 'profit_percentage_growth': '*'},
                                                                  'return_rate': {'current_return_rate': '*', 'prior_return_rate': '*', 'return_rate_percentage_growth': '*'}, 'returns': {'current_returns': '*', 'prior_returns': '*', 'returns_percentage_growth': '*'},
                                                                  'roi': {'current_roi': '*', 'prior_roi': '*', 'roi_percentage_growth': '*'}, 'total_cogs': {'current_total_cogs': '*', 'prior_total_cogs': '*', 'total_cogs_percentage_growth': '*'}, 'units_returned': {'current_units_returned': '*', 'prior_units_returned': '*', 'units_returned_percentage_growth': '*'},
                                                                  'units_sold': {'current_units_sold': '*', 'previous_year_units_sold': '*', 'prior_units_sold': '*', 'units_sold_percentage_growth': '*', 'units_sold_percentage_growth_previous_year': '*'}}, 'amazon': {'aov': {'aov_percentage_growth': '*', 'current_aov': '*', 'prior_aov': '*'},
                                                                                                                                                                                                                                                                           'gross_sales': {'current_gross_sales': '*', 'gross_sales_percentage_growth': '*', 'gross_sales_percentage_growth_previous_year': '*', 'previous_year_gross_sales': '*', 'prior_gross_sales': '*'}, 'margin': {'current_margin': '*', 'margin_percentage_growth': '*', 'prior_margin': '*'}, 'marketplace_fee': {'current_marketplace_fee': '*', 'marketplace_fee_percentage_growth': '*', 'prior_marketplace_fee': '*'},
                                                                                                                                                                                                                                                                           'profit': {'current_profit': '*', 'prior_profit': '*', 'profit_percentage_growth': '*'}, 'return_rate': {'current_return_rate': '*', 'prior_return_rate': '*', 'return_rate_percentage_growth': '*'}, 'returns': {'current_returns': '*', 'prior_returns': '*', 'returns_percentage_growth': '*'},
                                                                                                                                                                                                                                                                           'roi': {'current_roi': '*', 'prior_roi': '*', 'roi_percentage_growth': '*'}, 'total_cogs': {'current_total_cogs': '*', 'prior_total_cogs': '*', 'total_cogs_percentage_growth': '*'}, 'units_returned': {'current_units_returned': '*', 'prior_units_returned': '*', 'units_returned_percentage_growth': '*'},
                                                                                                                                                                                                                                                                           'units_sold': {'current_units_sold': '*', 'previous_year_units_sold': '*', 'prior_units_sold': '*', 'units_sold_percentage_growth': '*', 'units_sold_percentage_growth_previous_year': '*'}}, 'graph': [{'date': '*', 'gross_sales': '*', 'units_sold': '*'}]}}, 'message': 'Details Fetched Successfully.', 'status': True}

    query_params = {
        'from_date': datetime.now().date(),
        'to_date': datetime.now().date(),
        'brand': 'Cuticolor'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/marketplace-breakdown',
        query_string=query_params, headers=headers)
    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_mp_breakdown_missing_from_date(test_client):
    """
    TEST CASE: This is the first negative test case. It returns a failure response if request parameter does not have from_date.
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
        'brand': 'Cuticolor'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/marketplace-breakdown',
        query_string=query_params, headers=headers)
    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_mp_breakdown_missing_to_date(test_client):
    """
    TEST CASE: This is the second negative test case. It returns a failure response if request parameter does not have to_date.
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
        'brand': 'Cuticolor'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/marketplace-breakdown',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_mp_breakdown_missing_token(test_client):
    """
    TEST CASE: This is the third negative test case. It returns a failure response if request is invalid and unauthorized.
    """

    expected_response = {'message': 'Invalid Token.', 'status': False}

    query_params = {
        'to_date': datetime.now().date(),
        'from_date': datetime.now().date(),
        'brand': 'Cuticolor'
    }

    headers = {}

    api_response = test_client.get(
        '/api/v1/dashboard/marketplace-breakdown',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=401, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


# ------------------------ gross sales comp --------------------------#

def test_gross_sales_comp_positive(test_client):
    """
    TEST CASE: This is the first positive test case. It returns a successful response if request is valid and authorized
    """

    expected_response = {
        'data': {
            'result': {
                'conversion_rate': {
                    'conversion_rate_difference': '*',
                    'conversion_rate_percentage_growth': '*',
                    'current_conversion_rate': '*',
                    'prior_conversion_rate': '*'
                },
                'expense': {
                    'current_expense': '*',
                    'expense_difference': '*',
                    'expense_percentage_growth': '*',
                    'prior_expense': '*'
                },
                'gross_sales': {
                    'current_gross_sales': '*',
                    'gross_sales_difference': '*',
                    'gross_sales_percentage_growth': '*',
                    'prior_gross_sales': '*'
                },
                'margin': {
                    'current_margin': '*',
                    'margin_difference': '*',
                    'margin_percentage_growth': '*',
                    'prior_margin': 0
                },
                'net_profit': {
                    'current_net_profit': '*',
                    'net_profit_difference': '*',
                    'net_profit_percentage_growth': '*',
                    'prior_net_profit': '*'
                },
                'page_views': {
                    'current_page_views': '*',
                    'page_views_difference': '*',
                    'page_views_percentage_growth': '*',
                    'prior_page_views': '*'
                },
                'refund': {
                    'current_refund': '*',
                    'prior_refund': '*',
                    'refund_difference': '*',
                    'refund_percentage_growth': '*'
                },
                'unique_orders': {
                    'current_unique_orders': '*',
                    'prior_unique_orders': '*',
                    'unique_orders_difference': '*',
                    'unique_orders_percentage_growth': '*'
                },
                'unique_sku_sold': {
                    'current_unique_sku_sold': '*',
                    'prior_unique_sku_sold': '*',
                    'unique_sku_sold_difference': '*',
                    'unique_sku_sold_percentage_growth': '*'
                },
                'units_sold': {
                    'current_units_sold': '*',
                    'prior_units_sold': '*',
                    'units_sold_difference': '*',
                    'units_sold_percentage_growth': '*'
                }
            }
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    query_params = {
        'to_date': datetime.now().date(),
        'from_date': datetime.now().date(),
        'marketplace': 'AMAZON'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/gross-sales-comp',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_gross_sales_comp_positive_second(test_client):
    """
    TEST CASE: This is the seocnd positive test case. It returns a successful response if request is valid and authorized
    """

    expected_response = {
        'data': {
            'result': {
                'conversion_rate': {
                    'conversion_rate_difference': '*',
                    'conversion_rate_percentage_growth': '*',
                    'current_conversion_rate': '*',
                    'prior_conversion_rate': '*'
                },
                'expense': {
                    'current_expense': '*',
                    'expense_difference': '*',
                    'expense_percentage_growth': '*',
                    'prior_expense': '*'
                },
                'gross_sales': {
                    'current_gross_sales': '*',
                    'gross_sales_difference': '*',
                    'gross_sales_percentage_growth': '*',
                    'prior_gross_sales': '*'
                },
                'margin': {
                    'current_margin': '*',
                    'margin_difference': '*',
                    'margin_percentage_growth': '*',
                    'prior_margin': 0
                },
                'net_profit': {
                    'current_net_profit': '*',
                    'net_profit_difference': '*',
                    'net_profit_percentage_growth': '*',
                    'prior_net_profit': '*'
                },
                'page_views': {
                    'current_page_views': '*',
                    'page_views_difference': '*',
                    'page_views_percentage_growth': '*',
                    'prior_page_views': '*'
                },
                'refund': {
                    'current_refund': '*',
                    'prior_refund': '*',
                    'refund_difference': '*',
                    'refund_percentage_growth': '*'
                },
                'unique_orders': {
                    'current_unique_orders': '*',
                    'prior_unique_orders': '*',
                    'unique_orders_difference': '*',
                    'unique_orders_percentage_growth': '*'
                },
                'unique_sku_sold': {
                    'current_unique_sku_sold': '*',
                    'prior_unique_sku_sold': '*',
                    'unique_sku_sold_difference': '*',
                    'unique_sku_sold_percentage_growth': '*'
                },
                'units_sold': {
                    'current_units_sold': '*',
                    'prior_units_sold': '*',
                    'units_sold_difference': '*',
                    'units_sold_percentage_growth': '*'
                }
            }
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    query_params = {
        'to_date': datetime.now().date(),
        'from_date': datetime.now().date(),
        'marketplace': 'AMAZON',
        'brand': 'Cuticolor'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/gross-sales-comp',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_gross_sales_comp_negative_first(test_client):
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
        'brand': 'Cuticolor'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/gross-sales-comp',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_gross_sales_comp_negative_second(test_client):
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
        'brand': 'Cuticolor'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/gross-sales-comp',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)

    time.sleep(2)


def test_gross_sales_comp_negative_third(test_client):
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
        'brand': 'Cuticolor'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/gross-sales-comp',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_gross_sales_comp_missing_token(test_client):
    """
    TEST CASE: This is the third negative test case.
    It returns a failure response if token is missing from request headers.
    """

    expected_response = {'message': 'Invalid Token.', 'status': False}

    query_params = {
        'from_date': datetime.now().date(),
        'to_date': datetime.now().date(),
        'marketplace': 'AMAZON',
        'brand': 'Cuticolor'
    }

    headers = {}

    api_response = test_client.get(
        '/api/v1/dashboard/gross-sales-comp',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=401, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


# ---------------------- sales and trends -----------------------#

def test_sales_trends_positive_first(test_client):
    """
    TEST CASE: This is the first positive test case.
    It returns a successful response if request is valid and authenticated.
    """

    expected_response = {
        'data': {
            'result': {
                'least_selling_items': '*',
                'sales_trend_decreasing': '*',
                'sales_trend_increasing': '*',
                'top_selling_items': '*'
            }
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    query_params = {
        'size': 2,
        'to_date': datetime.now().date(),
        'from_date': datetime.now().date(),
        'marketplace': 'AMAZON'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/sales-and-trends',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_sales_trends_positive_second(test_client):
    """
    TEST CASE: This is the second positive test case.
    It returns a successful response if request is valid and authenticated.
    """

    expected_response = {
        'data': {
            'result': {
                'least_selling_items': '*',
                'sales_trend_decreasing': '*',
                'sales_trend_increasing': '*',
                'top_selling_items': '*'
            }
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    query_params = {
        'size': 2,
        'to_date': datetime.now().date(),
        'from_date': datetime.now().date(),
        'marketplace': 'AMAZON',
        'brand': 'Cuticolor'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/sales-and-trends',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_sales_trends_size_missing(test_client):
    """
    TEST CASE: This is the first negative test case.
    It returns a failure response if size is missing from request params.
    """

    expected_response = {
        'error': {
            'size': 'Size is required.'
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
        '/api/v1/dashboard/sales-and-trends',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_sales_trends_from_date_missing(test_client):
    """
    TEST CASE: This is the second negative test case.
    It returns a failure response if from date is missing from request params.
    """

    expected_response = {
        'error': {
            'from_date': 'From Date is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    query_params = {
        'size': 2,
        'to_date': datetime.now().date(),
        'marketplace': 'AMAZON'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/sales-and-trends',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_sales_trends_to_date_missing(test_client):
    """
    TEST CASE: This is the third negative test case.
    It returns a failure response if to date is missing from request params.
    """

    expected_response = {
        'error': {
            'to_date': 'To Date is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    query_params = {
        'size': 2,
        'from_date': datetime.now().date(),
        'marketplace': 'AMAZON'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/sales-and-trends',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_sales_trends_marketplace_missing(test_client):
    """
    TEST CASE: This is the fourth negative test case.
    It returns a failure response if marketplace is missing from request params.
    """

    expected_response = {
        'error': {
            'marketplace': 'Marketplace is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    query_params = {
        'size': 2,
        'to_date': datetime.now().date(),
        'from_date': datetime.now().date()
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/sales-and-trends',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_sales_trends_token_missing(test_client):
    """
    TEST CASE: This is the fifth negative test case.
    It returns a failure response if token is missing from request headers.
    """

    expected_response = {'message': 'Invalid Token.', 'status': False}

    query_params = {
        'size': 2,
        'to_date': datetime.now().date(),
        'from_date': datetime.now().date(),
        'marketplace': 'AMAZON'
    }

    headers = {}

    api_response = test_client.get(
        '/api/v1/dashboard/sales-and-trends',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=401, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


# ---------------- profit - loss ----------------#
def test_profit_loss_positive_first(test_client):
    """
    TEST CASE: This is the first positive test case.
    It returns a successful response if request is valid and authorized.
    """

    expected_response = {
        'data': {
            'result': {
                'graph': ['*'
                          ],
                'profit_and_loss': {
                    'ads_spend': {
                        'total_ads_spend': '*',
                        'total_ads_spend_percentage_growth': '*'
                    },
                    'ads_spend_breakdown': {
                        'sponsored_brand': {
                            'current_sponsored_brand_cost': '*',
                            'sponsored_brand_percentage_growth': '*'
                        },
                        'sponsored_display': {
                            'current_sponsored_display_cost': '*',
                            'sponsored_display_percentage_growth': '*'
                        },
                        'sponsored_product': {
                            'current_sponsored_product_cost': '*',
                            'sponsored_product_percentage_growth': '*'
                        }
                    },
                    'gross_sales': {
                        'current_gross_sales': '*',
                        'gross_sales_percentage_growth': '*'
                    },
                    'gross_sales_breakdown': {

                    },
                    'market_place_fee': {
                        'current_market_place_fee': '*',
                        'market_place_fee_percentage_growth': '*'
                    },
                    'marketplace_fee_breakdown': '*',
                    'net_profit': {
                        'current_net_profit': '*',
                        'net_profit_percentage_growth': '*'
                    },
                    'net_sales': {
                        'current_net_sales': '*',
                        'net_sales_percentage_growth': '*'
                    },
                    'other_fee': {
                        'current_other_fee': '*',
                        'other_fee_percentage_growth': '*'
                    },
                    'other_fee_breakdown': '*',
                    'refund': {
                        'current_refund': '*',
                        'refund_percentage_growth': '*'
                    },
                    'reimbursement': {
                        'current_reimbursement': '*',
                        'reimbursement_percentage_growth': '*'
                    },
                    'reimbursement_breakdown': '*',
                    'total_cogs': {
                        'current_total_cogs': '*',
                        'total_cogs_percentage_growth': '*'
                    }
                }
            }
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    query_params = {
        'to_date': datetime.now().date(),
        'from_date': datetime.now().date(),
        'marketplace': 'AMAZON'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/profit-loss',
        query_string=query_params, headers=headers)
    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_profit_loss_positive_second(test_client):
    """
    TEST CASE: This is the second positive test case.
    It returns a successful response if request is valid and authorized.
    """

    expected_response = {
        'data': {
            'result': {
                'graph': ['*'
                          ],
                'profit_and_loss': {
                    'ads_spend': {
                        'total_ads_spend': '*',
                        'total_ads_spend_percentage_growth': '*'
                    },
                    'ads_spend_breakdown': {
                        'sponsored_brand': {
                            'current_sponsored_brand_cost': '*',
                            'sponsored_brand_percentage_growth': '*'
                        },
                        'sponsored_display': {
                            'current_sponsored_display_cost': '*',
                            'sponsored_display_percentage_growth': '*'
                        },
                        'sponsored_product': {
                            'current_sponsored_product_cost': '*',
                            'sponsored_product_percentage_growth': '*'
                        }
                    },
                    'gross_sales': {
                        'current_gross_sales': '*',
                        'gross_sales_percentage_growth': '*'
                    },
                    'gross_sales_breakdown': {

                    },
                    'market_place_fee': {
                        'current_market_place_fee': '*',
                        'market_place_fee_percentage_growth': '*'
                    },
                    'marketplace_fee_breakdown': '*',
                    'net_profit': {
                        'current_net_profit': '*',
                        'net_profit_percentage_growth': '*'
                    },
                    'net_sales': {
                        'current_net_sales': '*',
                        'net_sales_percentage_growth': '*'
                    },
                    'other_fee': {
                        'current_other_fee': '*',
                        'other_fee_percentage_growth': '*'
                    },
                    'other_fee_breakdown': '*',
                    'refund': {
                        'current_refund': '*',
                        'refund_percentage_growth': '*'
                    },
                    'reimbursement': {
                        'current_reimbursement': '*',
                        'reimbursement_percentage_growth': '*'
                    },
                    'reimbursement_breakdown': '*',
                    'total_cogs': {
                        'current_total_cogs': '*',
                        'total_cogs_percentage_growth': '*'
                    }
                }
            }
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    query_params = {
        'to_date': datetime.now().date(),
        'from_date': datetime.now().date(),
        'marketplace': 'AMAZON',
        'brand': 'Cuticolor'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/profit-loss',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_profit_loss_missing_from_date(test_client):
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
        'marketplace': 'AMAZON'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/profit-loss',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_profit_loss_missing_to_date(test_client):
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
        'marketplace': 'AMAZON'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/profit-loss',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_profit_loss_missing_marketplace(test_client):
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
        'from_date': datetime.now().date(),
        'to_date': datetime.now().date()
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/profit-loss',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_profit_loss_missing_token(test_client):
    """
    TEST CASE: This is the fourth negative test case.
    It returns a failure response if token is invalid in request headers.
    """

    expected_response = {'message': 'Invalid Token.', 'status': False}

    query_params = {
        'from_date': datetime.now().date(),
        'to_date': datetime.now().date(),
        'market': 'AMAZON'
    }

    headers = {}

    api_response = test_client.get(
        '/api/v1/dashboard/profit-loss',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=401, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


# --------------------- product performance-------------------#
def test_product_performance_positive_first(test_client):
    """
    TEST CASE: This is the first positive test case.
    It returns a successful response if request is valid and authorized.
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
                    'total_items': 5,
                    'total_pages': 1
                }
            },
            'result': ['*'
                       ]
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
        '/api/v1/dashboard/product-performance',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_product_performance_positive_second(test_client):
    """
    TEST CASE: This is the second positive test case.
    It returns a successful response if request is valid and authorized.
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
                    'total_items': 4,
                    'total_pages': 1
                }
            },
            'result': ['*'
                       ]
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
        '/api/v1/dashboard/product-performance',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_product_performance_positive_third(test_client):
    """
    TEST CASE: This is the third positive test case.
    It returns a successful response if request is valid and authorized.
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
                    'total_items': 5,
                    'total_pages': 1
                }
            },
            'result': ['*'
                       ]
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    query_params = {
        'from_date': datetime.now().date(),
        'to_date': datetime.now().date(),
        'marketplace': 'AMAZON',
        'sort_order': 'ASC',
        'sort_by': 'GROSS_SALES'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/product-performance',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_product_performance_wrong_brand(test_client):
    """
    TEST CASE: This is the first negative test case.
    It returns a failure response if wrong brand is passed in parameter.
    """

    expected_response = {'message': 'No data found.', 'status': False}

    query_params = {
        'from_date': datetime.now().date(),
        'to_date': datetime.now().date(),
        'marketplace': 'AMAZON',
        'brand': 'Himalaya'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/product-performance',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=404, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_product_performance_missing_from_date(test_client):
    """
    TEST CASE: This is the second negative test case.
    It returns a failure response if from_date parameter is missing from request.
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
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/product-performance',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_product_performance_missing_to_date(test_client):
    """
    TEST CASE: This is the third negative test case.
    It returns a failure response if to_date parameter is missing from request.
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
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/product-performance',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_product_performance_missing_marketplace(test_client):
    """
    TEST CASE: This is the fourth negative test case.
    It returns a failure response if marketplace parameter is missing from request.
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
        '/api/v1/dashboard/product-performance',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


# -------------------product performance day graph------------------_#

def test_prod_perf_day_graph_positive(test_client):
    """
    TEST CASE: This is the first positive test case.
    It returns a successful response if request is valid and authorized.
    """

    expected_response = {
        'data': {
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
        '/api/v1/dashboard/product-performance/Cuti|Black/day-graph',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)

    time.sleep(1)


def test_prod_perf_day_graph_positive_second(test_client):
    """
    TEST CASE: This is the second positive test case.
    It returns a successful response if request is valid and authorized.
    """

    expected_response = {
        'data': {
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
        '/api/v1/dashboard/product-performance/VZ-6F58-V5RY/day-graph',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_prod_perf_day_graph_missing_from_date(test_client):
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
        '/api/v1/dashboard/product-performance/VZ-6F58-V5RY/day-graph',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_prod_perf_day_graph_missing_to_date(test_client):
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
        '/api/v1/dashboard/product-performance/VZ-6F58-V5RY/day-graph',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_prod_perf_day_graph_missing_marketplace(test_client):
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
        '/api/v1/dashboard/product-performance/VZ-6F58-V5RY/day-graph',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_prod_perf_day_graph_missing_sellersku(test_client):
    """
    TEST CASE: This is the fourth negative test case.
    It returns a failure response if seller sku is missing from request path.
    """

    expected_response = None

    query_params = {
        'from_date': datetime.now().date(),
        'to_date': datetime.now().date(),
        'marketplace': 'AMAZON'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/product-performance/day-graph',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=404, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_prod_perf_day_graph_missing_token(test_client):
    """
    TEST CASE: This is the fifth negative test case.
    It returns a failure response if token is missing or invalid and request is unauthorized.
    """

    expected_response = {'message': 'Invalid Token.', 'status': False}

    query_params = {
        'from_date': datetime.now().date(),
        'to_date': datetime.now().date(),
        'marketplace': 'AMAZON'
    }

    headers = {}

    api_response = test_client.get(
        '/api/v1/dashboard/product-performance/VZ-6F58-V5RY/day-graph',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=401, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)

# ------------------- product performance heatmap---------------#


def test_prod_perf_heatmap_positive(test_client):
    """
    TEST CASE: This is the first positive test case.
    It returns a successful response if request is valid and authorized.
    """

    expected_response = {
        'data': {
            'object': {
                'regional_sales_statistics': {
                    'sku': '*',
                    'total_gross_sales': '*',
                    'total_refunds': '*',
                    'total_units_returned': '*',
                    'total_units_sold': '*'
                }
            },
            'result': [
                {
                    'gross_sales': '*',
                    'refunds': '*',
                    'state': '*',
                    'units_returned': '*',
                    'units_sold': '*',
                    'zone': '*'
                }
            ]
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
        '/api/v1/dashboard/product-performance/Cuti|Black/heatmap',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_prod_perf_heatmap_positive_second(test_client):
    """
    TEST CASE: This is the second positive test case.
    It returns a successful response if request is valid and authorized.
    """

    expected_response = {
        'data': {
            'object': {
                'regional_sales_statistics': {
                    'sku': '*',
                    'total_gross_sales': '*',
                    'total_refunds': '*',
                    'total_units_returned': '*',
                    'total_units_sold': '*'
                }
            },
            'result': [
                {
                    'gross_sales': '*',
                    'refunds': '*',
                    'state': '*',
                    'units_returned': '*',
                    'units_sold': '*',
                    'zone': '*'
                }
            ]
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
        '/api/v1/dashboard/product-performance/VZ-6F58-V5RY/heatmap',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_prod_perf_heatmap_positive_third(test_client):
    """
    TEST CASE: This is the third positive test case.
    It returns a successful response if request is valid and authorized.
    """

    expected_response = {
        'data': {
            'object': {
                'regional_sales_statistics': {
                    'sku': '*',
                    'total_gross_sales': '*',
                    'total_refunds': '*',
                    'total_units_returned': '*',
                    'total_units_sold': '*'
                }
            },
            'result': [
                {
                    'gross_sales': '*',
                    'refunds': '*',
                    'state': '*',
                    'units_returned': '*',
                    'units_sold': '*',
                    'zone': '*'
                }
            ]
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    query_params = {
        'from_date': datetime.now().date(),
        'to_date': datetime.now().date(),
        'marketplace': 'AMAZON',
        'sort_order': 'DESC',
        'sort_by': 'GROSS_SALES'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/product-performance/VZ-6F58-V5RY/heatmap',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_prod_perf_heatmap_missing_from_date(test_client):
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
        '/api/v1/dashboard/product-performance/VZ-6F58-V5RY/heatmap',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_prod_perf_heatmap_missing_to_date(test_client):
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
        '/api/v1/dashboard/product-performance/VZ-6F58-V5RY/heatmap',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_prod_perf_heatmap_missing_marketplace(test_client):
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
        '/api/v1/dashboard/product-performance/VZ-6F58-V5RY/heatmap',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_prod_perf_heatmap_missing_sellersku(test_client):
    """
    TEST CASE: This is the fourth negative test case.
    It returns a failure response if seller sku is missing from request path.
    """

    expected_response = None

    query_params = {
        'from_date': datetime.now().date(),
        'to_date': datetime.now().date(),
        'marketplace': 'AMAZON'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/product-performance/heatmap',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=404, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_prod_perf_heatmap_missing_token(test_client):
    """
    TEST CASE: This is the fifth negative test case.
    It returns a failure response if token is missing or invalid and request is unauthorized.
    """

    expected_response = {'message': 'Invalid Token.', 'status': False}

    query_params = {
        'from_date': datetime.now().date(),
        'to_date': datetime.now().date(),
        'marketplace': 'AMAZON'
    }

    headers = {}

    api_response = test_client.get(
        '/api/v1/dashboard/product-performance/VZ-6F58-V5RY/heatmap',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=401, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


# ------------------- sales by region--------------------------------#

def test_sales_region_positive_first(test_client):
    """
    TEST CASE: This is the first positive test case.
    It returns a successful response if request is valid and authorized.
    """

    expected_response = {
        'data': {
            'objects': {
                'zonal_sales_statistics': ['*'],
                'zonal_total_gross_sales': '*',
                'zonal_total_refund_sales': '*'
            },
            'results': ['*']
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
        '/api/v1/dashboard/sales-by-region',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_sales_region_positive_second(test_client):
    """
    TEST CASE: This is the second positive test case.
    It returns a successful response if request is valid and authorized.
    """

    expected_response = {
        'data': {
            'objects': {
                'zonal_sales_statistics': ['*'],
                'zonal_total_gross_sales': '*',
                'zonal_total_refund_sales': '*'
            },
            'results': ['*']
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
        '/api/v1/dashboard/sales-by-region',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_sales_region_wrong_brand(test_client):
    """
    TEST CASE: This is the first negative test case.
    It returns a negative response if request parameter is not valid.
    """

    expected_response = {'message': 'No data found.', 'status': False}

    query_params = {
        'from_date': datetime.now().date(),
        'to_date': datetime.now().date(),
        'marketplace': 'AMAZON',
        'brand': 'Himalaya'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/dashboard/sales-by-region',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=404, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_sales_region_missing_from_date(test_client):
    """
    TEST CASE: This is the second negative test case.
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
        '/api/v1/dashboard/sales-by-region',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_sales_region_missing_to_date(test_client):
    """
    TEST CASE: This is the third negative test case.
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
        '/api/v1/dashboard/sales-by-region',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_sales_region_missing_marketplace(test_client):
    """
    TEST CASE: This is the fourth negative test case.
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
        '/api/v1/dashboard/sales-by-region',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_sales_region_missing_token(test_client):
    """
    TEST CASE: This is the fifth negative test case.
    It returns a failure response if request is unauthorized due to missing/invalid token in request header.
    """

    expected_response = {'message': 'Invalid Token.', 'status': False}

    query_params = {
        'from_date': datetime.now().date(),
        'to_date': datetime.now().date(),
        'marketplace': 'AMAZON'
    }

    headers = {}

    api_response = test_client.get(
        '/api/v1/dashboard/sales-by-region',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=401, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)
