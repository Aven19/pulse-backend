"""test cases for inventory module"""
from datetime import datetime
import os
import time

from tests.conftest import validate_response
from tests.conftest import validate_status_code


def test_ad_impact_graph_positive(test_client):
    """
        TEST CASE: This is a positive test case. Return a successful response if fields are validated and user login successfully.
    """

    expected_response = {
        'data': {
            'result': {
                'ad_sales': '*',
                'ad_spends': '*',
                'amazon_fees': '*',
                'cogs': '*',
                'net_profit': '*',
                'organic_sales': '*',
                'other_fees': '*',
                'other_revenue': '*',
                'refund': '*',
                'total_gross_sales': '*'
            }
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
        '/api/v1/marketing-report/ad-impact-graph', query_string=query_params, headers=headers, content_type='application/json'
    )
    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_ad_impact_graph_positive_second(test_client):
    """
        TEST CASE: This is a positive test case. Return a successful response if fields are validated and user login successfully.
    """

    expected_response = {
        'data': {
            'result': {
                'ad_sales': '*',
                'ad_spends': '*',
                'amazon_fees': '*',
                'cogs': '*',
                'net_profit': '*',
                'organic_sales': '*',
                'other_fees': '*',
                'other_revenue': '*',
                'refund': '*',
                'total_gross_sales': '*'
            }
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
        '/api/v1/marketing-report/ad-impact-graph', query_string=query_params, headers=headers, content_type='application/json'
    )

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_ad_impact_missing_from_date(test_client):
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
        '/api/v1/marketing-report/ad-impact-graph',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_ad_impact_missing_to_date(test_client):
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
        '/api/v1/marketing-report/ad-impact-graph',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_ad_impact_missing_marketplace(test_client):
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
        '/api/v1/marketing-report/ad-impact-graph',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_ad_impact_no_data_found(test_client):
    """
    TEST CASE: This is the fourth negative test case.
    It returns a failure response if no data found for given filters.
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
        '/api/v1/marketing-report/ad-impact-graph',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=404, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


# -------------------- cost vs metrics graph----------------------#

def test_cost_metric_graph_positive(test_client):
    """
        TEST CASE: This is a positive test case. Return a successful response if fields are validated and user login successfully.
    """

    expected_response = {
        'data': [
            {
                '_date': '*',
                'acos': '*',
                'conversion_rate': '*',
                'cpc': '*',
                'ctr': '*',
                'roas': '*',
                'tacos': '*',
                'total_ad_clicks': '*',
                'total_ad_impressions': '*',
                'total_ad_sales': '*',
                'total_ad_spends': '*',
                'total_gross_sales': '*',
                'total_page_views': '*',
                'total_sales': '*',
                'total_units_sold': '*'
            }
        ],
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
        '/api/v1/marketing-report/costvs-metrics', query_string=query_params, headers=headers, content_type='application/json'
    )
    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_cost_metrics_positive_second(test_client):
    """
        TEST CASE: This is a positive test case. Return a successful response if fields are validated and user login successfully.
    """

    expected_response = {
        'data': [
            {
                '_date': '*',
                'acos': '*',
                'conversion_rate': '*',
                'cpc': '*',
                'ctr': '*',
                'roas': '*',
                'tacos': '*',
                'total_ad_clicks': '*',
                'total_ad_impressions': '*',
                'total_ad_sales': '*',
                'total_ad_spends': '*',
                'total_gross_sales': '*',
                'total_page_views': '*',
                'total_sales': '*',
                'total_units_sold': '*'
            }
        ],
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
        '/api/v1/marketing-report/costvs-metrics', query_string=query_params, headers=headers, content_type='application/json'
    )

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_cost_metrics_graph_missing_from_date(test_client):
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
        '/api/v1/marketing-report/costvs-metrics',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_cost_metrics_graph_missing_to_date(test_client):
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
        '/api/v1/marketing-report/costvs-metrics',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)

    time.sleep(1)


def test_cost_metrics_graph_missing_marketplace(test_client):
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
        '/api/v1/marketing-report/costvs-metrics',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_cost_metrics_graph_no_data_found(test_client):
    """
    TEST CASE: This is the fourth negative test case.
    It returns a failure response if no data found for given filters.
    """

    expected_response = {
        'data': [], 'message': 'Details Fetched Successfully.', 'status': True}

    query_params = {
        'from_date': datetime.now().date(),
        'to_date': datetime.now().date(),
        'marketplace': 'AMAZON',
        'brand': 'Himalaya'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/marketing-report/costvs-metrics',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


# ------------------------sales-period--------------------------#

def test_sales_period_positive(test_client):
    """
        TEST CASE: This is a positive test case. Return a successful response if fields are validated and user login successfully.
    """

    expected_response = {
        'data': {
            'result': {
                'acos': {
                    'current': '*',
                    'difference': '*',
                    'growth_percentage': '*',
                    'prior': '*'
                },
                'conversion_rate': {
                    'current': '*',
                    'difference': '*',
                    'growth_percentage': '*',
                    'prior': '*'
                },
                'cpc': {
                    'current': '*',
                    'difference': '*',
                    'growth_percentage': '*',
                    'prior': '*'
                },
                'ctr': {
                    'current': '*',
                    'difference': '*',
                    'growth_percentage': '*',
                    'prior': '*'
                },
                'roas': {
                    'current': '*',
                    'difference': '*',
                    'growth_percentage': '*',
                    'prior': '*'
                },
                'tacos': {
                    'current': '*',
                    'difference': '*',
                    'growth_percentage': '*',
                    'prior': '*'
                },
                'total_ad_clicks': {
                    'current': '*',
                    'difference': '*',
                    'growth_percentage': '*',
                    'prior': '*'
                },
                'total_ad_impressions': {
                    'current': '*',
                    'difference': '*',
                    'growth_percentage': '*',
                    'prior': '*'
                },
                'total_ad_sales': {
                    'current': '*',
                    'difference': '*',
                    'growth_percentage': '*',
                    'prior': '*'
                },
                'total_ad_spends': {
                    'current': '*',
                    'difference': '*',
                    'growth_percentage': '*',
                    'prior': '*'
                },
                'total_page_views': {
                    'current': '*',
                    'difference': '*',
                    'growth_percentage': '*',
                    'prior': '*'
                },
                'total_sales': {
                    'current': '*',
                    'difference': '*',
                    'growth_percentage': '*',
                    'prior': '*'
                },
                'total_units_sold': {
                    'current': '*',
                    'difference': '*',
                    'growth_percentage': '*',
                    'prior': '*'
                }
            }
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
        '/api/v1/marketing-report/sales-period', query_string=query_params, headers=headers, content_type='application/json'
    )
    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_sales_period_positive_second(test_client):
    """
        TEST CASE: This is a positive test case. Return a successful response if fields are validated and user login successfully.
    """

    expected_response = {
        'data': {
            'result': {
                'acos': {
                    'current': '*',
                    'difference': '*',
                    'growth_percentage': '*',
                    'prior': '*'
                },
                'conversion_rate': {
                    'current': '*',
                    'difference': '*',
                    'growth_percentage': '*',
                    'prior': '*'
                },
                'cpc': {
                    'current': '*',
                    'difference': '*',
                    'growth_percentage': '*',
                    'prior': '*'
                },
                'ctr': {
                    'current': '*',
                    'difference': '*',
                    'growth_percentage': '*',
                    'prior': '*'
                },
                'roas': {
                    'current': '*',
                    'difference': '*',
                    'growth_percentage': '*',
                    'prior': '*'
                },
                'tacos': {
                    'current': '*',
                    'difference': '*',
                    'growth_percentage': '*',
                    'prior': '*'
                },
                'total_ad_clicks': {
                    'current': '*',
                    'difference': '*',
                    'growth_percentage': '*',
                    'prior': '*'
                },
                'total_ad_impressions': {
                    'current': '*',
                    'difference': '*',
                    'growth_percentage': '*',
                    'prior': '*'
                },
                'total_ad_sales': {
                    'current': '*',
                    'difference': '*',
                    'growth_percentage': '*',
                    'prior': '*'
                },
                'total_ad_spends': {
                    'current': '*',
                    'difference': '*',
                    'growth_percentage': '*',
                    'prior': '*'
                },
                'total_page_views': {
                    'current': '*',
                    'difference': '*',
                    'growth_percentage': '*',
                    'prior': '*'
                },
                'total_sales': {
                    'current': '*',
                    'difference': '*',
                    'growth_percentage': '*',
                    'prior': '*'
                },
                'total_units_sold': {
                    'current': '*',
                    'difference': '*',
                    'growth_percentage': '*',
                    'prior': '*'
                }
            }
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
        '/api/v1/marketing-report/sales-period', query_string=query_params, headers=headers, content_type='application/json'
    )

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_sales_period_missing_from_date(test_client):
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
        '/api/v1/marketing-report/sales-period',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_sales_period_missing_to_date(test_client):
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
        '/api/v1/marketing-report/sales-period',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_sales_period_missing_marketplace(test_client):
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
        '/api/v1/marketing-report/sales-period',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_sales_period_no_data_found(test_client):
    """
    TEST CASE: This is the fourth negative test case.
    It returns a failure response if no data found for given filters.
    """

    expected_response = {'data': {'result': {'acos': {'current': 0.0, 'difference': 0.0, 'growth_percentage': 0, 'prior': 0.0}, 'conversion_rate': {'current': 0.0, 'difference': 0.0, 'growth_percentage': 0, 'prior': 0.0}, 'cpc': {'current': 0.0, 'difference': 0.0, 'growth_percentage': 0, 'prior': 0.0}, 'ctr': {'current': 0.0, 'difference': 0.0, 'growth_percentage': 0, 'prior': 0.0}, 'roas': {'current': 0.0, 'difference': 0.0, 'growth_percentage': 0, 'prior': 0.0}, 'tacos': {'current': 0.0, 'difference': 0.0, 'growth_percentage': 0, 'prior': 0.0}, 'total_ad_clicks': {'current': 0, 'difference': 0, 'growth_percentage': 0,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             'prior': 0}, 'total_ad_impressions': {'current': 0, 'difference': 0, 'growth_percentage': 0, 'prior': 0}, 'total_ad_sales': {'current': 0.0, 'difference': 0.0, 'growth_percentage': 0, 'prior': 0.0}, 'total_ad_spends': {'current': 0.0, 'difference': 0.0, 'growth_percentage': 0, 'prior': 0.0}, 'total_page_views': {'current': 0, 'difference': 0, 'growth_percentage': 0, 'prior': 0}, 'total_sales': {'current': 0, 'difference': 0, 'growth_percentage': 0, 'prior': 0}, 'total_units_sold': {'current': 0, 'difference': 0, 'growth_percentage': 0, 'prior': 0}}}, 'message': 'Details Fetched Successfully.', 'status': True}

    query_params = {
        'from_date': datetime.now().date(),
        'to_date': datetime.now().date(),
        'marketplace': 'AMAZON',
        'brand': 'Himalaya'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/marketing-report/sales-period',
        query_string=query_params, headers=headers)
    assert validate_status_code(
        expected=200, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


# -------------------product performance-----------------------------#

def test_product_performance_positive(test_client):
    """
        TEST CASE: This is a positive test case. Return a successful response if fields are validated and user login successfully.
    """

    expected_response = {
        'data': {
            'objects': {
                'pagination_metadata': {
                    'current_page': 1,
                    'has_next_page': True,
                    'has_previous_page': False,
                    'next_page': 2,
                    'page_size': 2,
                    'previous_page': None,
                    'total_items': 5,
                    'total_pages': 3
                }
            },
            'result': [
                {
                    '_asin': 'B07ZVHHRQ3',
                    '_brand': 'Cuticolor',
                    '_category': 'Beauty',
                    '_product_image': 'https://m.media-amazon.com/images/I/71WxrT0QNeL.jpg',
                    '_product_name': 'Cuticolor Hair Coloring Cream, Hair Color, 60g + 60g - Dark Brown (Pack of 1)',
                    '_sku': '0Y-116Y-MEEN',
                    '_subcategory': 'Hair Oil & Styling',
                    'acos': 0.0,
                    'ad_conversion_rate': 0.0,
                    'category_rank': 3380,
                    'conversion_rate': 0.0,
                    'cpc': 0.0,
                    'ctr': 0.0,
                    'impressions': 0,
                    'order_from_ads': 0,
                    'organic_sales': 8316.0,
                    'organic_sales_percentage': 100.0,
                    'organic_sessions': 0,
                    'organic_units': 7,
                    'page_views': 0,
                    'roas': 0.0,
                    'sales_from_ads': 0.0,
                    'sessions': 0,
                    'subcategory_rank': 26,
                    'tacos': 0.0,
                    'total_ad_clicks': 0,
                    'total_ad_spends': 0.0,
                    'total_gross_sales': 8316.0,
                    'total_units_sold': '7',
                    'units_from_ads': 0
                },
                {
                    '_asin': 'B087N9R8BP',
                    '_brand': 'Sterlomax',
                    '_category': 'Health & Beauty',
                    '_product_image': 'https://m.media-amazon.com/images/I/71o03R8fHtL.jpg',
                    '_product_name': 'Sterlomax 80% Ethanol-Based Hand Rub Sanitizer and Disinfectant, 5 L, Pink',
                    '_sku': 'BK-WHD7-M42A',
                    '_subcategory': 'Hand Sanitizers',
                    'acos': 0.0,
                    'ad_conversion_rate': 0.0,
                    'category_rank': 6190,
                    'conversion_rate': 0.0,
                    'cpc': 0.0,
                    'ctr': 0.0,
                    'impressions': 0,
                    'order_from_ads': 0,
                    'organic_sales': 3338.0,
                    'organic_sales_percentage': 100.0,
                    'organic_sessions': 0,
                    'organic_units': 2,
                    'page_views': 0,
                    'roas': 0.0,
                    'sales_from_ads': 0.0,
                    'sessions': 0,
                    'subcategory_rank': 25,
                    'tacos': 0.0,
                    'total_ad_clicks': 0,
                    'total_ad_spends': 0.0,
                    'total_gross_sales': 3338.0,
                    'total_units_sold': '2',
                    'units_from_ads': 0
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
        'size': 2
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/marketing-report/product-performance', query_string=query_params, headers=headers, content_type='application/json'
    )

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_product_performance_positive_second(test_client):
    """
        TEST CASE: This is a positive test case. Return a successful response if fields are validated and user login successfully.
    """

    expected_response = {
        'data': {
            'objects': {
                'pagination_metadata': {
                    'current_page': 1,
                    'has_next_page': True,
                    'has_previous_page': False,
                    'next_page': 2,
                    'page_size': 2,
                    'previous_page': None,
                    'total_items': 4,
                    'total_pages': 2
                }
            },
            'result': [
                {
                    '_asin': 'B07ZVHHRQ3',
                    '_brand': 'Cuticolor',
                    '_category': 'Beauty',
                    '_product_image': 'https://m.media-amazon.com/images/I/71WxrT0QNeL.jpg',
                    '_product_name': 'Cuticolor Hair Coloring Cream, Hair Color, 60g + 60g - Dark Brown (Pack of 1)',
                    '_sku': '0Y-116Y-MEEN',
                    '_subcategory': 'Hair Oil & Styling',
                    'acos': 0.0,
                    'ad_conversion_rate': 0.0,
                    'category_rank': 3380,
                    'conversion_rate': 0.0,
                    'cpc': 0.0,
                    'ctr': 0.0,
                    'impressions': 0,
                    'order_from_ads': 0,
                    'organic_sales': 8316.0,
                    'organic_sales_percentage': 100.0,
                    'organic_sessions': 0,
                    'organic_units': 7,
                    'page_views': 0,
                    'roas': 0.0,
                    'sales_from_ads': 0.0,
                    'sessions': 0,
                    'subcategory_rank': 26,
                    'tacos': 0.0,
                    'total_ad_clicks': 0,
                    'total_ad_spends': 0.0,
                    'total_gross_sales': 8316.0,
                    'total_units_sold': '7',
                    'units_from_ads': 0
                },
                {
                    '_asin': 'B0BQWLZ5S1',
                    '_brand': 'Cuticolor',
                    '_category': 'Beauty',
                    '_product_image': 'https://m.media-amazon.com/images/I/71PRszLrIoL.jpg',
                    '_product_name': 'Cuticolor Hair Coloring Cream 120gm (New Pack) No PPD, No Ammonia (Black)',
                    '_sku': 'Cuti|Black',
                    '_subcategory': 'Chemical Hair Dyes',
                    'acos': 0.0,
                    'ad_conversion_rate': 0.0,
                    'category_rank': 2331,
                    'conversion_rate': 0.0,
                    'cpc': 0.0,
                    'ctr': 0.0,
                    'impressions': 0,
                    'order_from_ads': 0,
                    'organic_sales': 3435.0,
                    'organic_sales_percentage': 100.0,
                    'organic_sessions': 0,
                    'organic_units': 3,
                    'page_views': 0,
                    'roas': 0.0,
                    'sales_from_ads': 0.0,
                    'sessions': 0,
                    'subcategory_rank': 6,
                    'tacos': 0.0,
                    'total_ad_clicks': 0,
                    'total_ad_spends': 0.0,
                    'total_gross_sales': 3435.0,
                    'total_units_sold': '3',
                    'units_from_ads': 0
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
        'brand': 'Cuticolor',
        'size': 2
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/marketing-report/product-performance', query_string=query_params, headers=headers, content_type='application/json'
    )
    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_product_performance_missing_from_date(test_client):
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
        '/api/v1/marketing-report/product-performance',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_product_performance_missing_to_date(test_client):
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
        '/api/v1/marketing-report/product-performance',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_product_performance_missing_marketplace(test_client):
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
        '/api/v1/marketing-report/product-performance',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_product_performance_no_data_found(test_client):
    """
    TEST CASE: This is the fourth negative test case.
    It returns a failure response if no data found for given filters.
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
        '/api/v1/marketing-report/product-performance',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=404, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)

    time.sleep(1)

# -------------------product performance zone -----------------------------#


def test_performance_zone_positive(test_client):
    """
        TEST CASE: This is a positive test case. Return a successful response if fields are validated and user login successfully.
    """

    expected_response = {
        'data': {
            'objects': {
                'pagination_metadata': {
                    'current_page': 1,
                    'has_next_page': True,
                    'has_previous_page': False,
                    'next_page': 2,
                    'page_size': 2,
                    'previous_page': None,
                    'total_items': 9,
                    'total_pages': 5
                }
            },
            'result': [
                {
                    '_asin': 'B087N7L9QV',
                    '_brand': 'Sterlomax',
                    '_category': 'Health & Beauty',
                    '_product_image': 'https://m.media-amazon.com/images/I/51nLPBUIbLS.jpg',
                    '_product_name': 'SterloMax I75-75% Isopropyl Alcohol-based Hand Rub Sanitizer and Disinfectant',
                    '_sku': 'F3-4BQZ-7TEM',
                    '_subcategory': 'Hand Sanitizers',
                    'acos': 0,
                    'ad_conversion_rate': 0,
                    'category_rank': 1427,
                    'conversion_rate': 22.09302,
                    'cpc': 0,
                    'ctr': 0,
                    'impressions': 0,
                    'order_from_ads': 0,
                    'organic_sales': 4549.55,
                    'organic_sales_percentage': 100,
                    'organic_sessions': 65,
                    'organic_units': 19,
                    'page_views': 86,
                    'roas': 0,
                    'sales_from_ads': 0,
                    'sessions': 65,
                    'subcategory_rank': 4,
                    'tacos': 0,
                    'total_ad_clicks': 0,
                    'total_ad_spends': 0,
                    'total_gross_sales': 4549.55,
                    'total_units_sold': 19,
                    'units_from_ads': 0
                },
                {
                    '_asin': 'B087MZ777D',
                    '_brand': 'Sterlomax',
                    '_category': 'Health & Beauty',
                    '_product_image': 'https://m.media-amazon.com/images/I/51nLPBUIbLS.jpg',
                    '_product_name': 'Sterlomax 75% Isopropyl Alcohol-based Hand Rub Sanitizer and Disinfectant 500 ml -Pack of 4, Blue',
                    '_sku': 'SA-LBHR-PF5W',
                    '_subcategory': 'Hand Sanitizers',
                    'acos': 0,
                    'ad_conversion_rate': 0,
                    'category_rank': 1427,
                    'conversion_rate': 9.61538,
                    'cpc': 0,
                    'ctr': 0,
                    'impressions': 0,
                    'order_from_ads': 0,
                    'organic_sales': 3687.55,
                    'organic_sales_percentage': 100,
                    'organic_sessions': 43,
                    'organic_units': 5,
                    'page_views': 52,
                    'roas': 0,
                    'sales_from_ads': 0,
                    'sessions': 43,
                    'subcategory_rank': 4,
                    'tacos': 0,
                    'total_ad_clicks': 0,
                    'total_ad_spends': 0,
                    'total_gross_sales': 3687.55,
                    'total_units_sold': 5,
                    'units_from_ads': 0
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
        'zone': 'OPPORTUNITY_ZONE',
        'sort_by': 'TOTAL_SALES',
        'sort_order': 'ASC',
        'size': 2
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/marketing-report/performance-by-zone', query_string=query_params, headers=headers, content_type='application/json'
    )
    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_performance_zone_positive_second(test_client):
    """
        TEST CASE: This is a positive test case. Return a successful response if fields are validated and user login successfully.
    """

    expected_response = {
        'data': {
            'objects': {
                'pagination_metadata': {
                    'current_page': 1,
                    'has_next_page': False,
                    'has_previous_page': False,
                    'next_page': None,
                    'page_size': 2,
                    'previous_page': None,
                    'total_items': 2,
                    'total_pages': 1
                }
            },
            'result': [
                {
                    '_asin': 'B0B12FBHQ5',
                    '_brand': 'Cuticolor',
                    '_category': 'Beauty',
                    '_product_image': 'https://m.media-amazon.com/images/I/61t7w54WFeL.jpg',
                    '_product_name': 'Cuticolor Long Lasting No-ammonia, No PPD Natural Hair Color Cream 60 Gram, Black',
                    '_sku': 'Cuticolor || Black',
                    '_subcategory': 'Chemical Hair Dyes',
                    'acos': 0,
                    'ad_conversion_rate': 0,
                    'category_rank': 22445,
                    'conversion_rate': 4.61538,
                    'cpc': 0,
                    'ctr': 0,
                    'impressions': 0,
                    'order_from_ads': 0,
                    'organic_sales': 3600,
                    'organic_sales_percentage': 100,
                    'organic_sessions': 47,
                    'organic_units': 3,
                    'page_views': 65,
                    'roas': 0,
                    'sales_from_ads': 0,
                    'sessions': 47,
                    'subcategory_rank': 134,
                    'tacos': 0,
                    'total_ad_clicks': 0,
                    'total_ad_spends': 0,
                    'total_gross_sales': 3600,
                    'total_units_sold': 3,
                    'units_from_ads': 0
                },
                {
                    '_asin': 'B0B1XMH2MF',
                    '_brand': 'Cuticolor',
                    '_category': 'Beauty',
                    '_product_image': 'https://m.media-amazon.com/images/I/617I6vVCMQL.jpg',
                    '_product_name': 'Natural Long Lasting BLACK Hair Color Cream 60 Gram (Pack of 2) No-ammonia, No PPD…',
                    '_sku': 'Cuti/Black/2',
                    '_subcategory': 'Chemical Hair Dyes',
                    'acos': 0,
                    'ad_conversion_rate': 0,
                    'category_rank': 40872,
                    'conversion_rate': 50,
                    'cpc': 0,
                    'ctr': 0,
                    'impressions': 0,
                    'order_from_ads': 0,
                    'organic_sales': 2400,
                    'organic_sales_percentage': 100,
                    'organic_sessions': 2,
                    'organic_units': 1,
                    'page_views': 2,
                    'roas': 0,
                    'sales_from_ads': 0,
                    'sessions': 2,
                    'subcategory_rank': 223,
                    'tacos': 0,
                    'total_ad_clicks': 0,
                    'total_ad_spends': 0,
                    'total_gross_sales': 2400,
                    'total_units_sold': 1,
                    'units_from_ads': 0
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
        'brand': 'Cuticolor',
        'zone': 'OPPORTUNITY_ZONE',
        'sort_by': 'TOTAL_SALES',
        'sort_order': 'ASC',
        'size': 2
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/marketing-report/performance-by-zone', query_string=query_params, headers=headers, content_type='application/json'
    )
    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_performance_zone_missing_from_date(test_client):
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
        'marketplace': 'AMAZON',
        'zone': 'OPPORTUNITY_ZONE',
        'sort_by': 'TOTAL_SALES',
        'sort_order': 'ASC'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/marketing-report/performance-by-zone',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_performance_zone_missing_to_date(test_client):
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
        'marketplace': 'AMAZON',
        'zone': 'OPPORTUNITY_ZONE',
        'sort_by': 'TOTAL_SALES',
        'sort_order': 'ASC'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/marketing-report/performance-by-zone',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_performance_zone_missing_marketplace(test_client):
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
        'to_date': datetime.now().date(),
        'from_date': datetime.now().date(),
        'zone': 'OPPORTUNITY_ZONE',
        'sort_by': 'TOTAL_SALES',
        'sort_order': 'ASC'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/marketing-report/performance-by-zone',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_performance_zone_missing_zone(test_client):
    """
    TEST CASE: This is the fourth negative test case.
    It returns a failure response if marketplace is missing from request parameter
    """

    expected_response = {
        'error': {
            'zone': 'Zone is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    query_params = {
        'to_date': datetime.now().date(),
        'from_date': datetime.now().date(),
        'marketplace': 'AMAZON',
        'sort_by': 'TOTAL_SALES',
        'sort_order': 'ASC'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/marketing-report/performance-by-zone',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_performance_zone_no_data_found(test_client):
    """
    TEST CASE: This is the fifth negative test case.
    It returns a failure response if no data found for given filters.
    """

    expected_response = {'message': 'No data found.', 'status': False}

    query_params = {
        'from_date': datetime.now().date(),
        'to_date': datetime.now().date(),
        'marketplace': 'AMAZON',
        'brand': 'Himalaya',
        'zone': 'OPPORTUNITY_ZONE',
        'sort_by': 'TOTAL_SALES',
        'sort_order': 'ASC',
        'size': 2
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/marketing-report/performance-by-zone',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=404, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_performance_zone_missing_sortby(test_client):
    """
    TEST CASE: This is the sixth negative test case.
    It returns a failure response if marketplace is missing sort_by request parameter
    """

    expected_response = {
        'error': {
            'sort_by': 'Sort By is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    query_params = {
        'to_date': datetime.now().date(),
        'from_date': datetime.now().date(),
        'marketplace': 'AMAZON',
        'sort_order': 'ASC'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/marketing-report/performance-by-zone',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_performance_zone_missing_sortorder(test_client):
    """
    TEST CASE: This is the seventh negative test case.
    It returns a failure response if marketplace is missing sort_order request parameter
    """

    expected_response = {
        'error': {
            'sort_order': 'Sort Order is required.'
        },
        'message': 'Enter correct input.',
        'status': False
    }

    query_params = {
        'to_date': datetime.now().date(),
        'from_date': datetime.now().date(),
        'marketplace': 'AMAZON',
        'sort_by': 'TOTAL_SALES'
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    api_response = test_client.get(
        '/api/v1/marketing-report/performance-by-zone',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=400, received=api_response.status_code)

    assert validate_response(
        expected=expected_response, received=api_response.json)
