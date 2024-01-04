""" test module for billing and plan"""
import os

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


# -------------- test case for get-plans---------------------#

def test_get_plans_positive_first(test_client):
    """
    TEST CASE: This is the first positive test case. It returns a successful response.
    """

    expected_response = {
        'data': {
            'result': [
                {
                    'amount': 0.0,
                    'currency': 'INR',
                    'description': 'Free - 7 Day trial',
                    'discount': '0',
                    'feature': {
                        'brand analytics': {
                            'market_basket': True,
                            'repeat_customer': True,
                            'sales_by_geography': True,
                            'sentiment_analysis': True
                        },
                        'customer_support': True,
                        'executive_summary': {
                            'business_report': True,
                            'live_sales_and_units_analytics': True,
                            'marketplace_breakdown': True,
                            'sales_by_region': True
                        },
                        'financial_control_center': {
                            'item_performance': True,
                            'movers_and_shakers': True,
                            'profit_and_loss': True
                        },
                        'glance_view_planner': True,
                        'historical_data': '1 Month',
                        'inventory_anager': {
                            'inventory_by_item': True,
                            'inventory_by_location': True,
                            'inventory_status': True,
                            'x-ray': True
                        },
                        'keyword_planner': True,
                        'marketing_analytics': {
                            'cost_vs_advertising': True,
                            'growth_opportunities': True,
                            'impact_graph': True,
                            'marketing_item_performance': True,
                            'sales_conversion': True
                        },
                        'no_of_users': 1,
                        'number_of_orders_per_month': 10000,
                        'reimbursement_profit_recovery': True,
                        'review_automation': True
                    },
                    'name': 'Free',
                    'period': 'weekly',
                    'reference_plan_id': 'ECOMM_FREE_TRAIL',
                    'status': 'ACTIVE'
                },
                {
                    'amount': 500.0,
                    'currency': 'INR',
                    'description': 'Silver - monthly',
                    'discount': '0',
                    'feature': {
                        'brand analytics': {
                            'market_basket': False,
                            'repeat_customer': False,
                            'sales_by_geography': False,
                            'sentiment_analysis': False
                        },
                        'customer_support': False,
                        'executive_summary': {
                            'business_report': True,
                            'live_sales_and_units_analytics': True,
                            'marketplace_breakdown': True,
                            'sales_by_region': False
                        },
                        'financial_control_center': {
                            'item_performance': False,
                            'movers_and_shakers': True,
                            'profit_and_loss': True
                        },
                        'glance_view_planner': False,
                        'historical_data': '1 Month',
                        'inventory_anager': {
                            'inventory_by_item': True,
                            'inventory_by_location': False,
                            'inventory_status': False,
                            'x-ray': False
                        },
                        'keyword_planner': False,
                        'marketing_analytics': {
                            'cost_vs_advertising': True,
                            'growth_opportunities': False,
                            'impact_graph': True,
                            'marketing_item_performance': False,
                            'sales_conversion': True
                        },
                        'no_of_users': 1,
                        'number_of_orders_per_month': 10000,
                        'reimbursement_profit_recovery': False,
                        'review_automation': False
                    },
                    'name': 'Silver',
                    'period': 'monthly',
                    'reference_plan_id': 'plan_MhepjOZcOXNt5d',
                    'status': 'ACTIVE'
                },
                {
                    'amount': 6000.0,
                    'currency': 'INR',
                    'description': 'Silver - yearly',
                    'discount': '0',
                    'feature': {
                        'brand analytics': {
                            'market_basket': False,
                            'repeat_customer': False,
                            'sales_by_geography': False,
                            'sentiment_analysis': False
                        },
                        'customer_support': False,
                        'executive_summary': {
                            'business_report': True,
                            'live_sales_and_units_analytics': True,
                            'marketplace_breakdown': True,
                            'sales_by_region': False
                        },
                        'financial_control_center': {
                            'item_performance': False,
                            'movers_and_shakers': True,
                            'profit_and_loss': True
                        },
                        'glance_view_planner': False,
                        'historical_data': '1 Month',
                        'inventory_anager': {
                            'inventory_by_item': True,
                            'inventory_by_location': False,
                            'inventory_status': False,
                            'x-ray': False
                        },
                        'keyword_planner': False,
                        'marketing_analytics': {
                            'cost_vs_advertising': True,
                            'growth_opportunities': False,
                            'impact_graph': True,
                            'marketing_item_performance': False,
                            'sales_conversion': True
                        },
                        'no_of_users': 1,
                        'number_of_orders_per_month': 10000,
                        'reimbursement_profit_recovery': False,
                        'review_automation': False
                    },
                    'name': 'Silver',
                    'period': 'yearly',
                    'reference_plan_id': 'plan_Mheqe0yBkJgu7i',
                    'status': 'ACTIVE'
                },
                {
                    'amount': 700.0,
                    'currency': 'INR',
                    'description': 'Gold - monthly',
                    'discount': '0',
                    'feature': {
                        'brand analytics': {
                            'market_basket': True,
                            'repeat_customer': True,
                            'sales_by_geography': True,
                            'sentiment_analysis': True
                        },
                        'customer_support': True,
                        'executive_summary': {
                            'business_report': True,
                            'live_sales_and_units_analytics': True,
                            'marketplace_breakdown': True,
                            'sales_by_region': True
                        },
                        'financial_control_center': {
                            'item_performance': True,
                            'movers_and_shakers': True,
                            'profit_and_loss': True
                        },
                        'glance_view_planner': True,
                        'historical_data': '1 year',
                        'inventory_anager': {
                            'inventory_by_item': True,
                            'inventory_by_location': True,
                            'inventory_status': True,
                            'x-ray': True
                        },
                        'keyword_planner': True,
                        'marketing_analytics': {
                            'cost_vs_advertising': True,
                            'growth_opportunities': True,
                            'impact_graph': True,
                            'marketing_item_performance': True,
                            'sales_conversion': True
                        },
                        'no_of_users': 5,
                        'number_of_orders_per_month': None,
                        'reimbursement_profit_recovery': True,
                        'review_automation': True
                    },
                    'name': 'Gold',
                    'period': 'monthly',
                    'reference_plan_id': 'plan_MherOiEZmqT5Ma',
                    'status': 'ACTIVE'
                },
                {
                    'amount': 8400.0,
                    'currency': 'INR',
                    'description': 'Gold - yearly',
                    'discount': '0',
                    'feature': {
                        'brand analytics': {
                            'market_basket': True,
                            'repeat_customer': True,
                            'sales_by_geography': True,
                            'sentiment_analysis': True
                        },
                        'customer_support': True,
                        'executive_summary': {
                            'business_report': True,
                            'live_sales_and_units_analytics': True,
                            'marketplace_breakdown': True,
                            'sales_by_region': True
                        },
                        'financial_control_center': {
                            'item_performance': True,
                            'movers_and_shakers': True,
                            'profit_and_loss': True
                        },
                        'glance_view_planner': True,
                        'historical_data': '1 year',
                        'inventory_anager': {
                            'inventory_by_item': True,
                            'inventory_by_location': True,
                            'inventory_status': True,
                            'x-ray': True
                        },
                        'keyword_planner': True,
                        'marketing_analytics': {
                            'cost_vs_advertising': True,
                            'growth_opportunities': True,
                            'impact_graph': True,
                            'marketing_item_performance': True,
                            'sales_conversion': True
                        },
                        'no_of_users': 5,
                        'number_of_orders_per_month': None,
                        'reimbursement_profit_recovery': True,
                        'review_automation': True
                    },
                    'name': 'Gold',
                    'period': 'yearly',
                    'reference_plan_id': 'plan_MhertJNrNJb65Q',
                    'status': 'ACTIVE'
                },
                {
                    'amount': 1000.0,
                    'currency': 'INR',
                    'description': 'Enterprise - monthly',
                    'discount': '0',
                    'feature': {
                        'brand analytics': {
                            'market_basket': None,
                            'repeat_customer': None,
                            'sales_by_geography': None,
                            'sentiment_analysis': None
                        },
                        'customer_support': None,
                        'executive_summary': {
                            'business_report': None,
                            'live_sales_and_units_analytics': None,
                            'marketplace_breakdown': None,
                            'sales_by_region': None
                        },
                        'financial_control_center': {
                            'item_performance': None,
                            'movers_and_shakers': None,
                            'profit_and_loss': None
                        },
                        'glance_view_planner': None,
                        'historical_data': None,
                        'inventory_anager': {
                            'inventory_by_item': None,
                            'inventory_by_location': None,
                            'inventory_status': None,
                            'x-ray': None
                        },
                        'keyword_planner': None,
                        'marketing_analytics': {
                            'cost_vs_advertising': None,
                            'growth_opportunities': None,
                            'impact_graph': None,
                            'marketing_item_performance': None,
                            'sales_conversion': None
                        },
                        'no_of_users': None,
                        'number_of_orders_per_month': None,
                        'reimbursement_profit_recovery': None,
                        'review_automation': None
                    },
                    'name': 'Enterprise',
                    'period': 'monthly',
                    'reference_plan_id': 'plan_MhesfE4Kf6POPX',
                    'status': 'ACTIVE'
                },
                {
                    'amount': 12000.0,
                    'currency': 'INR',
                    'description': 'Enterprise - yearly',
                    'discount': 0,
                    'feature': {
                        'brand analytics': {
                            'market_basket': None,
                            'repeat_customer': None,
                            'sales_by_geography': None,
                            'sentiment_analysis': None
                        },
                        'customer_support': None,
                        'executive_summary': {
                            'business_report': None,
                            'live_sales_and_units_analytics': None,
                            'marketplace_breakdown': None,
                            'sales_by_region': None
                        },
                        'financial_control_center': {
                            'item_performance': None,
                            'movers_and_shakers': None,
                            'profit_and_loss': None
                        },
                        'glance_view_planner': None,
                        'historical_data': None,
                        'inventory_anager': {
                            'inventory_by_item': None,
                            'inventory_by_location': None,
                            'inventory_status': None,
                            'x-ray': None
                        },
                        'keyword_planner': None,
                        'marketing_analytics': {
                            'cost_vs_advertising': None,
                            'growth_opportunities': None,
                            'impact_graph': None,
                            'marketing_item_performance': None,
                            'sales_conversion': None
                        },
                        'no_of_users': None,
                        'number_of_orders_per_month': None,
                        'reimbursement_profit_recovery': None,
                        'review_automation': None
                    },
                    'name': 'Enterprise',
                    'period': 'yearly',
                    'reference_plan_id': 'plan_Mhet4RZ7hFGP4v',
                    'status': 'ACTIVE'
                }
            ]
        },
        'message': 'Details Fetched Successfully.',
        'status': True
    }

    headers = {
        'x_authorization': os.environ['x_authorization'], 'x_account': os.environ['x_account']}

    query_params = {
    }

    api_response = test_client.get(
        '/api/v1/billing-and-plans/get-plans',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=200, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)


def test_get_plans_missing_header(test_client):
    """
    TEST CASE: This is the first negative test case. It returns a failure response if request is not authorized.
    """

    expected_response = {'message': 'Invalid Token.', 'status': False}

    headers = {}

    query_params = {
    }

    api_response = test_client.get(
        '/api/v1/billing-and-plans/get-plans',
        query_string=query_params, headers=headers)

    assert validate_status_code(
        expected=401, received=api_response.status_code)
    assert validate_response(
        expected=expected_response, received=api_response.json)
