"""
This file contains the configuration of settings and initialization of the testing framework for the project.
"""

from datetime import datetime as dt
from datetime import timedelta
import json
import os
import uuid

from app import config_data
from app import db
from app.models.account import Account
from app.models.az_item_master import AzItemMaster
from app.models.az_ledger_summary import AzLedgerSummary
from app.models.az_order_report import AzOrderReport
from app.models.az_performance_zone import AzPerformanceZone
from app.models.az_product_performance import AzProductPerformance
from app.models.az_sales_traffic_asin import AzSalesTrafficAsin
from app.models.az_sales_traffic_summary import AzSalesTrafficSummary
from app.models.az_sponsored_brand import AzSponsoredBrand
from app.models.az_sponsored_display import AzSponsoredDisplay
from app.models.az_sponsored_product import AzSponsoredProduct
from app.models.postal_code_master import PostalCodeMaster
from app.models.subscription import Subscription
from app.models.user import User
from app.models.user_account import UserAccount
from app.models.user_invite import UserInvite
from flask_sqlalchemy import SQLAlchemy
from main import application
import pytest
from sqlalchemy.sql import update

"""Sample data values for testing"""
USER_EMAIL_ADDRESS = 'unittest@bombaysoftwares.com'
USER_EMAIL_ADDRESS_UPDATE = 'test@bombaysoftwares.com'
ADMIN_EMAIL_ADDRESS = 'testadmin@bombaysoftwares.com'
TEST_FILE_PATH = 'tests/assets/rivulet.jpg'
_db = SQLAlchemy
DEFAULT_EXP_CAT_UUID = uuid.uuid4()


@pytest.fixture(scope='session')
def app():
    """
        Initialize the new application instance for the testing with following settings :
            - Default db to test database.
            - Create Schema in the test db as per the models.
            - Yield the application Object.SQLALCHEMY_TEST_DATABASE_URI
            - Once test session is ended drop all the tables.
    """

    application.config.update({
        'TESTING': True,
    })
    application.config.update({
        'SQLALCHEMY_DATABASE_URI': config_data.get('SQLALCHEMY_TEST_DATABASE_URI')
    })
    # application.config.update({
    #     'SQLALCHEMY_TRACK_MODIFICATIONS': False
    # })

    with application.app_context():
        db.init_app(application)
        db.create_all()

    ctx = application.app_context()
    ctx.push()

    # do the testing
    yield application

    # tear down
    with application.app_context():
        db.session.remove()
        db.drop_all()
        if 'x_authorization' in os.environ:
            del os.environ['x_authorization']
        if 'x_account' in os.environ:
            del os.environ['x_account']
        if 'invite_uuid' in os.environ:
            del os.environ['invite_uuid']
        if 'invite_token' in os.environ:
            del os.environ['invite_token']

    ctx.pop()


@pytest.fixture(scope='session')
def test_client(app):
    """
        This method is being used to fetch the app object to test the dashboard endpoint test cases.
    """

    today_date = dt.now().date()
    three_days_early = today_date - timedelta(days=3)
    today_date_str = today_date.strftime('%Y-%m-%d')
    three_days_early_str = three_days_early.strftime('%Y-%m-%d')
    fifty_days_early = today_date - timedelta(days=50)
    fifty_days_early_str = fifty_days_early.strftime('%Y-%m-%d')

    User.add(email='testpurposephp@gmail.com', first_name='john', last_name='doe',
             password='qwerty@123Q')

    User.add(email='gopro@gmail.com', first_name='go', last_name='pro',
             password='lopkfmm')

    account = Account(uuid='7e36ba8f-073a-4dee-9ba5-d7880c3c3ba9', asp_id='B3D3499BOYNHX', primary_user_id=1, asp_credentials={'oauth_state': '6e36ba8f-073a-4dee-9ba5-d7880c3c3ea9', 'mws_auth_token': None, 'seller_partner_id': 'A3D3499BOYNHU', 'spapi_oauth_code': 'RHPkuEPXLtLEUMGSPDhA', 'spapi_oauth_code_updated_at': 1700129997, 'access_token': 'Atza|IwEBIObPkdJ4o55vTHNYsz3_zydaPkuGh_28_AtJ6ZDpUpvb3w6Tx-lmvabHsAAzaXDTb9o8Ro07VZKY3ySs0HVK2T8jqXLbIgUCHlDJTJ_YK8lqqmT9zLcr5q2CHq9dm5SMj1vEb6m7OQbZ6704EEB_g4woJSNh7omr2l5DevWQMeqqy4fJrIg5f_pXd_X4Yt7fSCkIZjYyF7QSVCtoOKbZF8dI8ySKzlYmg2iMhG7wLVULlS7OTNS3S05i4uwrc17BIU55Zeb9OX8qxardOwt-jfhtLAZCKLWhHI5VV-oceCBCIvHnUGraLW_5aQVLhfWFMUk', 'token_type': 'bearer', 'expires_in': 3600, 'refresh_token': 'Atzr|IwEBILorgk7E-NdK8zVP6lcIF-HXItFgE9y7UZOX7oIfp_jqF4wyiq1ujcq2eYSSXgcn_Iv44csjB53jT1T9rrN5OdhjM7cFktvqZm0DfJUCTZjUCfiG8Znht9WBkarhLG791HdFEshncjaAp2NKPPBGy9EHGgYFVGPUPSX82yxOhuXoALUTTfO2cUuNZANLBgWVC3_3qaWqtymnf2h8jVZoyjQltuG8YjyzlG1_T0HRuzk6DZKqdQk38smOt87D3xycOc4koxPIfcmE6vu4U1W29BWswO5eQOq5RtnYRu50_7FNjppXgbAPmiU9Q__WxHJKaC8', 'refresh_token_updated_at': 1700129998, 'created_at': 1700129997, 'updated_at': ''},
                      )

    db.session.add(account)

    Subscription.add_update(account_id='7e36ba8f-073a-4dee-9ba5-d7880c3c3ba9', reference_subscription_id='xyz',
                            plan_id=1, payment_id=1, status='active', start_date='2023-10-22 23:45:01', end_date='2029-10-22 23:45:01')

    UserAccount.add(user_id=1, account_id=1, brand=[
                    'Cuticolor', 'Sterlomax'], category=['Beauty'])
    UserAccount.add(user_id=2, account_id=1, brand=[
                    'Cuticolor', 'Sterlomax'], category=['Beauty'])

    user_invite_pending = UserInvite(uuid='11659bfa-d2c9-47e9-acc7-4bb8b94cddf4', first_name='ricky', last_name='ponting', email='point@xyz.com', status='PENDING', invited_by_user_id=1, invited_by_account_id=1,
                                     brand=['Cuticolor'], category=['Beauty'], created_at=1694504215, last_sent_at=1694504215, updated_at=1694504233)

    db.session.add(user_invite_pending)

    user_invite_accepted = UserInvite(uuid='239bfa-d2c9-47e9-acc7-4bb8b94cddf4', first_name='tom', last_name='jerry', email='tomjerry@xyz.com', status='ACCEPTED', invited_by_user_id=1, invited_by_account_id=1,
                                      brand=['Cuticolor', 'Sterlomax'], category=['Beauty'], created_at=1694504215, last_sent_at=1694504215, updated_at=1694504233)

    db.session.add(user_invite_accepted)

    db.session.commit()

    with open('tests/assets/az_item_master.json', 'r') as az_item_master:                   # type: ignore  # noqa: FKA100
        item_master_data = json.load(az_item_master)

    for item in item_master_data:
        item_master = AzItemMaster(account_id=item.get('account_id'), selling_partner_id=item.get('selling_partner_id'), item_name=item.get('item_name'), item_description=item.get('item_description'), listing_id=item.get('listing_id'),
                                   seller_sku=item.get('seller_sku'), price=0 if item.get('price') == '' else item.get('price'), asin=item.get('asin'), product_id=item.get('product_id'),
                                   fulfillment_channel=item.get('fulfillment_channel'), status=item.get('status'), max_retail_price=0 if item.get('max_retail_price') == '' else item.get('max_retail_price'),
                                   brand=item.get('brand'), category=item.get('category'),
                                   subcategory=item.get('subcategory'), category_rank=0 if item.get('category_rank') == '' else item.get('category_rank'), subcategory_rank=0 if item.get('subcategory_rank') == '' else item.get('subcategory_rank'), face_image=item.get('face_image'), cogs=50, created_at=1699350755)

        db.session.add(item_master)
        db.session.commit()

    with open('tests/assets/az_sales_traffic_asin.json', 'r') as sales_traffic_asin:            # type: ignore  # noqa: FKA100
        sales_traffic_asin_data = json.load(sales_traffic_asin)

    for data in sales_traffic_asin_data:
        AzSalesTrafficAsin.insert_or_update(account_id=data.get('account_id'), asp_id=data.get('asp_id'), parent_asin=data.get('parent_asin'), child_asin=data.get('child_asin'),
                                            payload_date=today_date_str if data.get('brand') != 'Sterlomax' else three_days_early_str, units_ordered=data.get('units_ordered'), units_ordered_b2b=data.get('units_ordered_b2b'),
                                            ordered_product_sales_amount=data.get('ordered_product_sales_amount'), ordered_product_sales_amount_b2b=data.get('ordered_product_sales_amount_b2b'),
                                            ordered_product_sales_currency_code=data.get(
                                                'ordered_product_sales_currency_code'),
                                            ordered_product_sales_currency_code_b2b=data.get(
                                                'ordered_product_sales_currency_code_b2b'),
                                            total_order_items=data.get('total_order_items'), total_order_items_b2b=data.get('total_order_items_b2b'),
                                            browser_sessions=data.get('browser_sessions'), browser_sessions_b2b=data.get('browser_sessions_b2b'),
                                            mobile_app_sessions=data.get('mobile_app_sessions'), mobile_app_sessions_b2b=data.get('mobile_app_sessions_b2b'),
                                            sessions=data.get('sessions'), sessions_b2b=data.get('sessions_b2b'),
                                            browser_session_percentage=data.get(
                                                'browser_session_percentage'),
                                            browser_session_percentage_b2b=data.get(
                                                'browser_session_percentage_b2b'),
                                            mobile_app_session_percentage=data.get(
                                                'mobile_app_session_percentage'),
                                            mobile_app_session_percentage_b2b=data.get(
                                                'mobile_app_session_percentage_b2b'),
                                            session_percentage=data.get('session_percentage'), session_percentage_b2b=data.get('session_percentage_b2b'),
                                            browser_page_views=data.get('browser_page_views'), browser_page_views_b2b=data.get('browser_page_views_b2b'),
                                            mobile_app_page_views=data.get('mobile_app_page_views'), mobile_app_page_views_b2b=data.get('mobile_app_page_views_b2b'),
                                            page_views=data.get('page_views'), page_views_b2b=data.get('page_views_b2b'),
                                            browser_page_views_percentage=data.get(
                                                'browser_page_views_percentage'),
                                            browser_page_views_percentage_b2b=data.get(
                                                'browser_page_views_percentage_b2b'),
                                            mobile_app_page_views_percentage=data.get(
                                                'mobile_app_page_views_percentage'),
                                            mobile_app_page_views_percentage_b2b=data.get(
                                                'mobile_app_page_views_percentage_b2b'),
                                            page_views_percentage=data.get('page_views_percentage'), page_views_percentage_b2b=data.get('page_views_percentage_b2b'),
                                            buy_box_percentage=data.get('buy_box_percentage'), buy_box_percentage_b2b=data.get('buy_box_percentage_b2b'),
                                            unit_session_percentage=data.get('unit_session_percentage'), unit_session_percentage_b2b=data.get('unit_session_percentage_b2b'),
                                            asin_granularity=data.get('asin_granularity'))

    with open('tests/assets/az_sales_traffic_summary.json', 'r') as sales_traffic_summary:              # type: ignore  # noqa: FKA100
        sales_traffic_summary_data = json.load(sales_traffic_summary)

    for data in sales_traffic_summary_data:
        AzSalesTrafficSummary.insert_or_update(account_id=data.get('account_id'), asp_id=data.get('asp_id'), date=today_date_str, ordered_product_sales_amount=data.get('ordered_product_sales_amount'),
                                               ordered_product_sales_currency_code=data.get('ordered_product_sales_currency_code'), units_ordered=data.get('units_ordered'),
                                               ordered_product_sales_amount_b2b=data.get('ordered_product_sales_amount_b2b'), ordered_product_sales_currency_code_b2b=data.get('ordered_product_sales_currency_code_b2b'),
                                               units_ordered_b2b=data.get('units_ordered_b2b'), total_order_items=data.get('total_order_items'), total_order_items_b2b=data.get('total_order_items_b2b'),
                                               average_sales_per_order_item_amount=data.get('average_sales_per_order_item_amount'), average_sales_per_order_item_currency_code=data.get('average_sales_per_order_item_currency_code'),
                                               average_sales_per_order_item_amount_b2b=data.get('average_sales_per_order_item_amount_b2b'), average_sales_per_order_item_currency_code_b2b=data.get('average_sales_per_order_item_currency_code_b2b'),
                                               average_units_per_order_item=data.get('average_units_per_order_item'), average_units_per_order_item_b2b=data.get('average_units_per_order_item_b2b'),
                                               average_selling_price_amount=data.get('average_selling_price_amount'), average_selling_price_currency_code=data.get('average_selling_price_currency_code'),
                                               average_selling_price_amount_b2b=data.get('average_selling_price_amount_b2b'), average_selling_price_currency_code_b2b=data.get('average_selling_price_currency_code_b2b'),
                                               units_refunded=data.get('units_refunded'), refund_rate=data.get('refund_rate'), claims_granted=data.get('claims_granted'), claims_amount_amount=data.get('claims_amount_amount'),
                                               claims_amount_currency_code=data.get('claims_amount_currency_code'), shipped_product_sales_amount=data.get('shipped_product_sales_amount'),
                                               shipped_product_sales_currency_code=data.get('shipped_product_sales_currency_code'), units_shipped=data.get('units_shipped'), orders_shipped=data.get('orders_shipped'),
                                               browser_page_views=data.get('browser_page_views'), browser_page_views_b2b=data.get('browser_page_views_b2b'), mobile_app_page_views=data.get('mobile_app_page_views'),
                                               mobile_app_page_views_b2b=data.get('mobile_app_page_views_b2b'), page_views=data.get('page_views'), page_views_b2b=data.get('page_views_b2b'),
                                               browser_sessions=data.get('browser_sessions'), browser_sessions_b2b=data.get('browser_sessions_b2b'), mobile_app_sessions=data.get('mobile_app_sessions'),
                                               mobile_app_sessions_b2b=data.get('mobile_app_sessions_b2b'), sessions=data.get('sessions'), sessions_b2b=data.get('sessions_b2b'),
                                               buy_box_percentage=data.get('buy_box_percentage'), buy_box_percentage_b2b=data.get('buy_box_percentage_b2b'), order_item_session_percentage=data.get('order_item_session_percentage'),
                                               order_item_session_percentage_b2b=data.get('order_item_session_percentage_b2b'), unit_session_percentage=data.get('unit_session_percentage'),
                                               unit_session_percentage_b2b=data.get('unit_session_percentage_b2b'), average_offer_count=data.get('average_offer_count'), average_parent_items=data.get('average_parent_items'),
                                               feedback_received=data.get('feedback_received'), negative_feedback_received=data.get('negative_feedback_received'),
                                               received_negative_feedback_rate=data.get('received_negative_feedback_rate'), date_granularity=data.get('date_granularity'))

    with open('tests/assets/az_order_report.json', 'r') as az_order_report:             # type: ignore  # noqa: FKA100
        az_order_report_data = json.load(az_order_report)

    for data in az_order_report_data:

        order = AzOrderReport.get_by_az_order_id_and_sku(
            selling_partner_id=data.get('selling_partner_id'), amazon_order_id=data.get('amazon_order_id'), sku=data.get('sku'))

        if order:
            AzOrderReport.update_orders(
                account_id=data.get('account_id'),                                              # type: ignore  # noqa: FKA100
                selling_partner_id=data.get('selling_partner_id'),                              # type: ignore  # noqa: FKA100
                amazon_order_id=data.get('amazon_order_id'),                                    # type: ignore  # noqa: FKA100
                merchant_order_id=data.get('merchant_order_id'), purchase_date=today_date_str,                                                                                                                                                               # type: ignore  # noqa: FKA100
                last_updated_date=data.get('last_updated_date'), order_status=data.get('order_status'), fulfillment_channel=data.get('fulfillment_channel'), sales_channel=data.get('sales_channel'), ship_service_level=data.get('ship_service_level'),            # type: ignore  # noqa: FKA100
                product_name=data.get('product_name'), sku=data.get('sku'), asin=data.get('asin'), item_status=data.get('item_status'), quantity=data.get('quantity', 0), currency=data.get('currency'),                                                            # type: ignore  # noqa: FKA100
                item_price=data.get('item_price', 0), item_tax=data.get('item_tax', 0), shipping_price=data.get('shipping_price', 0), shipping_tax=data.get('shipping_tax', 0),                                                                                     # type: ignore  # noqa: FKA100
                gift_wrap_price=data.get('gift_wrap_price', 0), gift_wrap_tax=data.get('gift_wrap_tax', 0), item_promotion_discount=data.get('item_promotion_discount', 0),                                                                                         # type: ignore  # noqa: FKA100
                ship_promotion_discount=data.get('ship_promotion_discount', 0), ship_city=data.get('ship_city'), ship_state=data.get('ship_state'), ship_postal_code=data.get('ship_postal_code'),                                                                  # type: ignore  # noqa: FKA100
                ship_country=data.get('ship_country'))                  # type: ignore  # noqa: FKA100
        else:
            AzOrderReport.add(account_id=data.get('account_id'), selling_partner_id=data.get('selling_partner_id'), amazon_order_id=data.get('amazon_order_id'), merchant_order_id=data.get('merchant_order_id'), purchase_date=today_date_str,              # type: ignore  # noqa: FKA100
                              last_updated_date=data.get('last_updated_date'), order_status=data.get(                                                                                                                                                               # type: ignore  # noqa: FKA100
                'order_status'), fulfillment_channel=data.get('fulfillment_channel'), sales_channel=data.get('sales_channel'),                                                                                                                                      # type: ignore  # noqa: FKA100
                ship_service_level=data.get('ship_service_level'), product_name=data.get(                                                                                                                                                                           # type: ignore  # noqa: FKA100
                'product_name'), sku=data.get('sku'), asin=data.get('asin'), item_status=data.get('item_status'),                                                                                                                                                   # type: ignore  # noqa: FKA100
                quantity=data.get('quantity', 0), currency=data.get('currency'), item_price=data.get('item_price', 0), item_tax=data.get(                                                                                                                           # type: ignore  # noqa: FKA100
                'item_tax', 0), shipping_price=data.get('shipping_price', 0), shipping_tax=data.get('shipping_tax', 0),                                                                                                                                             # type: ignore  # noqa: FKA100
                gift_wrap_price=data.get('gift_wrap_price', 0), gift_wrap_tax=data.get(                                                                                                                                         # type: ignore  # noqa: FKA100
                'gift_wrap_tax', 0), item_promotion_discount=data.get('item_promotion_discount', 0),                                                                                                                            # type: ignore  # noqa: FKA100
                ship_promotion_discount=data.get('ship_promotion_discount', 0), ship_city=data.get(                                                                                                                             # type: ignore  # noqa: FKA100
                'ship_city'), ship_state=data.get('ship_state'), ship_postal_code=data.get('ship_postal_code'),                                                                                                                 # type: ignore  # noqa: FKA100
                ship_country=data.get('ship_country'))                      # type: ignore  # noqa: FKA100

    with open('tests/assets/az_product_performance.json', 'r') as az_product_performance_file:             # type: ignore  # noqa: FKA100
        az_product_performance_data = json.load(az_product_performance_file)

    for data in az_product_performance_data:
        product_performance = AzProductPerformance(account_id=data.get('account_id'), asp_id=data.get('asp_id'), category=data.get('category'), brand=data.get('brand'), az_order_id=data.get('az_order_id'), seller_sku=data.get('seller_sku'), expenses=data.get('expenses'), gross_sales=data.get('gross_sales'),                                             # type: ignore  # noqa: FKA100
                                                   seller_order_id=data.get('seller_order_id'), market_place_fee=data.get('market_place_fee'), forward_fba_fee=data.get('forward_fba_fee'), reverse_fba_fee=data.get('reverse_fba_fee'),                                                                                                                         # type: ignore  # noqa: FKA100
                                                   units_sold=data.get('units_sold'), units_returned=data.get('units_returned'), returns=data.get('returns'), shipment_date=today_date_str if data.get('shipment_date') != '' else None, refund_date=fifty_days_early_str if data.get('refund_date') != '' else None, created_at=data.get('created_at'))             # type: ignore  # noqa: FKA100

        db.session.add(product_performance)
        db.session.commit()

    with open('tests/assets/az_ledger_summary.json', 'r') as az_ledger_summary_file:             # type: ignore  # noqa: FKA100
        az_ledger_summary = json.load(az_ledger_summary_file)

    for data in az_ledger_summary:
        ledger_summary = AzLedgerSummary(account_id='7e36ba8f-073a-4dee-9ba5-d7880c3c3ba9', asp_id='B3D3499BOYNHX', category='', brand='', date=today_date_str, fnsku=data.get('fnsku'), asin=data.get('asin'), msku=data.get('msku'),
                                         title=data.get('title'), disposition=data.get('disposition'), starting_warehouse_balance=data.get('starting_warehouse_balance'), in_transit_btw_warehouse=data.get('in_transit_btw_warehouse'), receipts=data.get('receipts'), customer_shipments=data.get('customer_shipments'),
                                         customer_returns=data.get('customer_returns'), vendor_returns=data.get('vendor_returns'), warehouse_transfer=data.get('warehouse_transfer'), found=data.get('found'), lost=data.get('lost'), damaged=data.get('damaged'),
                                         disposed=data.get('disposed'), other_events=data.get('other_events'), ending_warehouse_balance=data.get('ending_warehouse_balance'), unknown_events=data.get('unknown_events'), location=data.get('location'))             # type: ignore  # noqa: FKA100

        db.session.add(ledger_summary)
        db.session.commit()

    with open('app/static/files/postal_code_sep_12.json', 'r') as postal_code_file:             # type: ignore  # noqa: FKA100
        postal_code = json.load(postal_code_file)
        db.session.bulk_insert_mappings(PostalCodeMaster, postal_code)                          # type: ignore  # noqa: FKA100
        db.session.commit()

    with open('tests/assets/az_sponsored_brand.json', 'r') as sponsored_brand_file:             # type: ignore  # noqa: FKA100
        sponsored_brand = json.load(sponsored_brand_file)
        db.session.bulk_insert_mappings(AzSponsoredBrand, sponsored_brand)                          # type: ignore  # noqa: FKA100
        db.session.execute(update(AzSponsoredBrand).values(
            payload_date=today_date_str))
        db.session.commit()

    with open('tests/assets/az_sponsored_display.json', 'r') as sponsored_display_file:             # type: ignore  # noqa: FKA100
        sponsored_display = json.load(sponsored_display_file)
        db.session.bulk_insert_mappings(AzSponsoredDisplay, sponsored_display)                          # type: ignore  # noqa: FKA100

        db.session.execute(update(AzSponsoredDisplay).values(
            payload_date=today_date_str))
        db.session.commit()

    with open('tests/assets/az_sponsored_product.json', 'r') as sponsored_product_file:             # type: ignore  # noqa: FKA100
        sponsored_product = json.load(sponsored_product_file)
        db.session.bulk_insert_mappings(AzSponsoredProduct, sponsored_product)                          # type: ignore  # noqa: FKA100

        db.session.execute(update(AzSponsoredProduct).values(
            payload_date=today_date_str))
        db.session.commit()

    with open('tests/assets/az_performance_zone.json', 'r') as az_performance_zone_file:             # type: ignore  # noqa: FKA100
        az_performance_zone = json.load(az_performance_zone_file)
        db.session.bulk_insert_mappings(AzPerformanceZone, az_performance_zone)                          # type: ignore  # noqa: FKA100
        db.session.execute(update(AzPerformanceZone).values(
            metrics_date=today_date_str))
        db.session.commit()

    # login

    body = {
        'email': 'testpurposephp@gmail.com',
        'password': 'qwerty@123Q'
    }
    test_client_obj = app.test_client()
    api_response = test_client_obj.post(
        '/api/v1/user/authenticate', json=body, content_type='application/json'
    )

    os.environ['x_authorization'] = api_response.json.get(
        'data').get('objects').get('user').get('auth_token')
    os.environ['x_account'] = '7e36ba8f-073a-4dee-9ba5-d7880c3c3ba9'

    # ledger_summay_data = [{'date': '2023-06-15', 'fnsku': 'X000U2GC3F', 'asin': 'ABCDEFG',
    #                        'msku': '12345ABC', 'title': 'jewellery', 'disposition': 'SELLABLE', 'starting_warehouse_balance': 6,
    #                        'in_transit_btw_warehouse': 0, 'receipts': 0, 'customer_shipments': 0, 'customer_returns': 0, 'vendor_returns': 0,
    #                        'warehouse_transfer': 0, 'found': 0, 'lost': 0, 'damaged': 0, 'disposed': 0, 'other_events': 0, 'ending_warehouse_balance': 6,
    #                        'unknown_events': 0, 'location': 'QWER'},
    #                       {'date': '2023-06-15', 'fnsku': 'X000U2GC3F', 'asin': 'ABCDEFG',
    #                           'msku': '12345ABC', 'title': 'jewellery', 'disposition': 'SELLABLE', 'starting_warehouse_balance': 6,
    #                           'in_transit_btw_warehouse': 0, 'receipts': 0, 'customer_shipments': 0, 'customer_returns': 0, 'vendor_returns': 0,
    #                           'warehouse_transfer': 0, 'found': 0, 'lost': 0, 'damaged': 0, 'disposed': 0, 'other_events': 0, 'ending_warehouse_balance': 6,
    #                           'unknown_events': 0, 'location': 'QWER'},
    #                       {'date': '2023-06-15', 'fnsku': 'X000U2GC3F', 'asin': 'ZXCVB',
    #                           'msku': '12345ABC', 'title': 'jewellery', 'disposition': 'SELLABLE', 'starting_warehouse_balance': 6,
    #                           'in_transit_btw_warehouse': 0, 'receipts': 0, 'customer_shipments': 0, 'customer_returns': 0, 'vendor_returns': 0,
    #                           'warehouse_transfer': 0, 'found': 0, 'lost': 0, 'damaged': 0, 'disposed': 0, 'other_events': 0, 'ending_warehouse_balance': 6,
    #                           'unknown_events': 0, 'location': 'ABCD'}
    #                       ]

    # for ledger in ledger_summay_data:
    #     AzLedgerSummary.add(selling_partner_id=config_data.get('SP_ID'), date=ledger.get('date'), fnsku=ledger.get('fnsku'), asin=ledger.get('asin'),
    #                         msku=ledger.get('msku'), title=ledger.get('title'), disposition=ledger.get(
    #         'disposition'), starting_warehouse_balance=ledger.get('starting_warehouse_balance'),
    #         in_transit_btw_warehouse=ledger.get('in_transit_btw_warehouse'), receipts=ledger.get(
    #         'receipts'), customer_shipments=ledger.get('customer_shipments'),
    #         customer_returns=ledger.get('customer_returns'), vendor_returns=ledger.get(
    #         'vendor_returns'), warehouse_transfer=ledger.get('warehouse_transfer'),
    #         found=ledger.get('found'), lost=ledger.get('lost'), damaged=ledger.get('damaged'), disposed=ledger.get(
    #         'disposed'), other_events=ledger.get('other_events'), ending_warehouse_balance=ledger.get('ending_warehouse_balance'),
    #         unknown_events=ledger.get('unknown_events'), location=ledger.get('location'))

    yield app.test_client()


def validate_status_code(expected, received):
    """
        This method is a generic method being used to validate the status_code.
            - Checking if received response matches expected.
    """
    return expected == received


# def validate_response(expected, recieved):
#     """
#         This method is a generic method being used to validate the response.
#             - Checking if received response matches expected.
#     """
#     for (expected_key, expected_value), (recieved_key, recieved_value) in zip(
#             expected.items(), recieved.items()
#     ):
#         if expected_key == recieved_key:
#             if isinstance(expected_value, dict) and isinstance(recieved_value, dict):
#                 return validate_response(expected=expected_value, recieved=recieved_value)

#             if isinstance(expected_value, list) and isinstance(recieved_value, list):
#                 for r_val, e_val in zip_longest(recieved_value, expected_value):  # type: ignore  # noqa: FKA100
#                     if not validate_response(expected=e_val, recieved=r_val):
#                         return False
#                 return True

#             return expected_value == '*' or expected_value == recieved_value
#         return expected_value == '*'

def validate_response(**kwargs):
    """
        This method is a generic method being used to validate the response.
            - Checking if received response matches expected.
        1. checks if received and expected are dict
                if they are dict then sort them key wise so that seq of keys in both dict is same
                then iterate over all keys and check if they match
                now recursively call validate response on expected and received value of that key
        2. checks if received and expected are lists and then iterates over the list
                    and if yes then recursively calls validate response on each index  of expected  and received
        3. if received and expected are not dicts neither lists then it directly tries matching them and returns true if macthed
    """
    received = kwargs.get('received')
    expected = kwargs.get('expected')

    if type(received) == dict and type(expected) == dict:
        received_keys = list(kwargs.get('received').keys())
        received_keys.sort()
        sorted_received_dict = {i: kwargs.get(
            'received')[i] for i in received_keys}
        expected_keys = list(kwargs.get('expected').keys())
        expected_keys.sort()
        sorted_expected_dict = {i: kwargs.get(
            'expected')[i] for i in expected_keys}
        for (r_key, r_val), (e_key, e_val) in zip(sorted_received_dict.items(), sorted_expected_dict.items()):
            if r_key == e_key:
                if not validate_response(received=r_val, expected=e_val):
                    return False
            else:
                return False
        return True

    if type(received) == list and type(expected) == list:

        for rec, exp in zip(received, expected):
            if not validate_response(received=rec, expected=exp):
                return False
        return True

    if received == expected or expected == '*':
        return True
