"""Contain all the urls of apis"""
from app.views.asp_brand_analytics import BrandAnalyticsReports
from app.views.asp_fba_concessions_reports_view import FbaConcessionsReports
from app.views.asp_fba_inventory_api_view import AspFbaInventoryApiView
from app.views.asp_fba_payments_reports_view import FbaPaymentsReports
from app.views.asp_fba_replacements_report_view import create_fba_replacements_report
from app.views.asp_fba_replacements_report_view import get_fba_replacements_report
from app.views.asp_fba_replacements_report_view import verify_fba_replacements_report
from app.views.asp_fba_sales_reports import FbaSalesReports
from app.views.asp_finance_view import AspFinanceView
from app.views.asp_item_master_report_view import AspItemMasterReport
from app.views.asp_ledger_summary_view import create_ledger_summary_report
from app.views.asp_ledger_summary_view import get_ledger_summary_report
from app.views.asp_ledger_summary_view import verify_ledger_summary_report
from app.views.asp_order_report_view import AspOrderReportView
from app.views.asp_product_fees import AspProductFees
from app.views.asp_product_performance_view import AspProductPerformanceView
from app.views.asp_returns_report_view import create_returns_csv_prime_report
from app.views.asp_returns_report_view import get_returns_report_flat_file_attributes
from app.views.asp_returns_report_view import verify_returns_csv_prime_report
from app.views.asp_sales_traffic_view import SalesTrafficReport
from app.views.asp_settlement_report_v2_view import export_settlement_v2_csv
from app.views.asp_settlement_report_v2_view import get_settlement_report_v2
from app.views.asp_settlement_report_v2_view import request_settlement_report_v2
from app.views.asp_settlement_report_view import get_report
from app.views.asp_tax_report_view import create_gst_b2b_report
from app.views.asp_tax_report_view import create_gst_b2c_report
from app.views.asp_tax_report_view import create_stock_transfer_report
from app.views.asp_tax_report_view import get_gst_b2b_report
from app.views.asp_tax_report_view import get_gst_b2c_report
from app.views.asp_tax_report_view import get_stock_transfer_report
from app.views.asp_tax_report_view import verify_gst_b2b_report
from app.views.asp_tax_report_view import verify_gst_b2c_report
from app.views.asp_tax_report_view import verify_stock_transfer_report
from app.views.asp_vendor_retail_report_view import VendorRetailReports
from app.views.az_ad_auth import AzAdAuth
from app.views.az_ads_campaigns_report import AzAdsReport
from app.views.billing_and_plans_view import BillingPlansView
from app.views.common_view import get_attachment
from app.views.common_view import get_health_check
from app.views.dashboard_view import DashboardView
from app.views.marketing_report_view import MarketingReportView
from app.views.members_view import MembersView
from app.views.orders_api_view import AZOrdersView
from app.views.payment_view import PaymentView
from app.views.profile_view import ProfileView
from app.views.queue_task_view import QueueTaskView
from app.views.sales_api_view import AZSalesView
from app.views.sp_auth import SpAuth
from app.views.user_view import amazon_login
from app.views.user_view import authenticate
from app.views.user_view import create_user
from app.views.user_view import get_callback_template
from app.views.user_view import get_user_account_list
from app.views.user_view import get_user_info
from app.views.user_view import google_login
from app.views.user_view import idp_callback
from app.views.user_view import idp_logout
from flask import Blueprint
from flask import g
from flask import request


# template_dir = os.path.abspath('app/views/users')
v1_blueprints = Blueprint(name='v1', import_name='api1')


@v1_blueprints.before_request
def before_blueprint():
    """This method executed in the beginning of the request."""
    g.time_log = 0
    g.request_path = request.path


@v1_blueprints.after_request
def after_blueprint(response):
    """This method executed in the end of the request."""
    response.headers['Time-Log'] = g.time_log
    # logger.info(f'{response.status_code}: {g.request_path}: {g.time_log}')
    # Uncomment above line while debugging to see API response time in logger file.
    return response


##### User #####
v1_blueprints.add_url_rule(
    '/user/register', view_func=create_user, methods=['POST'])
v1_blueprints.add_url_rule(
    '/user/authenticate', view_func=authenticate, methods=['POST'])

##### COGNOTO IDP  #####
##### Initiate Google Login #####
v1_blueprints.add_url_rule(
    '/user/auth/google', view_func=google_login, methods=['GET'])

##### Initiate Amazon Login #####
v1_blueprints.add_url_rule(
    '/user/auth/amazon', view_func=amazon_login, methods=['GET'])

##### Backend IDP (Google/Amazon) Callback URL#####
v1_blueprints.add_url_rule(
    '/user/auth/idp/callback', view_func=idp_callback, methods=['POST'])

v1_blueprints.add_url_rule(
    '/user/auth/idp/logout', view_func=idp_logout, methods=['GET'])

##### Front End Callback Url #####
v1_blueprints.add_url_rule(
    '/user/auth/idp/callback/front-end', view_func=get_callback_template, methods=['GET'])

v1_blueprints.add_url_rule(
    '/user', view_func=get_user_info, methods=['GET'])
v1_blueprints.add_url_rule(
    '/user/account/list', view_func=get_user_account_list, methods=['GET'], defaults={'x-account': True})

##### Amazon Seller Connect #####
v1_blueprints.add_url_rule(
    '/account/connect/amazon', view_func=SpAuth.asp_authorisation, methods=['GET'], defaults={'x-account': True})
v1_blueprints.add_url_rule(
    '/account/connect/amazon/callback/front-end', view_func=SpAuth.get_az_callback_template, methods=['GET'])  # front end callback url
v1_blueprints.add_url_rule(
    '/account/connect/amazon/callback', view_func=SpAuth.asp_authorisation_callback, methods=['POST'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/account/connect/amazon/data-sync', view_func=SpAuth.asp_data_sync, methods=['POST'], defaults={'x-account': True})
v1_blueprints.add_url_rule(
    '/account/connect/amazon/sp-profile-info/store', view_func=SpAuth.store_sp_info, methods=['POST'])

v1_blueprints.add_url_rule(
    '/account/connect/amazon/lwa-refresh-token', view_func=SpAuth.app_lwa_refresh_token, methods=['GET'])
v1_blueprints.add_url_rule(
    '/account/connect/flipkart', view_func=SpAuth.fsp_authorisation, methods=['GET'], defaults={'x-account': True})
v1_blueprints.add_url_rule(
    '/account/connect/amazon/flipkart/front-end', view_func=SpAuth.get_fsp_callback_template, methods=['GET'])  # front end callback url
v1_blueprints.add_url_rule(
    '/account/connect/flipkart/callback', view_func=SpAuth.fsp_authorisation_callback, methods=['POST'], defaults={'x-account': True})

##### Item Master Report #####
v1_blueprints.add_url_rule(
    '/create-item-master-report', view_func=AspItemMasterReport.create_item_master_report, methods=['GET'], defaults={'x-account': True})
v1_blueprints.add_url_rule(
    '/verify-item-master-report', view_func=AspItemMasterReport.verify_item_master_report, methods=['GET'], defaults={'x-account': True})
v1_blueprints.add_url_rule(
    '/get-item-master-report', view_func=AspItemMasterReport.get_item_master_report, methods=['GET'], defaults={'x-account': True})

##### Settlement Report #####
v1_blueprints.add_url_rule(
    '/get-settlement-report', view_func=get_report, methods=['GET'])

##### Ledger summary report #####
v1_blueprints.add_url_rule(
    '/create-ledger-summary-report', view_func=create_ledger_summary_report, methods=['GET'])
v1_blueprints.add_url_rule(
    '/verify-ledger-summary-report', view_func=verify_ledger_summary_report, methods=['GET'])
v1_blueprints.add_url_rule(
    '/get-ledger-summary-report', view_func=get_ledger_summary_report, methods=['GET'])

##### brand analytics report #####
v1_blueprints.add_url_rule(
    '/create-brand-analytics-report', view_func=BrandAnalyticsReports.create_brand_analytics_report, methods=['GET'], defaults={'x-account': True})
v1_blueprints.add_url_rule(
    '/verify-brand-analytics-report', view_func=BrandAnalyticsReports.verify_brand_analytics_report, methods=['GET'], defaults={'x-account': True})
v1_blueprints.add_url_rule(
    '/get-brand-analytics-report', view_func=BrandAnalyticsReports.get_ba_report, methods=['GET'], defaults={'x-account': True})

##### vendor retail report #####
v1_blueprints.add_url_rule(
    '/create-vendor-retail-report', view_func=VendorRetailReports.create_vendor_retail_report, methods=['GET'], defaults={'x-account': True})
v1_blueprints.add_url_rule(
    '/verify-vendor-retail-report', view_func=VendorRetailReports.verify_vendor_retail_report, methods=['GET'], defaults={'x-account': True})
v1_blueprints.add_url_rule(
    '/get-vendor-retail-report', view_func=VendorRetailReports.get_vendor_retail_report, methods=['GET'], defaults={'x-account': True})
##### sales traffic report #####
v1_blueprints.add_url_rule(
    '/create-sales-traffic-report', view_func=SalesTrafficReport.create_sales_traffic_report, methods=['GET'], defaults={'x-account': True})
v1_blueprints.add_url_rule(
    '/verify-sales-traffic-report', view_func=SalesTrafficReport.verify_sales_traffic_report, methods=['GET'], defaults={'x-account': True})
v1_blueprints.add_url_rule(
    '/get-sales-traffic-report', view_func=SalesTrafficReport.get_sales_traffic_report, methods=['GET'], defaults={'x-account': True})

##### Order Report #####
v1_blueprints.add_url_rule(
    '/create-order-report', view_func=AspOrderReportView.create_order_report, methods=['GET'], defaults={'x-account': True})
v1_blueprints.add_url_rule(
    '/verify-order-report', view_func=AspOrderReportView.verify_order_report, methods=['GET'])
v1_blueprints.add_url_rule(
    '/get-order-report', view_func=AspOrderReportView.get_order_report, methods=['GET'])

v1_blueprints.add_url_rule(
    'create/order-report/data-invoicing', view_func=AspOrderReportView.create_order_report_data_invoicing, methods=['GET'])

v1_blueprints.add_url_rule(
    '/create-returns-csv-prime-report', view_func=create_returns_csv_prime_report, methods=['GET'])
v1_blueprints.add_url_rule(
    '/verify-returns-csv-prime-report/<report_id>', view_func=verify_returns_csv_prime_report, methods=['GET'])

v1_blueprints.add_url_rule(
    '/get-returns-report-flat-file-attributes/<report_id>', view_func=get_returns_report_flat_file_attributes, methods=['GET'])

##### Settlement Report V2#####
v1_blueprints.add_url_rule(
    '/settlement-report-v2', view_func=request_settlement_report_v2, methods=['GET'])
v1_blueprints.add_url_rule(
    '/settlement-report-v2/<reference_id>', view_func=get_settlement_report_v2, methods=['GET'])
v1_blueprints.add_url_rule(
    '/export-settlement-report-v2', view_func=export_settlement_v2_csv, methods=['GET'])

##### Tax Report #####

# GST MTR B2B Report
v1_blueprints.add_url_rule(
    '/tax-report/create-gst-merchant-tax-report/b2b', view_func=create_gst_b2b_report, methods=['GET'])
v1_blueprints.add_url_rule(
    '/tax-report/verify-gst-merchant-tax-report/b2b/<report_id>', view_func=verify_gst_b2b_report, methods=['GET'])
v1_blueprints.add_url_rule(
    '/tax-report/get-gst-merchant-tax-report/b2b/<report_id>', view_func=get_gst_b2b_report, methods=['GET'])

# GST MTR B2C Report
v1_blueprints.add_url_rule(
    '/tax-report/create-gst-merchant-tax-report/b2c', view_func=create_gst_b2c_report, methods=['GET'])
v1_blueprints.add_url_rule(
    '/tax-report/verify-gst-merchant-tax-report/b2c/<report_id>', view_func=verify_gst_b2c_report, methods=['GET'])
v1_blueprints.add_url_rule(
    '/tax-report/get-gst-merchant-tax-report/b2c/<report_id>', view_func=get_gst_b2c_report, methods=['GET'])

# Stock Transfer Report
v1_blueprints.add_url_rule(
    '/tax-report/create-stock-transfer-report', view_func=create_stock_transfer_report, methods=['GET'])
v1_blueprints.add_url_rule(
    '/tax-report/verify-stock-transfer-report/<report_id>', view_func=verify_stock_transfer_report, methods=['GET'])
v1_blueprints.add_url_rule(
    '/tax-report/get-stock-transfer-report/<report_id>', view_func=get_stock_transfer_report, methods=['GET'])

# Fulfillment by Amazon (FBA) reports
v1_blueprints.add_url_rule(
    '/create/fba/sales-reports/customer-shipment', view_func=FbaSalesReports.create_customer_shipment_sales, methods=['GET'], defaults={'x-account': True})
v1_blueprints.add_url_rule(
    '/verify/fba/sales-reports/customer-shipment', view_func=FbaSalesReports.verify_customer_shipment_sales, methods=['GET'], defaults={'x-account': True})
v1_blueprints.add_url_rule(
    '/download/fba/sales-reports/customer-shipment', view_func=FbaSalesReports.get_customer_shipment_sales, methods=['GET'], defaults={'x-account': True})

# Concessions Reports
v1_blueprints.add_url_rule(
    '/create-fba-returns-report', view_func=FbaConcessionsReports.create_returns_report, methods=['GET'], defaults={'x-account': True})
v1_blueprints.add_url_rule(
    '/verify-fba-returns-report', view_func=FbaConcessionsReports.verify_returns_report, methods=['GET'], defaults={'x-account': True})
v1_blueprints.add_url_rule(
    '/get-fba-returns-report', view_func=FbaConcessionsReports.get_returns_report, methods=['GET'], defaults={'x-account': True})

# Payment Reports
v1_blueprints.add_url_rule(
    '/create-fba-reimbursements-report', view_func=FbaPaymentsReports.create_reimbursements_report, methods=['GET'], defaults={'x-account': True})
v1_blueprints.add_url_rule(
    '/verify-fba-reimbursements-report', view_func=FbaPaymentsReports.verify_reimbursements_report, methods=['GET'], defaults={'x-account': True})
v1_blueprints.add_url_rule(
    '/get-fba-reimbursements-report', view_func=FbaPaymentsReports.get_reimbursements_report, methods=['GET'], defaults={'x-account': True})


v1_blueprints.add_url_rule(
    '/create-fba-replacements-report', view_func=create_fba_replacements_report, methods=['GET'])
v1_blueprints.add_url_rule(
    '/verify-fba-replacements-report/<reference_id>', view_func=verify_fba_replacements_report, methods=['GET'])
v1_blueprints.add_url_rule(
    '/get-fba-replacements-report/<reference_id>', view_func=get_fba_replacements_report, methods=['GET'])


# Dashboard Endpoint for sales stats
v1_blueprints.add_url_rule(
    '/dashboard/sales-statistics', view_func=DashboardView.get_sales_statistics, methods=['GET'], defaults={'x-account': True})

# Dashboard Endpoint for sales stats
v1_blueprints.add_url_rule(
    '/dashboard/sales-statistics-bar-graph', view_func=DashboardView.get_sales_statistics_bar_graph, methods=['GET'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/dashboard/sales-statistics/hourly-graph', view_func=DashboardView.get_hourly_sales_stats, methods=['GET'], defaults={'x-account': True})

#  Dashboard Endpoint for getting top/least selling items and sales trends
v1_blueprints.add_url_rule(
    '/dashboard/sales-and-trends', view_func=DashboardView.get_sales_and_trends, methods=['GET'], defaults={'x-account': True})

# Dashboard Endpoint for gross sales comp
v1_blueprints.add_url_rule(
    '/dashboard/gross-sales-comp', view_func=DashboardView.get_gross_sales_comp, methods=['GET'], defaults={'x-account': True})

#  Endpoint for updating brand
v1_blueprints.add_url_rule(
    '/dashboard/update-brand', view_func=DashboardView.update_item_master_brand, methods=['POST'], defaults={'x-account': True})

#  Endpoint for getting inventory by location
v1_blueprints.add_url_rule(
    '/dashboard/inventory-by-location', view_func=DashboardView.get_inventory_by_location, methods=['GET'], defaults={'x-account': True})

#  Endpoint for getting marketplace breakdown
v1_blueprints.add_url_rule(
    '/dashboard/marketplace-breakdown', view_func=DashboardView.get_marketplace_breakdown, methods=['GET'], defaults={'x-account': True})

#  Endpoint for getting profit-loss
v1_blueprints.add_url_rule(
    '/dashboard/profit-loss', view_func=DashboardView.get_profit_and_loss, methods=['GET'], defaults={'x-account': True})

# Endpoint for product
v1_blueprints.add_url_rule(
    '/product', view_func=AspItemMasterReport.get_product, methods=['GET'], defaults={'x-account': True})

# Endpoint for brand
v1_blueprints.add_url_rule(
    '/brand', view_func=AspItemMasterReport.get_brand, methods=['GET'], defaults={'x-account': True})

# Endpoint for category
v1_blueprints.add_url_rule(
    '/category', view_func=AspItemMasterReport.get_category, methods=['GET'], defaults={'x-account': True})

# Endpoint for product performance
v1_blueprints.add_url_rule(
    '/dashboard/product-performance', view_func=AspProductPerformanceView.get_product_performance, methods=['GET'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/dashboard/product-performance/<seller_sku>/day-graph', view_func=AspProductPerformanceView.get_product_performance_day_graph, methods=['GET'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/dashboard/product-performance/<seller_sku>/heatmap', view_func=AspProductPerformanceView.get_product_performance_heatmap, methods=['GET'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/dashboard/product-performance/export', view_func=AspProductPerformanceView.export, methods=['GET'], defaults={'x-account': True})

#  Endpoint for Refund Insights
v1_blueprints.add_url_rule(
    '/refund-insights', view_func=AspProductPerformanceView.get_refund_insights, methods=['GET'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/refund-insights/export', view_func=AspProductPerformanceView.export_unclaimend_report, methods=['GET'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/glance-view-planner', view_func=SalesTrafficReport.get_glance_summary, methods=['GET'], defaults={'x-account': True})

# Endpoint for inventory
v1_blueprints.add_url_rule(
    '/dashboard/inventory', view_func=AspItemMasterReport.get_inventory_levels, methods=['GET'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/dashboard/inventory/export', view_func=AspItemMasterReport.export_item_master_report, methods=['GET'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/dashboard/inventory/product-graph', view_func=AspItemMasterReport.get_inventory_stats, methods=['GET'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/dashboard/inventory/fulfillment-center-map-stats', view_func=AspItemMasterReport.get_inventory_stats_fc_level, methods=['GET'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/dashboard/inventory/<product>/attributes', view_func=AspItemMasterReport.get_product_attr_details, methods=['GET'], defaults={'x-account': True})

# Endpoint for updating cogs
v1_blueprints.add_url_rule(
    '/dashboard/inventory/update-cogs', view_func=DashboardView.update_item_master_cogs, methods=['POST'], defaults={'x-account': True})
v1_blueprints.add_url_rule(
    '/dashboard/inventory/import-cogs', view_func=AspItemMasterReport.import_item_master_cogs, methods=['POST'], defaults={'x-account': True})


# Endpoint for sales by region
v1_blueprints.add_url_rule(
    '/dashboard/sales-by-region', view_func=AspProductPerformanceView.get_product_sales_by_region, methods=['GET'], defaults={'x-account': True})


# view for orders API
v1_blueprints.add_url_rule(
    '/orders', view_func=AZOrdersView.get_orders, methods=['GET'], defaults={'x-account': True})
v1_blueprints.add_url_rule(
    '/order-items', view_func=AZOrdersView.get_order_items, methods=['GET'])


# Initiate Amazon Ad's API
v1_blueprints.add_url_rule(
    '/account/connect/amazon/ads-api', view_func=AzAdAuth.az_ads, methods=['GET'])
v1_blueprints.add_url_rule(
    '/account/connect/amazon/ads-api/callback/front-end', view_func=AzAdAuth.get_az_ads_callback_template, methods=['GET'])  # front end callback url
v1_blueprints.add_url_rule(
    '/account/connect/amazon/ads-api/callback', view_func=AzAdAuth.az_ads_callback, methods=['POST'], defaults={'x-account': True})
v1_blueprints.add_url_rule(
    '/account/connect/amazon/ads-profile-info/store', view_func=AzAdAuth.store_profile, methods=['POST'], defaults={'x-account': True})
v1_blueprints.add_url_rule(
    '/account/connect/amazon/ads-data-sync', view_func=AzAdsReport.az_ad_data_sync, methods=['GET'], defaults={'x-account': True})

# Ad's API
v1_blueprints.add_url_rule(
    '/ad/sponsored/advertised-product/request', view_func=AzAdsReport.create_product_advertised_report, methods=['GET'])
v1_blueprints.add_url_rule(
    '/ad/sponsored/advertised-product/verify', view_func=AzAdsReport.get_advertised_report_verified, methods=['POST'])
v1_blueprints.add_url_rule(
    '/ad/sponsored/advertised-product/retrieve', view_func=AzAdsReport.retrieve_advertised_report, methods=['POST'])

v1_blueprints.add_url_rule(
    '/ad/sponsored/brand/request', view_func=AzAdsReport.create_brand_report, methods=['GET'])
v1_blueprints.add_url_rule(
    '/ad/sponsored/brand/verify', view_func=AzAdsReport.get_brand_report_verified, methods=['POST'])
v1_blueprints.add_url_rule(
    '/ad/sponsored/brand/retrieve', view_func=AzAdsReport.retrieve_brand_report, methods=['POST'])

v1_blueprints.add_url_rule(
    '/ad/sponsored/display/request', view_func=AzAdsReport.create_display_report, methods=['GET'])
v1_blueprints.add_url_rule(
    '/ad/sponsored/display/verify', view_func=AzAdsReport.get_display_report_verified, methods=['POST'])
v1_blueprints.add_url_rule(
    '/ad/sponsored/display/retrieve', view_func=AzAdsReport.retrieve_display_report, methods=['POST'])

# Amazon Product Fees API
v1_blueprints.add_url_rule(
    '/amazon/fees-estimate-sku', view_func=AspProductFees.get_fees_estimate_by_sku, methods=['GET'])
v1_blueprints.add_url_rule(
    '/amazon/fees-estimates-asin', view_func=AspProductFees.get_fees_estimate_by_asin, methods=['GET'])

# Amazon Finance Event Api
v1_blueprints.add_url_rule(
    '/amazon/list-financial-events', view_func=AspFinanceView.get_financial_events, methods=['GET'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/amazon/financial-events/order-fees', view_func=AspFinanceView.get_order_fees, methods=['POST'], defaults={'x-account': True})

# Amazon Catalog API
v1_blueprints.add_url_rule(
    '/amazon/catalog/get-item-details', view_func=AspItemMasterReport.get_item_updated, methods=['POST'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/queue-task', view_func=QueueTaskView.get_list, methods=['GET'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/queue-task/status-list', view_func=QueueTaskView.get_status, methods=['GET'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/queue-task/export', view_func=QueueTaskView.export_queue, methods=['GET'], defaults={'x-account': True})

# Marketing Reports API
v1_blueprints.add_url_rule(
    '/marketing-report/ad-impact-graph', view_func=MarketingReportView.get_ad_graph_data, methods=['GET'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/marketing-report/sales-period', view_func=MarketingReportView.get_sales_period, methods=['GET'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/marketing-report/costvs-metrics', view_func=MarketingReportView.get_costvs_metrics, methods=['GET'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/marketing-report/product-performance', view_func=MarketingReportView.get_performance, methods=['GET'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/marketing-report/product-performance/export', view_func=MarketingReportView.get_performance_export, methods=['GET'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/marketing-report/performance-by-zone', view_func=MarketingReportView.get_ad_performance_by_zone, methods=['GET'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/marketing-report/performance-by-zone/create', view_func=MarketingReportView.create_ad_performance_by_zone, methods=['GET'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/marketing-report/performance-by-zone/export', view_func=MarketingReportView.export_performance_by_zone, methods=['GET'], defaults={'x-account': True})

# check worker flow - get sales and traffic report
v1_blueprints.add_url_rule(
    '/amazon/sales-traffic-report', view_func=SalesTrafficReport.get_sales_and_traffic_worker, methods=['GET'], defaults={'x-account': True})

# get attachment report
v1_blueprints.add_url_rule(
    '/get-attachment/export/<export_id>', view_func=get_attachment, methods=['GET'])

##### Health Check URL #####
v1_blueprints.add_url_rule(
    '/health-check', view_func=get_health_check, methods=['GET'])

# members api
v1_blueprints.add_url_rule(
    '/members/invite-user', view_func=MembersView.invite_user, methods=['POST'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/members/check-invite-validity', view_func=MembersView.is_valid_invite, methods=['GET'])

v1_blueprints.add_url_rule(
    '/members/set-password', view_func=MembersView.set_password, methods=['POST'])

v1_blueprints.add_url_rule(
    '/members/get-users', view_func=MembersView.get_users, methods=['GET'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/members/get-accounts', view_func=MembersView.get_accounts, methods=['GET'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/members/change-user-status', view_func=MembersView.change_user_status, methods=['POST'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/members/edit', view_func=MembersView.update_member_details, methods=['POST'], defaults={'x-account': True})

# billing and plans api
v1_blueprints.add_url_rule(
    '/billing-and-plans/get-plans', view_func=BillingPlansView.get_plans, methods=['GET'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/billing-and-plans/add-plans', view_func=BillingPlansView.add_plans, methods=['POST'], defaults={'x-account': True})

# payment api
v1_blueprints.add_url_rule(
    '/payment/get-all-plans', view_func=PaymentView.get_all_plans, methods=['GET'], defaults={'x-account': True})

# v1_blueprints.add_url_rule(
#     '/payment/create-plans', view_func=PaymentView.create_plan, methods=['GET'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/payment/callback', view_func=PaymentView.standard_callback, methods=['GET', 'POST'])

# v1_blueprints.add_url_rule(
#     '/payment/create-payment-link', view_func=PaymentView.create_payment_link, methods=['GET'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/payment/create-subscription', view_func=PaymentView.create_subscription, methods=['POST'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/payment/cancel-subscription', view_func=PaymentView.cancel_subscription, methods=['GET'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/payment/verify-payment-checkout', view_func=PaymentView.verify_payment_checkout, methods=['POST'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/payment/get-all-susbcriptions', view_func=PaymentView.get_all_subscriptions, methods=['GET'], defaults={'x-account': True})

# profile api

v1_blueprints.add_url_rule(
    '/profile/get', view_func=ProfileView.get_profile, methods=['GET'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/profile/add-update', view_func=ProfileView.add_update_profile, methods=['POST'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/profile/change-account-status', view_func=ProfileView.change_account_status, methods=['POST'], defaults={'x-account': True})

# sales api

v1_blueprints.add_url_rule(
    '/get-sales-api-report', view_func=AZSalesView.get_sales, methods=['GET'], defaults={'x-account': True})

v1_blueprints.add_url_rule(
    '/cancel-sales-api-reports', view_func=AZSalesView.cancel_reports, methods=['GET'])

v1_blueprints.add_url_rule(
    '/sync-sales-api', view_func=AZSalesView.sync_sales, methods=['GET'])

# Fba Inventory API

v1_blueprints.add_url_rule(
    '/get-fba-inventory-level', view_func=AspFbaInventoryApiView.get_fba_inventory_levels, methods=['POST'], defaults={'x-account': True})
