"""Contains Amazon Inventory related API definitions."""
from collections import defaultdict
from datetime import datetime
from datetime import timedelta
import io
import json
from typing import Any
from typing import Optional

from app import config_data
from app import db
from app import export_csv_q
from app import import_item_master_cogs_q
from app import item_master_update_catalog_q
from app import logger
from app.helpers.constants import ASpReportType
from app.helpers.constants import AttachmentType
from app.helpers.constants import AzInventoryLevelColumn
from app.helpers.constants import AzPaApiBaseURL
from app.helpers.constants import AzPaApiOperations
from app.helpers.constants import AzPaApiResources
from app.helpers.constants import EntityType
from app.helpers.constants import EXCEL_ALLOWED_EXTENSIONS
from app.helpers.constants import FulfillmentChannel
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import ItemMasterStatus
from app.helpers.constants import PAGE_DEFAULT
from app.helpers.constants import PAGE_LIMIT
from app.helpers.constants import QueueName
from app.helpers.constants import QueueTaskStatus
from app.helpers.constants import ResponseMessageKeys
from app.helpers.decorators import api_time_logger
from app.helpers.decorators import brand_filter
from app.helpers.decorators import token_required
from app.helpers.utility import enum_validator
from app.helpers.utility import field_type_validator
from app.helpers.utility import generate_paapi_awssigv4
from app.helpers.utility import get_amz_date
from app.helpers.utility import get_asp_market_place_ids
from app.helpers.utility import get_created_since
from app.helpers.utility import get_created_until
from app.helpers.utility import get_pagination_meta
from app.helpers.utility import required_validator
from app.helpers.utility import send_json_response
from app.models.attachment import Attachment
from app.models.az_item_master import AzItemMaster
from app.models.az_ledger_summary import AzLedgerSummary
from app.models.az_report import AzReport
from app.models.queue_task import QueueTask
from flask import request
import numpy as np
import pandas as pd
from providers.amazon_sp_client import AmazonReportEU
import requests
from sqlalchemy import func
from sqlalchemy import text
from workers.export_csv_data_worker import ExportCsvDataWorker
from workers.item_master_worker import ItemMasterWorker
from workers.s3_worker import upload_file_and_get_object_details


class AspItemMasterReport:
    """views for asp item master inventory apis"""

    @staticmethod
    @api_time_logger
    @token_required
    def create_item_master_report(user_object, account_object):
        """create inventory report for item master using sp-apis"""

        try:
            field_types = {'marketplace': str,
                           'from_date': str}

            required_fields = ['from_date']

            data = field_type_validator(
                request_data=request.args, field_types=field_types)

            if data['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=data['data'])

            is_valid = required_validator(
                request_data=request.args, required_fields=required_fields)

            if is_valid['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=is_valid['data'])

            account_id = account_object.uuid
            asp_id = account_object.asp_id
            credentials = account_object.retrieve_asp_credentials(account_object)[
                0]

            start_time = get_created_since(request.args.get('from_date'))
            end_time = get_created_until(request.args.get('to_date'))

            payload = {
                'reportType': ASpReportType.ITEM_MASTER_LIST_ALL_DATA.value,
                'dataStartTime': start_time,
                'dataEndTime': end_time,
                'marketplaceIds': get_asp_market_place_ids()
            }

            # creating AmazonReportEU object and passing the credentials
            report = AmazonReportEU(credentials=credentials)

            # calling create report function of report object and passing the payload
            response = report.create_report(payload=payload)
            # adding the report_type, report_id to the report table
            AzReport.add(account_id=account_id, seller_partner_id=asp_id,
                         type=ASpReportType.ITEM_MASTER_LIST_ALL_DATA.value, reference_id=response['reportId'])

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=response['reportId'], error=None)

        except Exception as exception_error:
            logger.error(
                f'Exception occured while creating item master report: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

    @staticmethod
    @api_time_logger
    @token_required
    def verify_item_master_report(user_object, account_object):
        """verify processing status of document based on reportID"""

        try:

            report_id = request.args.get('report_id')

            # asp_cred = account_object.asp_credentials
            account_id = account_object.uuid

            # querying Report table to get the entry for particular report_id
            get_report = AzReport.get_by_ref_id(
                account_id=account_id, reference_id=report_id)

            if not get_report:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=report_id)

            credentials = account_object.retrieve_asp_credentials(account_object)[
                0]

            # creating AmazonReportEU object and passing the credentials
            report = AmazonReportEU(credentials=credentials)

            # calling verify_report function of report object and passing the report_id
            report_status = report.verify_report(report_id)

            # checking the processing status of the report. if complete, we update status in the table entry for that particular report_id
            if report_status['processingStatus'] != 'DONE':
                AzReport.update_status(
                    reference_id=report_id, status=report_status['processingStatus'], document_id=None)
            else:
                AzReport.update_status(
                    reference_id=report_id, status=report_status['processingStatus'], document_id=report_status['reportDocumentId'])

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=report_status, error=None)

        except Exception as exception_error:
            logger.error(
                f'Exception occured while verifying item master report: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

    @staticmethod
    @api_time_logger
    @token_required
    def get_item_master_report(user_object, account_object):
        """retrieve inventory report for item master using sp-apis"""

        try:

            report_id = request.args.get('report_id')

            account_id = account_object.uuid
            asp_id = account_object.asp_id
            credentials = account_object.retrieve_asp_credentials(account_object)[
                0]

            # querying Report table to get the entry for particular report_id
            get_report_document_id = AzReport.get_by_ref_id(
                account_id=account_id, reference_id=report_id)

            if not get_report_document_id:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=report_id)

            # getting the document_id
            report_document_id = get_report_document_id.document_id
            # creating AmazonReportEU object and passing the credentials
            report = AmazonReportEU(credentials=credentials)

            # using retrieve_report function of report object to get report
            get_report = report.retrieve_report(report_document_id)

            # getting the report document url from the response
            if 'url' in get_report:
                file_url = get_report['url']
            else:
                logger.error(
                    'File url not found for item master report')
                return send_json_response(
                    http_status=500,
                    response_status=False,
                    message_key=ResponseMessageKeys.FAILED.value
                )

            # making request to the report document url to get the file data
            logger.info('Report Url:')
            logger.info(file_url)

            # making request to the report document url to get the file data
            file_data = requests.get(file_url, stream=True)
            # logger.info('file_data.text')
            # logger.info(file_data.text)

            data_frame = pd.read_csv(io.StringIO(
                file_data.text), sep='\t', index_col=False, skiprows=0)
            data_frame = data_frame.fillna(np.nan).replace([np.nan], [None])   # type: ignore  # noqa: FKA100

            # directory = config_data.get(
            #     'UPLOAD_FOLDER') + ASpReportType.ITEM_MASTER_LIST_ALL_DATA.value.lower()
            # if not os.path.exists(directory):
            #     os.makedirs(directory)

            # data_frame.to_csv(config_data.get('UPLOAD_FOLDER') + ASpReportType.ITEM_MASTER_LIST_ALL_DATA.value.lower()
            #                   + '/{}.csv'.format(report_document_id), index=False)
            # logger.info('Columns:')
            # logger.info(data_frame.columns)

            # # iterating dataframe rows and creating model object to save data into db
            for index, row in data_frame.iterrows():

                seller_sku = row.get('seller-sku').strip(
                ) if row.get('seller-sku') is not None else None

                if seller_sku:
                    logger.info('*' * 200)
                    logger.info('Item master records insert/update')
                    logger.info(f"***{row.get('seller-sku')}***")
                    logger.info(f"***{row.get('asin1')}***")

                    item = AzItemMaster.get_item_by_sku(
                        sku=seller_sku, account_id=account_id, selling_partner_id=asp_id)

                    _item_name = row.get(
                        'item-name') if row.get('item-name') is not None else None
                    _item_description = row.get(
                        'item-description') if row.get('item-description') is not None else None
                    _listing_id = row.get(
                        'listing-id') if row.get('listing-id') is not None else None
                    _price = row.get('price') if row.get(
                        'price') is not None else 0
                    _product_id = row.get(
                        'product-id') if row.get('product-id') is not None else None
                    _asin1 = row.get('asin1') if row.get(
                        'asin1') is not None else None
                    _fulfillment_channel = FulfillmentChannel.get_name(row.get(
                        'fulfillment-channel')) if row.get('fulfillment-channel') is not None else None
                    _status = ItemMasterStatus.get_name(
                        row.get('status').upper()) if row.get('status') is not None else None
                    _max_retail_price = row.get(
                        'maximum-retail-price') if row.get('maximum-retail-price') is not None else 0

                    if item:
                        logger.info('Item master records updation')
                        logger.info(f'Update Item for: {item.account_id}')
                        AzItemMaster.update_items(account_id=account_id, selling_partner_id=asp_id, item_name=_item_name, item_description=_item_description, listing_id=_listing_id,
                                                  price=_price, seller_sku=seller_sku, asin=_asin1, product_id=_product_id,
                                                  fulfillment_channel=_fulfillment_channel, status=_status, max_retail_price=_max_retail_price)
                    else:
                        logger.info('Item master records insertion')
                        logger.info(f'Update Item for: {account_id}')
                        AzItemMaster.add(account_id=account_id, selling_partner_id=asp_id, item_name=_item_name, item_description=_item_description, listing_id=_listing_id,
                                         seller_sku=seller_sku, price=_price, asin=_asin1, product_id=_product_id,
                                         fulfillment_channel=_fulfillment_channel, status=_status, max_retail_price=_max_retail_price)

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=get_report, error=None)

        except Exception as exception_error:
            logger.error(
                f'Exception occured while retrieving item master report: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def export_item_master_report(user_object, account_object, allowed_brands):
        """To export current item master report to update cogs"""

        logged_in_user = user_object.id
        account_id = account_object.uuid
        asp_id = account_object.asp_id

        marketplace = request.args.get('marketplace')
        from_date = request.args.get('from_date')
        to_date = request.args.get('to_date')
        category = request.args.getlist('category')
        brand = request.args.getlist('brand') if 'brand' in request.args and request.args.getlist(
            'brand') else allowed_brands
        product = request.args.getlist('product')

        # validation
        params = {}
        if marketplace:
            params['marketplace'] = marketplace
        if from_date:
            params['from_date'] = from_date
        if to_date:
            params['to_date'] = to_date
        if category:
            params['category'] = category
        if not brand:
            params['brand'] = brand
        else:
            if account_object.primary_user_id != user_object.id:
                valid_brands = [b for b in brand if b in allowed_brands]
                if len(valid_brands) != len(brand):
                    return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                              message_key=ResponseMessageKeys.INVALID_BRAND.value, data=None,
                                              error=ResponseMessageKeys.ENTER_CORRECT_INPUT.value)
                params['brand'] = valid_brands
        if product:
            params['product'] = product

        field_types = {'marketplace': str, 'product': list, 'brand': list, 'from_date': str, 'to_date': str,
                       'category': list, 'sort_column': str, 'sort_order': str}

        required_fields = ['marketplace', 'from_date', 'to_date']

        enum_fields = {
            'marketplace': (marketplace, 'ProductMarketplace')
        }

        valid_enum = enum_validator(enum_fields)

        if valid_enum['is_error']:
            return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=valid_enum['data'])

        data = field_type_validator(
            request_data=params, field_types=field_types)

        if data['is_error']:
            return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                      message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                      error=data['data'])

        is_valid = required_validator(
            request_data=params, required_fields=required_fields)

        if is_valid['is_error']:
            return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                      message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                      error=is_valid['data'])

        # max_date = db.session.query(
        #     func.max(AzLedgerSummary.date)).scalar()
        # max_date_previous = None

        # if max_date is not None:
        #     max_date_previous = max_date - timedelta(days=1)

        data = {
            'logged_in_user': logged_in_user,
            'account_id': account_id,
            'asp_id': asp_id
        }
        data.update(params)

        queue_task = QueueTask.add_queue_task(queue_name=QueueName.EXPORT_CSV,
                                              account_id=account_id,
                                              owner_id=logged_in_user,
                                              status=QueueTaskStatus.NEW.value,
                                              entity_type=EntityType.ITEM_MASTER.value,
                                              param=str(data), input_attachment_id=None, output_attachment_id=None)

        if queue_task:
            queue_task_dict = {
                'job_id': queue_task.id,
                'queue_name': queue_task.queue_name,
                'status': QueueTaskStatus.get_status(queue_task.status),
                'entity_type': EntityType.get_type(queue_task.entity_type)
            }
            data.update(queue_task_dict)
            queue_task.param = str(data)
            queue_task.save()

            export_csv_q.enqueue(ExportCsvDataWorker.export_csv, data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100
            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.EXPORT_QUEUED.value, data=queue_task_dict, error=None)

        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value
        )

    @staticmethod
    @api_time_logger
    @token_required
    def import_item_master_cogs(user_object, account_object):
        """To Import item master excel and update cogs"""

        logged_in_user = user_object.id
        account_id = account_object.uuid
        asp_id = account_object.asp_id

        file = request.files.get('file')

        if 'file' not in request.files or file.filename == '':
            return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                      error={'file': 'File is required'})

        if file and '.' in file.filename:
            file_extension = file.filename.rsplit(
                sep='.', maxsplit=1)[1].lower()
            if file_extension not in EXCEL_ALLOWED_EXTENSIONS:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error={'file': 'Invalid file format. Allowed formats are .xls and .xlsx'})

        if file:
            file_name = file.filename

            attachment_name, attachment_path, attachment_size = upload_file_and_get_object_details(
                file_obj=file,
                obj_name=file_name,
                entity_type=EntityType.ITEM_MASTER.value,
                attachment_type=AttachmentType.ITEM_MASTER_COGS_UPLOAD.value)

            attachment = Attachment.add(
                entity_type=EntityType.ITEM_MASTER.value,
                entity_id=None,
                name=attachment_name,
                path=attachment_path,
                size=attachment_size,
                sub_entity_type=None,
                sub_entity_id=None,
                description='Item Master Cogs Import',
            )

            data = {
                'user_id': logged_in_user,
                'attachment_id': attachment.id,
                'attachment_path': attachment.path,
                'attachment_name': attachment.name,
                'account_id': account_id,
                'asp_id': asp_id
            }

            queue_task = QueueTask.add_queue_task(queue_name=QueueName.ITEM_MASTER_COGS_IMPORT,
                                                  owner_id=logged_in_user,
                                                  account_id=account_id,
                                                  status=QueueTaskStatus.RUNNING.value,
                                                  entity_type=EntityType.ITEM_MASTER.value,
                                                  param=str(data),
                                                  input_attachment_id=attachment.id,
                                                  output_attachment_id=None)

            if queue_task:
                queue_task_dict = {
                    'job_id': queue_task.id,
                    'queue_name': queue_task.queue_name,
                    'status': QueueTaskStatus.get_status(queue_task.status),
                    'entity_type': EntityType.get_type(queue_task.entity_type)
                }
                data.update(queue_task_dict)
                queue_task.param = str(data)
                queue_task.save()
                import_item_master_cogs_q.enqueue(ItemMasterWorker.update_item_master_cogs, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100
                return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.EXPORT_QUEUED.value, data=queue_task_dict, error=None)

        return send_json_response(
            http_status=500,
            response_status=False,
            message_key=ResponseMessageKeys.FAILED.value
        )

    @staticmethod
    def get_inventory_health(stock_count: Any):
        if stock_count == 0:
            return 'Non Moving'
        elif 0 < stock_count <= 1:
            return 'Slow Moving'
        elif 1 < stock_count <= 7:
            return 'Reorder Now'
        elif 7 < stock_count <= 30:
            return 'Reorder Soon'
        elif 30 < stock_count <= 60:
            return 'In Stock'
        else:
            return 'Overstock'

    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def get_inventory_levels(user_object, account_object, allowed_brands):  # type: ignore  # noqa: C901
        """To get inventory Levels"""

        try:
            account_id = account_object.uuid
            asp_id = account_object.asp_id

            marketplace = request.args.get('marketplace')
            from_date = request.args.get('from_date')
            to_date = request.args.get('to_date')
            category = request.args.getlist('category')
            brand = request.args.getlist('brand') if 'brand' in request.args and request.args.getlist(
                'brand') else allowed_brands
            product = request.args.getlist('product')

            page = request.args.get(key='page', default=PAGE_DEFAULT)
            size = request.args.get(key='size', default=PAGE_LIMIT)
            sort_order = request.args.get('sort_order')
            sort_by = request.args.get('sort_by')
            fullfillment_channel = request.args.get('fullfillment_channel')
            status = request.args.get('status')

            # validation
            params = {}
            if marketplace:
                params['marketplace'] = marketplace
            if from_date:
                params['from_date'] = from_date
            if to_date:
                params['to_date'] = to_date
            if category:
                params['category'] = category
            if not brand:
                params['brand'] = brand
            else:
                if account_object.primary_user_id != user_object.id:
                    valid_brands = [b for b in brand if b in allowed_brands]
                    if len(valid_brands) != len(brand):
                        return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                                  message_key=ResponseMessageKeys.INVALID_BRAND.value, data=None,
                                                  error=ResponseMessageKeys.ENTER_CORRECT_INPUT.value)
                    params['brand'] = valid_brands
            if product:
                params['product'] = product
            if page:
                params['page'] = page
            if size:
                params['size'] = size
            if sort_order:
                params['sort_order'] = sort_order
            if sort_by:
                params['sort_by'] = sort_by
            if fullfillment_channel:
                params['fullfillment_channel'] = fullfillment_channel
            if status:
                params['status'] = status

            field_types = {'marketplace': str, 'product': list, 'brand': list, 'from_date': str, 'to_date': str,
                           'category': list, 'page': int, 'sort_order': str, 'sort_by': str, 'size': int, 'fullfillment_channel': str, 'status': str}

            max_date = db.session.query(
                func.max(AzLedgerSummary.date)
            ).filter(
                AzLedgerSummary.account_id == account_id,
                AzLedgerSummary.asp_id == asp_id,
            ).scalar()

            if from_date is None and to_date is None and max_date is not None:
                from_date = max_date.strftime('%Y-%m-%d')
                to_date = max_date.strftime('%Y-%m-%d')
                required_fields = ['marketplace']
            else:
                required_fields = ['marketplace', 'from_date', 'to_date']

            enum_fields = {
                'marketplace': (marketplace, 'ProductMarketplace'),
                'sort_order': (sort_order, 'SortingOrder'),
                'sort_by': (sort_by, 'AzInventoryLevelColumn'),
                'fullfillment_channel': (fullfillment_channel, 'FulfillmentChannel'),
                'status': (status, 'ItemMasterStatus')
            }

            valid_enum = enum_validator(enum_fields)

            if valid_enum['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=valid_enum['data'])

            data = field_type_validator(
                request_data=params, field_types=field_types)

            if data['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=data['data'])

            is_valid = required_validator(
                request_data=params, required_fields=required_fields)

            if is_valid['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=is_valid['data'])

            # Get live inventory from item master
            fba_inventory = False
            if from_date > max_date.strftime('%Y-%m-%d'):
                fba_inventory = True

            """ Show Only Latest and Previous Day Stock and Units Info"""
            if max_date is not None:

                get_stock_and_units_info = AzItemMaster.get_stock_and_units_info(account_id=account_id, asp_id=asp_id,
                                                                                 from_date=from_date, to_date=to_date,
                                                                                 category=tuple(category), brand=tuple(brand), product=tuple(product), fulfillment_channel=fullfillment_channel, status=status)

                if get_stock_and_units_info:

                    get_stock_and_units_info_data = {
                        'in_transit_stock': 0.0,
                        'in_transit_stock_growth_since_yesterday': 0,
                        'in_transit_stock_since_yesterday': 0.0,
                        'in_transit_units': 0,
                        'in_transit_units_growth_since_yesterday': 0.0,
                        'in_transit_units_since_yesterday': 716,
                        'sellable_stock': 0.0,
                        'sellable_stock_growth_since_yesterday': 0,
                        'sellable_stock_since_yesterday': 0.0,
                        'sellable_units': 0,
                        'sellable_units_growth_since_yesterday': 0.0,
                        'sellable_units_since_yesterday': 0,
                        'total_inventory_units': 0,
                        'total_inventory_units_growth_since_yesterday': 0.0,
                        'total_inventory_units_since_yesterday': 0,
                        'total_stock_growth_since_yesterday': 0,
                        'total_stock_inventory': 0.0,
                        'total_stock_inventory_since_yesterday': 0.0,
                        'unsellable_stock': 0.0,
                        'unsellable_stock_growth_since_yesterday': 0,
                        'unsellable_stock_since_yesterday': 0.0,
                        'unsellable_units': 0,
                        'unsellable_units_growth_since_yesterday': 0.0,
                        'unsellable_units_since_yesterday': 0
                    }

                    for stock_and_units_info in get_stock_and_units_info:

                        sellable_stock = float(
                            stock_and_units_info.sellable_stock) if stock_and_units_info.sellable_stock is not None else 0.0
                        sellable_stock_since_yesterday = float(
                            stock_and_units_info.sellable_stock_since_yesterday) if stock_and_units_info.sellable_stock_since_yesterday is not None else 0.0
                        sellable_units = stock_and_units_info.sellable_quantity if stock_and_units_info.sellable_quantity is not None else 0
                        sellable_units_since_yesterday = stock_and_units_info.since_yesterday_sellable_quantity if stock_and_units_info.since_yesterday_sellable_quantity is not None else 0

                        unsellable_stock = float(
                            stock_and_units_info.unsellable_stock) if stock_and_units_info.unsellable_stock is not None else 0.0
                        unsellable_stock_since_yesterday = float(
                            stock_and_units_info.unsellable_stock_since_yesterday) if stock_and_units_info.unsellable_stock_since_yesterday is not None else 0.0
                        unsellable_units = stock_and_units_info.unfulfillable_quantity if stock_and_units_info.unfulfillable_quantity is not None else 0
                        unsellable_units_since_yesterday = stock_and_units_info.since_yesterday_unfulfillable_quantity if stock_and_units_info.since_yesterday_unfulfillable_quantity is not None else 0

                        in_transit_stock = float(
                            stock_and_units_info.in_transit_stock) if stock_and_units_info.in_transit_stock is not None else 0.0
                        in_transit_stock_since_yesterday = float(
                            stock_and_units_info.in_transit_stock_since_yesterday) if stock_and_units_info.in_transit_stock_since_yesterday is not None else 0.0
                        in_transit_units = stock_and_units_info.in_transit_quantity if stock_and_units_info.in_transit_quantity is not None else 0
                        in_transit_units_since_yesterday = stock_and_units_info.since_yesterday_in_transit_quantity if stock_and_units_info.since_yesterday_in_transit_quantity is not None else 0

                        get_stock_and_units_info_data.update({
                            'sellable_stock': float(sellable_stock),
                            'sellable_stock_since_yesterday': float(sellable_stock_since_yesterday),
                            'sellable_stock_growth_since_yesterday': float((sellable_stock - sellable_stock_since_yesterday) / sellable_stock_since_yesterday * 100) if sellable_stock_since_yesterday is not None and sellable_stock_since_yesterday != 0 else 0,

                            'sellable_units': sellable_units,
                            'sellable_units_since_yesterday': sellable_units_since_yesterday,
                            'sellable_units_growth_since_yesterday': (sellable_units - sellable_units_since_yesterday) / sellable_units_since_yesterday * 100 if sellable_units_since_yesterday is not None and sellable_units_since_yesterday != 0 else 0,

                            'unsellable_stock': float(unsellable_stock),
                            'unsellable_stock_since_yesterday': float(unsellable_stock_since_yesterday),
                            'unsellable_stock_growth_since_yesterday': float((unsellable_stock - unsellable_stock_since_yesterday) / unsellable_stock_since_yesterday * 100) if unsellable_stock_since_yesterday is not None and unsellable_stock_since_yesterday != 0 else 0,

                            'unsellable_units': unsellable_units,
                            'unsellable_units_since_yesterday': unsellable_units_since_yesterday,
                            'unsellable_units_growth_since_yesterday': float((unsellable_units - unsellable_units_since_yesterday) / unsellable_units_since_yesterday * 100) if unsellable_units_since_yesterday is not None and unsellable_units_since_yesterday != 0 else 0,

                            'in_transit_stock': float(in_transit_stock),
                            'in_transit_stock_since_yesterday': float(in_transit_stock_since_yesterday),
                            'in_transit_stock_growth_since_yesterday': float((in_transit_stock - in_transit_stock_since_yesterday) / in_transit_stock_since_yesterday * 100) if in_transit_stock_since_yesterday is not None and in_transit_stock_since_yesterday != 0 else 0,

                            'in_transit_units': in_transit_units,
                            'in_transit_units_since_yesterday': in_transit_units_since_yesterday,
                            'in_transit_units_growth_since_yesterday': float((in_transit_units - in_transit_units_since_yesterday) / in_transit_units_since_yesterday * 100) if in_transit_units_since_yesterday is not None and in_transit_units_since_yesterday != 0 else 0
                        })

                    if fba_inventory:

                        fba_inv_json = AzItemMaster.get_fba_stock_and_units_info(account_id=account_id, asp_id=asp_id,
                                                                                 from_date=from_date, to_date=to_date,
                                                                                 category=tuple(category), brand=tuple(brand), product=tuple(product), fulfillment_channel=fullfillment_channel, status=status)

                        for fba in fba_inv_json:

                            get_stock_and_units_info_data.update({
                                'sellable_stock': float(fba.sellable_stock) if fba.sellable_stock is not None else 0.0,
                                'sellable_units': int(fba.sellable_quantity) if fba.sellable_quantity is not None else 0,
                                'unsellable_stock': float(fba.unfulfillable_stock) if fba.unfulfillable_stock is not None else 0.0,
                                'unsellable_units': int(fba.unfulfillable_quantity) if fba.unfulfillable_quantity is not None else 0,
                                'in_transit_stock': float(fba.in_transit_stock) if fba.in_transit_stock is not None else 0.0,
                                'in_transit_units': int(fba.in_transit_quantity) if fba.in_transit_quantity is not None else 0
                            })

                    total_inventory_stock = get_stock_and_units_info_data.get('sellable_stock') + get_stock_and_units_info_data.get(
                        'unsellable_stock') + get_stock_and_units_info_data.get('in_transit_stock')
                    total_inventory_stock_since_yesterday = get_stock_and_units_info_data.get('unsellable_stock_since_yesterday') + get_stock_and_units_info_data.get(
                        'unsellable_stock_since_yesterday') + get_stock_and_units_info_data.get('in_transit_stock_since_yesterday')
                    total_inventory_untis = get_stock_and_units_info_data.get('sellable_units') + get_stock_and_units_info_data.get(
                        'unsellable_units') + get_stock_and_units_info_data.get('in_transit_units')
                    total_inventory_untis_since_yesterday = get_stock_and_units_info_data.get('sellable_units_since_yesterday') + get_stock_and_units_info_data.get(
                        'unsellable_units_since_yesterday') + get_stock_and_units_info_data.get('in_transit_units_since_yesterday')

                    get_stock_and_units_info_data['total_stock_inventory'] = float(
                        total_inventory_stock)
                    get_stock_and_units_info_data['total_stock_inventory_since_yesterday'] = float(
                        total_inventory_stock_since_yesterday)
                    get_stock_and_units_info_data['total_stock_growth_since_yesterday'] = float(
                        (total_inventory_stock - total_inventory_stock_since_yesterday) / total_inventory_stock_since_yesterday * 100) if total_inventory_stock_since_yesterday is not None and total_inventory_stock_since_yesterday != 0 else 0
                    get_stock_and_units_info_data['total_inventory_units'] = total_inventory_untis
                    get_stock_and_units_info_data['total_inventory_units_since_yesterday'] = total_inventory_untis_since_yesterday
                    get_stock_and_units_info_data['total_inventory_units_growth_since_yesterday'] = float(
                        (total_inventory_untis - total_inventory_untis_since_yesterday) / total_inventory_untis_since_yesterday * 100) if total_inventory_untis_since_yesterday is not None and total_inventory_untis_since_yesterday != 0 else 0

            result = []

            inventory, total_count, total_request_count = AzItemMaster.get_item_level(
                account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date, category=tuple(category), brand=tuple(brand), product=tuple(product), sort_order=sort_order, sort_by=AzInventoryLevelColumn.get_column(sort_by), page=page, size=size, fulfillment_channel=fullfillment_channel, status=status)

            if inventory:

                # _asins = []
                for __product in inventory:

                    _unfulfillable_breakup_dict = {'customer_damaged': 0, 'warehouse_damaged': 0,
                                                   'expired': 0, 'distributor_damaged': 0, 'carrier_damaged': 0, 'defective': 0}

                    if fba_inventory:
                        sellable_quantity = __product.fba_sellable_quantity if __product.fba_sellable_quantity is not None else 0
                        unfulfillable_quantity = __product.fba_unfulfillable_quantity if __product.fba_unfulfillable_quantity is not None else 0
                        in_transit_quantity = __product.fba_in_transit_quantity if __product.fba_in_transit_quantity is not None else 0
                        _total_quantity = __product.total_fba_quantity if __product.total_fba_quantity is not None else 0

                        if __product.fba_unfulfillable_breakup:
                            _unfulfillable_breakup_dict.update({
                                'customer_damaged': __product.fba_unfulfillable_breakup.get('customerDamagedQuantity', 0),    # type: ignore  # noqa: FKA100
                                'warehouse_damaged': __product.fba_unfulfillable_breakup.get('warehouseDamagedQuantity', 0),    # type: ignore  # noqa: FKA100
                                'expired': __product.fba_unfulfillable_breakup.get('expiredQuantity', 0),    # type: ignore  # noqa: FKA100
                                'distributor_damaged': __product.fba_unfulfillable_breakup.get('distributorDamagedQuantity', 0),    # type: ignore  # noqa: FKA100
                                'carrier_damaged': __product.fba_unfulfillable_breakup.get('carrierDamagedQuantity', 0),    # type: ignore  # noqa: FKA100
                                'defective': __product.fba_unfulfillable_breakup.get('defectiveQuantity', 0),    # type: ignore  # noqa: FKA100
                                'total_unfulfillable_quantity': __product.fba_unfulfillable_breakup.get('totalUnfulfillableQuantity', 0),    # type: ignore  # noqa: FKA100
                                'fba_inventory': fba_inventory
                            })

                    else:

                        sellable_quantity = __product.sellable_quantity if __product.sellable_quantity is not None else 0
                        unfulfillable_quantity = __product.unfulfillable_quantity if __product.unfulfillable_quantity is not None else 0
                        in_transit_quantity = __product.in_transit_quantity if __product.in_transit_quantity is not None else 0
                        _total_quantity = int(
                            __product.total_quantity) if __product.total_quantity is not None else 0

                        _unfulfillable_breakup_dict.update({
                            'customer_damaged': int(__product.customer_damaged) if __product.customer_damaged is not None else 0,
                            'warehouse_damaged': int(__product.warehouse_damaged) if __product.warehouse_damaged is not None else 0,
                            'expired': int(__product.expired) if __product.expired is not None else 0,
                            'distributor_damaged': int(__product.distributor_damaged) if __product.distributor_damaged is not None else 0,
                            'carrier_damaged': int(__product.carrier_damaged) if __product.carrier_damaged is not None else 0,
                            'defective': int(__product.defective) if __product.defective is not None else 0,
                            'total_unfulfillable_quantity': unfulfillable_quantity,
                            'fba_inventory': fba_inventory
                        })

                    avg_daily_sales_30_days = __product.avg_daily_sales_30_days if __product.avg_daily_sales_30_days is not None else 0
                    avg_daily_sales_30_days_prior = __product.avg_daily_sales_30_days_prior if __product.avg_daily_sales_30_days_prior is not None else 0
                    value_of_stock = float(
                        __product.value_of_stock) if __product.value_of_stock is not None else 0

                    days_of_inventory = (sellable_quantity + in_transit_quantity) / \
                        avg_daily_sales_30_days if avg_daily_sales_30_days is not None and avg_daily_sales_30_days != 0 else 0.0
                    in_stock_rate = (sellable_quantity + in_transit_quantity) / (avg_daily_sales_30_days * 30) * \
                        100 if avg_daily_sales_30_days is not None and avg_daily_sales_30_days != 0 else 0.0

                    item_data = {
                        '_asin': __product.asin,
                        '_sku': __product.seller_sku if __product.seller_sku is not None else '',
                        '_product_name': __product.item_name.strip() if __product.item_name is not None else '',
                        '_product_image': __product.face_image.strip() if __product.face_image is not None else '',
                        '_category': __product.category.strip() if __product.category is not None else '',
                        '_brand': __product.brand.strip() if __product.brand is not None else '',
                        '_price': float(__product.price) if __product.price is not None else 0,
                        '_product_cost': float(__product.product_cost) if __product.product_cost is not None else 0,
                        '_sellable_quantity': sellable_quantity,
                        '_total_quantity': _total_quantity,
                        '_unfulfillable_quantity': unfulfillable_quantity,
                        '_in_transit_quantity': in_transit_quantity,
                        '_days_of_inventory': days_of_inventory,
                        '_inventory_health': AspItemMasterReport.get_inventory_health(days_of_inventory),
                        '_in_stock_rate': in_stock_rate,
                        '_value_of_stock': value_of_stock,
                        'approx_sales': float(__product.average_sales) if __product.average_sales is not None else 0.0,
                        'avg_daily_sales_30_days': {
                            'current': avg_daily_sales_30_days,
                            'growth_percentage': (avg_daily_sales_30_days - avg_daily_sales_30_days_prior) / avg_daily_sales_30_days_prior * 100 if avg_daily_sales_30_days_prior is not None and avg_daily_sales_30_days_prior != 0 else 0.0
                        },
                        '_unfulfillable_quantity_breakup': _unfulfillable_breakup_dict,
                        '_fulfillment_channel': FulfillmentChannel.get_name(__product.fulfillment_channel) if __product.fulfillment_channel is not None else None,
                        '_status': ItemMasterStatus.get_name(__product.status.upper()) if __product.status is not None else None
                    }
                    # _asins.append(__product.asin)
                    result.append(item_data)

                # average_asin_sales = AzSalesTrafficAsin.get_asin_approx_sales(
                #     account_id=account_id, asp_id=asp_id, from_date=from_date, product=tuple(_asins))

                # average_sales_mapping = {}
                # for get_average_sales in average_asin_sales:
                #     average_sales_mapping[get_average_sales.child_asin] = float(
                #         get_average_sales.average_sales) if get_average_sales.average_sales != 0 else 0.0

                # for item_data in result:
                #     asin = item_data['_asin']
                #     if asin in average_sales_mapping:
                #         item_data['approx_sales'] = average_sales_mapping[asin]

                objects = {
                    'from_date': to_date,
                    'stock_and_units_info': get_stock_and_units_info_data,
                    'pagination_metadata': get_pagination_meta(current_page=1 if page is None else int(page), page_size=int(size), total_items=total_request_count)
                }

                data = {
                    'result': result,
                    'objects': objects
                }

                return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=data, error=None)

            return send_json_response(
                http_status=404,
                response_status=False,
                message_key=ResponseMessageKeys.NO_DATA_FOUND.value,
                error=None
            )

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed getting Inventory levels API data: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def get_inventory_stats(user_object, account_object, allowed_brands):   # type: ignore  # noqa: C901
        """To get inventory sellable, unfulfillable, in-transit and total inventory quantity for asin"""

        try:
            account_id = account_object.uuid
            asp_id = account_object.asp_id
            marketplace = request.args.get('marketplace')
            from_date = request.args.get('from_date')
            to_date = request.args.get('to_date')
            category = request.args.getlist('category')
            brand = request.args.getlist('brand') if 'brand' in request.args and request.args.getlist(
                'brand') else allowed_brands
            product = request.args.getlist('product')
            page = request.args.get(key='page', default=PAGE_DEFAULT)
            size = request.args.get(key='size', default=PAGE_LIMIT)

            # validation
            params = {}
            if marketplace:
                params['marketplace'] = marketplace
            if category:
                params['category'] = category
            if not brand:
                params['brand'] = brand
            else:
                if account_object.primary_user_id != user_object.id:
                    valid_brands = [b for b in brand if b in allowed_brands]
                    if len(valid_brands) != len(brand):
                        return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                                  message_key=ResponseMessageKeys.INVALID_BRAND.value, data=None,
                                                  error=ResponseMessageKeys.ENTER_CORRECT_INPUT.value)
                    params['brand'] = valid_brands
            if product:
                params['product'] = product
            if page:
                params['page'] = page
            if size:
                params['size'] = size
            if from_date:
                params['from_date'] = from_date
            if to_date:
                params['to_date'] = to_date

            field_types = {'marketplace': str, 'product': list, 'brand': list,
                           'category': list, 'from_date': str, 'to_date': str, 'page': int, 'size': int}

            required_fields = ['marketplace', 'product']

            enum_fields = {
                'marketplace': (marketplace, 'ProductMarketplace')
            }

            valid_enum = enum_validator(enum_fields)

            if valid_enum['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=valid_enum['data'])

            data = field_type_validator(
                request_data=params, field_types=field_types)

            if data['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=data['data'])

            is_valid = required_validator(
                request_data=params, required_fields=required_fields)

            if is_valid['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=is_valid['data'])

            inventory_summary_by_days, total_count, total_count_result = AzItemMaster.get_inventory_summary_by_days(
                account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))

            result = []

            if inventory_summary_by_days:

                for inventory_summary_by_day in inventory_summary_by_days:
                    date = inventory_summary_by_day.date if inventory_summary_by_day.date is not None else 0
                    units_sold = int(
                        inventory_summary_by_day.units_sold) if inventory_summary_by_day.units_sold is not None else 0
                    sellable_quantity = int(
                        inventory_summary_by_day.sellable_quantity) if inventory_summary_by_day.sellable_quantity is not None else 0
                    unfulfillable_quantity = int(
                        inventory_summary_by_day.unfulfillable_quantity) if inventory_summary_by_day.unfulfillable_quantity is not None else 0
                    in_transit_quantity = int(
                        inventory_summary_by_day.in_transit_quantity) if inventory_summary_by_day.in_transit_quantity is not None else 0
                    total_quantity = int(
                        inventory_summary_by_day.total_quantity) if inventory_summary_by_day.total_quantity is not None else 0

                    item_data = {
                        'date': date.strftime('%Y-%m-%d'),
                        'units_sold': units_sold,
                        'sellable_quantity': sellable_quantity,
                        'sellable_percentage_over_total_quantity': sellable_quantity / total_quantity * 100 if total_quantity != 0 else 0,
                        'unfulfillable_quantity': unfulfillable_quantity,
                        'unfulfillable_percentage_over_total_quantity': unfulfillable_quantity / total_quantity * 100 if total_quantity != 0 else 0,
                        'in_transit_quantity': in_transit_quantity,
                        'in_transit_percentage_over_total_quantity': in_transit_quantity / total_quantity * 100 if total_quantity != 0 else 0,
                        'total_quantity': total_quantity,
                    }

                    result.append(item_data)

                fba_inv_json = AzItemMaster.get_fba_stock_and_units_info(account_id=account_id, asp_id=asp_id,
                                                                         from_date=from_date, to_date=to_date,
                                                                         category=tuple(category), brand=tuple(brand), product=tuple(product))
                for fba in fba_inv_json:

                    fba_item_data = {
                        'units_sold': 0,
                        'sellable_quantity': int(fba.sellable_quantity),
                        'sellable_percentage_over_total_quantity': int(fba.sellable_quantity) / int(fba.total_quantity) * 100 if fba.total_quantity != 0 else 0,
                        'unfulfillable_quantity': int(fba.unfulfillable_quantity),
                        'unfulfillable_percentage_over_total_quantity': int(fba.unfulfillable_quantity) / int(fba.total_quantity) * 100 if fba.total_quantity != 0 else 0,
                        'in_transit_quantity': int(fba.in_transit_quantity),
                        'in_transit_percentage_over_total_quantity': int(fba.in_transit_quantity) / int(fba.total_quantity) * 100 if fba.total_quantity != 0 else 0,
                        'total_quantity': int(fba.total_quantity),
                    }

                max_date = max((item['date'] for item in result), default=None)

                max_date = max_date or from_date

                max_date = max_date if isinstance(max_date, datetime) else datetime.strptime(max_date, '%Y-%m-%d')   # type: ignore  # noqa: FKA100

                current_date = datetime.now()

                while max_date.date() < current_date.date():
                    max_date += timedelta(days=1)

                    if any(item['date'] == max_date.strftime('%Y-%m-%d') for item in result):
                        continue

                    fba_item_data['date'] = max_date.strftime('%Y-%m-%d')
                    result.append(fba_item_data.copy())

                objects = {
                    'pagination_metadata': get_pagination_meta(current_page=1 if page is None else int(page), page_size=int(size), total_items=total_count_result)
                }

                data = {
                    'result': result,
                    'objects': objects
                }

                return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=data, error=None)

            return send_json_response(
                http_status=404,
                response_status=False,
                message_key=ResponseMessageKeys.NO_DATA_FOUND.value,
                error=None
            )

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed getting Inventory stats API data: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def get_inventory_stats_fc_level(user_object, account_object, allowed_brands):  # type: ignore  # noqa: C901
        """To get inventory sellable, unfulfillable, in-transit and total inventory quantity for asin at Fulfillment Center level"""

        try:
            account_id = account_object.uuid
            asp_id = account_object.asp_id
            marketplace = request.args.get('marketplace')
            category = request.args.getlist('category')
            brand = request.args.getlist('brand') if 'brand' in request.args and request.args.getlist(
                'brand') else allowed_brands
            product = request.args.getlist('product')

            from_date = request.args.get('from_date')
            to_date = request.args.get('to_date')

            # validation
            params = {}
            if marketplace:
                params['marketplace'] = marketplace
            if category:
                params['category'] = category
            if not brand:
                params['brand'] = brand
            else:
                if account_object.primary_user_id != user_object.id:
                    valid_brands = [b for b in brand if b in allowed_brands]
                    if len(valid_brands) != len(brand):
                        return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                                  message_key=ResponseMessageKeys.INVALID_BRAND.value, data=None,
                                                  error=ResponseMessageKeys.ENTER_CORRECT_INPUT.value)
                    params['brand'] = valid_brands
            if product:
                params['product'] = product
            if from_date:
                params['from_date'] = from_date
            if to_date:
                params['to_date'] = to_date

            field_types = {'marketplace': str, 'product': list,
                           'brand': list, 'category': list}

            max_date = db.session.query(
                func.max(AzLedgerSummary.date)
            ).filter(
                AzLedgerSummary.account_id == account_id,
                AzLedgerSummary.asp_id == asp_id,
            ).scalar()

            if from_date is None and to_date is None and max_date is not None:
                from_date = max_date.strftime('%Y-%m-%d')
                to_date = max_date.strftime('%Y-%m-%d')
                required_fields = ['marketplace', 'product']
            else:
                required_fields = ['marketplace',
                                   'product', 'from_date', 'to_date']

            enum_fields = {
                'marketplace': (marketplace, 'ProductMarketplace')
            }

            valid_enum = enum_validator(enum_fields)

            if valid_enum['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=valid_enum['data'])

            data = field_type_validator(
                request_data=params, field_types=field_types)

            if data['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=data['data'])

            is_valid = required_validator(
                request_data=params, required_fields=required_fields)

            if is_valid['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=is_valid['data'])

            inventory_summary_at_fc_levels, total_count, total_count_result = AzItemMaster.get_inventory_summary_at_fc_level(
                account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date,
                category=tuple(category), brand=tuple(brand), product=tuple(product))

            with open('app/static/files/fc_by_zone.json') as fc_file:
                fulfillment_center_data = json.load(fc_file)

            fulfillment_center_mapping = {}
            for entry in fulfillment_center_data:
                fulfillment_center_mapping[entry['Fulfillment Center']] = {
                    'State': entry['State'],
                    'Zone': entry['Zone']
                }

            zone_sum = defaultdict(lambda: {
                                   'sellable_quantity': 0, 'unfulfillable_quantity': 0, 'in_transit_quantity': 0, 'total_quantity': 0})

            total_quantity = 0
            sellable_quantity = 0
            unfulfillable_quantity = 0
            in_transit_quantity = 0

            result = []

            if inventory_summary_at_fc_levels:

                for inventory_summary_at_fc in inventory_summary_at_fc_levels:
                    fulfillment_center = inventory_summary_at_fc.fulfillment_center

                    location_data = fulfillment_center_mapping.get(
                        fulfillment_center)
                    zone = location_data['Zone'] if location_data else ''
                    state = location_data['State'] if location_data else ''

                    item_data = {
                        'fulfillment_center': fulfillment_center,
                        'state': state,
                        'zone': zone,
                        'sellable_quantity': inventory_summary_at_fc.sellable_quantity,
                        'unfulfillable_quantity': inventory_summary_at_fc.unfulfillable_quantity,
                        'in_transit_quantity': inventory_summary_at_fc.in_transit_quantity,
                        'total_quantity': inventory_summary_at_fc.total_quantity,
                    }

                    sellable_quantity += inventory_summary_at_fc.sellable_quantity
                    unfulfillable_quantity += inventory_summary_at_fc.unfulfillable_quantity
                    in_transit_quantity += inventory_summary_at_fc.in_transit_quantity

                    result.append(item_data)

                    if zone:
                        zone_sum[zone]['sellable_quantity'] += inventory_summary_at_fc.sellable_quantity
                        zone_sum[zone]['unfulfillable_quantity'] += inventory_summary_at_fc.unfulfillable_quantity
                        zone_sum[zone]['in_transit_quantity'] += inventory_summary_at_fc.in_transit_quantity
                        zone_sum[zone]['total_quantity'] += inventory_summary_at_fc.total_quantity

                zone_stats = [{'stats': stats, 'zone': zone}
                              for zone, stats in zone_sum.items()]

                total_quantity = sellable_quantity + unfulfillable_quantity + in_transit_quantity
                disposition_ratios_over_total_quantity = {
                    'sellable': {
                        'quantity': sellable_quantity,
                        'percentage': sellable_quantity / total_quantity * 100 if total_quantity != 0 else 0,

                    },
                    'unfulfillable': {
                        'quantity': unfulfillable_quantity,
                        'percentage': unfulfillable_quantity / total_quantity * 100 if total_quantity != 0 else 0,

                    },
                    'in_transit': {
                        'quantity': in_transit_quantity,
                        'percentage': in_transit_quantity / total_quantity * 100 if total_quantity != 0 else 0,

                    },
                    'total_quantity': total_quantity
                }

                objects = {
                    'zonal_statistics': zone_stats,
                    'disposition_ratios_over_total_quantity': disposition_ratios_over_total_quantity
                }

                data = {
                    'result': result,
                    'objects': objects
                }

                return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=data, error=None)

            return send_json_response(
                http_status=404,
                response_status=False,
                message_key=ResponseMessageKeys.NO_DATA_FOUND.value,
                error=None
            )

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed getting Inventory stats FC level data: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    def get_product_attr_details(user_object, account_object):
        """To get Inventory levels for a product"""

        try:
            # account_id = account_object.uuid
            # asp_id = account_object.asp_id
            marketplace = request.args.get('marketplace')
            product = request.view_args.get('product')

            # # validation
            params = {}
            if marketplace:
                params['marketplace'] = marketplace
            if product:
                params['product'] = product

            field_types = {'marketplace': str, 'product': str}

            required_fields = ['marketplace', 'product']

            enum_fields = {
                'marketplace': (marketplace, 'ProductMarketplace')
            }

            valid_enum = enum_validator(enum_fields)

            if valid_enum['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=valid_enum['data'])

            data = field_type_validator(
                request_data=params, field_types=field_types)

            if data['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=data['data'])

            is_valid = required_validator(
                request_data=params, required_fields=required_fields)

            if is_valid['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=is_valid['data'])

            service = 'ProductAdvertisingAPI'
            method = 'POST'
            host = AzPaApiBaseURL.IN.value[8:]
            resource_path = '/paapi5/getitems'
            url = AzPaApiBaseURL.IN.value + resource_path
            utc_timestamp = datetime.utcnow()

            payload = {
                'ItemIds': [product],
                'Resources': [enum.value for enum in AzPaApiResources],
                'Merchant': 'All',
                'PartnerTag': config_data.get('PAAPI_PARTNER_TAG'),
                'PartnerType': 'Associates',
                'Marketplace': 'www.amazon.in'
            }

            headers = {
                'Host': host,
                'Accept': 'application/json, text/javascript',
                'Accept-Language': 'en-US',
                'Content-Type': 'application/json; charset=UTF-8',
                'X-Amz-Date': get_amz_date(utc_timestamp),
                'X-Amz-Target': AzPaApiOperations.get_target(AzPaApiOperations.GET_ITEMS.value),
                'Content-Encoding': 'amz-1.0'
            }

            auth = generate_paapi_awssigv4(
                host=host,
                service=service,
                method_name=method,
                timestamp=utc_timestamp,
                headers=headers,
                resource_path=resource_path,
                payload=payload
            )

            response = requests.request(method, url, headers=auth.get_headers(), json=payload)   # type: ignore  # noqa: FKA100

            if response.status_code != 200:
                return send_json_response(
                    http_status=200,
                    response_status=False,
                    message_key=ResponseMessageKeys.FAILED.value,
                    error=None
                )

            decoded_json_data = json.loads(response.text)

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=decoded_json_data, error=None)

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed Fetching Product Advertising API: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    def get_item_updated(user_object, account_object):
        """Retrieves details for an item in the Amazon catalog and updates it."""
        try:

            logged_in_user = user_object.id
            account_id = account_object.uuid
            asp_id = account_object.asp_id
            credentials = account_object.retrieve_asp_credentials(account_object)[
                0]

            data = request.get_json(force=True)

            asin = data.get('asin', None)   # type: ignore  # noqa: FKA100

            if asin:

                params = {
                    'identifiers': asin,
                    'identifiersType': 'ASIN',
                    'includedData': 'attributes,images,salesRanks,summaries',
                    'marketplaceIds': get_asp_market_place_ids()
                }

                report = AmazonReportEU(credentials=credentials)

                response = report.get_catalog_items(params=params)

                for item in response['items']:

                    _asin, _brand, _face_image, _other_images, _category_rank, _subcategory_rank, _category, _subcategory = AspItemMasterReport._get_item_data(
                        item)

                    AzItemMaster.update_catalog_item(account_id=account_id, asp_id=asp_id, asin=_asin.strip(), brand=_brand,
                                                     category=_category, subcategory=_subcategory, category_rank=_category_rank,
                                                     subcategory_rank=_subcategory_rank, face_image=_face_image, other_images=_other_images)
            else:

                item_total_count = AzItemMaster.get_total_records(
                    account_id=account_id, asp_id=asp_id)

                if item_total_count > 0:

                    queue_task = QueueTask.add_queue_task(queue_name=QueueName.ITEM_MASTER_UPDATE_CATALOG,
                                                          owner_id=logged_in_user,
                                                          account_id=account_id,
                                                          status=QueueTaskStatus.NEW.value,
                                                          entity_type=EntityType.ITEM_MASTER.value,
                                                          param=str(
                                                              data),
                                                          input_attachment_id=None,
                                                          output_attachment_id=None)

                    if queue_task:
                        queue_task_dict = {
                            'job_id': queue_task.id,
                            'queue_name': queue_task.queue_name,
                            'status': QueueTaskStatus.get_status(queue_task.status),
                            'entity_type': EntityType.get_type(queue_task.entity_type)
                        }
                        data.update(queue_task_dict)
                        queue_task.param = str(data)
                        queue_task.save()
                        item_master_update_catalog_q.enqueue(ItemMasterWorker.update_catalog_items, data=data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.UPDATED.value, error=None)

        except Exception as exception_error:
            logger.error(
                f'Exception occured while AspCatalogView Item: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value
            )

    @staticmethod
    def _get_item_data(item):
        """
        Private static method to extract data from an item in response.

        Args:
            item (dict): The dictionary representing an item from the response.

        Returns:
            Tuple: A tuple containing the extracted data in the following order:
                (_asin, _brand, _face_image, _other_images, _category_rank, _subcategory_rank, _category, _subcategory)
        """

        # ASIN
        _asin = item.get('asin')

        # Brand
        get_brand = item.get('attributes', {}).get('brand')   # type: ignore  # noqa: FKA100
        _brand = next((brand.get('value')
                      for brand in get_brand), None) if get_brand else None

        # Images
        images_array = item.get('images', [])   # type: ignore  # noqa: FKA100
        _face_image = None
        _other_images = []

        image_count = 1
        for images in images_array[0].get('images'):
            # get_images = images.get('images')
            if image_count == 1:
                _face_image = images.get('link')
            _other_images.append(images)
            image_count += 1

        # Sales ranks
        _category_rank = None
        _subcategory_rank = None
        sales_ranks = item.get('salesRanks', [])   # type: ignore  # noqa: FKA100
        for rank in sales_ranks:
            category_ranking = rank.get('displayGroupRanks', [])   # type: ignore  # noqa: FKA100
            sub_category_ranking = rank.get('classificationRanks', [])   # type: ignore  # noqa: FKA100
            _category_rank = next((category_rank.get('rank')
                                  for category_rank in category_ranking), None)
            _subcategory_rank = next((sub_category_rank.get(
                'rank') for sub_category_rank in sub_category_ranking), None)

        # Category and Subcategory
        summaries = item.get('summaries', [])   # type: ignore  # noqa: FKA100
        _category = summaries[0].get('websiteDisplayGroupName') if summaries else None   # type: ignore  # noqa: FKA100
        _subcategory = summaries[0].get('browseClassification', {}).get('displayName') if summaries else None    # type: ignore  # noqa: FKA100

        return _asin, _brand, _face_image, _other_images, _category_rank, _subcategory_rank, _category, _subcategory

    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def get_product(user_object, account_object, allowed_brands):
        """endpoint for getting product asin and name"""
        try:

            marketplace = request.args.get('marketplace', default=None)
            category = request.args.getlist('category')
            brand = request.args.getlist('brand') if 'brand' in request.args and request.args.getlist(
                'brand') else allowed_brands

            q = request.args.get('q', default=None)
            page = request.args.get('page', default=PAGE_DEFAULT)
            size = request.args.get('size', default=PAGE_LIMIT)

            account_id = account_object.uuid
            selling_partner_id = account_object.asp_id

            # validation
            params = {}

            if marketplace:
                params['marketplace'] = marketplace
            if q:
                params['q'] = q
            if category:
                params['category'] = category
            if not brand:
                params['brand'] = brand
            else:
                if account_object.primary_user_id != user_object.id:
                    valid_brands = [b for b in brand if b in allowed_brands]
                    if len(valid_brands) != len(brand):
                        return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                                  message_key=ResponseMessageKeys.INVALID_BRAND.value, data=None,
                                                  error=ResponseMessageKeys.ENTER_CORRECT_INPUT.value)
                    params['brand'] = valid_brands
            if page:
                params['page'] = page
            if size:
                params['size'] = size

            field_types = {'q': str, 'category': list, 'brand': list,
                           'marketplace': str, 'page': int, 'size': int}

            required_fields = ['marketplace']

            enum_fields = {
                'marketplace': (marketplace, 'ProductMarketplace')
            }

            valid_enum = enum_validator(enum_fields)

            if valid_enum['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=valid_enum['data'])

            data = field_type_validator(
                request_data=params, field_types=field_types)

            if data['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=data['data'])

            is_valid = required_validator(
                request_data=params, required_fields=required_fields)

            if is_valid['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=is_valid['data'])

            products, total_count, total_count_result = AspItemMasterReport.__get_product_metrics(
                account_id=account_id, selling_partner_id=selling_partner_id, page=int(page), size=int(size), search_query=q,
                category=tuple(category), brand=tuple(brand))

            result_dict = {'result': [], 'objects': {}}

            if products:
                for product in products:
                    result_dict['result'].append(
                        {
                            'id': product.asin,
                            'name': product.item_name
                        }
                    )

                pagination_metadata = get_pagination_meta(current_page=int(
                    page), page_size=int(size), total_items=total_count_result)
                result_dict['objects']['pagination_metadata'] = pagination_metadata

            else:
                return send_json_response(
                    http_status=404,
                    response_status=True,
                    data=None,
                    message_key=ResponseMessageKeys.NO_DATA_FOUND.value,
                    error=None
                )

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=result_dict, error=None)

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed getting product: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def get_brand(user_object, account_object, allowed_brands):                  # type: ignore  # noqa: C901
        """endpoint for getting brands from az_item_master"""

        try:
            marketplace = request.args.get('marketplace', default=None)
            q = request.args.get('q', default=None)

            category = request.args.getlist('category')
            brand = allowed_brands if allowed_brands is not None else ''

            page = request.args.get('page', default=PAGE_DEFAULT)
            size = request.args.get('size', default=PAGE_LIMIT)

            account_id = account_object.uuid
            selling_partner_id = account_object.asp_id

            params = {}

            if marketplace:
                params['marketplace'] = marketplace
            if q:
                params['q'] = q
            if category:
                params['category'] = category
            if page:
                params['page'] = page
            if size:
                params['size'] = size

            field_types = {'marketplace': str,
                           'page': int, 'size': int, 'q': str, 'category': list}

            required_fields = ['marketplace']

            enum_fields = {
                'marketplace': (marketplace, 'ProductMarketplace')
            }

            valid_enum = enum_validator(enum_fields)

            if valid_enum['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=valid_enum['data'])

            data = field_type_validator(
                request_data=params, field_types=field_types)

            if data['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=data['data'])

            is_valid = required_validator(
                request_data=params, required_fields=required_fields)

            if is_valid['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=is_valid['data'])

            result_dict = {'result': [], 'objects': {}}

            brands, total_count, total_count_result = AspItemMasterReport.__get_brand(
                account_id=account_id, selling_partner_id=selling_partner_id, page=int(page), size=int(size), search_query=q, category=tuple(category), brand=tuple(brand))

            if not brands:
                return send_json_response(
                    http_status=404,
                    response_status=True,
                    data=None,
                    message_key=ResponseMessageKeys.NO_DATA_FOUND.value,
                    error=None
                )

            for brand in brands:
                result_dict['result'].append({'brand': brand.brand_name})

            pagination_metadata = get_pagination_meta(current_page=int(
                page), page_size=int(size), total_items=total_count_result)
            result_dict['objects']['pagination_metadata'] = pagination_metadata

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=result_dict, error=None)

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed getting brand: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    @brand_filter
    def get_category(user_object, account_object, allowed_brands):
        """endpoint for getting categories from az_item_master"""

        try:
            marketplace = request.args.get('marketplace', default=None)
            q = request.args.get('q', default=None)
            page = request.args.get('page', default=PAGE_DEFAULT)
            size = request.args.get('size', default=PAGE_LIMIT)
            brand = allowed_brands if allowed_brands is not None else ''

            account_id = account_object.uuid
            selling_partner_id = account_object.asp_id

            params = {}

            if marketplace:
                params['marketplace'] = marketplace
            if q:
                params['q'] = q
            if page:
                params['page'] = page
            if size:
                params['size'] = size

            field_types = {'marketplace': str,
                           'page': int, 'size': int, 'q': str}

            required_fields = ['marketplace']

            enum_fields = {
                'marketplace': (marketplace, 'ProductMarketplace')
            }

            valid_enum = enum_validator(enum_fields)

            if valid_enum['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False, message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None, error=valid_enum['data'])

            data = field_type_validator(
                request_data=params, field_types=field_types)

            if data['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=data['data'])

            is_valid = required_validator(
                request_data=params, required_fields=required_fields)

            if is_valid['is_error']:
                return send_json_response(http_status=HttpStatusCode.BAD_REQUEST.value, response_status=False,
                                          message_key=ResponseMessageKeys.ENTER_CORRECT_INPUT.value, data=None,
                                          error=is_valid['data'])

            result_dict = {'result': [], 'objects': {}}

            categories, total_items = AspItemMasterReport.__get_category(
                account_id=account_id, selling_partner_id=selling_partner_id, page=int(page), size=int(size), search_query=q, brand=tuple(brand))

            if not categories:
                return send_json_response(
                    http_status=404,
                    response_status=True,
                    data=None,
                    message_key=ResponseMessageKeys.NO_DATA_FOUND.value,
                    error=None
                )

            for category in categories:
                result_dict['result'].append(
                    {'category': category.category_name})

            pagination_metadata = get_pagination_meta(current_page=int(
                page), page_size=int(size), total_items=total_items)
            result_dict['objects']['pagination_metadata'] = pagination_metadata

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=result_dict, error=None)

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed getting category: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    def __get_product_metrics(account_id: str, selling_partner_id: str, page: int, size: int, search_query: Optional[str] = None, category: Optional[tuple] = None, brand: Optional[tuple] = None):
        """get product asin and name from az_item_master"""

        params = {
            'account_id': account_id,
            'asp_id': selling_partner_id,
            'search_query': f'%{search_query}%' if search_query else None,
            'category': category if category else None,
            'brand': brand if brand else None
        }

        raw_query = 'SELECT DISTINCT asin, item_name FROM az_item_master WHERE account_id = :account_id AND selling_partner_id = :asp_id'

        if search_query:
            raw_query += ' AND (asin ILIKE :search_query or item_name ILIKE :search_query)'

        if category:
            raw_query += ' AND category IN :category'

        if brand:
            raw_query += ' AND brand IN :brand'

        count_query = f'SELECT COUNT(*) FROM ({raw_query}) AS total_count_query'

        total_count_result = db.session.execute(text(count_query), params).scalar()  # type: ignore  # noqa: FKA100

        if page and size:
            page = int(page) - 1
            size = int(size)
            raw_query = raw_query + f' LIMIT {size} OFFSET {page * size}'

        results = db.session.execute(text(raw_query), params).fetchall()  # type: ignore  # noqa: FKA100
        total_count = len(results)

        return results, total_count, total_count_result

    @staticmethod
    def __get_brand(account_id: str, selling_partner_id: str, page: int, size: int, search_query: Optional[str] = None, category: Optional[tuple] = None, brand: Optional[tuple] = None):
        """function to get distinct brands from az"""

        params = {
            'account_id': account_id,
            'asp_id': selling_partner_id,
            'search_query': f'%{search_query}%' if search_query else None,
            'category': category if category else None,
            'brand': brand if brand else None
        }

        raw_query = 'SELECT DISTINCT brand AS brand_name FROM az_item_master WHERE account_id = :account_id AND selling_partner_id = :asp_id AND brand is not null'

        if search_query:
            raw_query += ' AND brand ILIKE :search_query'

        if category:
            raw_query += ' AND category IN :category'

        if brand:
            raw_query += ' AND brand IN :brand'

        count_query = f'SELECT COUNT(*) FROM ({raw_query}) AS total_count_query'
        total_count_result = db.session.execute(text(count_query), params).scalar()  # type: ignore  # noqa: FKA100

        if page and size:
            page = int(page) - 1
            size = int(size)
            raw_query = raw_query + f' LIMIT {size} OFFSET {page * size}'

        results = db.session.execute(text(raw_query), params).fetchall()     # type: ignore  # noqa: FKA100
        total_count = len(results)

        return results, total_count, total_count_result

    @staticmethod
    def __get_category(account_id: str, selling_partner_id: str, page: int, size: int, search_query: Optional[str], brand: Optional[tuple] = None):
        """function to get distinct category from az"""

        offset = (page - 1) * size

        # Base condition
        condition_a = 'where account_id = :account_id and selling_partner_id = :asp_id and category is not null'

        # Add brand condition if 'brand' is provided
        if brand:
            condition_a += ' and brand IN :brand'

        # Add search query condition if 'search_query' is provided
        if search_query:
            condition_a += ' and category ilike :search_query'

        raw_query = f'''
        select distinct category as category_name from az_item_master
        {condition_a}
        limit :limit offset :offset
        '''

        categories = db.session.execute(text(raw_query), {                 # type: ignore  # noqa: FKA100
            'account_id': account_id,
            'asp_id': selling_partner_id,
            'brand': brand,
            'search_query': f'%{search_query}%',
            'limit': size,
            'offset': offset
        }).fetchall()

        count_query = f'''SELECT COUNT(*) FROM (
            select distinct category as category_name from az_item_master
            {condition_a}) AS total_count'''

        total_count = db.session.execute(text(count_query), {                 # type: ignore  # noqa: FKA100
            'account_id': account_id,
            'asp_id': selling_partner_id,
            'brand': brand,
            'search_query': f'%{search_query}%'
        }).scalar()

        return categories, total_count
