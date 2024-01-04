import io
import json
import os
import shutil
import time
import traceback

from app import app
from app import config_data
from app import db
from app import item_master_q
from app import logger
from app.helpers.constants import ASpReportProcessingStatus
from app.helpers.constants import ASpReportType
from app.helpers.constants import AttachmentType
from app.helpers.constants import EntityType
from app.helpers.constants import FulfillmentChannel
from app.helpers.constants import ItemMasterStatus
from app.helpers.constants import QueueName
from app.helpers.constants import QueueTaskStatus
from app.helpers.constants import TimePeriod
from app.helpers.utility import get_asp_market_place_ids
from app.helpers.utility import get_from_to_date_by_timestamp
from app.helpers.utility import is_valid_numeric
from app.models.account import Account
from app.models.attachment import Attachment
from app.models.az_item_master import AzItemMaster
from app.models.az_report import AzReport
from app.models.queue_task import QueueTask
from excel2json import convert_from_file
import numpy as np
import pandas as pd
from providers.amazon_sp_client import AmazonReportEU
from providers.mail import send_error_notification
import requests
from werkzeug.datastructures import FileStorage
from workers.s3_worker import upload_file_and_get_object_details
import xlrd
import xlsxwriter


class ItemMasterWorker:
    """Class to create, verify, store item master from amazon API's"""

    @classmethod
    def create_item_master_report(cls, data):
        with app.app_context():
            job_id = data.get('job_id')

            if job_id is None:
                logger.error(
                    "Queue Task with job_id '{}' not found".format(job_id))
                return

            queue_task = QueueTask.get_by_id(job_id)

            if not queue_task:
                logger.error(
                    "Queue Task with job_id '{}' not found".format(job_id))
                return

            queue_task.status = QueueTaskStatus.RUNNING.value
            queue_task.save()

            account_id = queue_task.account_id
            # user_id = queue_task.owner_id
            default_sync = data.get('default_sync', False)  # type: ignore  # noqa: FKA100

            account = Account.get_by_uuid(uuid=account_id)

            if not account:
                logger.error(
                    "Queue Task with job_id '{}' failed. Account : {} not found".format(job_id, account_id))
                raise Exception

            try:

                logger.info('*' * 200)
                logger.info('Inside Item master create report')

                asp_id = account.asp_id
                credentials = account.retrieve_asp_credentials(account)[0]

                if default_sync:
                    default_time_period = data.get('default_time_period', TimePeriod.LAST_30_DAYS.value)  # type: ignore  # noqa: FKA100
                    start_datetime, end_datetime = get_from_to_date_by_timestamp(
                        default_time_period)
                else:
                    start_datetime = data.get('start_datetime')
                    end_datetime = data.get('end_datetime')

                payload = {
                    'reportType': ASpReportType.ITEM_MASTER_LIST_ALL_DATA.value,
                    'dataStartTime': start_datetime,
                    'dataEndTime': end_datetime,
                    'marketplaceIds': get_asp_market_place_ids()
                }

                logger.info('account_id: {}, asp_id: {}, credentials: {}, payload: {}'.format(
                    account_id, asp_id, credentials, payload))

                # creating AmazonReportEU object and passing the credentials
                report = AmazonReportEU(credentials=credentials)

                # calling create report function of report object and passing the payload
                response = report.create_report(payload=payload)

                logger.info('reportId: {}'.format(response['reportId']))

                # adding the report_type, report_id to the report table
                AzReport.add(account_id=account_id, seller_partner_id=asp_id, request_start_time=start_datetime,
                             request_end_time=end_datetime, type=ASpReportType.ITEM_MASTER_LIST_ALL_DATA.value, reference_id=response['reportId'])

                if queue_task:
                    queue_task.status = QueueTaskStatus.COMPLETED.value
                    queue_task.save()

                #     queue_task_report_verify = QueueTask.add_queue_task(queue_name=QueueName.ITEM_MASTER_REPORT,
                #                                                         owner_id=user_id,
                #                                                         status=QueueTaskStatus.NEW.value,
                #                                                         entity_type=EntityType.ITEM_MASTER.value,
                #                                                         param=str(data), input_attachment_id=None, output_attachment_id=None)

                #     if queue_task_report_verify:
                #         queue_task_dict = {
                #             'job_id': queue_task_report_verify.id,
                #             'queue_name': queue_task_report_verify.queue_name,
                #             'status': QueueTaskStatus.get_status(queue_task_report_verify.status),
                #             'entity_type': EntityType.get_type(queue_task_report_verify.entity_type),
                #             'seller_partner_id': asp_id,
                #             'user_id': user_id,
                #             'account_id': account_id,
                #             'reference_id': response['reportId']
                #         }

                #         # very every minute
                #         item_master_q.enqueue_in(timedelta(minutes=5), ItemMasterWorker.verify_item_master_report, data=queue_task_dict, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100

                # else:
                #     raise Exception

            except Exception as e:
                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                error_message = 'Error while creating report in ItemMasterWorker.create_item_master_report(): ' + \
                    str(e)
                logger.error(error_message)
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='ItemMasterWorker (ITEM MASTER REPORT) Create Report Failure',
                                        template='emails/slack_email.html', data={}, error_message=error_message, traceback_info=traceback.format_exc())

    @classmethod
    def verify_item_master_report(cls, data):
        """This should queue every minute to verify report"""
        with app.app_context():
            job_id = data.get('job_id')
            user_id = data.get('user_id')
            account_id = data.get('account_id')
            reference_id = data.get('reference_id')

            if job_id is None:
                logger.error(
                    "Queue Task with job_id '{}' not found".format(job_id))
                return

            queue_task = QueueTask.get_by_id(job_id)

            if not queue_task:
                logger.error(
                    "Queue Task with job_id '{}' not found".format(job_id))
                return
            else:
                if account_id is None and account_id is None:
                    raise Exception

            account = Account.get_by_uuid(uuid=account_id)

            if not account:
                logger.error(
                    "Queue Task with job_id '{}' failed. Account : {} not found".format(job_id, account_id))
                raise Exception

            try:

                logger.info('*' * 200)
                logger.info('Inside Item master verify report')

                asp_id = account.asp_id
                credentials = account.retrieve_asp_credentials(account)[0]
                logger.info('account_id: {}, asp_id: {}, credentials: {}'.format(
                    account_id, asp_id, credentials))

                # querying Report table to get the entry for particular report_id
                get_report = AzReport.get_by_ref_id(
                    account_id=account_id, reference_id=reference_id)

                if not get_report:
                    raise Exception

                # creating AmazonReportEU object and passing the credentials
                report = AmazonReportEU(credentials=credentials)

                # calling verify_report function of report object and passing the report_id
                report_status = report.verify_report(reference_id)
                logger.info('processingStatus: {}'.format(
                    report_status['processingStatus']))

                # checking the processing status of the report. if complete, we update status in the table entry for that particular reference_id
                if report_status['processingStatus'] != 'DONE':
                    AzReport.update_status(
                        reference_id=reference_id, status=report_status['processingStatus'], document_id=None)
                else:
                    AzReport.update_status(
                        reference_id=reference_id, status=report_status['processingStatus'], document_id=report_status['reportDocumentId'])

                if queue_task:
                    queue_task.status = QueueTaskStatus.COMPLETED.value
                    queue_task.save()

                    queue_task_retrieve_verify = QueueTask.add_queue_task(queue_name=QueueName.ITEM_MASTER_REPORT,
                                                                          owner_id=user_id,
                                                                          status=QueueTaskStatus.NEW.value,
                                                                          entity_type=EntityType.ITEM_MASTER.value,
                                                                          param=str(data), input_attachment_id=None, output_attachment_id=None)

                    if queue_task_retrieve_verify:
                        queue_task_dict = {
                            'job_id': queue_task_retrieve_verify.id,
                            'queue_name': queue_task_retrieve_verify.queue_name,
                            'status': QueueTaskStatus.get_status(queue_task_retrieve_verify.status),
                            'entity_type': EntityType.get_type(queue_task_retrieve_verify.entity_type),
                            'seller_partner_id': asp_id,
                            'account_id': account_id,
                            'reference_id': reference_id
                        }

                        item_master_q.enqueue(ItemMasterWorker.get_item_master_report, data=queue_task_dict, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100

                else:
                    raise Exception

            except Exception as e:
                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                error_message = 'Error while verifying report in ItemMasterWorker.verify_item_master_report(): ' + \
                    str(e)
                logger.error(error_message)
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='Error while verifying report in ItemMasterWorker',
                                        template='emails/slack_email.html', data={}, error_message=error_message, traceback_info=traceback.format_exc())

    @classmethod
    def get_item_master_report(cls, data):
        """This should queue every minute to verify report"""
        with app.app_context():
            job_id = data.get('job_id')
            account_id = data.get('account_id')
            reference_id = data.get('reference_id')

            if job_id is None:
                logger.error(
                    "Queue Task with job_id '{}' not found".format(job_id))
                return

            queue_task = QueueTask.get_by_id(job_id)

            if not queue_task:
                logger.error(
                    "Queue Task with job_id '{}' not found".format(job_id))
                return
            else:
                if reference_id is None and account_id is None:
                    raise Exception

            account = Account.get_by_uuid(uuid=account_id)

            if not account:
                logger.error(
                    "Queue Task with job_id '{}' failed. Account : {} not found".format(job_id, account_id))
                raise Exception

            try:

                logger.info('*' * 200)
                logger.info('Inside Item master get report')

                asp_id = account.asp_id
                credentials = account.retrieve_asp_credentials(account)[0]
                logger.info('account_id: {}, asp_id: {}, credentials: {}'.format(
                    account_id, asp_id, credentials))

                # querying Report table to get the entry for particular report_id
                get_report_document_id = AzReport.get_by_ref_id(account_id=account_id,
                                                                reference_id=reference_id)

                if not get_report_document_id:
                    raise Exception

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
                    raise Exception

                logger.info('Item master file url for {}, {}: {}'.format(
                    account_id, asp_id, file_url))

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

                get_report_document_id.status = ASpReportProcessingStatus.COMPLETED.value
                get_report_document_id.status_updated_at = int(time.time())
                db.session.commit()

                if queue_task:
                    queue_task.status = QueueTaskStatus.COMPLETED.value
                    queue_task.save()
                else:
                    raise Exception

            except Exception as e:
                if get_report_document_id:
                    get_report_document_id.status = ASpReportProcessingStatus.ERROR.value
                    get_report_document_id.status_updated_at = int(time.time())
                    db.session.commit()
                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                error_message = 'Error while retrieving report in ItemMasterWorker.get_item_master_report(): ' + \
                    str(e)
                logger.error(error_message)
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='ItemMasterWorker Download Report Failure',
                                        template='emails/slack_email.html', data={}, error_message=error_message, traceback_info=traceback.format_exc())

    @classmethod
    def update_item_master_cogs(cls, data):
        with app.app_context():
            job_id = data.get('job_id')
            account_id = data.get('account_id')
            asp_id = data.get('asp_id')

            if job_id is None:
                logger.error('Job Id not found')
                return

            queue_task = QueueTask.get_by_id(job_id)

            if not queue_task:
                logger.error(
                    "Queue Task with job_id '{}' not found".format(job_id))
                return

            if account_id is None or asp_id is None:
                logger.error('Account ID or Selling Partner Id not found')
                raise Exception

            try:
                attachment_path = data['attachment_path'] + \
                    data['attachment_name']
                output_attachment_id = None
                path = attachment_path

                folder_name = attachment_path.split('/')[1].split('.')[0]

                error_file_name = 'item_master_output' + \
                    str(int(time.time())) + '.xlsx'

                import_dir = os.path.join(config_data.get('UPLOAD_FOLDER'), 'tmp', 'item_master_import')  # type: ignore  # noqa: FKA100

                file_path = import_dir + '/' + folder_name

                os.umask(0)

                if not os.path.exists(file_path):
                    os.makedirs(file_path)

                logger.warning('PATH :')
                logger.warning(path)
                logger.warning('FILE PATH :')
                logger.warning(file_path)

                convert_from_file(filepath=path, location=file_path)

                json_file = [pos_json for pos_json in os.listdir(file_path) if
                             pos_json.endswith('.json')][0]

                with open(file_path + '/' + json_file) as json_obj:
                    item_master = json.load(json_obj)

                shutil.rmtree(import_dir)
                os.makedirs(import_dir)

                output_json = []
                row_number = 0

                keys_of_file = list(item_master[0].keys())
                workbook = xlsxwriter.Workbook(error_file_name)
                worksheet = workbook.add_worksheet()
                worksheet.write_row(0, 0, keys_of_file)  # type: ignore  # noqa: FKA100

                # logger.warning('Item Master')
                # logger.warning(item_master)

                for item in item_master:

                    current_item = cls.validate_item_master(item)

                    seller_sku = item['Seller Sku']

                    item_master = AzItemMaster.get_by_seller_sku(
                        account_id=account_id, asp_id=asp_id, seller_sku=seller_sku)

                    logger.warning('COGS VALIDATION')
                    logger.warning(current_item['error'])
                    logger.warning(current_item['product_cost'])

                    if item_master and current_item:
                        if current_item['error'].strip() == '':
                            if item_master:
                                logger.warning(
                                    f'{item_master.seller_sku} - Product Cost:')
                                logger.warning(current_item['product_cost'])
                                item_master.cogs = current_item['product_cost']
                                db.session.commit()
                            else:
                                row_number = row_number + 1
                                current_item[
                                    'error'] = 'Error While updating cogs Try Again (Possible reasons: seller sku not found) ,'
                                worksheet.write_row(row_number, 0, current_item.values())  # type: ignore  # noqa: FKA100
                        else:
                            row_number = row_number + 1
                            current_item['error'] = current_item['error']
                            worksheet.write_row(row_number, 0, current_item.values())  # type: ignore  # noqa: FKA100

                        output_json.append(current_item)

                workbook.close()
                new_workbook = xlrd.open_workbook(error_file_name)
                sheet_one = new_workbook.sheet_by_index(0)

                if sheet_one.nrows > 1:
                    error_file = FileStorage(stream=open(error_file_name, 'rb'), filename=error_file_name)  # type: ignore  # noqa: FKA100

                    attachment_name, _attachment_path, attachment_size = upload_file_and_get_object_details(
                        file_obj=error_file,
                        obj_name=error_file_name,
                        entity_type=EntityType.ITEM_MASTER.value,
                        attachment_type=AttachmentType.ITEM_MASTER_COGS_UPLOAD.value)

                    output_attachment = Attachment.add(
                        entity_type=EntityType.ITEM_MASTER.value,
                        entity_id=None,
                        name=attachment_name,
                        path=_attachment_path,
                        size=attachment_size,
                        sub_entity_type=None,
                        sub_entity_id=None,
                        description='Item Master Import Cogs, Output File With Error.',
                    )

                    output_attachment_id = output_attachment.id

                if os.path.exists(error_file_name):
                    os.remove(path=error_file_name)

                # logger.warning('OUTPUT JSON')
                # logger.warning(output_json)

                if queue_task:
                    queue_task.status = QueueTaskStatus.COMPLETED.value
                    queue_task.output_attachment_id = output_attachment_id
                    queue_task.save()
                else:
                    raise Exception

            except Exception as e:
                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                error_message = 'Error while updating ItemMasterWorker.import_item_master_cogs_excel(): ' + \
                    str(e)
                logger.error(error_message)
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='Error while updating ItemMasterWorker',
                                        template='emails/slack_email.html', data={}, error_message=error_message, traceback_info=traceback.format_exc())

    @classmethod
    def validate_item_master(cls, item_master):
        """Validate Item Master"""
        item_master_obj = {}
        error = ''
        try:
            # logger.warning('item_master')
            # logger.warning(item_master)

            item_master_obj['seller_sku'] = item_master['Seller Sku']
            item_master_obj['asin'] = item_master['Product']
            item_master_obj['item_name'] = item_master['Product Name']
            item_master_obj['category'] = item_master['Category']
            item_master_obj['brand'] = item_master['Brand']
            item_master_obj['price'] = item_master['Price']
            item_master_obj['product_cost'] = item_master['Product Cost']
            item_master_obj['total_quantity'] = item_master['Total Quantity']
            item_master_obj['sellable_quantity'] = item_master['Sellable Quantity']
            item_master_obj['unfulfillable_quantity'] = item_master['Unfulfillable Quantity']
            item_master_obj['in_transit_quantity'] = item_master['In-transit Quantity']
            item_master_obj['avg_daily_sales_30_days'] = item_master['Average Daily 30 Days Sale']
            item_master_obj['days_of_inventory'] = item_master['Days of Inventory']
            item_master_obj['in_stock_rate'] = item_master['In Stock Rate']
            item_master_obj['value_of_stock'] = item_master['Value Of Stock']

            if not is_valid_numeric(item_master_obj['product_cost']):
                error = error + \
                    f"[***** Product Cost]: {item_master_obj['product_cost']} is not a valid numeric value, "

        except Exception as exception:
            # ex_type, ex_value, ex_traceback = sys.exc_info()
            # trace_back = traceback.extract_tb(ex_traceback)
            # stack_trace = list()  # type: ignore  # noqa: FKA100

            # for trace in trace_back:
            #     stack_trace.append(
            #         'File : %s , Line : %d, Func.Name : %s, Message : %s' % (
            #             trace[0], trace[1], trace[2], trace[3]))

            logger.warning('EXCEPTION validate Item Master')
            logger.warning(exception)
            # logger.warning(stack_trace)

        item_master_obj['error'] = error

        return item_master_obj

    @classmethod
    def item_master_export(cls, data):

        asp_id = data.get('asp_id')
        account_id = data.get('account_id')
        from_date = data.get('from_date')
        to_date = data.get('to_date')
        category = data.get('category') if data.get(
            'category') is not None else ''
        brand = data.get('brand') if data.get('brand') is not None else ''
        product = data.get('product') if data.get(
            'product') is not None else ''

        inventory, total_count, total_request_count = AzItemMaster.get_item_level(
            account_id=account_id, asp_id=asp_id, from_date=from_date, to_date=to_date, category=tuple(category), brand=tuple(brand), product=tuple(product))

        if inventory:

            result = []

            for __product in inventory:
                sellable_quantity = __product.sellable_quantity if __product.sellable_quantity is not None else 0
                unfulfillable_quantity = __product.unfulfillable_quantity if __product.unfulfillable_quantity is not None else 0
                avg_daily_sales_30_days = __product.avg_daily_sales_30_days if __product.avg_daily_sales_30_days is not None else 0
                value_of_stock = float(
                    __product.value_of_stock) if __product.value_of_stock is not None else 0
                in_transit_quantity = __product.in_transit_quantity if __product.in_transit_quantity is not None else 0

                days_of_inventory = (sellable_quantity + in_transit_quantity) / \
                    avg_daily_sales_30_days if avg_daily_sales_30_days is not None and avg_daily_sales_30_days != 0 else 0.0
                in_stock_rate = (sellable_quantity + in_transit_quantity) / (avg_daily_sales_30_days * 30) * \
                    100 if avg_daily_sales_30_days is not None and avg_daily_sales_30_days != 0 else 0.0

                item_data = {
                    '_asin': __product.asin.strip(),
                    '_sku': __product.seller_sku if __product.seller_sku is not None else '',
                    '_product_name': __product.item_name.strip() if __product.item_name is not None else '',
                    '_product_image': __product.face_image.strip() if __product.face_image is not None else '',
                    '_category': __product.category.strip() if __product.category is not None else '',
                    '_brand': __product.brand.strip() if __product.brand is not None else '',
                    '_price': float(__product.price) if __product.price is not None else 0,
                    '_product_cost': float(__product.product_cost) if __product.product_cost is not None else 0,
                    '_average_sales': float(__product.average_sales) if __product.average_sales is not None else 0.0,
                    '_sellable_quantity': sellable_quantity,
                    '_total_quantity': int(__product.total_quantity) if __product.total_quantity is not None else 0,
                    '_unfulfillable_quantity': unfulfillable_quantity,
                    '_in_transit_quantity': in_transit_quantity,
                    '_days_of_inventory': days_of_inventory,
                    '_in_stock_rate': in_stock_rate,
                    '_value_of_stock': value_of_stock,
                    'avg_daily_sales_30_days': avg_daily_sales_30_days
                }
                result.append(item_data)

            # column_names = ['seller_sku', 'asin', 'item_name', 'category', 'brand', 'price', 'product_cost',
            #                 'sellable_quantity', 'unfulfillable_quantity', 'in_transit_quantity', 'avg_daily_sales_30_days', 'avg_daily_sales_30_days_prior',
            #                 'days_of_inventory', 'in_stock_rate', 'value_of_stock']

            # data = []
            # data.append(column_names)  # Add column headers

            # for item in inventory:
            #     # Select the desired columns from the row dictionary
            #     selected_columns = [str(getattr(item, column_name))
            #                         for column_name in column_names]
            #     data.append(selected_columns)

            directory_path = f"{config_data.get('UPLOAD_FOLDER')}{'tmp/csv_exports/'}{asp_id.lower()}"
            os.makedirs(directory_path, exist_ok=True)
            logger.info(
                'Directory path for item master export cogs: %s', directory_path)

            file_name = 'item_master_export'

            export_file_path = f'{directory_path}/{file_name}.xlsx'

            # Write the data to an Excel file
            workbook = xlsxwriter.Workbook(export_file_path)
            worksheet = workbook.add_worksheet()

            column_names = ['Seller Sku', 'Product', 'Product Name', 'Category', 'Brand', 'Price', 'Product Cost', 'Avg Buy Box % (30 Days)',
                            'Total Quantity', 'Sellable Quantity', 'Unfulfillable Quantity', 'In-transit Quantity', 'Average Daily 30 Days Sale',
                            'Days of Inventory', 'In Stock Rate', 'Value Of Stock']

            for col, header in enumerate(column_names):
                worksheet.write(0, col, header)    # type: ignore  # noqa: FKA100

            for row, item in enumerate(result, start=1):
                data_fields = [
                    '_sku', '_asin', '_product_name', '_category', '_brand', '_price', '_product_cost', '_average_sales',
                    '_total_quantity',
                    '_sellable_quantity',
                    '_unfulfillable_quantity',
                    '_in_transit_quantity',
                    'avg_daily_sales_30_days',
                    '_days_of_inventory', '_in_stock_rate',
                    '_value_of_stock'
                ]

                for col, field in enumerate(data_fields):
                    worksheet.write(row, col, item[field])    # type: ignore  # noqa: FKA100

            workbook.close()

            # # Write column headers
            # for col_num, column_name in enumerate(column_names):
            #     worksheet.write(0, col_num, column_name)  # type: ignore  # noqa: FKA100

            # # Write data rows
            # for row_num, item in enumerate(inventory):
            #     selected_columns = [str(getattr(item, column_name)) if getattr(item, column_name) is not None else ''
            #                         for column_name in column_names]
            #     for col_num, value in enumerate(selected_columns):
            #         worksheet.write(row_num + 1, col_num, value)  # type: ignore  # noqa: FKA100

            # workbook.close()

            return file_name, export_file_path
        else:
            return None, None

    @classmethod
    def update_catalog_items(cls, data):
        """Update Item Master catalog"""
        with app.app_context():
            from app.views.asp_item_master_report_view import AspItemMasterReport
            from app.models.az_product_performance import AzProductPerformance
            from app.models.az_sales_traffic_asin import AzSalesTrafficAsin
            from app.models.az_fba_customer_shipment_sales import AzFbaCustomerShipmentSales
            from app.models.az_fba_reimbursements import AzFbaReimbursements
            from app.models.az_fba_replacement import AzFbaReplacement
            from app.models.az_fba_returns import AzFbaReturns
            from app.models.az_order_report import AzOrderReport
            from app.models.az_ledger_summary import AzLedgerSummary
            from app.models.az_financial_event import AzFinancialEvent
            from app.models.az_sponsored_display import AzSponsoredDisplay
            from app.models.az_sponsored_product import AzSponsoredProduct

            job_id = data.get('job_id')

            if job_id is None:
                logger.error(
                    "Queue Task with job_id '{}' not found".format(job_id))
                return

            queue_task = QueueTask.get_by_id(job_id)

            if not queue_task:
                logger.error(
                    "Queue Task with job_id '{}' not found".format(job_id))
                return

            account_id = queue_task.account_id

            account = Account.get_by_uuid(uuid=account_id)

            if not account:
                logger.error(
                    "Queue Task with job_id '{}' failed. Account : {} not found".format(job_id, account_id))
                raise Exception

            try:

                if queue_task:
                    queue_task.status = QueueTaskStatus.RUNNING.value
                    queue_task.save()

                asp_id = account.asp_id

                items, total_count = AzItemMaster.get_all_records(
                    account_id=account_id, asp_id=asp_id)

                if items and total_count > 0:

                    credentials = account.retrieve_asp_credentials(account)[0]

                    # Fetch the ASINs from the items and create a list of ASINs
                    asin_list = [item.asin for item in items]

                    # Maximum count of identifiers allowed in one request
                    max_identifiers_count = 20

                    # Initialize index and counter
                    index = 0
                    counter = 0

                    while index < len(asin_list):
                        # Get the next batch of ASINs
                        batch_asins = asin_list[index:index
                                                + max_identifiers_count]

                        # Convert the list of ASINs to a comma-separated string
                        asin_str = ', '.join(batch_asins)

                        params = {
                            'identifiers': f'{asin_str}',
                            'identifiersType': 'ASIN',
                            'includedData': 'attributes,images,salesRanks,summaries',
                            'marketplaceIds': get_asp_market_place_ids()
                        }

                        count = 1
                        while True:

                            report = AmazonReportEU(credentials=credentials)

                            batch_response = report.get_catalog_items(
                                params=params)

                            for item in batch_response['items']:

                                _asin, _brand, _face_image, _other_images, _category_rank, _subcategory_rank, _category, _subcategory = AspItemMasterReport._get_item_data(
                                    item)

                                updated_data = AzItemMaster.update_catalog_item(account_id=account_id, asp_id=asp_id, asin=_asin, brand=_brand,
                                                                                category=_category, subcategory=_subcategory, category_rank=_category_rank,
                                                                                subcategory_rank=_subcategory_rank, face_image=_face_image, other_images=_other_images)

                                if updated_data:
                                    logger.info('*' * 200)
                                    if updated_data.seller_sku:
                                        logger.info(
                                            'seller_sku: %s', updated_data.seller_sku)

                                        # update all table's with brand's
                                        AzProductPerformance.store_brand_category_by_sku(
                                            account_id=account_id, asp_id=asp_id, seller_sku=updated_data.seller_sku, category=_category, brand=_brand)
                                        AzFbaCustomerShipmentSales.store_brand_category_by_sku(
                                            account_id=account_id, asp_id=asp_id, seller_sku=updated_data.seller_sku, category=_category, brand=_brand)
                                        AzFbaReimbursements.store_brand_category_by_sku(
                                            account_id=account_id, asp_id=asp_id, seller_sku=updated_data.seller_sku, category=_category, brand=_brand)
                                        AzFbaReplacement.store_brand_category_by_sku(
                                            account_id=account_id, asp_id=asp_id, seller_sku=updated_data.seller_sku, category=_category, brand=_brand)
                                        AzFbaReturns.store_brand_category_by_sku(
                                            account_id=account_id, asp_id=asp_id, seller_sku=updated_data.seller_sku, category=_category, brand=_brand)
                                        AzOrderReport.store_brand_category_by_sku(
                                            account_id=account_id, asp_id=asp_id, seller_sku=updated_data.seller_sku, category=_category, brand=_brand)
                                        AzLedgerSummary.store_brand_category_by_sku(
                                            account_id=account_id, asp_id=asp_id, seller_sku=updated_data.seller_sku, category=_category, brand=_brand)
                                        AzFinancialEvent.store_brand_category_by_sku(
                                            account_id=account_id, asp_id=asp_id, seller_sku=updated_data.seller_sku, category=_category, brand=_brand)
                                        AzSalesTrafficAsin.store_brand_category_by_asin(
                                            account_id=account_id, asp_id=asp_id, asin=updated_data.asin, category=_category, brand=_brand)

                                        # Update brand's for Ad's
                                        AzSponsoredDisplay.store_brand_category_by_sku(
                                            account_id=account_id, asp_id=asp_id, seller_sku=updated_data.seller_sku, category=_category, brand=_brand)
                                        AzSponsoredProduct.store_brand_category_by_sku(
                                            account_id=account_id, asp_id=asp_id, seller_sku=updated_data.seller_sku, category=_category, brand=_brand)

                                    else:
                                        logger.info(
                                            'Empty Sku for Asin: %s', _asin)

                            if 'nextToken' not in batch_response['pagination']:
                                break

                            next_token = batch_response['pagination']['nextToken']

                            params = {
                                'identifiers': f'{asin_str}',
                                'identifiersType': 'ASIN',
                                'includedData': 'attributes,images,salesRanks,summaries',
                                'marketplaceIds': get_asp_market_place_ids(),
                                'pageToken': next_token
                            }
                            count += 1
                            time.sleep(60)

                        # Increment the index and counter for the next batch
                        index += max_identifiers_count
                        counter += 1

                if queue_task:
                    queue_task.status = QueueTaskStatus.COMPLETED.value
                    queue_task.save()

            except Exception as e:
                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                error_message = 'Error while Updating catalog items in ItemMasterWorker.update_catalog_items(): ' + \
                    str(e)
                logger.error(error_message)
                logger.error(traceback.format_exc())
                send_error_notification(email_to=config_data.get('SLACK').get('NOTIFICATION_EMAIL'), subject='Error while Updating catalog items in ItemMasterWorker',
                                        template='emails/slack_email.html', data={}, error_message=error_message, traceback_info=traceback.format_exc())
