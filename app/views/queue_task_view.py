"""
Contains API definitions for managing and searching queue tasks.
"""
import csv
import io
import json

from app import db
from app import logger
from app.helpers.constants import EntityType
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import PAGE_DEFAULT
from app.helpers.constants import PAGE_LIMIT
from app.helpers.constants import QueueName
from app.helpers.constants import QueueTaskStatus
from app.helpers.constants import ResponseMessageKeys
from app.helpers.decorators import api_time_logger
from app.helpers.decorators import token_required
from app.helpers.utility import enum_validator
from app.helpers.utility import field_type_validator
from app.helpers.utility import get_date_and_time_from_timestamp
from app.helpers.utility import get_pagination_meta
from app.helpers.utility import required_validator
from app.helpers.utility import send_json_response
from app.models.queue_task import QueueTask
from flask import request
from flask import Response
from sqlalchemy import or_


class QueueTaskView:
    """
    This class represents a view for managing tasks in a queue.
    """

    @staticmethod
    def search(user_id: int, account_id: str, page=None, size=None, sort=None, q=None, status_list=None):
        """Search for queue's according to filter's"""
        result_query = db.session.query(QueueTask).filter(
            QueueTask.owner_id == user_id, QueueTask.account_id == account_id)
        total_count = None

        if status_list:
            result_query = result_query.filter(
                QueueTask.status.in_(status_list))

        if q:
            if isinstance(q, str):
                # Handle the case where q is a single string
                result_query = result_query.filter(
                    or_(QueueTask.queue_name.ilike('%' + q + '%')))
            elif isinstance(q, list):
                # Handle the case where q is a list of search terms
                search_conditions = [QueueTask.queue_name.ilike(
                    '%' + term + '%') for term in q]
                result_query = result_query.filter(or_(*search_conditions))

            total_count = result_query.count()

        if total_count is None:
            total_count = result_query.count()

        if sort is None:
            result_query = result_query.order_by(QueueTask.id.desc())

        if page and size:
            page = int(page) - 1
            size = int(size)
            result_query = result_query.limit(
                size).offset(page * size)

        result = result_query.all()
        return result, total_count

    @api_time_logger
    @token_required
    def get_list(user_object, account_object):
        """get list of queues by status"""
        try:

            logged_in_user = user_object.id
            account_id = account_object.uuid

            page = request.args.get(key='page', default=PAGE_DEFAULT)
            size = request.args.get(key='size', default=PAGE_LIMIT)
            q = request.args.get(key='q')
            status_list = request.args.getlist(key='status_list')

            field_types = {'q': str, 'page': int, 'size': int}

            required_fields = []

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

            if q is None:
                q = [QueueName.EXPORT_CSV, QueueName.EXPORT_EXCEL,
                     QueueName.ITEM_MASTER_COGS_IMPORT]

            queue_task_list, total_count = QueueTaskView.search(
                user_id=logged_in_user, account_id=account_id, page=page, size=size, q=q, status_list=status_list)

            if queue_task_list:

                objects = {}

                _queue_task_list = []

                objects.update({
                    'pagination_metadata': get_pagination_meta(current_page=1 if page is None else int(page), page_size=int(size), total_items=total_count)
                })

                for queue_task in queue_task_list:
                    param_data = json.loads(queue_task.param.replace("'", '"')) if queue_task.param is not None else None   # type: ignore  # noqa: FKA100
                    _queue_task_list.append({
                        'queue_name': queue_task.queue_name,
                        'entity_type': EntityType.get_type(queue_task.entity_type),
                        'user': user_object.first_name if user_object.first_name is not None else '',
                        'input_file': queue_task.input_attachment_id if queue_task.input_attachment_id is not None else 'NA',
                        'output_file': queue_task.output_attachment_id if queue_task.output_attachment_id is not None else 'NA',
                        'status': QueueTaskStatus.get_status(queue_task.status),
                        'param': param_data,
                        'created_at': get_date_and_time_from_timestamp(queue_task.created_at)
                    })

                data = {
                    'result': _queue_task_list,
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
            """ Exception while fetching queue task report """
            logger.error(
                f'GET -> Error while fetching queue task report: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @api_time_logger
    @token_required
    def get_status(user_object, account_object):
        """get list of queues task status"""
        try:

            status_list = []
            for status_member in QueueTaskStatus:
                status_list.append({
                    'id': status_member.value,
                    'name': QueueTaskStatus.get_status(status_member.value)
                })

            data = {'result': status_list}

            return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True, message_key=ResponseMessageKeys.SUCCESS.value, data=data, error=None)

        except Exception as exception_error:
            """ Exception while fetching queue task status """
            logger.error(
                f'GET -> Error while fetching queue task status: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )

    @staticmethod
    @api_time_logger
    @token_required
    def export_queue(user_object, account_object):
        """Endpoint for exporting queue task list"""
        try:
            logged_in_user = user_object.id
            account_id = account_object.uuid

            report_type = request.args.getlist('report_type')
            status_list = request.args.getlist(key='status_list')
            marketplace = request.args.get('marketplace')

            # validation
            params = {}
            if marketplace:
                params['marketplace'] = marketplace
            if report_type:
                params['report_type'] = report_type
            if status_list:
                params['status_list'] = status_list

            field_types = {'marketplace': str, 'report_type': list}

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

            queue_task_list, total_count = QueueTaskView.search(
                user_id=logged_in_user, account_id=account_id, q=report_type, status_list=status_list)

            if queue_task_list:
                result = []

                for _queue in queue_task_list:
                    item = {
                        'Id': _queue.id,
                        'Owner Id': user_object.first_name if user_object.id == _queue.owner_id and user_object.first_name is not None else '',
                        'Account Id': _queue.account_id,
                        'Queue Name': _queue.queue_name,
                        'Status': QueueTaskStatus.get_status(_queue.status) if _queue.status is not None else '',
                        'Input Attachment Id': _queue.input_attachment_id if _queue.input_attachment_id is not None else 'NA',
                        'Output Attachment Id': _queue.output_attachment_id if _queue.output_attachment_id is not None else 'NA',
                        'Created_at': get_date_and_time_from_timestamp(_queue.created_at),
                        'Entity Type': EntityType.get_type(_queue.entity_type),
                        'Params': _queue.param
                    }
                    result.append(item)

                output = io.StringIO()
                csv_writer = csv.DictWriter(output, fieldnames=item.keys())

                # Write the CSV header
                csv_writer.writeheader()

                # Write the data rows
                csv_writer.writerows(result)

                # Create a response with the CSV data
                response = Response(output.getvalue(), content_type='text/csv')
                response.headers[
                    'Content-Disposition'] = 'attachment; filename=queue-task-list.csv'

                return response

            return send_json_response(
                http_status=404,
                response_status=False,
                message_key=ResponseMessageKeys.NO_DATA_FOUND.value,
                error=None
            )

        except Exception as exception_error:
            logger.error(
                f'GET -> Failed Exporting Queue Task List: {exception_error}')
            return send_json_response(
                http_status=500,
                response_status=False,
                message_key=ResponseMessageKeys.FAILED.value,
            )
