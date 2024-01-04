import os
import traceback

from app import app
from app import logger
from app.helpers.constants import AttachmentType
from app.helpers.constants import EntityType
from app.helpers.constants import QueueTaskStatus
from app.helpers.constants import SubEntityType
from app.models.attachment import Attachment
from app.models.queue_task import QueueTask
from werkzeug.datastructures import FileStorage
from workers.item_master_worker import ItemMasterWorker
from workers.marketing_reports_worker import MarketingReportsWorker
from workers.performance_by_zone_worker import PerformanceByZoneWorker
from workers.product_performance_worker import ProductPerformanceWorker
from workers.s3_worker import upload_file_and_get_object_details


class ExportCsvDataWorker:
    @classmethod
    def export_csv(cls, data):
        with app.app_context():
            logger.info('*' * 50)
            logger.info('EXPORT CSV Queue data %s', str(data))

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

            try:

                if queue_task.entity_type == EntityType.ITEM_MASTER.value:
                    data['file_name'], data['export_file_path'] = ItemMasterWorker.item_master_export(
                        data)
                if queue_task.entity_type == EntityType.PRODUCT_PERFORMANCE.value:
                    data['file_name'], data['export_file_path'] = ProductPerformanceWorker.product_performance_export(
                        data)
                if queue_task.entity_type == EntityType.ADS_PERFORMANCE_BY_ZONE.value:
                    data['file_name'], data['export_file_path'] = PerformanceByZoneWorker.performance_by_zone_export(
                        data)
                if queue_task.entity_type == EntityType.MR_PRODUCT_PERFORMANCE.value:
                    data['file_name'], data['export_file_path'] = MarketingReportsWorker.product_performance_export(
                        data)

                if not data['file_name'] or not data['export_file_path']:
                    logger.warning(
                        'Export Failed Possible reason no data found.')
                    raise Exception

                export_file_path = data['export_file_path']
                export_file_name = os.path.basename(export_file_path)

                file_storage = FileStorage(stream=open(export_file_path, 'rb'), filename=export_file_name)  # type: ignore  # noqa: FKA100

                attachment_name, attachment_path, attachment_size = upload_file_and_get_object_details(
                    file_obj=file_storage,
                    obj_name=data['file_name'],
                    entity_type=queue_task.entity_type,
                    attachment_type=AttachmentType.get_type(queue_task.entity_type))

                output_attachment = Attachment.add(
                    entity_type=queue_task.entity_type,
                    entity_id=None,
                    name=attachment_name,
                    path=attachment_path,
                    size=attachment_size,
                    sub_entity_type=SubEntityType.EXPORT_FILE.value,
                    sub_entity_id=None,
                    description='Export File,' + data.get('entity_type'),
                )

                if output_attachment:
                    if os.path.exists(data['export_file_path']):
                        os.remove(path=data['export_file_path'])
                        if queue_task:
                            queue_task.status = QueueTaskStatus.COMPLETED.value
                            queue_task.output_attachment_id = output_attachment.id
                            queue_task.save()
                    else:
                        raise Exception
                else:
                    if queue_task:
                        queue_task.status = QueueTaskStatus.ERROR.value
                        queue_task.save(queue_task)

            except Exception as e:
                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()
                logger.error("Queue Task with job_id '{}' failed. Exception: {}".format(
                    data.get('job_id'), str(e)))
                logger.error(traceback.format_exc())
