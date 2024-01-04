import datetime
import os
import traceback
from typing import Any

from app import config_data
from app import ENVIRONMENT_LIST
from app import logger
from app import S3_RESOURCE
from app.helpers.utility import get_bucket_name
from magic import Magic
import openpyxl


"""entity_type means user, obj_name can be user name and attachment_type can be profile_photo"""


def upload_file_and_get_object_details(file_obj: Any, obj_name: str, entity_type: str, attachment_type: str, is_report: bool = False) -> tuple:
    """This method will upload file to bitbucket and return file details."""
    try:
        environment = config_data.get('APP_ENV')

        folder = f'media/{entity_type}/{attachment_type}/'.lower()

        if environment not in ENVIRONMENT_LIST:
            folder = f'{config_data.get("UPLOAD_FOLDER")}{entity_type}/{attachment_type}/'.lower()

        if not os.path.exists(folder):
            os.makedirs(folder)

        if not is_report:
            extension = file_obj.filename.split('.')[1]
            name = file_obj.filename.split('.')[0]
            file_name = name.replace(' ', '_') + '_' + str(datetime.datetime.now().timestamp()).replace('.', '')  # type: ignore  # noqa: FKA100
            file_name_with_extension = file_name + '.' + extension

            temp_path = os.path.join(folder, file_obj.filename)  # type: ignore  # noqa: FKA100
            # Attempt to save the file using file_obj.save(temp_path)
            file_obj.save(temp_path)

            if file_obj.filename.lower().endswith('.xlsx'):
                excel = openpyxl.load_workbook(file_obj)
                excel.active.title = file_name
                new_file_name = file_name_with_extension
                excel.save(folder + new_file_name)

            size = os.stat(temp_path).st_size

            if environment in ENVIRONMENT_LIST:
                bucket = get_bucket_name()
                S3_RESOURCE.Bucket(bucket).upload_file(temp_path, f'{folder}{file_name_with_extension}', ExtraArgs={'ACL': 'public-read', 'ContentType': Magic(mime=True).from_file(temp_path)})    # type: ignore  # noqa: FKA100
            else:
                folder = f'media/{entity_type}/{attachment_type}/'.lower()

            if os.path.exists(temp_path):
                os.remove(temp_path)

            return file_name_with_extension, folder, size
        else:
            bucket = get_bucket_name()
            size = get_file_size_by_path(file_obj)
            S3_RESOURCE.Bucket(bucket).upload_file(file_obj, f'{folder}{obj_name}', ExtraArgs={'ACL': 'public-read', 'ContentType': Magic(mime=True).from_file(file_obj)})    # type: ignore  # noqa: FKA100
            if os.path.exists(file_obj):
                os.remove(file_obj)
            return obj_name, folder, size

    except Exception as e:  # type: ignore  # noqa: F841
        logger.error(traceback.format_exc())
        return '', '', 0


# def delete_file_from_bucket(key):
#     """This method is used to delete files from bucket with given key."""
#     bucket = os.environ.get('AWS.S3_BUCKET')

#     try:
#         s3_object = S3_RESOURCE.Object(bucket, key)  # type: ignore  # noqa: FKA100

#         s3_object.delete()

#     except Exception as e:
#         logger.error('Error while deleting file from bucket: {0}'.format(e))
#     return


def get_file_size_by_path(path):
    size = os.stat(path).st_size
    size /= 125  # convert from bytes to kilobits
    size_notation = 'kb'
    if size > 1024:
        size /= 1024  # convert from kb to mb
        size_notation = 'mb'
    size = str(size) + size_notation
    return size
