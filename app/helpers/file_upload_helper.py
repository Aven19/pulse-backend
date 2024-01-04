"""File upload related helper"""
from typing import Any

from app.helpers.utility import get_file_schema
from app.models.attachment import Attachment
from workers.s3_worker import upload_file_and_get_object_details


def upload_file_s3(file_obj: Any, obj_name: str, entity_type: Any, attachment_type: str, sub_entity_type=None, description=None) -> dict:
    """
    Upload and return file object schema.

    :param file_obj: The file object to upload and format.
    :param obj_name: Name of the object.
    :param entity_type: Type of the entity.
    :param attachment_type: Type of the attachment.
    :param sub_entity_type: Type of the sub-entity (default: None).
    :param description: Description of the file (default: None).

    :return: A dictionary containing file-related schema.
    """

    file_details = get_file_schema()

    if file_obj:
        """Upload and Get the file details"""
        file_name, file_path, file_size = upload_file_and_get_object_details(file_obj=file_obj,
                                                                             obj_name=obj_name,
                                                                             entity_type=entity_type,
                                                                             attachment_type=attachment_type)

        """Add file details to attachment table"""
        attachment = Attachment.add(
            entity_type=entity_type,
            entity_id=None,
            name=file_name,
            path=file_path,
            size=file_size,
            sub_entity_type=sub_entity_type,
            sub_entity_id=None,
            description=description,
        )

        """ Add the file details to the schema"""

        file_details = get_file_schema()

        if attachment:
            file_details['file_attachment_id'] = attachment.id

        file_details['file_name'] = file_name
        file_details['file_path'] = file_path
        file_details['file_size'] = file_size

        """Return file related schema"""
        return file_details

    return file_details
