"""Contains some common API and admin panel API definitions."""
from app.helpers.constants import HttpStatusCode
from app.helpers.constants import ResponseMessageKeys
from app.helpers.utility import get_object_url
from app.helpers.utility import send_json_response
from app.models.attachment import Attachment


def get_health_check() -> tuple:
    """ API created for health check."""
    return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True,
                              message_key=ResponseMessageKeys.SUCCESS.value, data=None,
                              error=None)


def get_attachment(export_id: int):
    """ API to get attachment url from s3."""

    get_attachment = Attachment.get_by_id(export_id)

    get_s3_url = None

    if get_attachment:

        get_s3_url = get_object_url(get_attachment.path + get_attachment.name)

        data = {
            'result': {
                'url': get_s3_url
            }
        }

        return send_json_response(http_status=HttpStatusCode.OK.value, response_status=True,
                                  message_key=ResponseMessageKeys.SUCCESS.value, data=data,
                                  error=None)

    return send_json_response(
        http_status=404,
        response_status=False,
        message_key=ResponseMessageKeys.NO_DATA_FOUND.value,
        error=None)
