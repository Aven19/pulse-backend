import json

from app import app
from app import config_data
from app import logger
from app.helpers.constants import QueueTaskStatus
from app.models.queue_task import QueueTask
from flask import render_template
from flask_mail import Mail
from flask_mail import Message
import jwt
# from app.helpers.constants import ConfigurationKey
# from app.helpers.utility import EmailStatus
# from app.models.configuration import Configuration
# from app.models.email_log import EmailLog

app.config['MAIL_SERVER'] = config_data['MAIL']['MAIL_SERVER']
app.config['MAIL_PORT'] = config_data['MAIL']['MAIL_PORT']
app.config['MAIL_USERNAME'] = config_data['MAIL']['MAIL_USERNAME']
app.config['MAIL_PASSWORD'] = config_data['MAIL']['MAIL_PASSWORD']
app.config['MAIL_USE_TLS'] = config_data['MAIL']['MAIL_USE_TLS']
app.config['MAIL_USE_SSL'] = config_data['MAIL']['MAIL_USE_SSL']
app.config['MAIL_DEFAULT_SENDER'] = config_data['MAIL']['MAIL_DEFAULT_SENDER']
mail = Mail(app)


class EmailWorker:
    """worker for sending email"""

    @classmethod
    def send_invite_mail(cls, data):
        """This method is used to send emails."""

        with app.app_context():

            msg = Message(data.get('subject'), sender=(config_data['MAIL']['MAIL_DEFAULT_SENDER_NAME'],
                                                       config_data['MAIL']['MAIL_DEFAULT_SENDER']), recipients=[data.get('email_to')])

            logger.info('*' * 50)
            logger.info('SES EMAIL DELIVERY Queue data %s', str(data))

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
                msg.html = render_template('emails/send_email.html', data=data)

                logger.info(
                    '---------------Mail LOADED successfully--------------------------')

                token = jwt.encode(payload=data, key=config_data['JWT_SALT'])

                headers = {
                    'X-Mailgun-Variables': json.dumps({'email_details': token})}

                msg.extra_headers = headers

                response = mail.send(msg)

                if queue_task:
                    queue_task.status = QueueTaskStatus.COMPLETED.value
                    queue_task.save()

                logger.info('Mail sent successfully : {0}'.format(response))

            except Exception as e:

                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                logger.error("Queue Task with job_id '{}' failed. Exception: {}".format(
                    data.get('job_id'), str(e)))

                logger.error('Unable to send mail: ' + str(e))

    @classmethod
    def send_expiry_mail(cls, data):
        """This method is used to send expiry emails."""

        with app.app_context():

            msg = Message(data.get('subject'), sender=(config_data['MAIL']['MAIL_DEFAULT_SENDER_NAME'],
                                                       config_data['MAIL']['MAIL_DEFAULT_SENDER']), recipients=[data.get('email_to')])

            logger.info('*' * 50)
            logger.info('SES EMAIL DELIVERY Queue data %s', str(data))

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
                msg.html = render_template(
                    'emails/subscription_expired.html', data=data)

                logger.info(
                    '---------------Mail LOADED successfully--------------------------')

                token = jwt.encode(payload=data, key=config_data['JWT_SALT'])

                headers = {
                    'X-Mailgun-Variables': json.dumps({'email_details': token})}

                msg.extra_headers = headers

                response = mail.send(msg)

                if queue_task:
                    queue_task.status = QueueTaskStatus.COMPLETED.value
                    queue_task.save()

                logger.info('Mail sent successfully : {0}'.format(response))

            except Exception as e:

                if queue_task:
                    queue_task.status = QueueTaskStatus.ERROR.value
                    queue_task.save()

                logger.error("Queue Task with job_id '{}' failed. Exception: {}".format(
                    data.get('job_id'), str(e)))
                logger.error('Unable to send mail: ' + str(e))
