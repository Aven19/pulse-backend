import json
import traceback

from app import app
from app import config_data
from app import ENVIRONMENT_LIST
from app import logger
from app.helpers.constants import APP_NAME
from flask import render_template
from flask_mail import Mail
from flask_mail import Message
import jwt

app.config['MAIL_SERVER'] = config_data['MAIL']['MAIL_SERVER']
app.config['MAIL_PORT'] = config_data['MAIL']['MAIL_PORT']
app.config['MAIL_USERNAME'] = config_data['MAIL']['MAIL_USERNAME']
app.config['MAIL_PASSWORD'] = config_data['MAIL']['MAIL_PASSWORD']
app.config['MAIL_USE_TLS'] = config_data['MAIL']['MAIL_USE_TLS']
app.config['MAIL_USE_SSL'] = config_data['MAIL']['MAIL_USE_SSL']
app.config['MAIL_DEFAULT_SENDER'] = config_data['MAIL']['MAIL_DEFAULT_SENDER']
mail = Mail(app)


def send_mail(email_to, subject, template, data={}):
    """This method is used to send emails."""
    try:
        environment = config_data.get('APP_ENV')

        if environment in ENVIRONMENT_LIST:

            data.update({
                'app_name': APP_NAME
            })

            msg = Message(subject, sender=(config_data['MAIL']['MAIL_DEFAULT_SENDER_NAME'],
                                           config_data['MAIL']['MAIL_DEFAULT_SENDER']), recipients=[email_to])

            with app.app_context():
                msg.html = render_template(template, data=data)
                token = jwt.encode(payload=data, key=config_data['JWT_SALT'])
                headers = {
                    'X-Mailgun-Variables': json.dumps({'email_details': token})}
                msg.extra_headers = headers
                response = mail.send(msg)
                logger.info('Mail sent successfully : {0}'.format(response))

    except Exception as e:
        logger.error('Unable to send mail: ' + str(e))
        logger.error(traceback.format_exc())


def send_error_notification(email_to, subject, template, data={}, error_message=None, traceback_info=None):
    """This method is used to send error notifications emails."""
    try:

        environment = config_data.get('APP_ENV')

        if environment in ENVIRONMENT_LIST:
            data.update({
                'app_name': APP_NAME,
                'env': environment,
                'error_message': error_message,
                'traceback_info': traceback_info
            })

            msg = Message(subject, sender=(config_data['MAIL']['MAIL_DEFAULT_SENDER_NAME'],
                                           config_data['MAIL']['MAIL_DEFAULT_SENDER']), recipients=[email_to])

            with app.app_context():
                msg.html = render_template(template, data=data)
                token = jwt.encode(payload=data, key=config_data['JWT_SALT'])
                headers = {
                    'X-Mailgun-Variables': json.dumps({'email_details': token})}
                msg.extra_headers = headers
                response = mail.send(msg)
                logger.info('Mail sent successfully : {0}'.format(response))

    except Exception as e:
        logger.error('Unable to send error notification email: ' + str(e))
        logger.error(traceback.format_exc())
