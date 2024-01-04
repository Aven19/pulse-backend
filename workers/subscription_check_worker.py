"""class for subscription check methods"""


class SubscriptionCheckWorker:
    """worker for checking subscription status"""

    @classmethod
    def subscription_check(cls):
        """This method is used to set subscription as expired if current time > subscription_end_time"""
        from app import app
        from app import logger
        with app.app_context():
            import time
            from app import db

            from app.helpers.constants import SubscriptionStates
            from app.models.subscription import Subscription
            from app.models.account import Account
            from app.models.user import User
            logger.info('**********in subscription_check function***********')

            try:

                subscription_list = db.session.query(Subscription).all()

                current_time = int(time.time())

                for subscription in subscription_list:
                    subscription_end_time = subscription.end_date
                    if current_time > int(subscription_end_time.timestamp()) and subscription.status != SubscriptionStates.EXPIRED.value:
                        logger.info(
                            '*************** changed to expired*******************')
                        subscription.status = SubscriptionStates.EXPIRED.value

                        # to do send mail to user for renewal
                        account_id = subscription.account_id

                        account = Account.get_by_uuid(uuid=account_id)

                        primary_user_id = account.primary_user_id

                        primary_user = User.get_by_id(primary_user_id)

                        mail_data = {
                            'subject': 'Subscription Expired',
                            'email_to': primary_user.email,
                            'account_legal_name': account.legal_name,
                            'first_name': primary_user.first_name
                        }

                        subscription.updated_at = current_time
                        subscription.deactivated_at = current_time

                        db.session.commit()

                        logger.info(
                            '---------------------------expired--------------------------')
                        SubscriptionCheckWorker.enqueue_expiry_email(
                            data=mail_data)
                        logger.info(
                            '---------------------------called enqueue expiry mail--------------------------')

            except Exception as e:
                logger.error('failed checking subscription status: ' + str(e))

    @staticmethod
    def enqueue_expiry_email(data):
        """enqueues email invite in SES EMAIL DELIVERY queue"""

        from app import logger
        from app import ses_email_delivery_q
        from app import config_data
        from app.helpers.constants import QueueTaskStatus
        from app.models.queue_task import QueueTask
        from app.helpers.constants import EntityType, QueueName
        from workers.email_worker import EmailWorker

        logger.info(
            '--------------------expiry email enqueued----------------')

        queue_task = QueueTask.add_queue_task(queue_name=QueueName.SES_EMAIL_DELIVERY,
                                              account_id=None,
                                              owner_id=None,
                                              status=QueueTaskStatus.NEW.value,
                                              entity_type=EntityType.SES_EMAIL_DELIVERY.value,
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

            ses_email_delivery_q.enqueue(EmailWorker.send_expiry_mail, data, job_timeout=config_data.get('RQ_JOB_TIMEOUT'))  # type: ignore  # noqa: FKA100
