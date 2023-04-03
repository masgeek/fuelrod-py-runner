import concurrent.futures
import string
import time

import sqlalchemy
from sqlalchemy import desc
from sqlalchemy.orm import sessionmaker

from fuelrod.fuelrod_api import MessageStatus, MessagingService
from my_logger import MyLogger
from orm.database_conn import MyDb
from orm.fuelrod import MessageQueue, SmsCampaign


class CampaignRepo:

    def __init__(self, fuelrod_token=None):
        self.db_engine = MyDb()
        self.session = sessionmaker(bind=self.db_engine)
        self.logging = MyLogger()
        self.msg_service = MessagingService(token=fuelrod_token)

    def load_unprocessed_campaigns(self, campaign_status: MessageStatus, limit=1):
        self.logging.info(f"Loading campaigns with status {campaign_status.name} limited at {limit} records")

        return self.session().query(SmsCampaign) \
            .filter_by(campaign_status=campaign_status.name) \
            .limit(limit=limit) \
            .all()

    # noinspection PyTypeChecker
    def update_campaign(self, campaign_id: int, status: MessageStatus):
        my_session = self.session()
        is_saved = False
        try:
            sms_campaign: SmsCampaign = my_session.query(SmsCampaign) \
                .filter(SmsCampaign.id == campaign_id) \
                .first()
            sms_campaign.campaign_status = status.name
            sms_campaign.updated_at = sqlalchemy.func.now()

            my_session.add(sms_campaign)
            my_session.commit()
            self.logging.info(f"Updated campaign with status {status.name}")
            is_saved = True
        except Exception as ex:
            my_session.rollback()
            self.logging.critical(f"An exception has occurred {ex}")
        finally:
            self.logging.debug("Closing database session")
            my_session.close()

        return is_saved

    # noinspection PyTypeChecker
    def process_queue(self, campaign_id: int, username: string, limit: int = 1000):
        my_session = self.session()

        messages = my_session.query(MessageQueue) \
            .filter(MessageQueue.campaign_id == campaign_id,
                    MessageQueue.message_status == MessageStatus.IN_PROGRESS.name) \
            .order_by(desc(MessageQueue.sms_count)) \
            .limit(limit=limit) \
            .all()

        message_count: int = 1
        total_messages: int = len(messages)
        try:
            if total_messages <= 0:
                self.logging.info(
                    f"All messages have been sent updating campaign status to `{MessageStatus.COMPLETED.name}`")
                self.update_campaign(campaign_id=campaign_id, status=MessageStatus.COMPLETED)
            else:
                msg: MessageQueue
                campaign_payload = []
                for msg in messages:
                    self.logging.info(
                        f"Processing message {message_count} of {total_messages} length {msg.sms_count}")
                    message_count = message_count + 1
                    msg.message_sent = True
                    msg.message_status = MessageStatus.MESSAGE_SENT.name
                    sms_payload = {
                        'username': username,
                        'campaign_id': campaign_id,
                        'message': msg.message,
                        'phone_number': msg.phone_number,
                        'hash': msg.message_hash
                    }
                    campaign_payload.append(sms_payload)

                start = time.time()
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.map(self.msg_service.send_campaign, username, campaign_payload)
                end = time.time()
                self.logging.debug(f'Campaign processed in {abs(end - start)} seconds')

            my_session.commit()
        except Exception as ex:
            my_session.rollback()
            self.logging.critical(f"An exception has occurred {ex}. rolling back transactions")
        finally:
            self.logging.debug("Closing database session")
            my_session.close()
