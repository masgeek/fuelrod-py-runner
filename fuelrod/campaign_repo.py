import sqlalchemy
from sqlalchemy import desc
from sqlalchemy.orm import sessionmaker

from fuelrod.fuelrod_api import MessageStatus
from my_logger import MyLogger
from orm.database_conn import MyDb
from orm.fuelrod import MessageQueue, SmsCampaign


class CampaignRepo:

    def __init__(self):
        self.db_engine = MyDb()
        self.session = sessionmaker(bind=self.db_engine)
        self.logging = MyLogger()

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
    def process_queue(self, campaign_id: int, limit: int = 200):
        my_session = self.session()

        messages = my_session.query(MessageQueue) \
            .filter(MessageQueue.campaign_id == campaign_id,
                    MessageQueue.message_status == MessageStatus.USER_OPTED_OUT.name) \
            .order_by(desc(MessageQueue.sms_count)) \
            .limit(limit=limit) \
            .all()

        message_count: int = 1
        total_messages = len(messages)
        if total_messages >= 0:
            # Update the campaign
            self.update_campaign(campaign_id=campaign_id, status=MessageStatus.IN_PROGRESS)
            return True

        message: MessageQueue
        for message in messages:
            self.logging.info(f"Processing message {message_count} of {total_messages} length {message.sms_count}")
            message_count = message_count + 1
            message.message_status = MessageStatus.IN_PROGRESS.name
            my_session.add(message)

        my_session.commit()
        my_session.close()
        return True
