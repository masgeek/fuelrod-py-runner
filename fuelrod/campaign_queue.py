import sqlalchemy
from sqlalchemy import desc
from sqlalchemy.orm import sessionmaker

from MessageStatus import MessageStatus
from orm.fuelrod import MessageQueue, SmsCampaign


class FuelrodQueue:
    """

    @param db_engine:
    """

    def __init__(self, db_engine):
        self.db_engine = db_engine
        self.session = sessionmaker(bind=self.db_engine)

    def has_campaign(self, campaign_status: MessageStatus):
        return self.session().query(SmsCampaign) \
            .filter_by(campaign_status=campaign_status) \
            .one_or_none()

    # noinspection PyTypeChecker
    def update_campaign(self, campaign_id: int, status: MessageStatus):
        my_session = self.session()
        try:
            sms_campaign: SmsCampaign = my_session.query(SmsCampaign) \
                .filter(SmsCampaign.id == campaign_id) \
                .first()
            sms_campaign.campaign_status = status.name
            sms_campaign.updated_at = sqlalchemy.func.now()

            my_session.add(sms_campaign)
            my_session.commit()
            print(f"Updated campaign with status {status.name}")
            return True
        except Exception as ex:
            my_session.rollback()
            print(f"An exception has occurred {ex}")
        return False

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
            print(f"Processing message {message_count} of {total_messages} length {message.sms_count}")
            message_count = message_count + 1
            message.message_status = MessageStatus.IN_PROGRESS.name
            my_session.add(message)

        my_session.commit()
        my_session.close()
        return True
