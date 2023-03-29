from os import environ

from dotenv import load_dotenv

from my_logger import MyLogger
from fuelrod.fuelrod_api import MessageStatus
from fuelrod.campaign_queue import Campaign

load_dotenv(verbose=True)
debug_db = environ.get('DEBUG_DB', False)
log_level = environ.get('LOG_LEVEL', 'INFO')

logging = MyLogger()

messages = Campaign()

campaigns = messages.load_unprocessed_campaigns(campaign_status=MessageStatus.COMPLETED)

total_campaigns = len(campaigns)
if total_campaigns <= 0:
    logging.info("No pending campaigns")
else:
    logging.info(f"Processing {total_campaigns} campaigns")
    for campaign in campaigns:
        user = campaign.users
        if user.flag_reason == MessageStatus.ACTIVE.name:
            logging.warning(f"The client {user.client_name} has unpaid invoices")
            # messages.update_campaign(campaign_id=campaign.id, status=MessageStatus.IN_PROGRESS)
        else:
            logging.info(f"Processing {campaign.campaign_name.upper()} campaign of  id: {campaign.id}")
            # messages.process_queue(campaign.id)
