import json
from os import environ

from dotenv import load_dotenv

from my_logger import MyLogger
from fuelrod.fuelrod_api import MessageStatus, SmsUser
from fuelrod.user_repo import UserRepo
from fuelrod.campaign_repo import CampaignRepo

load_dotenv(verbose=True)
debug_db = environ.get('DEBUG_DB', False)
log_level = environ.get('LOG_LEVEL', 'INFO')
fuelrod_base_url = environ.get('SMS_BASE_URL')
api_username = environ.get('SMS_API_USER')
api_pass = environ.get('SMS_API_PASS')

logging = MyLogger()

campaignRepo = CampaignRepo()
userRepo = UserRepo()
apiUser = SmsUser(fuelrod_base_url=fuelrod_base_url, my_logger=logging)
token = apiUser.auth_token(username=api_username, password=api_pass)
campaigns = campaignRepo.load_unprocessed_campaigns(campaign_status=MessageStatus.COMPLETED, limit=10)

total_campaigns = len(campaigns)
if total_campaigns <= 0:
    logging.info("No pending campaign(s)")
else:
    logging.info(f"Processing {total_campaigns} campaign(s)")
    for campaign in campaigns:
        user = userRepo.load_user(campaign.user_uuid)
        if user.flag_reason == MessageStatus.ACTIVE.name:
            logging.info(f"Processing message for {user.client_name}")
            creditInfo = apiUser.credit_info(user_uuid=user.uuid, token=token)
            logging.debug(f"The credit info is \n{json.dumps(creditInfo, indent=4)}")

            status_string = creditInfo['status']
            status_enum = MessageStatus.__getitem__(status_string)

            campaignRepo.update_campaign(campaign.id, status_enum)
            if creditInfo['canSend']:
                logging.info(f"Processing campaign `{campaign.campaign_name.upper()}`")
            else:
                logging.warning("Cannot process this campaign due to insufficient credits")
