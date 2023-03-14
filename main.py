from os import environ

from dotenv import load_dotenv
from sqlalchemy import create_engine

from MessageStatus import MessageStatus
from account import sms_user
from campaign import campaign_queue as msg

load_dotenv(verbose=True)

engine = create_engine("mysql://fuelrod:fuelrod@localhost/fuelrod", echo=True, echo_pool=False, hide_parameters=True)

messages = msg.FuelrodQueue(db_engine=engine)
apiUser = sms_user.SmsUser()

api_username = environ.get('API_USER')
api_pass = environ.get('API_PASS')

resp = apiUser.auth_token(username=api_username, password=api_pass)
token = resp['accessToken']

campaign = messages.has_campaign(campaign_status=MessageStatus.IN_PROGRESS)

if not campaign:
    print("No pending campaigns")
else:
    user = campaign.users
    if user.flag_reason == MessageStatus.ACTIVE.name:
        print(f"The client {user.client_name} has unpaid invoices")
        messages.update_campaign(campaign_id=campaign.id, status=MessageStatus.IN_PROGRESS)
    else:
        print(f"Processing {campaign.campaign_name.upper()} campaign of  id: {campaign.id}")
        messages.process_queue(campaign.id)
