import logging.config
from os import environ, path

from dotenv import load_dotenv
from sqlalchemy import create_engine

from MessageStatus import MessageStatus
from fuelrod import campaign_queue as msg

load_dotenv(verbose=True)

engine = create_engine("mysql://fuelrod:fuelrod@localhost/fuelrod", echo=True, echo_pool=False, hide_parameters=True)

log_level = environ.get('LOG_LEVEL', 'INFO')

logfileError = path.join(path.dirname(path.abspath(__file__)), "logs/errors.log")
logfileInfo = path.join(path.dirname(path.abspath(__file__)), "logs/info.log")

c_handler = logging.StreamHandler()
f_handler = logging.FileHandler(logfileError, 'w', 'utf-8')
f_info_handler = logging.FileHandler(logfileInfo, 'w', 'utf-8')
c_handler.setLevel(logging.DEBUG)
f_info_handler.setLevel(logging.INFO)
f_handler.setLevel(logging.ERROR)

logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',
                    handlers=[c_handler, f_handler, f_info_handler],
                    level=log_level)

messages = msg.FuelrodQueue(db_engine=engine)

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
