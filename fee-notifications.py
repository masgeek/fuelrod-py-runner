from os import environ
from dotenv import load_dotenv
from sqlalchemy import create_engine

from MessageStatus import MessageStatus
from account import sms_user
from fee import fee_payment
from campaign import campaign_queue as msg

load_dotenv(verbose=True)
api_username = environ.get('API_USER')
api_pass = environ.get('API_PASS')

fee_api_user = environ.get('FEE_API_USER')
fee_api_pass = environ.get('FEE_API_PASS')

engine = create_engine("mysql://fuelrod:fuelrod@localhost/fuelrod", echo=True, echo_pool=False, hide_parameters=True)

apiUser = sms_user.SmsUser()
feeProcessing = fee_payment.FeePayment(api_user=fee_api_user, api_pass=fee_api_pass)

resp = apiUser.auth_token(username=api_username, password=api_pass)
token = resp['accessToken']

fee_endpoints_resp = apiUser.fee_endpoints(token=token)
fee_endpoints = fee_endpoints_resp['content']

for endpoint in fee_endpoints:
    result = feeProcessing.process_notifications(username=endpoint['username'], endpoint=endpoint['endpoint'])
