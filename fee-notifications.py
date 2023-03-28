import concurrent.futures
import json
import logging
import logging.config
from os import environ, path

from dotenv import load_dotenv
from sqlalchemy import create_engine

from fee import fee_payment
from fuelrod import sms_user, sms_notification

load_dotenv(verbose=True)
fuelrod_base_url = environ.get('SMS_BASE_URL')
api_username = environ.get('SMS_API_USER')
api_pass = environ.get('SMS_API_PASS')

fee_api_user = environ.get('FEE_API_USER')
fee_api_pass = environ.get('FEE_API_PASS')
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

engine = create_engine("mysql://fuelrod:fuelrod@localhost/fuelrod", echo=True, echo_pool=False, hide_parameters=True)

apiUser = sms_user.SmsUser()
feeProcessing = fee_payment.FeePayment(api_user=fee_api_user, api_pass=fee_api_pass, my_logger=logging)

resp = apiUser.auth_token(username=api_username, password=api_pass)
token = resp['accessToken']

fee_endpoints_resp = apiUser.fee_endpoints(token=token)
fee_endpoints = fee_endpoints_resp['content']

smsNotification = sms_notification.SmsNotification(base_url=fuelrod_base_url, token=token)


def process_sms_notifications(username, end_point, page_size, page_no=1):
    logging.info(f'processing data for {username} for page number {page_no}')
    yield feeProcessing.process_notifications(username=username,
                                              endpoint=end_point,
                                              page_size=page_size,
                                              page_number=page_no)


def update_sent_message(sms_future):
    sms = sms_future.result()
    print(json.dumps(sms, indent=4))
    update_resp = feeProcessing.update_sms_notification(
        message_id=sms['id'],
        username=sms['username'],
        endpoint=sms['endpoint'])
    logging.debug(next(update_resp))


if __name__ == "__main__":
    for fee_endpoint in fee_endpoints:
        __username = fee_endpoint['username']
        __end_point = fee_endpoint['endpoint']
        all_results = []
        sent_messages = []
        _page_number = 1
        _page_size = 250
        _results = feeProcessing.process_notifications(username=__username, endpoint=__end_point, page_size=_page_size)
        resp_value = next(_results)
        last_page = resp_value['last_page']
        # print(json.dumps(resp_value['links'], indent=4))
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(process_sms_notifications,
                                __username,
                                __end_point,
                                _page_size,
                                _page_number) for _page_number in range(1, last_page + 1)
            }
            for future in concurrent.futures.as_completed(futures):
                try:
                    results = future.result()
                    resp_val = next(next(results))
                    next_page_url = resp_val['next_page_url']
                    all_results.extend(resp_val['data'])
                    logging.debug(f"Last page number {next_page_url}")
                except Exception as exc:
                    logging.error(f"Exception: {exc}", exc_info=True)

        logging.info(f"Size of results is {len(all_results)}")

        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Process each item in the dictionary asynchronously and send the result to the REST API
            for message in all_results:
                sent_messages.append({
                    "endpoint": __end_point,
                    "username": __username,
                    "id": message['id']
                })

                _result = executor.submit(smsNotification.send_sms, message, __end_point)
                _result.add_done_callback(update_sent_message)
