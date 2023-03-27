import json
from os import environ

import concurrent.futures

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine

from fuelrod import sms_user
from fee import fee_payment

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


def process_sms_notifications(username, end_point, page_size, page_no=1):
    print(f'processing data for {username} for page number {page_no}')
    yield feeProcessing.process_notifications(username=username,
                                              endpoint=end_point,
                                              page_size=page_size,
                                              page_number=page_no)


def send_sms(sms_message):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    response = requests.post('http://localhost:9000/api/v1/sms/fee-notification', headers=headers, json=sms_message)
    response.raise_for_status()


if __name__ == "__main__":
    for fee_endpoint in fee_endpoints:
        __username = fee_endpoint['username']
        __end_point = fee_endpoint['endpoint']
        all_results = []
        sent_messages = []
        _page_number = 1
        _page_size = 5
        _results = feeProcessing.process_notifications(username=__username, endpoint=__end_point, page_size=_page_size)
        resp_value = next(_results)
        last_page = 3  # resp_value['last_page']
        # print(json.dumps(resp_value['links'], indent=4))
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(process_sms_notifications,
                                __username,
                                __end_point,
                                _page_size,
                                _page_number) for _page_number in range(1, last_page + 1)
            ]
            for future in concurrent.futures.as_completed(futures):
                try:
                    results = future.result()
                    resp_val = next(next(results))
                    next_page_url = resp_val['next_page_url']
                    all_results.extend(resp_val['data'])
                    print(f"Last page number {next_page_url}")
                except Exception as exc:
                    print(f"Exception: {exc}")
        print(f"Size of results is {len(all_results)}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # Process each item in the dictionary asynchronously and send the result to the REST API
            for message in all_results:
                sent_messages.append({
                    "endpoint": __end_point,
                    "username": __username,
                    "id": message['id']
                }
                )

                _future = executor.submit(send_sms, message)

        for sent in sent_messages:
            print(sent['endpoint'])
