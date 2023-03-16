from os import environ

import concurrent.futures
from dotenv import load_dotenv
from sqlalchemy import create_engine

from account import sms_user
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


def fetch_notifications(username, end_point, page_no):
    _results = feeProcessing.process_notifications(username=username, endpoint=end_point, page_number=page_no)
    yield _results


if __name__ == "__main__":
    for endpoint in fee_endpoints:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            print(f'processing data for {endpoint["username"]}')
            results = []
            page_number = 1
            next_page_url = None
            while True:
                print(f"Processing message on page `{page_number}`")
                # yield_resp = feeProcessing.process_notifications(username=endpoint['username'],
                #                                                  endpoint=endpoint['endpoint'],
                #                                                  page_number=page_number)

                futures = [executor.submit(fetch_notifications(
                    username=endpoint['username'],
                    end_point=endpoint['endpoint'],
                    page_no=page_number))]

                for future in concurrent.futures.as_completed(futures):
                    try:
                        results = future.result()
                    except Exception as exc:
                        print(f"Exception: {exc}")
                    
                    # for resp_val in yield_resp:
                    #     results.extend(resp_val['data'])
                    #     next_page_url = resp_val['next_page_url']
                    #     page_number = page_number + 1

                    if next_page_url is None:
                        print("Exiting loop, no more data")
                        break
                print(len(results))
