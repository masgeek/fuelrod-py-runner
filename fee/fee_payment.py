from datetime import datetime
from calendar import timegm
import string
import requests
import json
from cachetools import cached, TTLCache
from requests import HTTPError
import os, time

cache = TTLCache(maxsize=100, ttl=86400)


class FeePayment:
    def __init__(self, api_user: string, api_pass: string, my_logger: object = None):
        self.api_user = api_user
        self.api_pass = api_pass
        self.my_logger = my_logger

    def process_notifications(self, username, endpoint, page_number=1):
        # _url = endpoint + "/api/sms-notifications
        _url = f"http://fee-processor.test/api/sms-notifications/username/{username}?page={page_number}"
        token = self._get_api_token(endpoint, username)

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }
        with requests.Session() as session:
            _response = session.get(url=_url, headers=headers)
            _response.raise_for_status()
            yield _response.json()

    @cached(cache=cache)
    def _get_api_token(self, endpoint, username):
        _url = endpoint + "/api/token"
        payload = {
            "username": self.api_user,
            "password": self.api_pass
        }
        token = self._read_token_file(endpoint, username)
        if token is not None:
            return token

        try:
            _response = requests.post(url=_url, json=payload, verify=False)
            _response.raise_for_status()
            resp = _response.json()
            # save to external json file
            with open(f'fee_token/{username}-fee-token.json', 'w') as json_file_obj:
                json.dump(resp['data'], json_file_obj, indent=4)
            token = resp['data']['token']
        except HTTPError as http_err:
            print(f'Unable to authenticate user {http_err}')
        except Exception as err:
            print(f'Other error occurred {err}')

        return token

    def _read_token_file(self, endpoint, username):
        token_json_file = f'fee_token/{username}-fee-token.json'
        token = None
        try:
            with open(token_json_file, 'r') as json_file_obj:
                token_data = json.load(json_file_obj)

                current_time = timegm((datetime.utcnow().utctimetuple()))
                expiry_time = 0
                if 'token' in token_data:
                    token = token_data['token']
                    expiry_time = token_data['expires']

                token_expired = current_time > expiry_time
                if token_expired:
                    print(f'No token file or token has expired for {username},  fetching new one')

        except Exception as err:
            print(f'Error reading {token_json_file} file {err}')

        return token
