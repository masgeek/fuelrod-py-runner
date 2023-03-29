import enum
import json

import requests
from calendar import timegm
from datetime import datetime
import requests
from cachetools import cached, TTLCache
from requests import HTTPError

cache = TTLCache(maxsize=100, ttl=86400)


class MessageStatus(enum.Enum):
    IN_PROGRESS = 'IN_PROGRESS'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'
    MESSAGE_SENT = 'MESSAGE_SENT'
    PAUSED_NO_CREDIT = 'PAUSED_NO_CREDIT'
    UNPAID_INVOICES = 'UNPAID_INVOICES'
    ACTIVE = 'ACTIVE'
    DUPLICATE_MESSAGE = 'DUPLICATE_MESSAGE'
    PAUSED = 'PAUSED'
    INSUFFICIENT_CREDITS = 'INSUFFICIENT_CREDITS'
    STATUS_PENDING = 'STATUS_PENDING'
    SUCCESS = 'SUCCESS'
    USER_OPTED_OUT = 'USER_OPTED_OUT'


class SmsUser:

    def __init__(self, fuelrod_base_url, my_logger):
        self.base_url = fuelrod_base_url
        self.my_logger = my_logger

    @cached(cache=cache)
    def auth_token(self, username, password):
        self.my_logger.info(f"Authenticating user {username}")
        _url = self.base_url + "/v1/account/auth"

        payload = {
            "username": username,
            "password": password
        }
        token = self._read_token_file()
        if token is not None:
            self.my_logger.debug("Found token in file using it instead of refreshing")
            return token
        try:
            self.my_logger.debug("Fetching new token from aPI")
            _response = requests.post(url=_url, json=payload)
            _response.raise_for_status()
            resp = _response.json()
            with open(f'token/fuelrod-token.json', 'w') as json_file_obj:
                json.dump(resp, json_file_obj, indent=4)

            token = resp['accessToken']
        except HTTPError as http_err:
            self.my_logger.error(f'Unable to authenticate user {http_err}')
        except Exception as err:
            self.my_logger.error(f'Other error occurred {err}')

        return token

    def fee_endpoints(self, token):
        _url = self.base_url + "/v1/fee-endpoints"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }
        try:
            with requests.session() as session:
                _response = session.get(url=_url, headers=headers)
                _response.raise_for_status()
                resp = _response.json()
                return resp['content']
        except HTTPError as http_err:
            self.my_logger.error(f'Unable to fetch fee endpoints -> {http_err}')
        except Exception as err:
            self.my_logger.error(f'Other error occurred -> {err}')

    def _read_token_file(self):
        token_json_file = f'token/fuelrod-token.json'
        token = None
        try:
            with open(token_json_file, 'r') as json_file_obj:
                token_data = json.load(json_file_obj)
                current_time = timegm((datetime.utcnow().utctimetuple()))
                expiry_time = 0
                if 'accessToken' in token_data:
                    token = token_data['accessToken']
                    expiry_time = token_data['expiry']
                token_expired = current_time > expiry_time
                if token_expired:
                    token = None
                    self.my_logger.info(f'Token has expired at {expiry_time}')

        except Exception as err:
            self.my_logger.critical(f'Error reading {token_json_file} file {err}')

        return token


class MessagingService:
    def __init__(self, fuelrod_base_url, my_logger):
        self.base_url = fuelrod_base_url
        self.my_logger = my_logger

    pass
