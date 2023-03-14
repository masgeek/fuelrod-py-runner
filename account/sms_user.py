import requests
import sqlalchemy
from sqlalchemy import desc, asc
from sqlalchemy.orm import sessionmaker


class SmsUser:

    def __init__(self, db_engine=None, base_url='http://localhost:9000/api'):
        self.db_engine = db_engine
        self.base_url = base_url

    def auth_token(self, username, password):
        _url = self.base_url + "/v1/account/auth"
        headers = {
            "Content-Type": "application/json",
        }

        payload = {
            "username": username,
            "password": password
        }

        _response = requests.post(url=_url, json=payload)
        # _response = requests.request("POST", _url, json=payload, headers=headers)
        _response.raise_for_status()
        return _response.json()

    def fee_endpoints(self, token):
        _url = self.base_url + "/v1/fee-endpoints"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }
        _response = requests.get(url=_url, headers=headers)
        _response.raise_for_status()
        return _response.json()
