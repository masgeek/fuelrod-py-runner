import requests
from cachetools import cached, TTLCache

cache = TTLCache(maxsize=100, ttl=86400)


class FeePayment:
    def __init__(self, username, password):
        self.username = username
        self.password = password

    def process_notifications(self, username, endpoint):
        # _url = endpoint + "/api/sms-notifications"
        _url = "http://fee-processor.test/api/sms-notifications"

        token = self.get_api_token(endpoint)
        return token
        # headers = {
        #     "Content-Type": "application/json",
        #     "Authorization": f"Bearer {token}"
        # }
        # _response = requests.get(url=_url, headers=headers)
        # _response.raise_for_status()
        # return _response.json()

    @cached(cache=cache)
    def get_api_token(self, endpoint):
        _url = endpoint + "/api/token"
        payload = {
            "username": self.username,
            "password": self.password
        }

        _response = requests.post(url=_url, json=payload, verify=False)
        _response.raise_for_status()
        resp = _response.json()

        print(resp['data'])
        pass
