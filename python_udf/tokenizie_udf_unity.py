CREATE OR REPLACE FUNCTION fortanix_tokenize(column_name STRING, config MAP<STRING, STRING>)
RETURNS STRING
LANGUAGE PYTHON
AS $$
import requests
import base64
import json

class MissingKey(Exception):
    """
    Exception raised for missing keys in the input config
    """

    def __init__(self, key):
        super().__init__(f'Input config is missing required key `{key}`')

class Config:
    def __init__(self, config):
        self.config = config

    def get_or_err(self, key):
        value = config.get(key)
        if value is None:
            raise MissingKey(key)
        return value

def tokenize(token, config):
    fortanix_api_endpoint = config.get_or_err('fortanix_api_endpoint')
    fortanix_api_key = config.get_or_err('fortanix_api_key')
    key_id = config.get_or_err('key_id')

    url = f'{fortanix_api_endpoint}/crypto/v1/encrypt'

    headers = {
        "Authorization": f"Basic {fortanix_api_key}",
        "Content-Type": "application/json"
    }

    # for clarity .encode() and .decode() below are from converting
    # between bytes and str. You can't pass str directly to b64encode
    base64_message = base64.b64encode(token.encode()).decode()

    payload = {
        "key": {
            "kid": key_id
        },
        "plain": base64_message,
        "alg": "AES",
        "mode": "FPE"
    }

    response = requests.post(url, headers=headers, data=json.dumps(payload))

    if response.status_code == 200:
        tokenized_data = response.json().get('cipher')
        return base64.b64decode(tokenized_data).decode()
    else:
        raise Exception(f"Fortanix tokenize failed: {response.text}")

return tokenize(column_name, Config(config))
$$;

