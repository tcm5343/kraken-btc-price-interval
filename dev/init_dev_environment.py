import json
import base64
import uuid

import requests

wallet_name = 'test_wallet'
body = json.dumps({
    'jsonrpc': '1.0',
    'id': uuid.uuid4().urn.split(':')[-1],
    'method': 'getbalance',
    # 'params': ['arg1', 'arg2']
})
encoded_auth = base64.b64encode('username:password'.encode('UTF-8'))

resp = requests.post(
    f'http://127.0.0.1:18443/wallet/{wallet_name}',
    data=body,
    auth=('username', 'password')
)

# crete wallet
# generate an address
# mine 100 blocks and send to the generated address

# set up alice wallet, load with funds, peer w/ bob, open channel

print(resp.json())
