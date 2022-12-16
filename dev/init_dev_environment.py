import json
import uuid
import sys
import logging

import requests

logging.basicConfig(
    stream=sys.stdout,
    format="%(levelname)s - %(asctime)s - %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)
wallet_name = 'test_wallet'


def __call_bitcoin_core(method: str, args: list):
    body = json.dumps({
        'jsonrpc': '1.0',
        'id': uuid.uuid4().urn.split(':')[-1],
        'method': method,
        'params': args
    })
    resp = requests.post(
        f'http://127.0.0.1:18443/wallet/{wallet_name}',
        data=body,
        auth=('username', 'password')
    )
    return resp.json()


def create_btc_core_wallet():
    create_wallet_resp = __call_bitcoin_core('createwallet', ['lightning_ledger_dev_wallet'])
    if create_wallet_resp.get('error'):
        if create_wallet_resp['error']['code'] == -4:
            log.info('wallet already exists')
        else:
            raise Exception(f'Error creating wallet: {create_wallet_resp}')
    else:
        log.info('new wallet created')


# crete wallet
create_btc_core_wallet()
# generate an address
# mine 100 blocks and send to the generated address

# set up alice wallet, load with funds, peer w/ bob, open channel

