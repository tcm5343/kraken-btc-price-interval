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
wallet_name = 'lightning_ledger_dev_wallet'


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
    create_wallet_resp = __call_bitcoin_core('createwallet', [wallet_name])
    if create_wallet_resp.get('error'):
        if create_wallet_resp['error']['code'] == -4:
            log.info(f'{wallet_name} already exists')
        else:
            raise Exception(f'Error creating {wallet_name}: {create_wallet_resp}')
    else:
        log.info(f'{wallet_name} created')


create_btc_core_wallet()
address = __call_bitcoin_core('getnewaddress', [])['result']
log.info(address)
mine_resp = __call_bitcoin_core('generatetoaddress', [101, address])
log.info(mine_resp)

