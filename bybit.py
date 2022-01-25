"""
This module collects all necessary data for one Pair
from the Bybit exchange

General
- Docs are found at:
  https://bybit-exchange.github.io/docs/inverse/?console#t-marketdata
- Requests go to:
  https://api-testnet.bybit.com/v2/public
- All Bybit responses include:
  "ret_code": 0, - error code 0 means success
  "ret_msg": "OK", - error message
  "ext_code": "",
  "ext_info": "",
  "result": [], - actual response
  "time_now": "1567109419.049271"

Error handling
- Always check the ret_code is 0
- Otherwise, return the error (ret_msg)

Other
! Dual-Price Mechanism:
https://help.bybit.com/hc/en-us/articles/360039261074-What-is-Dual-Price-mechanism-
"""

import os
from os.path import join
import json
import requests
import time
import argparse
import threading
from loguru import logger

from easydict import EasyDict as edict

# TODO: Eliminate duplicate function and mapping
def map_currency(currency, currency_map):
    """
    Returns the currency symbol as specified by the exchange API docs.
    NOTE: Some exchanges (kraken) use different naming conventions. (e.g. BTC->XBT)
    """
    if currency not in currency_map.keys():
        return currency
    return currency_map[currency]

def map_pair(pair, currency_map=None, debug=False):
    """
    Returns the pair symbol as specified by the exchange API docs.
    """
    if debug:
        logger.debug(f'pair_save_name: {pair}, pair: {"".join(pair.split("-"))}')
    if currency_map is None:
        return ''.join(pair.split('-'))

    return map_currency(pair.split('-')[0], currency_map) + \
           map_currency(pair.split('-')[1], currency_map)

BASE_SAVE_DIR = '../../datasets/'

BYBIT_BASE_ARGS = edict({'time': None,
                          'pair': None,
                         'savedir': None,
                         'order_book': 0,
                          'depth': 10,
                          'candles': 0,
                          'granularity': 60,
                          'trades': 0,
                          'ticker': 0})

DEBUG = False
API_LINK = 'https://api-testnet.bybit.com/v2/public/'
PAIR = 'BTCUSD'
LIMIT = 500
DEPTH = 10
INTERVAL = '1'
SINCE = '1581231260'


def get_order_book(pair=PAIR, depth=DEPTH, debug=DEBUG):
    """
    Returns level 2 order book
    Each side has a depth of 25
    """
    api_command = API_LINK + f'orderBook/L2?symbol={pair}'
    return make_request(api_command, debug)


def get_trades(pair=PAIR, limit=LIMIT, debug=DEBUG):
    """
    Returns last limit trades (500 by default)
    """
    api_command = API_LINK + f'trading-records?symbol={pair}&limit={limit}'
    return make_request(api_command, debug)

# Not supported
# def get_spreads(pair=PAIR, since):
#     """
#     Returns last recent spreads
#     """
#     raise NotImplementedError
#     api_command = API_LINK + f'Spreads?pair={pair}&since={since}'
#     return make_request(api_command)


def get_candles(pair=PAIR, interval=INTERVAL, since=SINCE, debug=DEBUG):
    """
    Returns last candles
    Defaults to 200 candles
    """
    api_command = API_LINK + \
        f'kline/list?symbol={pair}&interval={interval}&from={since}'
    return make_request(api_command, debug)


def get_ticker(pair=PAIR, interval=INTERVAL, since=SINCE, debug=DEBUG):
    """
    Returns ticker info (last candle)
    """
    api_command = API_LINK + \
        f'kline/list?symbol={pair}&interval={interval}&from={since}&limit=1'
    return make_request(api_command, debug)


BASE_SAVE_DIR = '../../datasets/'
FN_MAPPING = {
    'order_book': get_order_book,
    'candles': get_candles,
    'trades': get_trades,
    'ticker': get_ticker,
    # 'spread': get_spreads,
}


def store_info(save_dir, pair, pair_save_name, collection_time, info_type, **kwargs):
    """
    Logs API request response by
        info_type: FN_MAPPING.keys()
        timestamp: Unix time of request
    """
    logger.info(f'Collecting {info_type} data for {pair_save_name}')
    with open(join(save_dir, info_type) + '.txt', 'w') as file:
        start = time.time()
        while time.time() - start < collection_time:
            file.write(json.dumps({
                'ts': time.time(),
                'response': FN_MAPPING[info_type](pair=pair, **kwargs)
            }))
            file.write('\n')

    logger.info(f'Finished collecting {info_type} data for {pair_save_name}')


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--savedir', type=str, default=BASE_SAVE_DIR,
                        help='Base save directory')
    parser.add_argument('--time', type=int, default=5,
                        help='Time in seconds for which to run the data collector')
    # Pair
    parser.add_argument('--pair', type=str,
                        help='Pair to collect data on. Acceptable format is BASE-QUOTE')
    # Order book
    parser.add_argument('--order_book', type=int, default=0,
                        help='Get a list of open orders for a product. The amount of detail shown can be customized'
                             'with the level parameter.')
    parser.add_argument('--depth', type=int, default=10,
                        help='Depth of order book desired')
    # Candles
    parser.add_argument('--candles', type=int, default=0,
                        help='Historic rates for a product. Rates are returned in grouped buckets. '
                             'Candle schema is of the form [timestamp, price_low, price_high, price_open, price_close]')
    parser.add_argument('--granularity', type=int, default=0,
                        help='The granularity field must be one of the following values: '
                             '{60, 300, 900, 3600, 21600, 86400}. Otherwise, your request will be rejected. '
                             'These values correspond to timeslices representing one minute, five minutes, '
                             'fifteen minutes, one hour, six hours, and one day, respectively.')
    # # 24H, 30D stats
    # parser.add_argument('--spreads', type=int, default=0,
    #                     help='Collect 24h, 30D stats')
    # Trades
    parser.add_argument('--trades', type=int, default=0,
                        help='Gets a list the latest trades for a product.')
    # Ticker
    parser.add_argument('--ticker', type=int, default=0,
                        help='Gets snapshot information about the last trade (tick), best bid/ask and 24h volume.')

    parser_args = parser.parse_args()

    return parser_args


def main(args):
    pair_save_name = args.pair
    pair = map_pair(pair_save_name)
    collection_time = args.time
    # Ob
    use_ob = args.order_book
    if use_ob:
        depth = args.depth
    # Candles
    use_candles = args.candles
    if use_candles:
        candles_granularity = args.granularity
    # # Stats
    # use_spreads = args.spreads
    # Trades
    use_trades = args.trades
    # Ticker
    use_ticker = args.ticker

    save_dir = join(*[args.savedir, pair_save_name, 'bybit'])
    os.makedirs(save_dir, exist_ok=True)
    default_args = (save_dir, pair, pair_save_name, collection_time)

    threads = []
    if use_ticker:
        make_thread(threads, default_args, 'ticker', {})
    if use_ob:
        make_thread(threads, default_args, 'order_book', {'depth': depth})
    # if use_spreads:
    #     t = threading.Thread(
    #         target=store_info,
    #         args=(save_dir, pair, collection_time, 'spread'),
    #     )
    #     threads.append(t)
    if use_trades:
        make_thread(threads, default_args, 'trades', {})
    if use_candles:
        make_thread(threads, default_args, 'candles', {
                    'granularity': candles_granularity})

    for t in threads:
        t.start()


if __name__ == "__main__":
    args = parse_arguments()
    main(args)

### Helpers ###


def make_request(url, debug=DEBUG):
    """
    Makes a request and handles the response
    Returns result or error message
    """
    if debug:
        logger.info(f'GET {url}')
    resp = requests.get(url).json()
    if resp['ret_code'] == 0:
        return resp['result']
    return resp['ret_msg']


def make_thread(threads, args, function, kwargs):
    """
    Executes function in a thread
    Returns the thread object
    """
    t = threading.Thread(
        target=store_info,
        args=(*args, function),
        kwargs=kwargs
    )
    threads.append(t)
