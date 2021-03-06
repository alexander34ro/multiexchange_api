"""
This module will collect all the necessary data for one Pair from
the Kraken exchange
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


def map_pair(pair, currency_map, debug=False):
    """
    Returns the pair symbol as specified by the exchange API docs.
    """
    if debug:
        logger.debug(f'pair: {pair}')
    return map_currency(pair.split('-')[0], currency_map) + \
           map_currency(pair.split('-')[1], currency_map)

KRAKEN_BASE_ARGS = edict({'time': None,
                          'pair': None,
                          'savedir': None,
                          'order_book': 0,
                          'depth': 10,
                          'candles': 0,
                          'granularity': 60,
                          'spreads': 0,
                          'trades': 0,
                          'ticker': 0})

API_LINK = 'https://api.kraken.com/0/public/'
KRAKEN_NAME_CONVENTION = {
    'BTC': 'XBT'
    }

INVERSE_KRAKEN_NAME_CONVENTION = {
    'XBT': 'BTC'
    }

BASE_SAVE_DIR = '../../datasets/'


def get_order_book(pair, depth):
    """Returns order book by depth"""
    api_command = API_LINK + f'Depth?pair={pair}&count={depth}'
    resp = requests.get(api_command).json()
    if not resp['error']:  # empty
        return resp
    return resp['error']


def get_trades(pair, since):
    """Returns last 1000 trades by default"""
    api_command = API_LINK + f'Trades?pair={pair}&since={since}'
    resp = requests.get(api_command).json()
    if not resp['error']:  # empty
        return resp
    return resp['error']


def get_spreads(pair, since):
    """Returns last recent spreads"""
    api_command = API_LINK + f'Spreads?pair={pair}&since={since}'
    resp = requests.get(api_command).json()
    if not resp['error']:  # empty
        return resp
    return resp['error']


def get_candles(pair, granularity, since=None):
    """
    Returns last candles
    Note:  the last entry in the OHLC array is for the current, not-yet-committed frame and will always be present,
           regardless of the value of since.
    """
    if since is None:
        api_command = API_LINK + f'OHLC?pair={pair}&interval={granularity}'
    else:
        api_command = API_LINK + f'OHLC?pair={pair}&interval={granularity}&since={since}'
    resp = requests.get(api_command).json()
    if not resp['error']:  # empty
        return resp
    return resp['error']


def get_ticker(pair):
    """
    Returns ticker info.
    Note:Today's prices start at midnight UTC
    """
    api_command = API_LINK + f'Ticker?pair={pair}'
    resp = requests.get(api_command).json()
    if not resp['error']:  # empty
        return resp
    return resp['error']


def store_info(save_dir, api_pair_symbol, pair_print_name, collection_time, info_type, **kwargs):
    """
    Logs API request response by
        info_type: FN_MAPPING.keys()
        timestamp: Unix time of request
    """
    logger.info(f"Collecting {info_type} data for {pair_print_name} to {save_dir}")
    with open(join(save_dir, info_type) + '.txt', 'w') as file:
        start = time.time()
        while time.time() - start < collection_time:
            file.write(json.dumps({
                'ts': time.time(),
                'response': next(iter(FN_MAPPING[info_type](pair=api_pair_symbol, **kwargs)['result'].values()))
            }))
            file.write('\n')

    logger.info(f'Finished collecting {info_type} data for {pair_print_name}')


FN_MAPPING = {
    'order_book': get_order_book,
    'candles': get_candles,
    'trades': get_trades,
    'ticker': get_ticker,
    'spread': get_spreads,
}

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
                        help='Depth of order book ')
    # Candles
    parser.add_argument('--candles', type=int, default=0,
                        help='Historic rates for a product. Rates are returned in grouped buckets. '
                             'Candle schema is of the form [timestamp, price_low, price_high, price_open, price_close]')
    parser.add_argument('--granularity', type=int, default=0,
                        help='The granularity field must be one of the following values: '
                             '{60, 300, 900, 3600, 21600, 86400}. Otherwise, your request will be rejected. '
                             'These values correspond to timeslices representing one minute, five minutes, '
                             'fifteen minutes, one hour, six hours, and one day, respectively.')
    # 24H, 30D stats
    parser.add_argument('--spreads', type=int, default=0,
                        help='Collect 24h, 30D stats')
    # Trades
    parser.add_argument('--trades', type=int, default=0,
                        help='Gets a list the latest trades for a product.')
    # Ticker
    parser.add_argument('--ticker', type=int, default=0,
                        help='Gets snapshot information about the last trade (tick), best bid/ask and 24h volume.')

    parser_args = parser.parse_args()

    return parser_args


def main(args):
    collection_time = args.time
    pair = args.pair
    api_pair_symbol = map_pair(pair, KRAKEN_NAME_CONVENTION)
    # Ob
    use_ob = args.order_book
    if use_ob: depth = args.depth
    # Candles
    use_candles = args.candles
    if use_candles: candles_granularity = args.granularity
    # Stats
    use_spreads = args.spreads
    # Trades
    use_trades = args.trades
    # Ticker
    use_ticker = args.ticker

    save_dir = join(*[args.savedir, pair, 'kraken'])
    os.makedirs(save_dir, exist_ok=True)
    threads = []
    if use_ticker:
        t = threading.Thread(target=store_info,
                             args=(save_dir, api_pair_symbol, pair, collection_time, 'ticker'),
                             )
        threads.append(t)

    if use_ob:
        t = threading.Thread(target=store_info,
                             args=(save_dir, api_pair_symbol, pair, collection_time, 'order_book'),
                             kwargs={'depth': depth}
                             )
        threads.append(t)

    if use_spreads:
        t = threading.Thread(target=store_info,
                             args=(save_dir, api_pair_symbol, pair, collection_time, 'spread'),
                             )
        threads.append(t)

    if use_trades:
        t = threading.Thread(target=store_info,
                             args=(save_dir, api_pair_symbol, pair, collection_time, 'trades'),
                             )
        threads.append(t)

    if use_candles:
        t = threading.Thread(target=store_info,
                             args=(save_dir, api_pair_symbol, pair, collection_time, 'candles'),
                             kwargs={'granularity': candles_granularity}
                             )
        threads.append(t)

    for t in threads:
        t.start()


if __name__ == "__main__":
    args = parse_arguments()
    main(args)
