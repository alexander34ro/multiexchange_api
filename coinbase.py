"""
This module will collect all the necessary data for one Pair from
the Coinbase Pro exchange
"""
import os
from os.path import join
import json
import cbpro
import time
import argparse
import threading
from loguru import logger

from easydict import EasyDict as edict

CBPRO_BASE_ARGS = edict({'time': None,
                          'pair': None,
                         'savedir': None,
                         'order_book': 0,
                          'ob_depth': 10,
                          'ob_level': 2,
                          'candles': 0,
                          'granularity': 60,
                          'stats': 0,
                          'trades': 0,
                          'ticker': 0})

BASE_SAVE_DIR = '../../datasets/'
PUBLIC_CLIENT = cbpro.PublicClient()


def get_product_order_book(product_id, level=2, depth=10):
    """
    Buffer function to avoid getting a huge order book for nothing
    TODO: make this a real buffer so as to avoid losing info from the API because of processing time
    """
    response = PUBLIC_CLIENT.get_product_order_book(product_id=product_id, level=level)
    response['bids'] = response['bids'][:depth]
    response['asks'] = response['asks'][:depth]
    return response


FN_MAPPING = {
    'order_book': get_product_order_book,
    'candles': PUBLIC_CLIENT.get_product_historic_rates,
    'trades': PUBLIC_CLIENT.get_product_trades,
    'ticker': PUBLIC_CLIENT.get_product_ticker,
    'stats': PUBLIC_CLIENT.get_product_24hr_stats,
}


def store_info(save_dir, pair, collection_time, info_type, **kwargs):
    """
    Logs API request response by
        info_type: FN_MAPPING.keys()
        timestamp: Unix time of request
    """
    logger.info(f'Collecting {info_type} data for {pair}')
    with open(join(save_dir, info_type) + '.txt', 'w') as file:
        start = time.time()
        while time.time() - start < collection_time:
            file.write(json.dumps({
                'ts': time.time(),
                'response': FN_MAPPING[info_type](product_id=pair, **kwargs)
                }))
            file.write('\n')

    logger.info(f'Finished collecting {info_type} data for {pair}')


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
    parser.add_argument('--ob_level', type=int, default=3,
                        help='Level of order book desired. Accepted values: 1, 2, 3')
    parser.add_argument('--ob_depth', type=int, default=10,
                        help='Depth of the order book. Realistically shouldn-t be larger than 10')
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
    parser.add_argument('--stats', type=int, default=0,
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
    # Ob
    use_ob = args.order_book
    if use_ob:
        ob_level = args.ob_level
        ob_depth = args.ob_depth
    # Candles
    use_candles = args.candles
    if use_candles: candles_granularity = args.granularity
    # Stats
    use_stats = args.stats
    # Trades
    use_trades = args.trades
    # Ticker
    use_ticker = args.ticker

    save_dir = join(*[args.savedir, pair, 'cb_pro'])
    os.makedirs(save_dir, exist_ok=True)
    threads = []
    if use_ticker:
        t = threading.Thread(target=store_info,
                             args=(save_dir, pair, collection_time, 'ticker'),
                             )
        threads.append(t)

    if use_ob:
        t = threading.Thread(target=store_info,
                             args=(save_dir, pair, collection_time, 'order_book'),
                             kwargs={'level': ob_level,
                                     'depth': ob_depth}
                             )
        threads.append(t)

    if use_stats:
        t = threading.Thread(target=store_info,
                             args=(save_dir, pair, collection_time, 'stats'),
                             )
        threads.append(t)

    if use_trades:
        t = threading.Thread(target=store_info,
                             args=(save_dir, pair, collection_time, 'trades'),
                             )
        threads.append(t)

    if use_candles:
        t = threading.Thread(target=store_info,
                             args=(save_dir, pair, collection_time, 'candles'),
                             kwargs={'granularity': candles_granularity}
                             )
        threads.append(t)

    for t in threads:
        t.start()




if __name__ == "__main__":
    args = parse_arguments()
    main(args)
