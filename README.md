# MultiExchangeAPI

API to retrieve market data from multiple exchanges.

## Coinbase

Retrieve market data (asks, bids, ticker) from Coinbase.

Based on https://github.com/danpaquin/coinbasepro-python

### Run

Requires cbpro. Response data is saved in `data.txt`.

```python
pip3 install cbpro
python3 coinbase.py
```

## Bybit

Retrieve market data (asks, bids, ticker) from Bybit.

### Run

Requires X. Response data is saved in `data.txt`.

```python
pip3 install X
python3 bybit.py
```

## Bitstamp

Retrieve market data (asks, bids, ticker) from Bitstamp.

### Run

Requires X. Response data is saved in `data.txt`.

```python
pip3 install X
python3 bitstamp.py
```
