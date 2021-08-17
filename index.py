import json
import cbpro
public_client = cbpro.PublicClient()

with open('data.txt', 'a') as file:
  while True:
    file.write(json.dumps({
      'bids-asks': public_client.get_product_order_book('BTC-USD', level=2),
      'ticker': public_client.get_product_ticker(product_id='BTC-USD')
    }))
    file.write('\n')
