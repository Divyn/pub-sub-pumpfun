import asyncio
import json
import websockets
from google.cloud import pubsub_v1

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json"

url = "wss://streaming.bitquery.io/eap?token=ory_atU"
query = """
subscription MyQuery {
  Solana {
    DEXTrades(
      where: {
        Trade: { Dex: { ProtocolName: { is: "pump" } } }
        Transaction: { Result: { Success: true } }
      }
    ) {
      Trade {
        Dex {
          ProtocolFamily
          ProtocolName
        }
        Buy {
          Amount
          Account {
            Address
          }
        }
        Sell {
          Amount
          Account {
            Address
          }
        }
      }
      Transaction {
        Signature
      }
    }
  }
}
"""

# Google Pub/Sub details
project_id = "your-id"
topic_id = "bitquery-data-stream"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

async def fetch_and_publish():
    async with websockets.connect(url, subprotocols=["graphql-ws"]) as websocket:
        # Step 1: Initialize connection
        await websocket.send(json.dumps({"type": "connection_init"}))
        
        # Wait for connection_ack
        while True:
            response = await websocket.recv()
            response_data = json.loads(response)
            if response_data.get("type") == "connection_ack":
                print("Connection acknowledged.")
                break

        # Step 2: Send subscription query
        await websocket.send(json.dumps({"type": "start", "id": "1", "payload": {"query": query}}))

        # Step 3: Listen and publish messages to Pub/Sub
        while True:
            response = await websocket.recv()
            data = json.loads(response)
            print("data recd")
            # Process only subscription data
            if data.get("type") == "data" and "payload" in data:
                trades = data['payload']['data'].get('Solana', {}).get('DEXTrades', [])
                
                for trade in trades:
                    message = {
                        "protocol_family": trade['Trade']['Dex']['ProtocolFamily'],
                        "protocol_name": trade['Trade']['Dex']['ProtocolName'],
                        "buy_amount": trade['Trade']['Buy']['Amount'],
                        "buy_account": trade['Trade']['Buy']['Account']['Address'],
                        "sell_amount": trade['Trade']['Sell']['Amount'],
                        "sell_account": trade['Trade']['Sell']['Account']['Address'],
                        "transaction_signature": trade['Transaction']['Signature']
                    }
                    await publish_to_pubsub(message)

async def publish_to_pubsub(message):
    print(f"Publishing message: {message}")
    future = publisher.publish(topic_path, json.dumps(message).encode("utf-8"))
    future.result()  # Wait for the message to be successfully published
    print("Message published.")

async def main():
    try:
        await fetch_and_publish()
    except Exception as e:
        print(f"Error occurred: {e}")

# Run the main function
asyncio.run(main())
