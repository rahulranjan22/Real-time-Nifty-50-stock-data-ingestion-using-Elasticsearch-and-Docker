import requests
import time
import logging
import os
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError, RequestError, ConnectionError

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ElasticSearch configuration
ELASTICSEARCH_HOST = os.getenv('ELASTICSEARCH_HOST')
ELASTICSEARCH_USERNAME = os.getenv('ELASTICSEARCH_USERNAME')
ELASTICSEARCH_PASSWORD = os.getenv('ELASTICSEARCH_PASSWORD')
NSE_API_URL = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"

def fetch_nifty50_data():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36'
    }
    try:
        response = requests.get(NSE_API_URL, headers=headers)
        response.raise_for_status()
        logger.debug("Fetched data from NSE API successfully.")
        logger.debug(f"Response JSON: {response.json()}")  # Log the full JSON response
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Error fetching data from NSE API: {e}")
        return {}

def ingest_to_elasticsearch(es, index, data):
    for record in data:
        try:
            es.index(index=index, body=record)
            logger.debug(f"Document indexed: {record}")
        except NotFoundError as e:
            logger.error(f"Index not found: {e}")
        except RequestError as e:
            logger.error(f"Request error: {e}")
        except ConnectionError as e:
            logger.error(f"Connection error: {e}")
        except Exception as e:
            logger.error(f"Error indexing document: {e}")

def main():
    es = Elasticsearch(
        [ELASTICSEARCH_HOST],
        basic_auth=(ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD)
    )
    
    index_name = "nifty50-stock-prices"

    while True:
        try:
            data = fetch_nifty50_data().get('data', [])
            if not data:
                logger.warning("No data received or data is empty.")
                continue
            
            records = []

            for item in data:
                record = {
                    'priority': item.get('priority'),
                    'symbol': item.get('symbol'),
                    'identifier': item.get('identifier'),
                    'series': item.get('series'),
                    'open': item.get('open'),
                    'dayHigh': item.get('dayHigh'),
                    'dayLow': item.get('dayLow'),
                    'lastPrice': item.get('lastPrice'),
                    'previousClose': item.get('previousClose'),
                    'change': item.get('change'),
                    'pChange': item.get('pChange'),
                    'totalTradedVolume': item.get('totalTradedVolume'),
                    'totalTradedValue': item.get('totalTradedValue'),
                    'lastUpdateTime': item.get('lastUpdateTime'),
                    'yearHigh': item.get('yearHigh'),
                    'ffmc': item.get('ffmc'),
                    'yearLow': item.get('yearLow'),
                    'nearWKH': item.get('nearWKH'),
                    'nearWKL': item.get('nearWKL'),
                    'perChange365d': item.get('perChange365d'),
                    'date365dAgo': item.get('date365dAgo'),
                    'chart365dPath': item.get('chart365dPath'),
                    'date30dAgo': item.get('date30dAgo'),
                    'perChange30d': item.get('perChange30d'),
                    'chart30dPath': item.get('chart30dPath'),
                    'chartTodayPath': item.get('chartTodayPath'),
                    'meta': item.get('meta'),
                    'timestamp': datetime.now()
                }
                records.append(record)

            if records:
                ingest_to_elasticsearch(es, index_name, records)
                logger.info(f"Ingested {len(records)} records to Elasticsearch.")
            else:
                logger.warning("No records to ingest.")
        
        except Exception as e:
            logger.error(f"Error occurred: {e}")
        
        time.sleep(3600)  # Run every hour

if __name__ == "__main__":
    main()
