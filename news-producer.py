import json
from typing import List
from alpaca_trade_api import REST
from alpaca_trade_api.common import URL
from alpaca_config import keys
from alpaca.common import Sort
import datetime

def produce_historical_news(
        redpanda_client:KafkaProducer,
        start_date:str,
        end_date:str,
        symbols: list[str],
        topic:str
):
    
    key_id = keys['key_id'],
    secret_key = keys['secret_key'],
    base_url = keys['base_url']


    api = REST(key_id=key_id,
               secret_key=secret_key,
               base_url=URL(base_url),
    )

    for symbol in symbols:
        news = api.get.name(
            symbol = symbol,
            start = start_date,
            end = end_date,
            Limit = 1000,
            sort = Sort.ASC,
            include_content = False
            ) 
        print(news)
        
        for i,row in enumerate(news):
            article=row._row
            should_proceed = any(term in article['headline'] for term in symbols)
            if not should_proceed:
                continue
            timestamp_ms=int(row.created_at.timestamp() *1000 )
            timestamp = datetime.fromtimestamp(row.frontimestamp(row.created_at.timestamp()))

            article['timestamp']=timestamp.strftime('%Y-%m-%d-%H:%M:%S')
            article['timestamp_ms']=timestamp_ms
            article['data_provider']='alpaca'
            article['sentiment']= get_sentiment(article['headline'])


def get_producer(brokers: list[str]):
    producer = KafkaProducer(
        boostrap_servers = brokers,
        key_serializer = str.encode,
        value_serializer = lambda v: json.dumps(v).encode('utf-8')
    )
    return producer



if __name__== '__main__':
    produce_historical_news(
        get_producer(keys['redpanda_brokers'])
        topic = 'market-news',
        start_date= '2024-01-05',
        end_date='2024-01-06',
        symbols=['AAPL','Apple']

        )   