"""the file where DAG will run from"""
"includes data ingestion"

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests
import time
from kafka import KafkaProducer



def get_data():

    res =requests.get('https://randomuser.me/api/')
    res = res.json()['results'][0]

    return res

def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}"\
                        f"{location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data



def stream_data():

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    #stream data to kafka from api for 1 mins
    while True:

        if time.time() > curr_time + 60:
            break
        
        try:

            res = get_data()
            res = format_data(res)
            producer.send('users_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f"An error occured {e}")
            
            continue




default_args = {
    'owner' : 'olaoluwa',
    'start_date' : datetime(2023,12,28,5,00)
}


with DAG (dag_id='user_automation',
          default_args=default_args,
          schedule_interval='@daily',
          catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

