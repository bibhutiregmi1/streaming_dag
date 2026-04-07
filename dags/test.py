import logging
from datetime import datetime
import json, requests, time
from airflow.sdk import dag,task
from confluent_kafka import Producer



def get_data():
    res = requests.get('http://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    return res


def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = (f"{str(location['street']['number'])} {str(location['street']['name'])} "
                       f"{str(location['city'])} {str(location['state'])} {str(location['country'])}")
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    return data


def stream_data():
    # Configuration
    conf = {'bootstrap.servers': "localhost:9092"}

    # Create Producer instance
    p = Producer(conf)

    def delivery_report(err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    # Produce data
    # p.produce('my_topic1', key='test_key', value='hello world', callback=delivery_report)
    curr_time = time.time()
    while True:
        if time.time() > curr_time + 60:
            break
        try:
            res = get_data()
            res = format_data(res)
            p.produce('users_created', key="user1", value=json.dumps(res).encode('utf-8'), callback=delivery_report)

            p.flush(10)
            logging.info(json.dumps(res, indent=3))
        except Exception as e:
            logging.error(f"An error occured: {e}")
            continue


    #  print(json.dumps(res, indent=3))

stream_data()