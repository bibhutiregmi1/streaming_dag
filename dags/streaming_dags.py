from datetime import datetime
import json, requests
from airflow.sdk import dag,task


@dag(start_date=datetime(2025, 1, 1), schedule="@daily")
def streaming_dag():
    @task
    def get_data():
        res = requests.get('http://randomuser.me/api/')
        res = res.json()
        res = res['results'][0]
        return res

    @task
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

    @task
    def stream_data():
        res = get_data()
        res = format_data(res)
        print(json.dumps(res, indent=3))

    get_data()>>format_data()>>stream_data()
streaming_dag()