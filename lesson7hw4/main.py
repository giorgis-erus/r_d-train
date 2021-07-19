import requests
import json
import os
from datetime import date, datetime, timedelta
from requests.exceptions import HTTPError
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import yaml


class Config:
    def __init__(self,path):
        with open(path, 'r') as yaml_file:
            self.__config = yaml.load(yaml_file, Loader=yaml.BaseLoader)

    def get_config(self, application):
        return self.__config.get(application)


def app(**kwargs):
    config = kwargs['key1']
    process_date = kwargs['execution_date']
    fmt = '%Y-%m-%d'
    process_date = process_date.strftime(fmt)
    if not process_date:
        process_date = str(date.today())

    try:
        url = config['url'] + '/auth'
        payload = {'username': config['username'], 'password': config['password']}
        headers = {"content-type": "application/json"}
        res = requests.post(url, headers=headers, data=json.dumps(payload))
        res.raise_for_status()
        token = res.json()['access_token']
        url = config['url'] + '/out_of_stock'
        headers = {"content-type": "application/json", "Authorization": "JWT " + token}
        data = {"date": process_date}
        res = requests.get(url, headers=headers, data=json.dumps(data))
        res.raise_for_status()
        os.makedirs(os.path.join('/','home', 'user', 'airflow', config['directory'], process_date), exist_ok=True)
        with open(os.path.join('/','home', 'user','airflow', config['directory'], process_date, data['date']+'.json'), 'w') as json_file:
            data = res.json()
            data = [item for item in data]
        #  data = [item['Product_id'] for item in data] - если в итоговых файлах нужны только product_id
            json.dump(data, json_file)
    except HTTPError as e:
        print("Error: {}".format(e), f'and parameter date: {process_date}')


# print(os.path.join('home','user','airflow','dags', 'config_file.yaml'))

config2 = Config(os.path.join('/','home','user','airflow','dags', 'config_file.yaml'))

dag = DAG(
    dag_id='Python_Dag_R_D_API',
    description='Extract_data_from_R_D_API',
    start_date=datetime(2021,7,18,13,30),
    end_date=datetime(2021,8,23,14,30),
    schedule_interval='@daily'
)

t1 = PythonOperator(
    task_id='python_task',
    dag=dag,
    python_callable=app,
    op_kwargs={'key1': config2.get_config('product_app')},
    provide_context=True
)
