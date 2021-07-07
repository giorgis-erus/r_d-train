import requests
import json
import os
from config import Config
from datetime import date, datetime, timedelta
from requests.exceptions import HTTPError


def app(config, process_date=None):

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
        os.makedirs(os.path.join(config['directory'], process_date), exist_ok=True)
        with open(os.path.join(config['directory'], process_date, data['date']+'.json'), 'w') as json_file:
            data = res.json()
            data = [item for item in data]
        #  data = [item['Product_id'] for item in data] - если в итоговых файлах нужны только product_id
            json.dump(data, json_file)
    except HTTPError as e:
        print("Error: {}".format(e), f'and parameter date: {process_date}')


if __name__ == '__main__':
    config = Config(os.path.join('.', 'config.yaml'))
    date_from = datetime.strptime(config.get_config('product_app')['date_from'], '%Y-%m-%d').date()
    # добавляем один день к date_to, чтобы включить  правую границу
    date_to = datetime.strptime(config.get_config('product_app')['date_to'], '%Y-%m-%d').date() + timedelta(days=1)
    if date_from <= date_to:
        if date_from == date_to:
            dates = [config.get_config('product_app')['date_from']]
        else:
            dates = [str(date.fromordinal(i)) for i in range(date_from.toordinal(), date_to.toordinal())]
        for dt in dates:
            app(config=config.get_config('product_app'), process_date=dt)
    else:
        print("\"date_to\" should be greater than \"date_from\". Check it in config.yaml")
