import requests
import json
import os
from config import Config
from datetime import date


def app(config, process_date=None):

    if not process_date:
        process_date = str(date.today())
    os.makedirs(os.path.join(config['directory'], process_date), exist_ok=True)

    try:
        url = config['url'] + '/auth'
        payload = {'username': config['username'], 'password': config['password']}
        headers = {"content-type": "application/json"}
        res = requests.post(url, headers=headers, data=json.dumps(payload))
        token = res.json()['access_token']
        url = config['url'] + '/out_of_stock'
        headers = {"content-type": "application/json", "Authorization": "JWT " + token}
        data = {"date": process_date}
        res = requests.get(url, headers=headers, data=json.dumps(data))
        print(process_date)
        if type(res.json()) == list:

            with open(os.path.join(config['directory'], process_date, data['date']+'.json'), 'w') as json_file:
                data = res.json()
                data = [item['product_id'] for item in data]
                json.dump(data, json_file)
    except HTTPError:
        print('ConnectionError')

if __name__ == '__main__':
    config = Config(os.path.join('.', 'config.yaml'))
    dates = [str(date.fromordinal(i)) for i in range(date(2019,6,1).toordinal(), date.today().toordinal())]
    for dt in dates:
        app(config=config.get_config('product_app'), process_date=dt)

