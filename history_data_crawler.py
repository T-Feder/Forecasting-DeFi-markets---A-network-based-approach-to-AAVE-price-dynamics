import requests
import pandas as pd
import numpy as np
import time
import pickle
import json
import os
import random

url = 'https://docs.google.com/spreadsheets/d/1ObrsfW-RB2CWmN2QhpKCz4z_FpS6UA-hajFEUZDlEFQ/export'

apis = pd.read_csv(f'{url}?gid=1357965727&format=csv')
apis = apis.set_index(['Version', 'Network'])['API'].to_dict()

first = pd.read_csv(f'{url}?gid=1357965727&format=csv')
first = first.set_index(['Version', 'Network'])['Date'].to_dict()

tables = dict()
tables = dict()
tables['V2'] = pd.read_csv(f'{url}?gid=287406065&format=csv')
tables['V3'] = pd.read_csv(f'{url}?gid=860445127&format=csv')

def run_query(api, query): # A simple function to use requests.post to make the API call. Note the json= section.
    request = requests.post(api, json={'query': query})
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception("Query failed to run by returning code of {}. {}".format(request.status_code, query))

def get_query_string(table, start_time, end_time, skip):
    e = dict()
    for col in table.columns:
        q = table[col].str.split(':').str[0]
        #q.loc[4:11] = q.loc[4:11].apply(lambda x: x + '{id}' if isinstance(x, str) else x)
        q = list(q.dropna())
        h = col + "(first:1000 skip:" + str(skip) + " orderBy:timestamp where:{timestamp_gte: " + str(start_time) + ", timestamp_lt: " + str(end_time) + "})"
        e[col] = h + '{' + ',\n'.join(q) + '}'
    return e

def query_events(api, table, start_time, end_time):
    events = dict()
    query = '{' + '\n'.join(get_query_string(table, start_time, end_time, 0).values()) + '}'
    result = run_query(api, query)
    skip = 1000
    while True:
        q_strings = get_query_string(table, start_time, end_time, skip)
        query = '{'
        for key in result['data'].keys():
            if key not in events:
                events[key] = result['data'][key]
            else:
                events[key].extend(result['data'][key])
            if len(result['data'][key]) >= 1000:
                query += q_strings[key]
        if query == '{':
            break
        if skip > 5000:
            raise Exception("This is unacceptable.")
            break
        query += '}'
        result = run_query(api, query)
        skip += 1000
    return events

for (version, network), api in apis.items():
    if network != 'Avalanche':
        continue

    print("Start", version, network)

    table = tables[version]
    start_date, end_date = first[(version, network)], '2023-11-20'

    if network == 'Mainnet':
        all_dates = pd.date_range(start=start_date, end=end_date).tolist()              # For Mainnet
    else:
        all_dates = pd.date_range(start=start_date, end=end_date, freq='6H').tolist()   # For Layer 2
    all_dates = list(map(lambda date: int(date.timestamp()), all_dates))

    folder = f'raw_data/{version}_{network}'
    if not os.path.exists(folder):
        os.makedirs(folder)
        print('Create folder', folder)

    all_dates_stack = all_dates.copy()[::-1]
    count = 0
    while len(all_dates_stack) > 1:
        (start_time, end_time) = int(all_dates_stack.pop()), int(all_dates_stack[-1])
        
        if os.path.isfile(f'{folder}/histories_{start_time}_{end_time}.json'):
            continue
        
        try:
            temp = query_events(api, table, start_time, end_time)
            with open(f'{folder}/histories_{start_time}_{end_time}.json', 'w') as outfile:
                outfile.write(json.dumps(temp))
            print("Write", start_time, end_time)
        except:
            print("Extend the period", start_time, end_time)
            all_dates_stack.extend(np.linspace(int(start_time), int(end_time), num=5)[:-1][::-1])
            
        count += 1
        if count % 100 == 0:
            print(count)
            time.sleep(10)
        if count % 1000 == 0:
            time.sleep(100)
    
    print("Finish", version, network)
    time.sleep(300)