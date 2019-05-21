
import pandas as pd
from aot_client import AotClient
import datetime
import pandas as pd
from aot_client import AotClient
from aot_client import F
from bs4 import BeautifulSoup
import requests

client = AotClient()


def get_nodes(): # Returns a DataFrame with information on nodes. 
    '''
    Returns a DataFrame with information on nodes. 
    Current columns = 
                    ['node_id', 'project_id', 'vsn', 'address', 'lat', 'lon', 'description',
                    'start_timestamp', 'end_timestamp', 'Unnamed: 9']
    '''
    r = requests.get('https://aot-file-browser.plenar.io/data-sets/chicago-complete')
    soup = BeautifulSoup(r.text, 'lxml')
    nodes = pd.read_html(str(soup.findAll(class_='table')[2]))[0]
    
    return nodes


def get_sensors(): #  Returns a DataFrame with information on sensors. 
    '''
    Returns a DataFrame with information on sensors. 
    Current columns = 
                    ['ontology', 'subsystem', 'sensor', 'parameter', 'hrf_unit',
                     'hrf_minval', 'hrf_maxval', 'datasheet']
    '''
    r = requests.get('https://aot-file-browser.plenar.io/data-sets/chicago-complete')
    soup = BeautifulSoup(r.text, 'lxml')
    sensors = pd.read_html(str(soup.findAll(class_='table')[3]))[0]

    sensors['sensor_path'] = (
        sensors.subsystem + '.' + 
        sensors.sensor + '.' + 
        sensors.parameter)
    
    return sensors


def AoT_filter(endpoint, params): # for filtering the API call
    '''
    for filtering the API call

    --------------------------------------

    f = a dictionary of filters where the key is a string and the values are a tuple.
    for example
        f &= ('sensor', 'image.image_detector.person_total') = {'sensor': [('image.image_detector.person_total',)]}
        would send an api request to https://api-of-things.plenar.io/api/observations?order=desc%3Atimestamp&page=1&sensor=image.image_detector.person_total&size=200

        or
        f &= ('sensor', 'image.image_detector.person_total', 'hello world') = {'sensor': [('image.image_detector.person_total', 'hello world')]}
        would not be a valid request.
    '''
    f = F()
    f &= (endpoint, params)
    return f


def filtered_observations(params, sensor=True): # for returning a df that would then be plotted.
    
    '''
    queries the observations end point using paramaters. 

    see possible params https://arrayofthings.docs.apiary.io/#reference/0/observations-endpoint/list-the-observations

    the below returned a df of 2000 mean values grouped every five minutes for chemense.h2s.concentration from 5/13/19 - 5/21/19

    f = F()
    f &= ('sensor', 'chemsense.h2s.concentration') 
    f &= ('time_bucket', 'avg', '5 minutes')
    r = client.list_observations(filters=f)._data
    df = pd.DataFrame(r)
    df['params'] = str(f.to_query_params())

    '''
    if sensor:
        f = AoT_filter('sensor', params)
        df = pd.DataFrame(client.list_observations(filters=f)._data)

        # adding in a column witb the parameters.
        df['params'] = str(f.to_query_params())

        return df


