
import datetime
import pandas as pd
import tarfile


def time_x_mins_ago(minutes):
    '''
    Get formatted time to pass to API filter, relative to current time
    '''
    t = (datetime.datetime.now() - 
         datetime.timedelta(minutes=minutes) + 
         datetime.timedelta(hours=5))  # convert timezone from central to UTC
    t = t.isoformat()
    
    return t[0:19]


def process_observations(obs_df):
    obs_df = obs_df.copy()
    obs_df['timestamp'] = pd.to_datetime(obs_df['timestamp'], utc=True)
    obs_df['timestamp'] = obs_df['timestamp'].dt.tz_convert('US/Central')
    
    # extract lat/lon to columns
    obs_df['coords'] = obs_df['location'].apply(
        lambda x: x['geometry']['coordinates'])
    obs_df[['lon', 'lat']] = pd.DataFrame(
        obs_df['coords'].tolist(), columns=['lon', 'lat'])
    obs_df = obs_df.drop(columns=['coords'])
    
    # fix positive lon values
    mask = obs_df['lon'] > 0
    if sum(mask) > 0:
        print(f'fixed {sum(mask)} rows with positive lon value')
        obs_df.loc[mask, 'lon'] = obs_df.loc[mask, 'lon'] * -1

    # remove lat/lon values at 0
    mask = (obs_df['lon'] != 0) & (obs_df['lat'] != 0)
    if len(obs_df) - sum(mask) > 0:
        print(f'removed {len(obs_df) - sum(mask)} rows with lat/lon at 0')
        obs_df = obs_df.loc[mask]

    # remove lat values less than 40 degrees
    mask = (obs_df['lat'] > 40)
    if len(obs_df) - sum(mask) > 0:
        print(f'removed {len(obs_df) - sum(mask)} '
              'rows with lat/lon outside Chicago region')
        obs_df = obs_df.loc[mask]
    
    return obs_df


def unpack_response(response, page_limit=1000):
    try:
        pages = []
        for i, page in enumerate(response):
            if i + 1 > page_limit:
                break
            pages.extend(page.data)
    except HTTPError as e:
        print(e)    
    finally:
        return pages


def tar_to_csv(file):
    """
    converting tar files to csv files incase we want to store historical.
    """

    tf = tarfile.TarFile(file)

    for f in tf:
        if 'data.csv' in str(f):
            # save into some type of folder with only csv's for historical data. 
            # so using extract. 
            return pd.read_csv(f)
        else:
            pass
