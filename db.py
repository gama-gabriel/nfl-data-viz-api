import json
import nfl_data_py as nfl
import pandas as pd
import datetime
import numpy
import time
import requests
import pyarrow.parquet as pq
from io import BytesIO

def update_raw(url='https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_2023.parquet', save_path='data/raw/play_by_play_2022.parquet'):
    start_time = time.time()

    response = requests.get(url)
    
    if response.status_code == 200:
        parquet_data = BytesIO(response.content)
        
        table = pq.read_table(parquet_data)
        
        pq.write_table(table, save_path)

    end_time = time.time()

    # # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the result
    print(f"Script took {elapsed_time} seconds to run.")
        
desc = nfl.import_team_desc()

url1 = 'data/raw/play_by_play_2023.parquet'
url2 = 'data/raw/pbp_participation_2023.parquet'
url3 = 'data/raw/ftn_charting_2023.parquet'

df1 = pd.read_parquet(url1)
df2 = pd.read_parquet(url2)
df3 = pd.read_parquet(url3)
df3 = df3.rename(columns = {'nflverse_game_id': 'game_id', 'nflverse_play_id': 'play_id'})
new_df = pd.merge(df1, df2, how='left', on=['play_id','old_game_id'])
ftn = pd.merge(df1, df3, how='outer', on=['game_id', 'play_id'])

def get_off_epa(new_df=new_df, desc=desc):
    start_time = time.time()

    # df2 = pd.read_parquet(url2)
    # new_df = pd.merge(df1, df2, how='left',  on=['play_id','old_game_id'])
    # new_df = pd.read_parquet(url1, engine='auto')


    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_name': 'full_name'})

    off_epa = new_df[(new_df['pass'] == 1) | (new_df['rush'] == 1)].groupby('posteam')['epa'].mean().reset_index().rename(columns = {'epa': 'offensive epa', 'posteam': 'team'})
    end_time = time.time()

    # # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the result
    print(f"Script took {elapsed_time} seconds to run.")
    return off_epa 


def get_off_succ(new_df=new_df):
    pos_plays = new_df[((new_df['pass'] == 1) | (new_df['rush'] == 1)) & (new_df['epa'] > 0)].groupby('posteam').size().reset_index(name='off positive plays').rename(columns={'posteam': 'team'})
    neg_plays = new_df[((new_df['pass'] == 1) | (new_df['rush'] == 1)) & (new_df['epa'] <= 0)].groupby('posteam').size().reset_index(name='off negative plays').rename(columns={'posteam': 'team'})
    
    succ_rate = pd.merge(pos_plays, neg_plays, how='outer')
    succ_rate['off success rate'] = succ_rate['off positive plays'] / (succ_rate['off positive plays'] + succ_rate['off negative plays'])
    return succ_rate


def get_def_epa(new_df=new_df):
    def_epa = new_df[(new_df['pass'] == 1) | (new_df['rush'] == 1)].groupby('defteam')['epa'].mean().reset_index().rename(columns = {'epa': 'defensive epa', 'defteam': 'team'})
    return def_epa


def get_def_succ(new_df=new_df):
    pos_plays = new_df[((new_df['pass'] == 1) | (new_df['rush'] == 1)) & (new_df['epa'] > 0)].groupby('defteam').size().reset_index(name='def positive plays').rename(columns={'defteam': 'team'})
    neg_plays = new_df[((new_df['pass'] == 1) | (new_df['rush'] == 1)) & (new_df['epa'] <= 0)].groupby('defteam').size().reset_index(name='def negative plays').rename(columns={'defteam': 'team'})
    
    succ_rate = pd.merge(pos_plays, neg_plays, how='outer')
    succ_rate['def success rate'] = succ_rate['def positive plays'] / (succ_rate['def positive plays'] + succ_rate['def negative plays'])
    return succ_rate


def get_epa(desc=desc):
    start_time = time.time()

    off_epa = get_off_epa()
    def_epa = get_def_epa()

    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_name': 'full_name'})

    epa = pd.merge(pd.merge(off_epa, def_epa, how='outer'), desc, how='outer').dropna()
    epa_json = json.dumps([{'data': {'x': row['offensive epa'], 'y': row['defensive epa']}, 'name': row['team'], 'logo': row['logo'], 'color': row['color']} for _, row in epa.iterrows()])

    with open('data/general/epa.json', 'w') as file:
        file.write(epa_json)
    end_time = time.time()

    # # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the result
    print(f"Script took {elapsed_time} seconds to run.")

def get_succ(desc=desc):
    off_succ = get_off_succ()
    def_succ = get_def_succ()

    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_name': 'full_name'})
    
    succ = pd.merge(pd.merge(off_succ, def_succ, how='outer'), desc, how='outer').dropna()
    succ_json = json.dumps([{'data': {'x': row['off success rate'], 'y': row['def success rate']}, 'name': row['team'], 'logo': row['logo'], 'color': row['color']} for _, row in succ.iterrows()])

    with open('data/general/succ.json', 'w') as file:
        file.write(succ_json)


def get_off_dropback(new_df=new_df, desc=desc):
    start_time = time.time()

    epa = new_df[(new_df['pass'] == 1)].groupby('posteam')['epa'].mean().reset_index().rename(columns = {'epa': 'dropback epa', 'posteam': 'team'})
    
    pos_plays = new_df[((new_df['pass'] == 1) & (new_df['epa'] > 0))].groupby('posteam').size().reset_index(name='dropback positive plays').rename(columns = {'posteam': 'team'})
    neg_plays = new_df[((new_df['pass'] == 1) & (new_df['epa'] <= 0))].groupby('posteam').size().reset_index(name='dropback negative plays').rename(columns = {'posteam': 'team'})

    succ = pd.merge(pos_plays, neg_plays, how='outer')
    succ['dropback success rate'] = succ['dropback positive plays'] / (succ['dropback positive plays'] + succ['dropback negative plays'])

    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_name': 'full_name'})

    dropback = pd.merge(pd.merge(epa, succ, how='outer'), desc, how='outer').dropna()

    dropback_json = json.dumps([{'data': {'x': row['dropback epa'], 'y': row['dropback success rate']}, 'name': row['team'], 'logo': row['logo'], 'color': row['color']} for _, row in dropback.iterrows()])

    with open('data/general/off_dropback.json', 'w') as file:
        file.write(dropback_json)

    end_time = time.time()

    # # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the result
    print(f"Script took {elapsed_time} seconds to run.")


def get_def_dropback(new_df=new_df, desc=desc):
    start_time = time.time()

    epa = new_df[(new_df['pass'] == 1)].groupby('defteam')['epa'].mean().reset_index().rename(columns = {'epa': 'dropback epa', 'defteam': 'team'})
    
    pos_plays = new_df[((new_df['pass'] == 1) & (new_df['epa'] > 0))].groupby('defteam').size().reset_index(name='dropback positive plays').rename(columns = {'defteam': 'team'})
    neg_plays = new_df[((new_df['pass'] == 1) & (new_df['epa'] <= 0))].groupby('defteam').size().reset_index(name='dropback negative plays').rename(columns = {'defteam': 'team'})

    succ = pd.merge(pos_plays, neg_plays, how='outer')
    succ['dropback success rate'] = succ['dropback positive plays'] / (succ['dropback positive plays'] + succ['dropback negative plays'])

    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_name': 'full_name'})

    dropback = pd.merge(pd.merge(epa, succ, how='outer'), desc, how='outer').dropna()

    dropback_json = json.dumps([{'data': {'x': row['dropback epa'], 'y': row['dropback success rate']}, 'name': row['team'], 'logo': row['logo'], 'color': row['color']} for _, row in dropback.iterrows()])

    with open('data/general/def_dropback.json', 'w') as file:
        file.write(dropback_json)

    end_time = time.time()

    # # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the result
    print(f"Script took {elapsed_time} seconds to run.")


def get_off_rush(new_df=new_df, desc=desc):
    start_time = time.time()

    epa = new_df[(new_df['rush'] == 1)].groupby('posteam')['epa'].mean().reset_index().rename(columns = {'epa': 'dropback epa', 'posteam': 'team'})
    
    pos_plays = new_df[((new_df['rush'] == 1) & (new_df['epa'] > 0))].groupby('posteam').size().reset_index(name='dropback positive plays').rename(columns = {'posteam': 'team'})
    neg_plays = new_df[((new_df['rush'] == 1) & (new_df['epa'] <= 0))].groupby('posteam').size().reset_index(name='dropback negative plays').rename(columns = {'posteam': 'team'})

    succ = pd.merge(pos_plays, neg_plays, how='outer')
    succ['dropback success rate'] = succ['dropback positive plays'] / (succ['dropback positive plays'] + succ['dropback negative plays'])

    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_name': 'full_name'})

    rush = pd.merge(pd.merge(epa, succ, how='outer'), desc, how='outer').dropna()

    rush_json = json.dumps([{'data': {'x': row['dropback epa'], 'y': row['dropback success rate']}, 'name': row['team'], 'logo': row['logo'], 'color': row['color']} for _, row in rush.iterrows()])

    with open('data/general/off_rush.json', 'w') as file:
        file.write(rush_json)

    end_time = time.time()

    # # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the result
    print(f"Script took {elapsed_time} seconds to run.")



def get_def_rush(new_df=new_df, desc=desc):
    start_time = time.time()

    epa = new_df[(new_df['rush'] == 1)].groupby('defteam')['epa'].mean().reset_index().rename(columns = {'epa': 'dropback epa', 'defteam': 'team'})
    
    pos_plays = new_df[((new_df['rush'] == 1) & (new_df['epa'] > 0))].groupby('defteam').size().reset_index(name='dropback positive plays').rename(columns = {'defteam': 'team'})
    neg_plays = new_df[((new_df['rush'] == 1) & (new_df['epa'] <= 0))].groupby('defteam').size().reset_index(name='dropback negative plays').rename(columns = {'defteam': 'team'})

    succ = pd.merge(pos_plays, neg_plays, how='outer')
    succ['dropback success rate'] = succ['dropback positive plays'] / (succ['dropback positive plays'] + succ['dropback negative plays'])

    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_name': 'full_name'})

    rush = pd.merge(pd.merge(epa, succ, how='outer'), desc, how='outer').dropna()

    rush_json = json.dumps([{'data': {'x': row['dropback epa'], 'y': row['dropback success rate']}, 'name': row['team'], 'logo': row['logo'], 'color': row['color']} for _, row in rush.iterrows()])

    with open('data/general/def_rush.json', 'w') as file:
        file.write(rush_json)

    end_time = time.time()

    # # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the result
    print(f"Script took {elapsed_time} seconds to run.")


def get_off_dropback_rush(new_df=new_df, desc=desc):
    start_time = time.time()

    dropback = new_df[(new_df['pass'] == 1)].groupby('posteam')['epa'].mean().reset_index().rename(columns= {'posteam': 'team', 'epa': 'dropback epa'})

    rush = new_df[(new_df['rush'] == 1)].groupby('posteam')['epa'].mean().reset_index().rename(columns= {'posteam': 'team', 'epa': 'rush epa'})

    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_name': 'full_name'})

    data = pd.merge(pd.merge(dropback, rush, how='outer'), desc, how='outer').dropna()

    data_json = json.dumps([{'data': {'x': row['dropback epa'], 'y': row['rush epa']}, 'name': row['team'], 'logo': row['logo'], 'color': row['color']} for _, row in data.iterrows()])

    with open('data/general/off_dropback_rush.json', 'w') as file:
        file.write(data_json)

    end_time = time.time()

    # # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the result
    print(f"Script took {elapsed_time} seconds to run.")


def get_def_dropback_rush(new_df=new_df, desc=desc):
    start_time = time.time()

    dropback = new_df[(new_df['pass'] == 1)].groupby('defteam')['epa'].mean().reset_index().rename(columns= {'defteam': 'team', 'epa': 'dropback epa'})

    rush = new_df[(new_df['rush'] == 1)].groupby('defteam')['epa'].mean().reset_index().rename(columns= {'defteam': 'team', 'epa': 'rush epa'})

    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_name': 'full_name'})

    data = pd.merge(pd.merge(dropback, rush, how='outer'), desc, how='outer').dropna()

    data_json = json.dumps([{'data': {'x': row['dropback epa'], 'y': row['rush epa']}, 'name': row['team'], 'logo': row['logo'], 'color': row['color']} for _, row in data.iterrows()])

    with open('data/general/def_dropback_rush.json', 'w') as file:
        file.write(data_json)

    end_time = time.time()

    # # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the result
    print(f"Script took {elapsed_time} seconds to run.")


def get_off_succ_epa(desc=desc):
    off_epa = get_off_epa()
    off_succ = get_off_succ()

    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_name': 'full_name'})

    off_succ_epa = pd.merge(pd.merge(off_epa, off_succ, how='outer'), desc, how='outer').dropna()
    data_json = json.dumps([{'data': {'x': row['offensive epa'], 'y': row['off success rate']}, 'name': row['team'], 'logo': row['logo'], 'color': row['color']} for _, row in off_succ_epa.iterrows()])

    with open('data/general/off_succ_epa.json', 'w') as file:
        file.write(data_json)


def get_def_succ_epa(desc=desc):
    def_epa = get_def_epa()
    def_succ = get_def_succ()

    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_name': 'full_name'})
    def_succ_epa = pd.merge(pd.merge(def_epa, def_succ, how='outer'), desc, how='outer').dropna()
    data_json = json.dumps([{'data': {'x': row['defensive epa'], 'y': row['def success rate']}, 'name': row['team'], 'logo': row['logo'], 'color': row['color']} for _, row in def_succ_epa.iterrows()])

    with open('data/general/def_succ_epa.json', 'w') as file:
        file.write(data_json)


def get_off_early_downs(new_df=new_df, desc=desc):
    start_time = time.time()

    epa = new_df[(((new_df['down'] == 1) | (new_df['down'] == 2)) & ((new_df['pass'] == 1) | (new_df['rush'] == 1)))].groupby('posteam')['epa'].mean().reset_index().rename(columns = {'posteam': 'team'})
    
    pos_plays = new_df[(((new_df['down'] == 1) | (new_df['down'] == 2)) & ((new_df['pass'] == 1) | (new_df['rush'] == 1)) & (new_df['epa'] > 0))].groupby('posteam').size().reset_index(name='positive plays').rename(columns = {'posteam': 'team'})
    neg_plays = new_df[(((new_df['down'] == 1) | (new_df['down'] == 2)) & ((new_df['pass'] == 1) | (new_df['rush'] == 1)) & (new_df['epa'] <= 0))].groupby('posteam').size().reset_index(name='negative plays').rename(columns = {'posteam': 'team'})

    succ = pd.merge(pos_plays, neg_plays, how='outer')
    succ['success rate'] = succ['positive plays'] / (succ['positive plays'] + succ['negative plays'])

    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_name': 'full_name'})

    data = pd.merge(pd.merge(epa, succ, how='outer'), desc, how='outer').dropna()

    data_json = json.dumps([{'data': {'x': row['epa'], 'y': row['success rate']}, 'name': row['team'], 'logo': row['logo'], 'color': row['color']} for _, row in data.iterrows()])

    with open('data/general/off_early_downs.json', 'w') as file:
        file.write(data_json)
    
    end_time = time.time()

    # # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the result
    print(f"Script took {elapsed_time} seconds to run.")


def get_def_early_downs(new_df=new_df, desc=desc):
    start_time = time.time()

    epa = new_df[(((new_df['down'] == 1) | (new_df['down'] == 2)) & ((new_df['pass'] == 1) | (new_df['rush'] == 1)))].groupby('defteam')['epa'].mean().reset_index().rename(columns = {'defteam': 'team'})
    
    pos_plays = new_df[(((new_df['down'] == 1) | (new_df['down'] == 2)) & ((new_df['pass'] == 1) | (new_df['rush'] == 1)) & (new_df['epa'] > 0))].groupby('defteam').size().reset_index(name='positive plays').rename(columns = {'defteam': 'team'})
    neg_plays = new_df[(((new_df['down'] == 1) | (new_df['down'] == 2)) & ((new_df['pass'] == 1) | (new_df['rush'] == 1)) & (new_df['epa'] <= 0))].groupby('defteam').size().reset_index(name='negative plays').rename(columns = {'defteam': 'team'})

    succ = pd.merge(pos_plays, neg_plays, how='outer')
    succ['success rate'] = succ['positive plays'] / (succ['positive plays'] + succ['negative plays'])

    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_name': 'full_name'})

    data = pd.merge(pd.merge(epa, succ, how='outer'), desc, how='outer').dropna()

    data_json = json.dumps([{'data': {'x': row['epa'], 'y': row['success rate']}, 'name': row['team'], 'logo': row['logo'], 'color': row['color']} for _, row in data.iterrows()])

    with open('data/general/def_early_downs.json', 'w') as file:
        file.write(data_json)
    
    end_time = time.time()

    # # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the result
    print(f"Script took {elapsed_time} seconds to run.")


def get_off_late_downs(new_df=new_df, desc=desc):
    start_time = time.time()

    epa = new_df[(((new_df['down'] == 3) | (new_df['down'] == 4)) & ((new_df['pass'] == 1) | (new_df['rush'] == 1)))].groupby('posteam')['epa'].mean().reset_index().rename(columns = {'posteam': 'team'})
    
    pos_plays = new_df[(((new_df['down'] == 3) | (new_df['down'] == 4)) & ((new_df['pass'] == 1) | (new_df['rush'] == 1)) & (new_df['epa'] > 0))].groupby('posteam').size().reset_index(name='positive plays').rename(columns = {'posteam': 'team'})
    neg_plays = new_df[(((new_df['down'] == 3) | (new_df['down'] == 4)) & ((new_df['pass'] == 1) | (new_df['rush'] == 1)) & (new_df['epa'] <= 0))].groupby('posteam').size().reset_index(name='negative plays').rename(columns = {'posteam': 'team'})

    succ = pd.merge(pos_plays, neg_plays, how='outer')
    succ['success rate'] = succ['positive plays'] / (succ['positive plays'] + succ['negative plays'])

    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_name': 'full_name'})

    data = pd.merge(pd.merge(epa, succ, how='outer'), desc, how='outer').dropna()

    data_json = json.dumps([{'data': {'x': row['epa'], 'y': row['success rate']}, 'name': row['team'], 'logo': row['logo'], 'color': row['color']} for _, row in data.iterrows()])

    with open('data/general/off_late_downs.json', 'w') as file:
        file.write(data_json)
    
    end_time = time.time()

    # # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the result
    print(f"Script took {elapsed_time} seconds to run.")


def get_def_late_downs(new_df=new_df, desc=desc):
    start_time = time.time()

    epa = new_df[(((new_df['down'] == 3) | (new_df['down'] == 4)) & ((new_df['pass'] == 1) | (new_df['rush'] == 1)))].groupby('defteam')['epa'].mean().reset_index().rename(columns = {'defteam': 'team'})
    
    pos_plays = new_df[(((new_df['down'] == 3) | (new_df['down'] == 4)) & ((new_df['pass'] == 1) | (new_df['rush'] == 1)) & (new_df['epa'] > 0))].groupby('defteam').size().reset_index(name='positive plays').rename(columns = {'defteam': 'team'})
    neg_plays = new_df[(((new_df['down'] == 3) | (new_df['down'] == 4)) & ((new_df['pass'] == 1) | (new_df['rush'] == 1)) & (new_df['epa'] <= 0))].groupby('defteam').size().reset_index(name='negative plays').rename(columns = {'defteam': 'team'})

    succ = pd.merge(pos_plays, neg_plays, how='outer')
    succ['success rate'] = succ['positive plays'] / (succ['positive plays'] + succ['negative plays'])

    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_name': 'full_name'})

    data = pd.merge(pd.merge(epa, succ, how='outer'), desc, how='outer').dropna()

    data_json = json.dumps([{'data': {'x': row['epa'], 'y': row['success rate']}, 'name': row['team'], 'logo': row['logo'], 'color': row['color']} for _, row in data.iterrows()])

    with open('data/general/def_late_downs.json', 'w') as file:
        file.write(data_json)
    
    end_time = time.time()

    # # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the result
    print(f"Script took {elapsed_time} seconds to run.")


def get_off_screens(new_df=ftn, desc=desc):
    start_time = time.time()
    
    epa = new_df[(new_df['is_screen_pass'] == True)].groupby('posteam')['epa'].mean().reset_index().rename(columns = {'posteam': 'team', 'epa': 'screen epa'})

    pos_plays = new_df[((new_df['is_screen_pass'] == True) & (new_df['epa'] > 0))].groupby('posteam').size().reset_index(name='positive plays').rename(columns = {'posteam': 'team'})
    neg_plays = new_df[((new_df['is_screen_pass'] == True) & (new_df['epa'] <= 0))].groupby('posteam').size().reset_index(name='negative plays').rename(columns = {'posteam': 'team'})

    succ = pd.merge(pos_plays, neg_plays, how='outer')
    succ['number of plays'] = succ['positive plays'] + succ['negative plays']
    succ['success rate'] = succ['positive plays'] / succ['number of plays']

    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color', 'team_color2']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_color2': 'secondary color', 'team_name': 'full_name'})

    data = pd.merge(pd.merge(epa, succ, how='outer'), desc, how='outer').dropna()

    data_json = json.dumps(([{'data': {'x': row['screen epa'], 'y': row['success rate'], 'r': row['number of plays'], 'name': row['team']}, 'primary color': row['color'], 'secondary color': row['secondary color']} for _, row in data.iterrows()]))

    with open('data/general/off_screen.json', 'w') as file:
        file.write(data_json)
    
    end_time = time.time()

    # # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the result
    print(f"Script took {elapsed_time} seconds to run.")


def get_def_screens(new_df=ftn, desc=desc):
    start_time = time.time()
    
    epa = new_df[(new_df['is_screen_pass'] == True)].groupby('defteam')['epa'].mean().reset_index().rename(columns = {'defteam': 'team', 'epa': 'screen epa'})

    pos_plays = new_df[((new_df['is_screen_pass'] == True) & (new_df['epa'] > 0))].groupby('defteam').size().reset_index(name='positive plays').rename(columns = {'defteam': 'team'})
    neg_plays = new_df[((new_df['is_screen_pass'] == True) & (new_df['epa'] <= 0))].groupby('defteam').size().reset_index(name='negative plays').rename(columns = {'defteam': 'team'})

    succ = pd.merge(pos_plays, neg_plays, how='outer')
    succ['number of plays'] = succ['positive plays'] + succ['negative plays']
    succ['success rate'] = succ['positive plays'] / succ['number of plays']


    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color', 'team_color2']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_color2': 'secondary color', 'team_name': 'full_name'})

    data = pd.merge(pd.merge(epa, succ, how='outer'), desc, how='outer').dropna()

    data_json = json.dumps(([{'data': {'x': row['screen epa'], 'y': row['success rate'], 'r': row['number of plays'], 'name': row['team']}, 'primary color': row['color'], 'secondary color': row['secondary color']} for _, row in data.iterrows()]))

    with open('data/general/def_screen.json', 'w') as file:
        file.write(data_json)
    
    end_time = time.time()

    # # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the result
    print(f"Script took {elapsed_time} seconds to run.")

def get_off_trick(new_df=ftn, desc=desc):
    start_time = time.time()
    
    epa = new_df[(new_df['is_trick_play'] == True)].groupby('posteam')['epa'].mean().reset_index().rename(columns = {'posteam': 'team', 'epa': 'trick play epa'})

    pos_plays = new_df[((new_df['is_trick_play'] == True) & (new_df['epa'] > 0))].groupby('posteam').size().reset_index(name='positive plays').rename(columns = {'posteam': 'team'})
    neg_plays = new_df[((new_df['is_trick_play'] == True) & (new_df['epa'] <= 0))].groupby('posteam').size().reset_index(name='negative plays').rename(columns = {'posteam': 'team'})

    succ = pd.merge(pos_plays, neg_plays, how='outer')
    succ['number of plays'] = succ['positive plays'] + succ['negative plays']
    succ['success rate'] = succ['positive plays'] / succ['number of plays']

    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color', 'team_color2']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_color2': 'secondary color', 'team_name': 'full_name'})

    data = pd.merge(pd.merge(epa, succ, how='outer'), desc, how='outer').dropna()

    data_json = json.dumps(([{'data': {'x': row['trick play epa'], 'y': row['success rate'], 'r': row['number of plays'], 'name': row['team']}, 'primary color': row['color'], 'secondary color': row['secondary color']} for _, row in data.iterrows()]))

    with open('data/general/off_trick.json', 'w') as file:
        file.write(data_json)
    
    end_time = time.time()

    # # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the result
    print(f"Script took {elapsed_time} seconds to run.")


def get_def_trick(new_df=ftn, desc=desc):
    start_time = time.time()
    
    epa = new_df[(new_df['is_trick_play'] == True)].groupby('defteam')['epa'].mean().reset_index().rename(columns = {'defteam': 'team', 'epa': 'trick play epa'})

    pos_plays = new_df[((new_df['is_trick_play'] == True) & (new_df['epa'] > 0))].groupby('defteam').size().reset_index(name='positive plays').rename(columns = {'defteam': 'team'})
    neg_plays = new_df[((new_df['is_trick_play'] == True) & (new_df['epa'] <= 0))].groupby('defteam').size().reset_index(name='negative plays').rename(columns = {'defteam': 'team'})

    succ = pd.merge(pos_plays, neg_plays, how='outer')
    succ['number of plays'] = succ['positive plays'] + succ['negative plays']
    succ['success rate'] = succ['positive plays'] / succ['number of plays']


    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color', 'team_color2']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_color2': 'secondary color', 'team_name': 'full_name'})

    data = pd.merge(pd.merge(epa, succ, how='outer'), desc, how='outer').dropna()

    data_json = json.dumps(([{'data': {'x': row['trick play epa'], 'y': row['success rate'], 'r': row['number of plays'], 'name': row['team']}, 'primary color': row['color'], 'secondary color': row['secondary color']} for _, row in data.iterrows()]))

    with open('data/general/def_trick.json', 'w') as file:
        file.write(data_json)
    
    end_time = time.time()

    # # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the result
    print(f"Script took {elapsed_time} seconds to run.")


def get_off_motion(new_df=ftn, desc=desc):
    start_time = time.time()
    
    epa = new_df[(new_df['is_motion'] == True)].groupby('posteam')['epa'].mean().reset_index().rename(columns = {'posteam': 'team', 'epa': 'motion epa'})

    pos_plays = new_df[((new_df['is_motion'] == True) & (new_df['epa'] > 0))].groupby('posteam').size().reset_index(name='positive plays').rename(columns = {'posteam': 'team'})
    neg_plays = new_df[((new_df['is_motion'] == True) & (new_df['epa'] <= 0))].groupby('posteam').size().reset_index(name='negative plays').rename(columns = {'posteam': 'team'})

    succ = pd.merge(pos_plays, neg_plays, how='outer')
    succ['number of plays'] = succ['positive plays'] + succ['negative plays']
    succ['success rate'] = succ['positive plays'] / succ['number of plays']

    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color', 'team_color2']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_color2': 'secondary color', 'team_name': 'full_name'})

    data = pd.merge(pd.merge(epa, succ, how='outer'), desc, how='outer').dropna()

    data_json = json.dumps(([{'data': {'x': row['motion epa'], 'y': row['success rate'], 'r': row['number of plays'], 'name': row['team']}, 'primary color': row['color'], 'secondary color': row['secondary color']} for _, row in data.iterrows()]))

    with open('data/general/off_motion.json', 'w') as file:
        file.write(data_json)
    
    end_time = time.time()

    # # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the result
    print(f"Script took {elapsed_time} seconds to run.")


def get_def_motion(new_df=ftn, desc=desc):
    start_time = time.time()
    
    epa = new_df[(new_df['is_motion'] == True)].groupby('defteam')['epa'].mean().reset_index().rename(columns = {'defteam': 'team', 'epa': 'motion epa'})

    pos_plays = new_df[((new_df['is_motion'] == True) & (new_df['epa'] > 0))].groupby('defteam').size().reset_index(name='positive plays').rename(columns = {'defteam': 'team'})
    neg_plays = new_df[((new_df['is_motion'] == True) & (new_df['epa'] <= 0))].groupby('defteam').size().reset_index(name='negative plays').rename(columns = {'defteam': 'team'})

    succ = pd.merge(pos_plays, neg_plays, how='outer')
    succ['number of plays'] = succ['positive plays'] + succ['negative plays']
    succ['success rate'] = succ['positive plays'] / succ['number of plays']


    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color', 'team_color2']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_color2': 'secondary color', 'team_name': 'full_name'})

    data = pd.merge(pd.merge(epa, succ, how='outer'), desc, how='outer').dropna()

    data_json = json.dumps(([{'data': {'x': row['motion epa'], 'y': row['success rate'], 'r': row['number of plays'], 'name': row['team']}, 'primary color': row['color'], 'secondary color': row['secondary color']} for _, row in data.iterrows()]))

    with open('data/general/def_motion.json', 'w') as file:
        file.write(data_json)
    
    end_time = time.time()

    # # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the result
    print(f"Script took {elapsed_time} seconds to run.")


def get_off_no_huddle(new_df=ftn, desc=desc):
    start_time = time.time()
    
    epa = new_df[(new_df['is_no_huddle'] == True)].groupby('posteam')['epa'].mean().reset_index().rename(columns = {'posteam': 'team', 'epa': 'epa'})

    pos_plays = new_df[((new_df['is_no_huddle'] == True) & (new_df['epa'] > 0))].groupby('posteam').size().reset_index(name='positive plays').rename(columns = {'posteam': 'team'})
    neg_plays = new_df[((new_df['is_no_huddle'] == True) & (new_df['epa'] <= 0))].groupby('posteam').size().reset_index(name='negative plays').rename(columns = {'posteam': 'team'})

    succ = pd.merge(pos_plays, neg_plays, how='outer')
    succ['number of plays'] = succ['positive plays'] + succ['negative plays']
    succ['success rate'] = succ['positive plays'] / succ['number of plays']

    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color', 'team_color2']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_color2': 'secondary color', 'team_name': 'full_name'})

    data = pd.merge(pd.merge(epa, succ, how='outer'), desc, how='outer').dropna()

    data_json = json.dumps(([{'data': {'x': row['epa'], 'y': row['success rate'], 'r': row['number of plays'], 'name': row['team']}, 'primary color': row['color'], 'secondary color': row['secondary color']} for _, row in data.iterrows()]))

    with open('data/general/off_no_huddle.json', 'w') as file:
        file.write(data_json)
    
    end_time = time.time()

    # # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the result
    print(f"Script took {elapsed_time} seconds to run.")


def get_def_no_huddle(new_df=ftn, desc=desc):
    start_time = time.time()
    
    epa = new_df[(new_df['is_no_huddle'] == True)].groupby('defteam')['epa'].mean().reset_index().rename(columns = {'defteam': 'team'})

    pos_plays = new_df[((new_df['is_no_huddle'] == True) & (new_df['epa'] > 0))].groupby('defteam').size().reset_index(name='positive plays').rename(columns = {'defteam': 'team'})
    neg_plays = new_df[((new_df['is_no_huddle'] == True) & (new_df['epa'] <= 0))].groupby('defteam').size().reset_index(name='negative plays').rename(columns = {'defteam': 'team'})

    succ = pd.merge(pos_plays, neg_plays, how='outer')
    succ['number of plays'] = succ['positive plays'] + succ['negative plays']
    succ['success rate'] = succ['positive plays'] / succ['number of plays']


    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color', 'team_color2']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_color2': 'secondary color', 'team_name': 'full_name'})

    data = pd.merge(pd.merge(epa, succ, how='outer'), desc, how='outer').dropna()

    data_json = json.dumps(([{'data': {'x': row['epa'], 'y': row['success rate'], 'r': row['number of plays'], 'name': row['team']}, 'primary color': row['color'], 'secondary color': row['secondary color']} for _, row in data.iterrows()]))

    with open('data/general/def_no_huddle.json', 'w') as file:
        file.write(data_json)
    
    end_time = time.time()

    # # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the result
    print(f"Script took {elapsed_time} seconds to run.")


def get_qb_pa():
    ftn = nfl.import_ftn_data([2023])
    new_df = nfl.import_pbp_data([2023])
    desc = nfl.import_team_desc()
    desc = desc[['team_abbr', 'team_color', 'team_color2']].rename(columns={'team_abbr': 'posteam', 'team_color': 'primary color', 'team_color2': 'secondary color'})


    ftn = ftn[['nflverse_game_id', 'nflverse_play_id', 'week', 'is_play_action']].rename(columns = {"nflverse_game_id": "game_id", "nflverse_play_id": "play_id"})
    merged = pd.merge(new_df, ftn, how='outer')

    pos_plays = merged[((merged['is_play_action'] == True) & (merged['epa'] > 0))].groupby(['passer_player_id', 'passer_player_name']).size().reset_index(name='positive plays').rename(columns={'passer_player_name': 'name'})
    neg_plays = merged[((merged['is_play_action'] == True) & (merged['epa'] <= 0))].groupby(['passer_player_id', 'passer_player_name']).size().reset_index(name='negative plays').rename(columns={'passer_player_name': 'name'})


    succ = pd.merge(pos_plays, neg_plays, how='outer')

    pa = merged[(merged['is_play_action'] == True)].groupby(['passer_player_id', 'passer_player_name', 'posteam'])['epa'].mean().reset_index(name='play action epa').rename(columns={'passer_player_name': 'name'})
    n = merged[(merged['pass'] == 1)].groupby(['passer_player_id', 'passer_player_name']).size().reset_index(name='number of passes').rename(columns={'passer_player_name': 'name'})

    pa = pd.merge(pd.merge(pd.merge(pa, succ, how='outer'), n, how='outer'), desc, how='outer')
    pa = pa[(pa['number of passes'] > 90)].sort_values('play action epa', ascending=False)
    pa['success rate'] = pa['positive plays'] / (pa['negative plays'] + pa['positive plays'])
    data_json = json.dumps([{'data': {'x': row['play action epa'], 'y': row['success rate'], 'r': row['number of passes'], 'name': row['name']}, 'primary color': row['primary color'], 'secondary color': row['secondary color']} for _, row in pa.iterrows()])
    with open('data/players/qb/qb_pa.json', 'w') as file:
        file.write(data_json)




get_off_no_huddle()

def tempo():
    with open('tempo.txt', 'a') as file:
        file.write(str(datetime.datetime.now()))
