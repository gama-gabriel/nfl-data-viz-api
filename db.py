import json
import nfl_data_py as nfl
import pandas as pd
import datetime
import time
import requests
import pyarrow.parquet as pq
from io import BytesIO

def update_raw(url='https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_', year=2023, save_path='data/raw/play_by_play_'):
    start_time = time.time()

    url = url + str(year) + '.parquet'
    save_path = save_path + str(year) + '.parquet'
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


def get_side_epa(side, new_df=new_df):
    keys = {'offense': {'group': 'posteam', 'label': 'offensive'}, 'defense': {'group': 'defteam', 'label': 'defensive'}}

    epa = new_df[(new_df['pass'] == 1) | (new_df['rush'] == 1)].groupby(keys[side]['group'])['epa'].mean().reset_index().rename(columns = {'epa': f'{keys[side]['label']} epa', keys[side]['group']: 'team'})
    return epa


def get_side_succ(side, new_df=new_df):
    keys = {'offense': {'group': 'posteam', 'label': 'off'}, 'defense': {'group': 'defteam', 'label': 'def'}}

    pos_plays = new_df[((new_df['pass'] == 1) | (new_df['rush'] == 1)) & (new_df['epa'] > 0)].groupby(keys[side]['group']).size().reset_index(name=f'{keys[side]['label']} positive plays').rename(columns={keys[side]['group']: 'team'})
    neg_plays = new_df[((new_df['pass'] == 1) | (new_df['rush'] == 1)) & (new_df['epa'] <= 0)].groupby(keys[side]['group']).size().reset_index(name=f'{keys[side]['label']} negative plays').rename(columns={keys[side]['group']: 'team'})

    succ_rate = pd.merge(pos_plays, neg_plays, how='outer')
    succ_rate[f'{keys[side]['label']} success rate'] = succ_rate[f'{keys[side]['label']} positive plays'] / (succ_rate[f'{keys[side]['label']} positive plays'] + succ_rate[f'{keys[side]['label']} negative plays'])
    return succ_rate


def get_epa(desc=desc):
    start_time = time.time()

    off_epa = get_side_epa(side='offense')
    def_epa = get_side_epa(side='defense')

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
    off_succ = get_side_succ(side='offense')
    def_succ = get_side_succ(side='defense')

    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_name': 'full_name'})
    
    succ = pd.merge(pd.merge(off_succ, def_succ, how='outer'), desc, how='outer').dropna()
    succ_json = json.dumps([{'data': {'x': row['off success rate'], 'y': row['def success rate']}, 'name': row['team'], 'logo': row['logo'], 'color': row['color']} for _, row in succ.iterrows()])

    with open('data/general/succ.json', 'w') as file:
        file.write(succ_json)


def get_side_play_type(side, play_type, new_df=new_df, desc=desc):
    keys = {'offense': {'group': 'posteam', 'label': 'off'}, 'defense': {'group': 'defteam', 'label': 'def'}, 'dropback': {'filter': 'pass'}, 'rush': {'filter': 'rush'}}
    
    start_time = time.time()

    epa = new_df[(new_df[keys[play_type]['filter']] == 1)].groupby(keys[side]['group'])['epa'].mean().reset_index().rename(columns = {'epa': f'{play_type} epa', keys[side]['group']: 'team'})

    pos_plays = new_df[((new_df[keys[play_type]['filter']] == 1) & (new_df['epa'] > 0))].groupby(keys[side]['group']).size().reset_index(name=f'{play_type} positive plays').rename(columns = {keys[side]['group']: 'team'})
    neg_plays = new_df[((new_df[keys[play_type]['filter']] == 1) & (new_df['epa'] <= 0))].groupby(keys[side]['group']).size().reset_index(name=f'{play_type} negative plays').rename(columns = {keys[side]['group']: 'team'})

    succ = pd.merge(pos_plays, neg_plays, how='outer')
    succ[f'{play_type} success rate'] = succ[f'{play_type} positive plays'] / (succ[f'{play_type} positive plays'] + succ[f'{play_type} negative plays'])

    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_name': 'full_name'})

    data = pd.merge(pd.merge(epa, succ, how='outer'), desc, how='outer').dropna()

    data_json = json.dumps([{'data': {'x': row[f'{play_type} epa'], 'y': row[f'{play_type} success rate']}, 'name': row['team'], 'logo': row['logo'], 'color': row['color']} for _, row in data.iterrows()])

    with open(f'data/general/{keys[side]['label']}_{play_type}.json', 'w') as file:
        file.write(data_json)

    end_time = time.time()

    # # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the result
    print(f"Script took {elapsed_time} seconds to run.")



def get_side_dropback_rush(side, new_df=new_df, desc=desc):
    keys = {'offense': {'group': 'posteam', 'label': 'off'}, 'defense': {'group': 'defteam', 'label': 'def'}}
    
    start_time = time.time()

    dropback = new_df[(new_df['pass'] == 1)].groupby(keys[side]['group'])['epa'].mean().reset_index().rename(columns= {keys[side]['group']: 'team', 'epa': 'dropback epa'})

    rush = new_df[(new_df['rush'] == 1)].groupby(keys[side]['group'])['epa'].mean().reset_index().rename(columns= {keys[side]['group']: 'team', 'epa': 'rush epa'})

    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_name': 'full_name'})

    data = pd.merge(pd.merge(dropback, rush, how='outer'), desc, how='outer').dropna()

    data_json = json.dumps([{'data': {'x': row['dropback epa'], 'y': row['rush epa']}, 'name': row['team'], 'logo': row['logo'], 'color': row['color']} for _, row in data.iterrows()])

    with open(f'data/general/{keys[side]['label']}_dropback_rush.json', 'w') as file:
        file.write(data_json)

    end_time = time.time()

    # # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the result
    print(f"Script took {elapsed_time} seconds to run.")


def get_side_succ_epa(side, desc=desc):
    keys = {'offense': {'desc': 'offensive', 'label': 'off'}, 'defense': {'desc': 'defensive', 'label': 'def'}}
    
    epa = get_side_epa(side=side)
    succ = get_side_succ(side=side)

    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_name': 'full_name'})

    succ_epa = pd.merge(pd.merge(epa, succ, how='outer'), desc, how='outer').dropna()
    data_json = json.dumps([{'data': {'x': row[f'{keys[side]['desc']} epa'], 'y': row[f'{keys[side]['label']} success rate']}, 'name': row['team'], 'logo': row['logo'], 'color': row['color']} for _, row in succ_epa.iterrows()])

    with open(f'data/general/{keys[side]['label']}_succ_epa.json', 'w') as file:
        file.write(data_json)


def get_side_group_downs(side, downs: list, new_df=new_df, desc=desc):
    keys = {'offense': {'group': 'posteam', 'label': 'off'}, 'defense': {'group': 'defteam', 'label': 'def'}}
    
    start_time = time.time()

    epa = new_df[(((new_df['down'] == downs[0]) | (new_df['down'] == downs[1])) & ((new_df['pass'] == 1) | (new_df['rush'] == 1)))].groupby(keys[side]['group'])['epa'].mean().reset_index().rename(columns = {keys[side]['group']: 'team'})
    
    pos_plays = new_df[(((new_df['down'] == downs[0]) | (new_df['down'] == downs[1])) & ((new_df['pass'] == 1) | (new_df['rush'] == 1)) & (new_df['epa'] > 0))].groupby(keys[side]['group']).size().reset_index(name='positive plays').rename(columns = {keys[side]['group']: 'team'})
    neg_plays = new_df[(((new_df['down'] == downs[0]) | (new_df['down'] == downs[1])) & ((new_df['pass'] == 1) | (new_df['rush'] == 1)) & (new_df['epa'] <= 0))].groupby(keys[side]['group']).size().reset_index(name='negative plays').rename(columns = {keys[side]['group']: 'team'})

    succ = pd.merge(pos_plays, neg_plays, how='outer')
    succ['success rate'] = succ['positive plays'] / (succ['positive plays'] + succ['negative plays'])

    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_name': 'full_name'})

    data = pd.merge(pd.merge(epa, succ, how='outer'), desc, how='outer').dropna()

    data_json = json.dumps([{'data': {'x': row['epa'], 'y': row['success rate']}, 'name': row['team'], 'logo': row['logo'], 'color': row['color']} for _, row in data.iterrows()])

    if downs == [1, 2]:
        with open(f'data/general/{keys[side]['label']}_early_downs.json', 'w') as file:
            file.write(data_json)
    else:
        if downs == [3, 4]:
            with open(f'data/general/{keys[side]['label']}_late_downs.json', 'w') as file:
                file.write(data_json)
    
    end_time = time.time()

    # # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the result
    print(f"Script took {elapsed_time} seconds to run.")


def get_side_ftn_condition(side, condition, new_df=ftn, desc=desc):
    # posible conditions: screen, trick play, motion, no huddle, rpo
    keys = {'offense': {'group': 'posteam', 'label': 'off'}, 'defense': {'group': 'defteam', 'label': 'def'}, 'screen': {'col': 'is_screen_pass', 'label': 'screen'}, 'trick play': {'col': 'is_trick_play', 'label': 'trick'}, 'motion': {'col': 'is_motion', 'label': 'motion'}, 'no huddle': {'col': 'is_no_huddle', 'label': 'no_huddle'}, 'rpo': {'col': 'is_rpo', 'label':'rpo'}}

    start_time = time.time()

    epa = new_df[(new_df[keys[condition]['col']] == True)].groupby(keys[side]['group'])['epa'].mean().reset_index().rename(columns = {keys[side]['group']: 'team', 'epa': f'{condition} epa'})

    pos_plays = new_df[((new_df[keys[condition]['col']] == True) & (new_df['epa'] > 0))].groupby(keys[side]['group']).size().reset_index(name='positive plays').rename(columns = {keys[side]['group']: 'team'})
    neg_plays = new_df[((new_df[keys[condition]['col']] == True) & (new_df['epa'] <= 0))].groupby(keys[side]['group']).size().reset_index(name='negative plays').rename(columns = {keys[side]['group']: 'team'})

    total = new_df[((new_df['pass'] == 1) | (new_df['rush'] == 1))].groupby(keys[side]['group']).size().reset_index(name='number of plays').rename(columns = {keys[side]['group']: 'team'})

    succ = pd.merge(pd.merge(pos_plays, neg_plays, how='outer'), total, how='outer')
    succ[f'{condition} rate'] = (succ['positive plays'] + succ['negative plays']) / succ['number of plays']
    succ['success rate'] = succ['positive plays'] / (succ['positive plays'] + succ['negative plays'])

    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color', 'team_color2']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_color2': 'secondary color', 'team_name': 'full_name'})

    data = pd.merge(pd.merge(epa, succ, how='outer'), desc, how='outer').dropna()

    data_json = json.dumps(([{'data': {'x': row[f'{condition} epa'], 'y': row['success rate'], 'r': row[f'{condition} rate'], 'name': row['team']}, 'primary color': row['color'], 'secondary color': row['secondary color']} for _, row in data.iterrows()]))

    with open(f'data/general/{keys[side]['label']}_{keys[condition]['label']}.json', 'w') as file:
        file.write(data_json)
    
    end_time = time.time()

    # # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the result
    print(f"Script took {elapsed_time} seconds to run.")


def get_off_short(new_df=new_df, desc=desc):
    start_time = time.time()
    
    epa = new_df[((new_df['pass'] == 1) | (new_df['rush'] == 1)) & (new_df['ydstogo'] <= 3)].groupby('posteam')['epa'].mean().reset_index().rename(columns= {'posteam': 'team'})

    conversion = new_df[((new_df['pass'] == 1) | (new_df['rush'] == 1)) & (new_df['ydstogo'] <= 3)].drop_duplicates(subset= ['series', 'posteam', 'game_id']).groupby('posteam')['series_success'].mean().reset_index().rename(columns={'posteam': 'team', 'series_success': 'series success'})

    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_name': 'full_name'})

    data = pd.merge(pd.merge(epa, conversion, how='outer'), desc, how='outer').dropna()

    data_json = json.dumps([{'data': {'x': row['epa'], 'y': row['series success']}, 'name': row['team'], 'logo': row['logo'], 'color': row['color']} for _, row in data.iterrows()])

    with open('data/general/off_short.json', 'w') as file:
        file.write(data_json)
    
    end_time = time.time()

    # # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the result
    print(f"Script took {elapsed_time} seconds to run.")


def get_def_short(new_df=new_df, desc=desc):
    start_time = time.time()
    
    epa = new_df[((new_df['pass'] == 1) | (new_df['rush'] == 1)) & (new_df['ydstogo'] <= 3)].groupby('defteam')['epa'].mean().reset_index().rename(columns= {'defteam': 'team'})

    conversion = new_df[((new_df['pass'] == 1) | (new_df['rush'] == 1)) & (new_df['ydstogo'] <= 3)].drop_duplicates(subset= ['series', 'defteam', 'game_id']).groupby('defteam')['series_success'].mean().reset_index().rename(columns={'defteam': 'team', 'series_success': 'series success'})

    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_name': 'full_name'})

    data = pd.merge(pd.merge(epa, conversion, how='outer'), desc, how='outer').dropna()

    data_json = json.dumps([{'data': {'x': row['epa'], 'y': row['series success']}, 'name': row['team'], 'logo': row['logo'], 'color': row['color']} for _, row in data.iterrows()])

    with open('data/general/def_short.json', 'w') as file:
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



# update_raw('https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_', 2023, 'data/raw/play_by_play_')
get_side_ftn_condition(side='offense', condition='motion')


def tempo():
    with open('tempo.txt', 'a') as file:
        file.write(str(datetime.datetime.now()))
