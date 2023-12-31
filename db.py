import json
import nfl_data_py as nfl
import pandas as pd
import datetime
import numpy
import time

desc = nfl.import_team_desc()

url1 = 'play_by_play_2023.parquet'
url2 = 'pbp_participation_2023.parquet'

df1 = pd.read_parquet(url1)
df2 = pd.read_parquet(url2)
new_df = pd.merge(df1,df2, how='left', on=['play_id','old_game_id'])

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
    pos_plays = new_df[((new_df['pass'] == 1) | (new_df['rush'] == 1)) & (new_df['epa'] > 0)].groupby('posteam').size().reset_index(name='positive plays').rename(columns={'posteam': 'team'})
    neg_plays = new_df[((new_df['pass'] == 1) | (new_df['rush'] == 1)) & (new_df['epa'] <= 0)].groupby('posteam').size().reset_index(name='negative plays').rename(columns={'posteam': 'team'})
    
    succ_rate = pd.merge(pos_plays, neg_plays, how='outer')
    succ_rate['success rate'] = succ_rate['positive plays'] / (succ_rate['positive plays'] + succ_rate['negative plays'])
    return succ_rate


def get_def_epa(new_df=new_df):
    def_epa = new_df[(new_df['pass'] == 1) | (new_df['rush'] == 1)].groupby('defteam')['epa'].mean().reset_index().rename(columns = {'epa': 'defensive epa', 'defteam': 'team'})
    return def_epa


def get_def_succ(new_df=new_df):
    pos_plays = new_df[((new_df['pass'] == 1) | (new_df['rush'] == 1)) & (new_df['epa'] > 0)].groupby('defteam').size().reset_index(name='positive plays').rename(columns={'defteam': 'team'})
    neg_plays = new_df[((new_df['pass'] == 1) | (new_df['rush'] == 1)) & (new_df['epa'] <= 0)].groupby('defteam').size().reset_index(name='negative plays').rename(columns={'defteam': 'team'})
    
    succ_rate = pd.merge(pos_plays, neg_plays, how='outer')
    succ_rate['success rate'] = succ_rate['positive plays'] / (succ_rate['positive plays'] + succ_rate['negative plays'])
    return succ_rate


def get_epa():
    start_time = time.time()

    off_epa = get_off_epa()
    def_epa = get_def_epa()
    desc = nfl.import_team_desc()
    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_name': 'full_name'})

    epa = pd.merge(pd.merge(off_epa, def_epa, how='outer'), desc, how='outer').dropna()
    epa_json = json.dumps([{'data': {'x': row['offensive epa'], 'y': row['defensive epa']}, 'name': row['team'], 'logo': row['logo'], 'color': row['color']} for _, row in epa.iterrows()])

    with open('epa.json', 'w') as file:
        file.write(epa_json)
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
    data_json = json.dumps([{'data': {'x': row['offensive epa'], 'y': row['success rate']}, 'name': row['team'], 'logo': row['logo'], 'color': row['color']} for _, row in off_succ_epa.iterrows()])

    with open('off_succ_epa.json', 'w') as file:
        file.write(data_json)


def get_def_succ_epa(desc=desc):
    def_epa = get_def_epa()
    def_succ = get_def_succ()

    desc = desc[['team_abbr', 'team_name', 'team_logo_espn', 'team_color']].rename(columns={'team_abbr': 'team', 'team_logo_espn': 'logo', 'team_color': 'color', 'team_name': 'full_name'})
    def_succ_epa = pd.merge(pd.merge(def_epa, def_succ, how='outer'), desc, how='outer').dropna()
    data_json = json.dumps([{'data': {'x': row['defensive epa'], 'y': row['success rate']}, 'name': row['team'], 'logo': row['logo'], 'color': row['color']} for _, row in def_succ_epa.iterrows()])

    with open('def_succ_epa.json', 'w') as file:
        file.write(data_json)


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
    with open('qb_pa.json', 'w') as file:
        file.write(data_json)





def tempo():
    with open('tempo.txt', 'a') as file:
        file.write(str(datetime.datetime.now()))
