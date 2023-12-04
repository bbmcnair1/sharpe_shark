import pandas as pd
import numpy as np
import nfl_data_py as nfl
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
pio.renderers.default = "browser"

'''
# create a list called years with ints from 2015-2018
years = list(range(2008, 2024))
pbp_data = nfl.import_pbp_data(years=years, columns=None, downcast=False, cache=False, alt_path=None)

# save to .csv
pbp_data.to_csv('pbp_data.csv', index=False)
'''

# load from .csv
pbp_data = pd.read_csv('pbp_data.csv', low_memory=False)

player_ids = nfl.import_ids()
player_ids = player_ids.loc[:, ['gsis_id', 'merge_name', 'position', 'birthdate', 'draft_year', 'height', 'weight']]
rusher_player_ids = player_ids.copy()
for col in rusher_player_ids.columns:
    rusher_player_ids.rename(columns={col: 'rusher_player_'+col}, inplace=True)

pbp_data = pbp_data.merge(rusher_player_ids, how='left', left_on='rusher_player_id', right_on='rusher_player_gsis_id')

cols_select = [
    # game data
    'game_id',
    'old_game_id',
    'game_date',
    'home_team',
    'away_team',
    'home_wp',
    'away_wp',
    'week',
    'season_type',
    # vegas
    'spread_line',
    'total_line',
    # play data
    'posteam',
    'defteam',
    'game_seconds_remaining',
    'down',
    'ydstogo',
    'yrdln',
    'yardline_100',
    'score_differential',
    'wp',
    'play_type',
    'passer_player_name',
    'rusher_player_name',
    'receiver_player_name',
    'yards_gained', ### ============== yards gained
    'passing_yards',
    'receiving_yards',
    'rushing_yards',
    'name',
    'touchdown',
    'pass_touchdown',
    'rush_touchdown',
    # conditions
    'temp',
    'wind',
    'roof',
    'surface',
    # rushing cols
    'rusher_player_id',
    'rusher',
    'rush_attempt',
    'run_gap',
    'rusher_player_position',
    'rusher_player_birthdate',
    'rusher_player_draft_year',
    'rusher_player_height',
    'rusher_player_weight'
    # passing cols
    'passer_player_id',
    'receiver_player_id',
    'passer',
    'qb_epa',
    'pass_attempt',
    'complete_pass',
    'pass_length',
    'air_yards',
    # other
    'cp',
    'cpoe',
]

# trim pbp_data down to only the columns we want
pbp_data = pbp_data.loc[:, cols_select].copy()

# if a column name appears twice, rename the second one by adding "2" to the end
for col in pbp_data.columns:
    if len(pbp_data.columns[pbp_data.columns == col]) > 1:
        print(col+' is a duplicate column')

# confirm column 'game_date' is in date format and then split out into year and month
pbp_data['game_date'] = pd.to_datetime(pbp_data['game_date'])
pbp_data['year'] = pbp_data['game_date'].dt.year
pbp_data['month'] = pbp_data['game_date'].dt.month

# print # rows, unique years, # unique game_id
print(f'pbp_data.shape: {pbp_data.shape}')
print(f'pbp_data.year.nunique(): {pbp_data.year.nunique()}')
print(f'pbp_data.game_id.nunique(): {pbp_data.game_id.nunique()}')

y = pbp_data.groupby(['game_id', 'rusher_player_id'])['rushing_yards'].sum().reset_index()

