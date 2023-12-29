import pandas as pd
import numpy as np
import nfl_data_py as nfl
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
pio.renderers.default = "browser"

# todo: check column trim
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
