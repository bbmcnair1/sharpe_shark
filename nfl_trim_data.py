import pandas as pd
import numpy as np
from typing import List


'''
    Columns
'''


def split_identifiers(df, column_name):
    if column_name not in df.columns:
        raise ValueError(f"Column {column_name} not found in DataFrame")
    for i in range(1, 12):
        df[f'player{i}'] = df[column_name].apply(lambda x: x.split(';')[i-1] if len(x.split(';')) >= i else None)
    df.drop(column_name, axis=1, inplace=True)
    return df


def trim_columns(df):
    col_dict = dict()
    col_dict['all'] = df.columns.tolist()
    col_dict['game'] = ['season', 'week', 'game_date', 'game_id', 'home_team', 'away_team']
    col_dict['time'] = ['qtr', 'time', 'game_seconds_remaining', 'drive']
    col_dict['play'] = ['play_id', 'posteam', 'possession_team', 'defteam', 'down', 'ydstogo', 'goal_to_go', 'yardline_100',
                        'play_type', 'pass', 'rush', 'rush_attempt', 'pass_attempt', 'qb_scramble', 'yards_gained', 'desc']
    col_dict['player'] = ['passer_player_id', 'passer_player_name', 'passer', 'passing_yards',
                          'receiver_player_id', 'receiver_player_name', 'receiver', 'receiving_yards',
                          'rusher_player_id', 'rusher_player_name', 'rusher', 'rushing_yards']
    col_dict['play_type'] = ['pass_length', 'pass_location', 'air_yards', 'yards_after_catch', 'run_location', 'run_gap']
    col_dict['touchdown'] = ['touchdown', 'pass_touchdown', 'rush_touchdown']
    col_dict['formation'] = ['time_to_throw', 'was_pressure', 'route', 'defense_man_zone_type', 'defense_coverage_type']
    col_dict['wp'] = ['wp', 'def_wp', 'home_wp', 'away_wp', 'wpa', 'vegas_wpa', 'vegas_home_wpa',
                      'home_wp_post', 'away_wp_post', 'vegas_wp', 'vegas_home_wp']
    col_dict['vegas'] = ['result', 'total', 'spread_line', 'total_line']
    col_dict['score'] = ['total_home_score', 'total_away_score', 'posteam_score', 'defteam_score',
                         'score_differential', 'posteam_score_post', 'defteam_score_post', 'score_differential_post']
    col_dict['other'] = ['cp', 'stadium', 'weather',
                         'away_score', 'home_score',
                         'home_coach', 'away_coach',
                         'first_down', 'xpass', 'pass_oe',
                         'offense_formation', 'offense_personnel', 'defense_personnel',
                         'defenders_in_box', 'offense_players']
    col_dict['other'] = [col for col in col_dict['other'] if col in df.columns.tolist()]

    cols_to_keep = []
    for key in ['game', 'time', 'play', 'player', 'play_type', 'touchdown', 'formation', 'wp', 'vegas', 'score', 'other']:
        if key != 'all':
            cols_to_keep += col_dict[key]

    if len(cols_to_keep) != len(set(cols_to_keep)):
        print('duplicate columns found')
        for col in cols_to_keep:
            if cols_to_keep.count(col) > 1:
                col_index = cols_to_keep.index(col, cols_to_keep.index(col) + 1)
                cols_to_keep.pop(col_index)

    cols_to_keep = [col for col in cols_to_keep if col in df.columns.tolist()]

    df = df.loc[:, cols_to_keep].copy()

    df.rename(columns={'pass': 'dropback', 'rush': 'designed_run'}, inplace=True)

    print('Starting Columns #: ', len(col_dict['all']))
    cols_dropped = len(col_dict['all']) - len(df.columns.tolist())
    print('Number of Columns Dropped: ', cols_dropped)

    # add some columns
    df['time_elapsed'] = df['game_seconds_remaining'].diff().fillna(0) * -1
    df['pass_completion'] = np.where(df['passer_player_name'].notnull()
                                     & df['receiving_yards'].notnull()
                                     & df['dropback'] == 1, 1, 0)
    df['scramble_yards'] = df['qb_scramble'] * df['rushing_yards']
    df['designed_rushing_yards'] = df['rushing_yards'] - df['scramble_yards']

    # split out identifiers into their own columns
    df = split_identifiers(df, 'offense_players')

    print('Number of Columns Added: ', cols_dropped - (len(col_dict['all']) - len(df.columns.tolist())))
    print('Ending Columns #: ', len(df.columns.tolist()))
    return df, col_dict


'''
    Rows
    - current logic drops rows with null values.
    - these plays correspond to kickoffs, timeouts, challenges, game start, 2-min warning, end quarter, penalties, and kneels 
'''


def trim_rows(df, play_type_filter: List[str] = None):
    print('\nStarting Rows #:', len(df))
    df_row_drop = pd.DataFrame(columns=df.columns)

    # drop some null values
    for col in ['posteam', 'posteam_type', 'defteam', 'side_of_field', 'down']:
        if col in df.columns.tolist():
            dropped_rows = df[df[col].isnull()]
            df_row_drop = pd.concat([df_row_drop, dropped_rows], ignore_index=True)
            df = df[df[col].notnull()].copy()

    # only keep specific play types
    if play_type_filter is not None:
        dropped_rows = df[~df['play_type'].isin(play_type_filter)]
        df_row_drop = pd.concat([df_row_drop, dropped_rows], ignore_index=True)
        df = df[df['play_type'].isin(play_type_filter)].copy()

    df.reset_index(drop=True, inplace=True)
    print('Ending Rows #:', len(df))
    print('Number of Rows Dropped: ', len(df_row_drop))

    return df, df_row_drop
