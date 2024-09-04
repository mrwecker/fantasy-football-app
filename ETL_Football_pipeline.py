# THIS IS JUST THE PIPELINE TO GET CSV - USE fantasy_app.py TO LAUNCH APP
import pandas as pd
import requests
import json
from datetime import datetime
import numpy as np
import logging
import os

# Configure logging
logging.basicConfig(
    filename='/Users/michaelwecker/Hyper_Island/Final_Project/logs/etl_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def fetch_data():
    logging.info('Fetching data from API...')
    try:
        url = 'https://fantasy.premierleague.com/api/bootstrap-static/'
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad status
        data = json.loads(response.content)
        
        players_df = pd.DataFrame(data['elements'])
        teams_df = pd.DataFrame(data['teams'])
        events_df = pd.DataFrame(data['events'])
        events_df['gameweek_date'] = pd.to_datetime(events_df['deadline_time']).dt.date
        events_df = events_df.drop(columns=['deadline_time'])
        
        # Determine the current gameweek based on today's date
        today = datetime.now().date()
        current_gameweek = events_df[events_df['gameweek_date'] >= today].iloc[0]['id']
        
        logging.info(f'Data fetched successfully. Current gameweek: {current_gameweek}')
        return players_df, teams_df, events_df, current_gameweek
    except Exception as e:
        logging.error(f'Error fetching data: {e}')
        raise

def merge_player_team_data(players_df, teams_df):
    logging.info('Merging player and team data...')
    try:
        merged_df = pd.merge(left=players_df, right=teams_df, how='left', left_on='team', right_on='id', suffixes=('_player', '_team'))
        logging.info('Merge complete')
        return merged_df
    except Exception as e:
        logging.error(f'Error merging data: {e}')
        raise

def select_columns(df):
    logging.info('Selecting columns...')
    try:
        selected_column_names = [
            'id_player', 'first_name', 'second_name', 'element_type', 'now_cost', 'name', 'total_points', 
            'goals_scored', 'assists', 'clean_sheets', 'points_per_game_rank', 'value_form', 'value_season', 
            'ict_index', 'expected_goals', 'expected_assists', 'expected_goal_involvements', 'influence_rank', 
            'form_rank', 'form_player', 'in_dreamteam', 'news', 'chance_of_playing_next_round', 'points_per_game', 
            'ict_index_rank', 'expected_goals_per_90', 'expected_assists_per_90', 'strength_overall_home', 
            'strength_overall_away', 'team', 'team_code'
        ]
        selected_df = df[selected_column_names]
        logging.info('Column selection complete')
        return selected_df
    except KeyError as e:
        logging.error(f'KeyError selecting columns: {e}')
        raise
    except Exception as e:
        logging.error(f'Error selecting columns: {e}')
        raise

def get_player_stats(player_id):
    logging.info(f'Fetching player stats for player ID: {player_id}')
    try:
        url = f'https://fantasy.premierleague.com/api/element-summary/{player_id}/'
        response = requests.get(url)
        response.raise_for_status()
        data = response.json().get('fixtures', [])
        fixtures_data = [
            {
                'event_name': fixture.get('event_name', None),
                'difficulty': fixture.get('difficulty', None),
            }
            for fixture in data
        ]
        player_stats_df = pd.DataFrame({
            'PlayerID': [player_id] * len(fixtures_data),
            'EventName': [fixture['event_name'] for fixture in fixtures_data],
            'Difficulty': [fixture['difficulty'] for fixture in fixtures_data],
        })
        logging.info(f'Player stats fetched for player ID: {player_id}')
        return player_stats_df
    except Exception as e:
        logging.error(f'Error fetching player stats for player ID {player_id}: {e}')
        raise

def compile_difficulty_data(player_ids):
    logging.info('Compiling difficulty data for all players...')
    try:
        all_player_data = []
        for player_id in player_ids:
            player_stats = get_player_stats(player_id)
            if player_stats is not None:
                all_player_data.append(player_stats)
        difficulty_df = pd.concat(all_player_data, ignore_index=True)
        logging.info('Difficulty data compilation complete')
        return difficulty_df
    except Exception as e:
        logging.error(f'Error compiling difficulty data: {e}')
        raise

def process_data(players_df, difficulty_df, current_gameweek):
    logging.info('Processing data...')
    try:
        pivot_df = difficulty_df.pivot_table(index='PlayerID', columns='EventName', values='Difficulty')
        pivot_df_sorted = pivot_df.sort_values(by='PlayerID', ascending=True)
        pivot_df_sorted.reset_index(inplace=True)

        full_df = pd.merge(left=players_df, right=pivot_df_sorted, how='left', left_on='id_player', right_on='PlayerID')
        
        element_mapping = {1: 'GK', 2: 'DEF', 3: 'MID', 4: 'FWD'}
        full_df["element_type"] = full_df["element_type"].replace(element_mapping)
        
        full_df.rename(columns={
            'id_player': 'Player ID', 
            'first_name': 'First Name', 
            'second_name': 'Last Name',
            'element_type': 'Position',
            'now_cost': 'Cost',
            'name': 'Team',
            'total_points': 'Total points',
            'goals_scored': 'Goals scored',
            'assists': 'Assists',
            'clean_sheets': 'Clean sheets',
            'points_per_game_rank': 'Points per game rank',
            'value_form': 'Value form',
            'value_season': 'Value season',
            'ict_index': 'ICT Index',
            'expected_goals': 'Expected goals',
            'expected_assists': 'Expected assists',
            'expected_goal_involvements': 'Expected goal involvements',
            'influence_rank': 'Influence rank',
            'form_rank': 'Form rank',
            'form_player': 'Player form',
            'in_dreamteam': 'In dream team',
            'news': 'News',
            'chance_of_playing_next_round': 'Chance of playing next round',
            'points_per_game': 'Points per game',
            'ict_index_rank': 'ICT Index rank',
            'expected_goals_per_90': 'Expected goals per 90',
            'expected_assists_per_90': 'Expected assists per 90',
            'strength_overall_home': 'Strength overall home',
            'strength_overall_away': 'Strength overall away'
        }, inplace=True)
        
        full_df["Player form"] = pd.to_numeric(full_df["Player form"], errors='coerce').fillna(0)
        full_df["Player form"] = full_df["Player form"].clip(lower=0)
        
        gameweek_columns = [col for col in full_df.columns if col.startswith('Gameweek')]
        sorted_gameweek_columns = sorted(gameweek_columns, key=lambda x: int(x.split()[1]))
        other_columns = [col for col in full_df.columns if not col.startswith('Gameweek')]
        full_df = full_df[other_columns + sorted_gameweek_columns]
        
        gameweek_start_column = full_df.columns.get_loc(f'Gameweek {current_gameweek}')
        
        def sum_next_5_non_nan(row):
            non_nan_values = row.iloc[gameweek_start_column:].dropna().head(5)
            return np.sum(non_nan_values)

        full_df["Difficulty_score"] = full_df.apply(sum_next_5_non_nan, axis=1).round(3)
        full_df["Fixture Difficulty Index"] = (full_df["Player form"] / full_df["Difficulty_score"]).round(3)
        full_df['full_name'] = full_df['First Name'] + ' ' + full_df['Last Name']
        
        logging.info('Data processing complete')
        return full_df
    except Exception as e:
        logging.error(f'Error processing data: {e}')
        raise

def main():
    logging.info('Starting ETL pipeline')
    try:
        players_df, teams_df, events_df, current_gameweek = fetch_data()
        player_teams_data = merge_player_team_data(players_df, teams_df)
        selected_df = select_columns(player_teams_data)
        player_ids = players_df["id"]
        difficulty_df = compile_difficulty_data(player_ids)
        processed_df = process_data(selected_df, difficulty_df, current_gameweek)

        base_path = os.path.dirname(__file__)  # Directory of the current script
        processed_df.to_csv(os.path.join(base_path, "data_processed_player_app.csv"), index=False)
        events_df.to_csv(os.path.join(base_path, "data_events.csv"), index=False)
        players_df.to_csv(os.path.join(base_path, "data_players.csv"), index=False)
        teams_df.to_csv(os.path.join(base_path, "data_teams.csv"), index=False)

        logging.info('ETL pipeline completed successfully')
    except Exception as e:
        logging.error(f'ETL pipeline failed: {e}')

if __name__ == "__main__":
    main()
