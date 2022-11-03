# import libraries
import requests
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime as dt
from datetime import date
import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import pathlib
import smtplib
import email_to # pip install email-to
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText  
from email.mime.image import MIMEImage
from pretty_html_table import build_table
from google.oauth2.service_account import Credentials #pip install google-api-python-client
import pandas_gbq
from io import StringIO
import pprint

# declare league variables
leagues = [225029328,1553294499] #33471516 - old league last year
num_of_leagues = len(leagues)
year = 2022             #change to 2022
reg_season_weeks = 14

#################################################################
####
#### function to insert data into google big query
####
#################################################################
def insert_gbq(data, target_table, insert_type):
    # declare google big query specific data 
    project_id = 'sagepath-analytics'
    credential_file = f'{current_directory}\sagepath-analytics-e41c031cf9a0.json'
    credential = Credentials.from_service_account_file(credential_file) #Location for BQ job, it needs to match with destination table location
    job_location = "us-east4" #https://cloud.google.com/bigquery/docs/locations

    # Update the in-memory credentials cache (added in pandas-gbq 0.7.0).
    pandas_gbq.context.credentials = credential
    pandas_gbq.context.project = project_id

    # temporarily store the dataframe as a csv in a string variable
    temp_csv_string = data.to_csv(sep=";", index=False)
    temp_csv_string_IO = StringIO(temp_csv_string)
    
    # create new dataframe from string variable
    pd_data = pd.read_csv(temp_csv_string_IO, sep=";")

    # save pandas dataframe to BQ
    pd_data.to_gbq(target_table, if_exists=insert_type)
    
    #################################################################
####
#### function to call the espn historical/current api for a particular league, year, and parameters
####
#################################################################
def call_espn(league, league_year, parameters, url_type):
    # declare base urls
    historical_url = f'https://fantasy.espn.com/apis/v3/games/ffl/leagueHistory/{str(league)}?seasonId={str(league_year)}'
    current_url = f'https://fantasy.espn.com/apis/v3/games/ffl/seasons/{league_year}/segments/0/leagues/{str(league)}'
    
    #figure out which api endpoint to call
    if url_type == 'historical':
        r = requests.get(historical_url, params=parameters).json()[0]
        url = historical_url
    elif url_type == 'current':
        r = requests.get(current_url, params=parameters).json()
        url = current_url
    else:
        url = url_type
        r = requests.get(url, params=parameters).json()
    
    print(f'league[{url_type}]: params: {parameters}')
    
    return r
  
  
  
  #################################################################
####
#### function to call the espn historical/current api for a particular league, year, and parameters
####
#################################################################
def call_espn_master(league, league_year, week): 
    # declare base urls
    url = f'https://fantasy.espn.com/apis/v3/games/ffl/seasons/{league_year}/segments/0/leagues/{league}?scoringPeriodId={week}&view=mBoxscore&view=mMatchupScore&view=mRoster&view=mSettings&view=mStatus&view=mTeam&view=modular&view=mNav'
    
    print(f'league[{league}] week[{week}]')
    
    response = requests.get(url).json()
    return response
  
  #################################################################
####
#### get response for each league for each espn api endpoint
####
#################################################################

# initialze response objects
teams_response = []
matchup_response = []
roster_response = []
settings_response = []
box_score_response = []
matchup_score_response = []
kona_player_info_response = []
player_wl_response = []
schedule_response = []
scoreboard_response = []

# create master dictionary
master_response = {}

for x in range(0, len(leagues)):
    print(f'{x}: {leagues[x]}')
    teams_response.append(call_espn(leagues[x], year, {"view": "mTeams"}, 'current'))
    matchup_response.append(call_espn(leagues[x], year, {"view": "mMatchup"}, 'current'))
    roster_response.append(call_espn(leagues[x], year, {"view": "mRoster"}, 'current'))
    settings_response.append(call_espn(leagues[x], year, {"view": "mSettings"}, 'current'))
    box_score_response.append(call_espn(leagues[x], year, {"view": "mBoxscore"}, 'current'))
    matchup_score_response.append(call_espn(leagues[x], year, {"view": "mMatchupScore"}, 'current'))
    kona_player_info_response.append(call_espn(leagues[x], year, {"view": "kona_player_info"}, 'current'))
    player_wl_response.append(call_espn(leagues[x], year, {"view": "player_wl"}, 'current'))
    schedule_response.append(call_espn(leagues[x], year, {"view": "mSchedule"}, 'current'))
    scoreboard_response.append(call_espn(leagues[x], year, {"view": "mScoreboard"}, 'current'))
    
    # get current setting variables
    regular_season_weeks = settings_response[0]['settings']['scheduleSettings']['matchupPeriodCount'] #the number of regular season games?
    current_week = settings_response[x]['status']['currentMatchupPeriod'] #the current matchup period?
    last_week_playoffs = settings_response[x]['status']['finalScoringPeriod'] #the last week of playoffs?

        
    # initialize leage section in response object
    master_response[leagues[x]] = {}
    
    for y in range (1, current_week+1):#current_week+1):
        # initalize week section in response object
        master_response[leagues[x]][y] = []
        
        # call espn master and store in dictionary
        r = call_espn_master(leagues[x], year, y)
        master_response[leagues[x]][y] = r


# declare variables
current_directory = os.getcwd() #get current directory


#################################################################
####
#### build dt_team object
####
#################################################################

df_teams = pd.DataFrame(columns=['league_id','division','league_name',
                                 'team_key','team_id','abbreviation',
                                 'team_name', 'wins','losses',
                                'ties','team_rank'])


#call each team indivodually and store in a response object
for x in range(0, len(leagues)):
    #print(f'{x}: {leagues[x]}')
    #espn_response.append(call_espn(leagues[x], year, params, 'historical'))
    
    # get league name from settings
    #league_name = espn_response[x][0]['settings']['name']
    league_id = leagues[x]
    league_name = settings_response[x]['settings']['name']
    
    # get team data
    for team in box_score_response[x]['teams']:
        team_id = team['id']
        team_abbrev = team['abbrev']
        team_division = team['divisionId']
        team_key = str(leagues[x]) + '-' + str(team_id)
        team_name = team['location'].strip() + ' ' + team['nickname'].strip()
        wins = team['record']['overall']['wins']
        losses = team['record']['overall']['losses']
        ties = team['record']['overall']['ties']
        team_rank = team['rankCalculatedFinal']
    
        values = {
                    'league_id': league_id,
                    'league_name': league_name,
                    'team_key': team_key, 
                    'abbreviation': team_abbrev, 
                    'team_id': str(team_id), 
                    'team_name': team_name, 
                    'division': team_division, 
                    'wins': wins,
                    'losses': losses, 
                    'ties': ties, 
                    'team_rank': team_rank
                 }
        
        df_teams = df_teams.append(values, ignore_index = True)
        #teamId[team_key] = team['location'].strip() + ' ' + team['nickname'].strip()  
        
# cleanup df for joining
df_teams['team_id'] = df_teams['team_id'].astype(int)


#################################################################
####
#### build df_matchup data frame to use in df_box_scores
####
#################################################################

# create empty dataframe for matchups
df_matchup = pd.DataFrame()

for x in range(0, len(leagues)):
    # create a prep df
    df_prep_matchup = pd.json_normalize(matchup_response[x]['schedule'])
    df_prep_matchup['league_id'] = leagues[x]  # add a column with the league id for joining
    # append to the mast df
    df_matchup = df_matchup.append(df_prep_matchup)

matchup_column_names = {
    'league_id':'league_id',
    'matchupPeriodId': 'week',
    'away.teamId':'away_team_id',
    'away.totalPoints':'away_score',
    'home.teamId':'home_team_id', 
    'home.totalPoints':'home_score',
}

df_matchup = df_matchup.reindex(columns=matchup_column_names).rename(columns=matchup_column_names)
df_matchup['week_type'] = ['regular' if week <= regular_season_weeks else 'playoff' for week in df_matchup['week']]

# cleanup df for joining
df_matchup = df_matchup.fillna('0')
df_matchup['home_team_id'] = df_matchup['home_team_id'].astype(int)
df_matchup['away_team_id'] = df_matchup['away_team_id'].astype(int)
df_matchup['league_id'] = df_matchup['league_id'].astype(int)


#################################################################
####
#### build df_box_score data frame
####
#################################################################
df_box_scores = pd.DataFrame()

# create df_prep off of home team scores
df_prep_box_scores = pd.merge(df_teams, df_matchup, left_on=['league_id','team_id'], right_on=['league_id','home_team_id'], how='left')
df_prep_box_scores = df_prep_box_scores[['league_id','team_key','team_id','division',
                                        'league_name','abbreviation','team_name',
                                        'wins','losses','ties','team_rank','week',
                                        'week_type','home_score','away_team_id',
                                        'away_score']]


df_prep_column_names = {
                    'league_id':'league_id',
                    'team_key':'team_key',
                    'team_id':'team_id',
                    'division':'division',
                    'league_name':'league_name',
                    'abbreviation':'abbreviation',
                    'team_name':'team_name',
                    'wins':'wins',
                    'losses':'losses',
                    'ties':'ties',
                    'team_rank':'team_rank',
                    'week':'week',
                    'week_type':'week_type',
                    'home_score':'score',
                    'away_team_id':'opponent_team_id',
                    'away_score':'opponent_score'
}

df_prep_box_scores = df_prep_box_scores.reindex(columns=df_prep_column_names).rename(columns=df_prep_column_names)


df_box_scores = df_box_scores.append(df_prep_box_scores[['league_id','team_key','team_id','division',
                                            'league_name','abbreviation','team_name',
                                            'wins','losses','ties','team_rank','week',
                                            'week_type','score','opponent_team_id',
                                            'opponent_score']])


# add away team scores
df_prep_box_scores = pd.merge(df_teams, df_matchup, left_on=['league_id','team_id'], right_on=['league_id','away_team_id'], how='left')
df_prep_box_scores = df_prep_box_scores[['league_id','team_key','team_id','division',
                                        'league_name','abbreviation','team_name',
                                        'wins','losses','ties','team_rank','week',
                                        'week_type','away_score','home_team_id',
                                        'home_score']]


df_prep_column_names = {
                    'league_id':'league_id',
                    'team_key':'team_key',
                    'team_id':'team_id',
                    'division':'division',
                    'league_name':'league_name',
                    'abbreviation':'abbreviation',
                    'team_name':'team_name',
                    'wins':'wins',
                    'losses':'losses',
                    'ties':'ties',
                    'team_rank':'team_rank',
                    'week':'week',
                    'week_type':'week_type',
                    'away_score':'score',
                    'home_team_id':'opponent_team_id',
                    'home_score':'opponent_score'
}

df_prep_box_scores = df_prep_box_scores.reindex(columns=df_prep_column_names).rename(columns=df_prep_column_names)


df_box_scores = df_box_scores.append(df_prep_box_scores[['league_id','team_key','team_id','division',
                                            'league_name','abbreviation','team_name',
                                            'wins','losses','ties','team_rank','week',
                                            'week_type','score','opponent_team_id',
                                            'opponent_score']])


# join df_teams again to get the name of the opponents
columns_to_drop = {
  'division_y','league_name_y','team_id_y','wins_y','losses_y','ties_y','team_rank_y'
}

df_box_scores = pd.merge(df_box_scores, df_teams, left_on=['league_id','opponent_team_id'], right_on=['league_id','team_id'], how='left').drop(columns=columns_to_drop)

# make column names pretty again
df_master_column_names = {
                    'league_id':'league_id',
                    'team_key_x':'team_key',
                    'team_id_x':'team_id',
                    'division_x':'division',
                    'league_name_x':'league_name',
                    'abbreviation_x':'abbreviation',
                    'team_name_x':'team_name',
                    'wins_x':'wins',
                    'losses_x':'losses',
                    'ties_x':'ties',
                    'team_rank_x':'team_rank',
                    'week':'week',
                    'week_type':'week_type',
                    'score':'score',
                    'opponent_team_id':'opponent_team_id',
                    'opponent_score':'opponent_score', 
                    'team_key_y':'opponent_team_key', 
                    'abbreviation_y':'opponent_abbreviation', 
                    'team_name_y':'opponent_team_name', 
}

df_box_scores = df_box_scores.reindex(columns=df_master_column_names).rename(columns=df_master_column_names)

# create timestamp to add to df_box_scores
time_stamp = dt.now().timestamp()
date_time = dt.fromtimestamp(time_stamp, tz=None)

# create new columns for current week and data run date
df_box_scores['current_week'] = current_week
df_box_scores['data_run'] = date_time

# fill null opponent team names with BYE for a bye week
df_box_scores['opponent_team_name'] = df_box_scores['opponent_team_name'].fillna('BYE')

# append to google big query
insert_gbq(df_box_scores, 'sffl.imp_box_scores', 'append')


#################################################################
####
#### build the master data frame of player roster and projections
####
#################################################################

# initialize dataframe
df_players = pd.DataFrame()

# create main 2 loops for league and week looping
for x in range(0, len(leagues)):
    for y in range (1, current_week):
        for matchup in master_response[leagues[x]][y]['schedule']:
            if 'rosterForCurrentScoringPeriod' in matchup['home'].keys():
                game_type = matchup['playoffTierType']
                team_id = matchup['home']['teamId']
                week_id = matchup['matchupPeriodId']
                team_score = matchup['home']['rosterForCurrentScoringPeriod']['appliedStatTotal']
                for rosters in matchup['home']['rosterForCurrentScoringPeriod']['entries']:
                    lineup_slot_id = rosters['lineupSlotId']
                    # calculate wheter players started this week or not
                    if lineup_slot_id >= 20 and lineup_slot_id != 23 :
                        lineup_status = 'benched'
                    else:
                        lineup_status = 'started'
                    player_id = rosters['playerPoolEntry']['player']['id']
                    player_name = rosters['playerPoolEntry']['player']['fullName']
                    for stats in range(0,len(rosters['playerPoolEntry']['player']['stats'])):
                        if stats == 0:
                            actual_score = round(rosters['playerPoolEntry']['player']['stats'][stats]['appliedTotal'],3)
                        elif stats == 1:
                            projected_score = round(rosters['playerPoolEntry']['player']['stats'][stats]['appliedTotal'],3)
                        score_type = stats
                        season_id = rosters['playerPoolEntry']['player']['stats'][stats]['seasonId']
                    #print(f'home team:{team_id}: player:{player_id} - {player_name} projected_score:{projected_score} actualscore:{actual_score} week:{week_id} season:{season_id}')
                    # append player dataframe
                    player_row = {
                        'league_id': leagues[x], 
                        'team_id': team_id,
                        'team_key': str(leagues[x]) + '-' + str(team_id),
                        'team_type': 'home',
                        'week' : y,
                        'game_type': game_type,
                        'team_score': team_score,
                        'player_id': player_id,
                        'lineup_slot_id': lineup_slot_id,
                        'lineup_status': lineup_status,
                        'player_name': player_name,
                        'player_id': player_id,
                        'player_projected_score' : projected_score, 
                        'player_actual_score' : actual_score, 
                        'player_season_id':season_id
                     }   
                    df_players = df_players.append(player_row, ignore_index=True) 
            if 'away' in matchup.keys():
                if 'rosterForCurrentScoringPeriod' in matchup['away'].keys(): 
                    game_type = matchup['playoffTierType']
                    team_id = matchup['away']['teamId']
                    week_id = matchup['matchupPeriodId']
                    team_score = matchup['away']['rosterForCurrentScoringPeriod']['appliedStatTotal']
                    for rosters in matchup['away']['rosterForCurrentScoringPeriod']['entries']:
                        lineup_slot_id = rosters['lineupSlotId']
                        # calculate wheter players started this week or not
                        if lineup_slot_id >= 20 and lineup_slot_id != 23 :
                            lineup_status = 'benched'
                        else:
                            lineup_status = 'started'
                        player_id = rosters['playerPoolEntry']['player']['id']
                        player_name = rosters['playerPoolEntry']['player']['fullName']
                        for stats in range(0,len(rosters['playerPoolEntry']['player']['stats'])):
                            if stats == 0:
                                actual_score = round(rosters['playerPoolEntry']['player']['stats'][stats]['appliedTotal'],3)
                            elif stats == 1:
                                projected_score = round(rosters['playerPoolEntry']['player']['stats'][stats]['appliedTotal'],3)
                            score_type = stats
                            season_id = rosters['playerPoolEntry']['player']['stats'][stats]['seasonId']
                        #print(f'away team:{team_id}: player:{player_id} - {player_name} projected_score:{projected_score} actualscore:{actual_score} week:{week_id} season:{season_id}')
                        # append player dataframe
                        player_row = {
                            'league_id': leagues[x], 
                            'team_id': team_id,
                            'team_key': str(leagues[x]) + '-' + str(team_id),
                            'team_type': 'away',
                            'week' : y,
                            'game_type': game_type,
                            'team_score': team_score,
                            'player_id': player_id,
                            'lineup_slot_id': lineup_slot_id,
                            'lineup_status': lineup_status,
                            'player_name': player_name,
                            'player_id': player_id,
                            'player_projected_score' : projected_score, 
                            'player_actual_score' : actual_score, 
                            'player_season_id':season_id
                         }   
                        df_players = df_players.append(player_row, ignore_index=True) 
                        
                        
                        # send player data to google big query
df_player_master = pd.merge(left = df_box_scores, right = df_players, how = 'left', on=['team_key','week'])

#rename specific column names
df_player_master.rename(columns={'league_id_x':'league_id', 'team_id_x':'team_id'}, inplace = True)

# join df_teams again to get the name of the opponents
columns_to_drop = {
  'league_id_y','team_id_y'
}

# make column names pretty again
df_player_master.drop(columns_to_drop, axis=1, inplace = True)

# append to google big query
insert_gbq(df_player_master, 'sffl.imp_player_weekly', 'append')


# get accolades
df_box_scores_current_week = df_box_scores.query(f'week == {current_week} - 1')

# get high scorer
league_a_most_pts_name = df_box_scores_current_week[df_box_scores_current_week['league_id']==leagues[0]].query('score == score.max()')['team_name'].values[0]
league_a_most_pts_pts = df_box_scores_current_week[df_box_scores_current_week['league_id']==leagues[0]].query('score == score.max()')['score'].values[0]
league_b_most_pts_name = df_box_scores_current_week[df_box_scores_current_week['league_id']==leagues[1]].query('score == score.max()')['team_name'].values[0]
league_b_most_pts_pts = df_box_scores_current_week[df_box_scores_current_week['league_id']==leagues[1]].query('score == score.max()')['score'].values[0]

# get low scorere
league_a_least_pts_name = df_box_scores_current_week[df_box_scores_current_week['league_id']==leagues[0]].query('score == score.min()')['team_name'].values[0]
league_a_least_pts_pts = df_box_scores_current_week[df_box_scores_current_week['league_id']==leagues[0]].query('score == score.min()')['score'].values[0]
league_b_least_pts_name = df_box_scores_current_week[df_box_scores_current_week['league_id']==leagues[1]].query('score == score.min()')['team_name'].values[0]
league_b_least_pts_pts = df_box_scores_current_week[df_box_scores_current_week['league_id']==leagues[1]].query('score == score.min()')['score'].values[0]

print(f'league A high scorer: {league_a_most_pts_name}: {league_a_most_pts_pts}')
print(f'league B high scorer: {league_b_most_pts_name}: {league_b_most_pts_pts}')
print(f'league A low scorer: {league_a_least_pts_name}: {league_a_least_pts_pts}')
print(f'league B low scorer: {league_b_least_pts_name}: {league_b_least_pts_pts}')



leage_a_best_record_name = df_box_scores_current_week[df_box_scores_current_week['league_id']==leagues[0]].query('wins == wins.max()')['team_name'].values[0]
leage_a_best_record_wins = df_box_scores_current_week[df_box_scores_current_week['league_id']==leagues[0]].query('wins == wins.max()')['wins'].values[0]
leage_b_best_record_name = df_box_scores_current_week[df_box_scores_current_week['league_id']==leagues[1]].query('wins == wins.max()')['team_name'].values[0]
leage_b_best_record_wins = df_box_scores_current_week[df_box_scores_current_week['league_id']==leagues[1]].query('wins == wins.max()')['wins'].values[0]

print(f'league A best record: {leage_a_best_record_name}: {leage_a_best_record_wins}')
print(f'league B best record: {leage_b_best_record_name}: {leage_b_best_record_wins}')
