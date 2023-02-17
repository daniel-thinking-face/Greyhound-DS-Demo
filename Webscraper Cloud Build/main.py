import re
import requests
import os

import pandas as pd
import numpy as np

pd.set_option('display.max_columns', None)

import pandas_gbq as gbq

from google.cloud import bigquery

from datetime import datetime, timedelta, date

from time import sleep

# for email
import smtplib

# Make the client instance
client = bigquery.Client(project='greyhound-project')

def gmail_login():

    server = smtplib.SMTP('smtp.gmail.com', 587)

    server.connect("smtp.gmail.com",587)

    server.ehlo()

    server.starttls()

    server.ehlo()

    server.login("greyhoundsproject@gmail.com", "airport10GENE")

    return server

def send_success_email(races, senders_list = ["daniel@thinking-face.com"]):
        
    # First login
    server = gmail_login()
    

    subject = 'Greyhound Webscraping email'

    msg = f"Subject: {subject}\n\nHello, \n\nSuccessful scrape performed.\n\n There were {races} added to the Database"

    # Real run email list
    server.sendmail("greyhoundsproject@gmail.com",senders_list,msg)
    
def send_no_update_email(senders_list = ["daniel@thinking-face.com"]):
        
    # First login
    server = gmail_login()

    subject = 'Greyhound Webscraping email'

    msg = f"Subject: {subject}\n\nHello, \n\nNo updates to the Table today.\n\n "

    # Real run email list
    server.sendmail("greyhoundsproject@gmail.com",senders_list,msg)



# Function for core data
def get_core_results(start_date, end_date, track = 'Crayford', race_type = 'race'):
    
    # Make container
    core_results = []
    
    # define template
    template_url = 'https://api.gbgb.org.uk/api/results?page={}&itemsPerPage=50&track={}&date={}&race_type={}'
    
    # Define headers
    req_headers = {'Accept': 'application/json, text/plain, */*',

    'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.192 Safari/537.36"}
    
    # Set up dates to loop through
    list_of_dates = [x.strftime('%Y-%m-%d') for x in pd.date_range(start_date, end_date)]
    
    # Loop through 
    for date_var in list_of_dates:
        
        # rate limit in case
        #sleep(1)
        
        # make url
        url = template_url.format(1, track, date_var, race_type)
        
        # Make request
        r = requests.get(url, headers = req_headers)
        
        # check status
        if r.status_code != 200:
            
            print('There was an error here')
            
            pass
        
        else:
            
            # Add to list
            core_results.extend(r.json().get('items', []))
            
            if r.json().get('meta').get('pageCount') > 1:
                
                max_pages = r.json().get('meta').get('pageCount') + 1
                
                # Loop through to get full results
                for pg in range(2, max_pages):
                    
                    # make url
                    url = template_url.format(pg, track, date_var, race_type)

                    # Make request
                    r = requests.get(url, headers = req_headers)
                    
                    # Add to list
                    core_results.extend(r.json().get('items', []))
                    
            else:
                
                continue
                
    # Make dataframe
    df = pd.DataFrame(core_results)
    
    if df.empty:
        
        return df
    
    # Reduce cols
    df = df[['meetingId', 'raceDate', 'raceId', 'trackName']].copy()
    
    return df


# define the function
def get_race_results_meta(meetingId):
    
    # Make container
    meta_results = []
    race_results = []
    
    # define template
    race_template = "https://api.gbgb.org.uk/api/results/meeting/{meet}?meeting={meet}"
    
    # Define headers
    req_headers = {'Accept': 'application/json, text/plain, */*',

    'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.192 Safari/537.36"}
    
    # define url
    url = race_template.format(meet = meetingId)
    
    # Now make request
    r = requests.get(url, headers = req_headers)
    
    # check status
    if r.status_code != 200:

        pass

    else:
        
        # Loop through meeting list - should only be one
        for meeting in r.json():
            
            # loop through races
            for race in meeting.get('races'):
                
                # Get trap info
                results = race.pop('traps', [])
                
                # make dataframe
                df_results = pd.DataFrame(results)
                
                df_results['raceId'] = race.get('raceId')
                
                # Add to list
                race_results.append(df_results)
                
                # Add to list
                meta_results.append(race)
                
    final_meta = pd.DataFrame(meta_results)
    
    # Add meeting ID
    final_meta['meetingId'] = meetingId
    
    final_result = pd.concat(race_results)
    
    return final_meta, final_result

# Max data function

def max_date_finder():

    max_date_job = client.query('select max(raceDate) from gbgb_db.race_details')
    rows = max_date_job.result()

    # Go through iterator object
    d_iter = iter(rows)
    first_row = next(d_iter)
    
    # Get max date and add a day
    date_max = first_row[0] + timedelta(days=1)

    # Now max date
    start_date = datetime.strftime(date_max, "%Y-%m-%d")
    return start_date

def trial_max_date_finder():

    max_date_job = client.query('select max(raceDate) from gbgb_db.trial_race_details')
    rows = max_date_job.result()

    # Go through iterator object
    d_iter = iter(rows)
    first_row = next(d_iter)
    
    # Get max date and add a day
    date_max = first_row[0] + timedelta(days=1)

    # Now max date
    start_date = datetime.strftime(date_max, "%Y-%m-%d")
    return start_date

# Master function
def update_races_db():
    
    '''
    Function to automatically update the Big Query database
    '''
    
    # Start date
    start_date = max_date_finder()

    # End date
    end_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    #end_date = '2021-04-19'

    # Meeting df
    meetings_df = get_core_results(start_date, end_date)

    # check if em,pty
    if meetings_df.empty:

        print('No further action')
        
        #send_no_update_email()

    else:

        # Make the raceDate col datetime
        meetings_df.raceDate = pd.to_datetime(meetings_df.raceDate, dayfirst=True)

        # Otherwise make the new tables
        # Containers
        meta_list = []
        results_list = []

        # Try without rate limit
        for meetId in meetings_df.meetingId.unique():

            _meta, _res = get_race_results_meta(meetId)

            # Append
            meta_list.append(_meta)
            results_list.append(_res)

        race_info_df = pd.concat(meta_list, ignore_index=True)
        trap_results = pd.concat(results_list, ignore_index=True)

        # Race Info
        # Tidy up
        race_info_df.raceGoing = race_info_df.raceGoing.astype('float64')
        race_info_df.raceNumber = race_info_df.raceNumber.astype('int64')
        trap_results.dogId = trap_results.dogId.astype('float64')

        # Add datetime column
        race_info_df['dateTime'] = pd.to_datetime(race_info_df.raceDate + 'T' + race_info_df.raceTime, dayfirst=True)
        race_info_df.raceDate = pd.to_datetime(race_info_df.raceDate, dayfirst=True)

        # Trap results
        trap_results = trap_results.drop('trapHandicap', 1)

        # Details table
        dg_cols = [c for c in trap_results.columns if c.startswith('dog')]

        # reduce
        greyhound_details = trap_results[dg_cols].copy().drop_duplicates()

        # filter trap results
        r_cols = [c for c in trap_results.columns if not c.startswith('dog') or (c == 'dogId')]
        trap_results = trap_results[r_cols].copy()

        # Ensure several columns are float
        fl_cols = [c for c in trap_results.columns if c.startswith('result') and c not in ['resultComment', 'resultBtnDistance']]

        # Ensure we convert empty space to float
        trap_results[fl_cols] = trap_results[fl_cols].applymap(lambda x: np.nan if x == '' else x)
        
        # create a dictionary
        dict_fl = {c: 'float64' for c in fl_cols}

        # change type
        trap_results = trap_results.astype(dict_fl)

        # Ensure float
        trap_results.dogId = trap_results.dogId.astype(float)

        # Get number of races for update email
        races_updated = race_info_df.shape[0]

        #### Save to database
        # write meetings_df to database
        gbq.to_gbq(meetings_df, 'gbgb_db.meeting_table', project_id='greyhound-project', 
                    if_exists='append')

        # Race details
        gbq.to_gbq(race_info_df, 'gbgb_db.race_details', project_id='greyhound-project', 
                    if_exists='append')

        # Trap results
        gbq.to_gbq(trap_results, 'gbgb_db.trap_results', project_id='greyhound-project',
                                  if_exists='append')

        # Greyhound details
        gbq.to_gbq(greyhound_details, 'gbgb_db.stage_dog_detail', project_id='greyhound-project', 
                    if_exists='replace')

        # Merge into table
        ## Merge SQL
        merge_sql = '''
        MERGE gbgb_db.dog_detail orig
        USING gbgb_db.stage_dog_detail staging
        ON
        staging.dogId = orig.dogId
        WHEN MATCHED THEN UPDATE SET
        orig.dogName = staging.dogName,
        orig.dogSeason = staging.dogSeason
        WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            dogBorn,
            dogColour,
            dogDam,
            dogId,
            dogName,
            dogSeason,
            dogSex,
            dogSire)
        VALUES (
            staging.dogBorn,
            staging.dogColour,
            staging.dogDam,
            staging.dogId,
            staging.dogName,
            staging.dogSeason,
            staging.dogSex,
            staging.dogSire)
        '''

        # Perform operation
        client.query(merge_sql)
        
        # send email
        #send_success_email(races_updated)
        
# Master function
def update_trials_db():
    
    '''
    Function to automatically update the Big Query database
    '''
    
    # Start date
    start_date = trial_max_date_finder()

    # End date
    end_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    #end_date = '2021-04-19'

    # Meeting df
    meetings_df = get_core_results(start_date, end_date, race_type='trial')

    # check if em,pty
    if meetings_df.empty:

        print('No further action')
        
        #send_no_update_email()

    else:

        # Make the raceDate col datetime
        meetings_df.raceDate = pd.to_datetime(meetings_df.raceDate, dayfirst=True)

        # Otherwise make the new tables
        # Containers
        meta_list = []
        results_list = []

        # Try without rate limit
        for meetId in meetings_df.meetingId.unique():

            _meta, _res = get_race_results_meta(meetId)

            # Append
            meta_list.append(_meta)
            results_list.append(_res)

        race_info_df = pd.concat(meta_list, ignore_index=True)
        trap_results = pd.concat(results_list, ignore_index=True)

        # Race Info
        # Tidy up
        race_info_df.raceGoing = race_info_df.raceGoing.astype('float64')
        race_info_df.raceNumber = race_info_df.raceNumber.astype('int64')
        trap_results.dogId = trap_results.dogId.astype('float64')

        # Add datetime column
        race_info_df['dateTime'] = pd.to_datetime(race_info_df.raceDate + 'T' + race_info_df.raceTime, dayfirst=True)
        race_info_df.raceDate = pd.to_datetime(race_info_df.raceDate, dayfirst=True)

        # Trap results
        trap_results = trap_results.drop('trapHandicap', 1)

        # Details table
        dg_cols = [c for c in trap_results.columns if c.startswith('dog')]

        # reduce
        greyhound_details = trap_results[dg_cols].copy().drop_duplicates()

        # filter trap results
        r_cols = [c for c in trap_results.columns if not c.startswith('dog') or (c == 'dogId')]
        trap_results = trap_results[r_cols].copy()

        # Ensure several columns are float
        fl_cols = [c for c in trap_results.columns if c.startswith('result') and c not in ['resultComment', 'resultBtnDistance']]

        # Ensure we convert empty space to float
        trap_results[fl_cols] = trap_results[fl_cols].applymap(lambda x: np.nan if x == '' else x)
        
        # create a dictionary
        dict_fl = {c: 'float64' for c in fl_cols}

        # change type
        trap_results = trap_results.astype(dict_fl)

        # Ensure float
        trap_results.dogId = trap_results.dogId.astype(float)

        # Get number of races for update email
        races_updated = race_info_df.shape[0]
        
        # Drop cols
        # define drop cols
        drp_cols = ['raceTitle', 'raceHandicap', 'racePrizes', 'raceForecast', 'raceTricast']
        
        # Now amend table
        race_info_df = race_info_df.drop(drp_cols, 1)
        
        #define
        tr_drp_cols = ['SP', 'resultMarketPos', 'resultMarketCnt', 'resultPriceNumerator', 'resultPriceDenominator']
        
        # Amend table
        trap_results = trap_results.drop(tr_drp_cols, 1)

        #### Save to database
        # write meetings_df to database
        gbq.to_gbq(meetings_df, 'gbgb_db.trial_meeting_table', project_id='greyhound-project', 
                    if_exists='append')

        # Race details
        gbq.to_gbq(race_info_df, 'gbgb_db.trial_race_details', project_id='greyhound-project', 
                    if_exists='append')

        # Trap results
        gbq.to_gbq(trap_results, 'gbgb_db.trial_trap_results', project_id='greyhound-project',
                                  if_exists='append')

        # Greyhound details
        gbq.to_gbq(greyhound_details, 'gbgb_db.trial_stage_dog_detail', project_id='greyhound-project', 
                    if_exists='replace')

        # Merge into table
        ## Merge SQL
        merge_sql = '''
        MERGE gbgb_db.trial_dog_detail orig
        USING gbgb_db.trial_stage_dog_detail staging
        ON
          staging.dogId = orig.dogId
        WHEN MATCHED THEN UPDATE SET
        orig.dogName = staging.dogName,
        orig.dogSeason = staging.dogSeason
        WHEN NOT MATCHED BY TARGET THEN
          INSERT (
            dogBorn,
            dogColour,
            dogDam,
            dogId,
            dogName,
            dogSeason,
            dogSex,
            dogSire)
          VALUES (
            staging.dogBorn,
            staging.dogColour,
            staging.dogDam,
            staging.dogId,
            staging.dogName,
            staging.dogSeason,
            staging.dogSex,
            staging.dogSire)
        '''

        # Perform operation
        client.query(merge_sql)
        
        # send email
        #send_success_email(races_updated)

def update_names_db():

    # Merge into table
    ## Merge SQL
    merge_sql = '''
    MERGE gbgb_db.name_lookup orig
    USING gbgb_db.stage_dog_detail staging
    ON
    staging.dogName = orig.dogName
    WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        dogId,
        dogName)
    VALUES (
        staging.dogId,
        staging.dogName
        )
    '''

    # Perform operation
    client.query(merge_sql)   

    ## Merge SQL
    merge_sql = '''
    MERGE gbgb_db.name_lookup orig
    USING gbgb_db.trial_stage_dog_detail staging
    ON
    staging.dogName = orig.dogName
    WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        dogId,
        dogName)
    VALUES (
        staging.dogId,
        staging.dogName
        )
    '''

    # Perform operation
    client.query(merge_sql)     
        
def update_greyhound_db(data, context):
    
    # perform functions
    update_races_db()
    update_trials_db()
    update_names_db()

if __name__ == '__main__':
    update_greyhound_db('data', 'context')