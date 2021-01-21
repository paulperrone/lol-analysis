# Housekeeping
import requests
import datetime
import pandas as pd
import io
import os


def download(directory, years, delete=True):
    """
    Parameters
    ----------
    directory : str
        A string containing the filepath to the working directory.
        (e.g. 'C:\\Users\\ProjektStation\\Documents\\OraclesElixir\\')
    year : str or list
        A string or list of strings containing years (e.g. ["2019", "2020"])
    delete : boolean
        A boolean (True/False) value. 
        If True, will delete files in directory upon download of new data.

    Returns
    -------
    A Pandas dataframe containing the most recent Oracle's Elixir data 
    for the years provided by the year parameter.
    A .csv file in the directory, with the most recent data, if downloaded.
    """
    # Defining Time Variables
    current_date = datetime.date.today()
    today = current_date.strftime('%Y%m%d')
    yesterday = current_date - datetime.timedelta(days = 1)
    yesterday = yesterday.strftime('%Y%m%d')
    
    # Other Variables
    url = ('https://oracleselixir-downloadable-match-data.'
        's3-us-west-2.amazonaws.com/')
    oe_data = pd.DataFrame()
    
    # Conditional Handling For Years
    if isinstance(years, str):
        years = [years]
    
    # Dynamic Data Import
    for year in years:
        file = f'{year}_LoL_esports_match_data_from_OraclesElixir_'
        current_files = [x for x in os.listdir() if x.startswith(file)]
        
        if delete and f'{file}{today}.csv' not in current_files:
             # If today's data not in Dir, optionally delete old versions
            for f in current_files:
                os.remove(f)
                
            try:
                # Try To Grab File For Current Date
                filepath = f'{url}{file}{today}.csv'
                
                r = requests.get(filepath, allow_redirects=True)
                data = r.content.decode('utf8')
                data = pd.read_csv(io.StringIO(data))
                data.to_csv(f'{directory}{file}{today}.csv', index=False)
                
            except:
                # Grab Yesterday's Data If Today's Does Not Exist
                filepath = f'{url}{file}{yesterday}.csv'
            
                r = requests.get(filepath, allow_redirects=True)
                data = r.content.decode('utf8')
                data = pd.read_csv(io.StringIO(data))
                data.to_csv(f'{directory}{file}{yesterday}.csv', index=False)

# Update to read only the most recent file for a single year
def read(directory):
    oe_data = pd.DataFrame()
    for csv in os.listdir(directory):
        data = pd.read_csv(f'{directory}{csv}')
        oe_data = pd.concat([oe_data, data], axis=0)
    return oe_data

def clean(oe_data, split_on, keep_identities, keep_leagues, keep_columns):
    """
    Parameters
    ----------
    oe_data : DataFrame
        A Pandas data frame containing Oracle's Elixir data.
    split_on : 'team', 'player' or None 
        Subset data for Team data or Player data. None for all data.
    keep_identities : list or None
        A list of strings of players or teams to keep in the data.
        Names provided must be an exact match.
        Does nothing if split_on is None.
    keep_leagues : list or None
        A list of strings of leagues to keep in the data.
        Names provided must be an exact match.
    keep_columns : list or None
        A list of strings for columns in the data to keep. 
        Names provided must be an exact match.

    Returns
    -------
    A Pandas dataframe of formatted, subset Oracle's Elixir data matching 
    the parameters provided above. 
    The date column will be formatted appropriately as a datetime object.
    Only games with datacompletness = complete will be kept.
    Any games with 'unknown team' or 'unknown player' will be dropped. 
    Any games with null game ids will be dropped.
    """
    oe_data['date'] = pd.to_datetime(oe_data['date'], format='%Y/%m/%d %H:%M:%S')
    oe_data = oe_data[oe_data['datacompleteness'] == 'complete']
    if keep_columns:
        oe_data = oe_data[keep_columns]
    if keep_leagues:
        oe_data = oe_data[oe_data.league.isin(keep_leagues)]
        
    oe_data['gameid'] = oe_data['gameid'].str.strip()
    oe_data = oe_data[oe_data['gameid'].notna()]
    
    if split_on == 'team':
        oe_data = oe_data[oe_data['position'] == 'team']
        if keep_identities:
            oe_data = oe_data[oe_data.team.isin(keep_identities)]
        
        # Drop Games With "Unknown Team" Lookup Failures
        dropgames = oe_data[oe_data['team'] == 'unknown team']
        dropgames = dropgames['gameid'].unique()
        oe_data = oe_data[~oe_data.gameid.isin(dropgames)]
        
    elif split_on == 'player':
        oe_data = oe_data[oe_data['position'] != 'team']
        if keep_identities:
            oe_data = oe_data[oe_data.player.isin(keep_identities)]
        
        # Drop Games With "Unknown Player" Lookup Failures
        dropgames = oe_data[oe_data['player'] == 'unknown player']
        dropgames = dropgames['gameid'].unique()
        oe_data = oe_data[~oe_data.gameid.isin(dropgames)]
        
    return oe_data