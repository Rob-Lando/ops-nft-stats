import requests
import json
import pandas as pd
import numpy as np
import seaborn as sns
import plotly.express as px
from pprint import pprint as pp
from datetime import datetime
import sqlite3

pd.options.display.max_columns = 100


timestamp = datetime.now()

def get_collection_stats(url_base,collection_names,endpoint,timestamp):

    """
    Function to pull real time NFT collection statistics from OpenSea api.

    Returns Pandas dataframe object with relevant NFT collection stats.

    Parameters:
        - url_base (str): base url for OpenSea API
        - collection_names (dict): dictionary of NFT collection names of interest
        - endpoint (str): endpoint from OpenSea API to query

            * Query url = url_base/collection_name/endpoint
    """

    timestamp = datetime.now()

    headers = {"accept": "application/json"}

    df = pd.DataFrame(

        {
            'collection':collection_names.values(),
            'response':[f"{url_base}/{collection_name}/{endpoint}" for collection_name in collection_names.values()]
        }
        
    )
    
    # get data via .map()
    df.loc[:,'response'] = [pd.DataFrame(json.loads(requests.get(i, headers=headers).text)).reset_index() for i in df.loc[:,'response']]
    # add timestamp column
    df.loc[:,'response'] = df.loc[:,'response'].map(lambda x: add_static_cols(x,{'timestamp':timestamp}))
    # pivot 
    df.loc[:,'response'] = df.loc[:,'response'].map(lambda x: x.pivot(index='timestamp', columns='index', values='stats').reset_index())


    stack = pd.concat(df.response.to_list()).reset_index(drop = True)
    stack.insert(1, "collection", df['collection'])
    

    return stack

#######################################################################################################################################################    

def add_static_cols(df,col_val_pairs):

    """ 
    Adds constant column to Pandas dataframe object.

    Returns Pandas dataframe object.

    Paramters:
        - df (dataframe): Dataframe object to modify
        - col_val_pairs (dict): dict where key values are intended static column names 
                                and values are the intened static values. 
    
    """

    for key,value in col_val_pairs.items():

        df[key] = value
    
    return df

#######################################################################################################################################################

def write_to_sqlite_db(df,db_path):

    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    fields = ",".join([f"\n\t{i} text" for i in df.columns])

    ddl = f"""CREATE TABLE IF NOT EXISTS open_sea_stats \n({fields}\n)"""

    c.execute(ddl)

    df.to_sql('open_sea_stats', conn, if_exists='append', index=False)

    conn.close()

######################################################################################################################################################

def truncate_sqlite_table(db_path,table_name):

    conn = sqlite3.connect(rf"{db_path}")

    c = conn.cursor()

    c.execute(f"DELETE FROM {table_name}")

    conn.commit()
    conn.close()

######################################################################################################################################################



base = "https://api.opensea.io/api/v1/collection"

collections = dict(enumerate([
                            'boredapeyachtclub',            'mutant-ape-yacht-club',
                            'cryptopunks',                  'meebits',
                            'proof-moonbirds',              'clonex',
                            'azuki',                        'doodles-official',
                            'cool-cats-nft',                'world-of-women-nft',
                            'murakami-flowers-seed',        'goblintownwtf',
                            'mfers',                        'onchainmonkey',
                            'cyberbrokers',                 'hashmasks',
                            'otherdeed',                    'sandbox',
                            'decentraland',                 'kaiju-kingz',
                            'deadfellaz',                   'veefriends',
                            'chromie-squiggle-by-snowfro',  'fidenza-by-tyler-hobbs',
                            'ringers-by-dmitri-cherniak',   'memories-of-qilin-by-emily-xie',
                            ]))

endpoint = 'stats' # OpenSea API Endpoint to query from 

local_db_name = 'open_sea_collection_stats'

write_to_sqlite_db(
    df = get_collection_stats(base,collections,endpoint,timestamp = timestamp),
    db_path = rf"C:\sqlite_dbs\{local_db_name}.db"
    )
print(datetime.now() - timestamp)
