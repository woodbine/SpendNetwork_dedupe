"""
Script for deduping a single postgres table. Set up to dedupe usm3
Follows the smae pipleine as pgsql_tenders (by and large). See that script for further details
"""

import dedupe
import os
import re
import collections
import time
import logging
import optparse
from dotenv import load_dotenv, find_dotenv
import psycopg2 as psy
import psycopg2.extras
from unidecode import unidecode
from pgsql_tenders import fetch_data, clean_data, custom_dedupe, debugger_setup, add_data_to_table, collect_labelled_data

# local database details (to use when testing)
dbname = "postgres"
user = "postgres"
password = "postgres"
host = "host"

# get the remote database details from .env
load_dotenv(find_dotenv())
host_remote = os.environ.get("HOST_REMOTE")
dbname_remote = os.environ.get("DBNAME_REMOTE")
user_remote = os.environ.get("USER_REMOTE")
password_remote = os.environ.get("PASSWORD_REMOTE")



def construct_usm3_query(fields, alphabetic_filter):
    """create query for pulling unmatched data from usm3"""

    query = "SELECT {} FROM blue.usm3 WHERE (sss LIKE '{}') AND (sid IS NULL)".format( ", ".join(fields), alphabetic_filter )
    return query

def create_usm3_results_table(table_name, table_schema):
    """
    creates a copy of usm3 but with an additional cluster_id column, for storing the dedupe results.
    Returns the column names from the new table.
    """
    print ('connecting to db...')
    con = psy.connect(host=host_remote, dbname=dbname_remote, user=user_remote, password=password_remote)
    c = con.cursor()

    # retrieve the column names to have in the dedupe table
    columns = "SELECT column_name FROM information_schema.columns WHERE table_name = 'usm3' AND table_schema = 'blue'"
    c.execute(columns)
    column_names = c.fetchall()
    column_names = [x[0] for x in column_names]
    column_names.insert(0, 'cluster_id')  # add cluster_id to the column names

    print ('creating results table {}...'.format(table_name))
    c.execute('DROP TABLE IF EXISTS {}.{}'.format(table_schema, table_name))
    field_string = ','.join('%s varchar(500)' % name for name in column_names)
    c.execute('CREATE TABLE {}.{} ({})'.format(table_schema, table_name, field_string))
    con.commit()

    con.close()
    return column_names


if __name__ == '__main__':

    # settings and training files
    # if settings file does not exist then new model will be trained
    usm3_settings_file = 'usm3_10k_learned_settings'
    usm3_training_file = 'usm3_10k_example_training.json'

    # select alphabetic segment of table you want to dedupe
    input_alphabetic_filter = "AB%"

    # specify fields to pull from the tenders table (make sure to pull all the fields you need for deduping later)
    input_fields = ["id",
                    "sss"
                    ]

    # name of results table and schema it's stored in
    results_schema = "blue"
    results_table = "usm3_deduped"

    # settings for pulling data to train the deduper
    training_alphabetic_filter = "AB%"

    # dedupe training fields
    usm3_fields = [
        {'field': 'sss', 'type': 'String'}
    ]

    start_time = time.time()
    debugger_setup()

    if os.path.exists(usm3_settings_file):
        # load from settings file
        print ('reading from', usm3_settings_file)
        with open(usm3_settings_file) as sf:
            deduper = dedupe.StaticDedupe(sf)

        # construct results table
        output_fields = create_usm3_results_table(results_table, results_schema)

        # construct the query for getting the data to be deduped
        select_query = construct_usm3_query(input_fields, input_alphabetic_filter)

        # get the data from the database using the query
        selected_data = fetch_data(select_query)

        #clean the data
        cleaned_data = clean_data(selected_data)

        # proceed to deduping:
        clusters = custom_dedupe(deduper, cleaned_data)

        # add the results to the table
        # make query to get the rest of the data we want in the table
        output_query = "SELECT * FROM blue.usm3 WHERE (sss LIKE '{}') AND (sid IS NULL)".format(input_alphabetic_filter)

        add_data_to_table(results_table, results_schema, output_query, output_fields, clusters, id_index=10)

    else:
        print("no settings file found. Importing data for training...")
        # import data for training
        select_query = construct_usm3_query(input_fields, training_alphabetic_filter)
        training_data = fetch_data(select_query)
        cleaned_training_data = clean_data(training_data)

        # get labelled training examples and train a new model
        collect_labelled_data(cleaned_training_data, usm3_fields, usm3_training_file, usm3_settings_file)
        print("training complete. Run script again to begin deduping with the newly trained model")

    print ('ran in', time.time() - start_time, 'seconds')