"""
script for deduping tender data.

Works as follows:

First checks to see if a settings file exists (which stores the deduping model).

IF settings file exists:

1) loads deduper from settings file.
2) creates a table for storing dedupe results.
3) pulls data to be deduped from database.
2) dedupes data using the loaded settings.
3) Adds the deduped data to table.

IF settings file doesn't exist:

1) pulls data from database to use to train a new deduping model
2) uses user-inputted training data to train and save a new deduping model (i.e. a new settings file)

Only outputs the clustered records (i.e. doesn't show records that didn't end up in a cluster)
relies on the "id" field being the first field read
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


# settings and training files
# if settings file does not exist then new model will be trained
settings_file = 'tender_settings_new_structure'
training_file = 'tender_training_new_structure.json'

# select where to get the data for deduping from, the country for deduping, and the "enddate" ranges
select_source = "ocds.ocds_tenders_view"
select_country = "United Kingdom"
select_date_ranges = [['2017-01-01', '2017-01-10'],
                      ['2017-01-11', '2017-01-21']]

# specify fields to pull from the tenders table (make sure to pull all the fields you need for deduping later)
input_fields = ["id",
                "title",
                "value",
                "description",
                "buyer",
                "postcode",
                "email"
                ]

# column names that you want in the results table (make sure to have the cluster_id field first)
output_fields = ["cluster_id"] + input_fields + ["startdate", "enddate"]

# name of results table
results_table = "ocds_tenders_deduped_new"

# settings for pulling data to train the deduper
training_source = select_source
training_country = select_country
training_date_range = ['2017-01-01', '2017-01-10']

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

def debugger_setup():
    optp = optparse.OptionParser()
    optp.add_option('-v', '--verbose', dest='verbose', action='count',
                    help='Increase verbosity (specify multiple times for more)'
                    )
    (opts, args) = optp.parse_args()
    log_level = logging.WARNING
    if opts.verbose == 1:
        log_level = logging.INFO
    elif opts.verbose >= 2:
        log_level = logging.DEBUG
    logging.getLogger().setLevel(log_level)
    return

def construct_query(fields, source, date_range, country):
    """create query for pulling data from db"""

    query = "select {} from {} where countryname = '{}' and enddate between '{}' and '{}'".format(", ".join(fields), source, country, date_range[0], date_range[1])
    return query

def preProcess(key, column):
    """takes in the key, value pair from data_select - then processes them for deduping later"""
    try:  # python 2/3 string differences
        column = column.decode('utf8')
    except AttributeError:
        pass
    if not isinstance(column, int):
        if not column:
            column = None
        else:
            # get rid of spaces/newlines
            column = unidecode(column)
            column = re.sub('  +', ' ', column)
            column = re.sub('\n', ' ', column)
            column = column.strip().strip('"').strip("'").lower().strip()
    # if the we're looking at value, attempt to turn column into float for price deduping,
    # if that doesn't work (because the column contains e.g. rogue characters) then set the value column to None
    if key == 'value' and column:
        try:
            column = float(column)
        except ValueError:
            column = None
    return column

def fetch_data(query):
    """ retrieve data from the db using query"""

    print ('connecting to db...')
    con = psy.connect(host=host_remote, dbname=dbname_remote, user=user_remote, password=password_remote)
    c = con.cursor(cursor_factory=psy.extras.RealDictCursor)

    print ('importing data...')
    c.execute(query)
    data = c.fetchall() # returns list, each item in list being a dictionary of the corresponding row returned from query

    con.close()

    return data

def clean_data(data):
    """clean data so it is ready for deduping"""

    data_d = {} # this is the dictionary I will use for deduping.
    # data_d is actually a dictionary of dictionaries. The keys are ids, with the values being another dictionary (containing fields and values)

    print("cleaning data...")
    for row in data:
        # clean each row in data using the preprocess function
        clean_row = [(k, preProcess(k, v)) for (k, v) in row.items()]
        row_id = int(row['id'])
        data_d[row_id] = dict(clean_row)

    return data_d

def collect_labelled_data(data_d):
    """collects labelled data, returns the deduper"""
    fields = [
        {'field': 'title', 'type': 'String'},
        {'field': 'value', 'type': 'Price'},
        #{'field': 'description', 'type': 'Text'},
        {'field': 'buyer', 'type': 'String'},
        {'field': 'postcode', 'type': 'String'},
        {'field': 'email', 'type': 'String'}
    ]

    deduper = dedupe.Dedupe(fields)

    deduper.sample(data_d, 75000)

    # check if a training file exists. If it does load it.
    if os.path.exists(training_file):
        print ('reading labeled examples from ', training_file)
        with open(training_file) as tf:
            deduper.readTraining(tf)

    # active labelling phase
    print ('starting active labeling...')
    dedupe.consoleLabel(deduper)

    # train the deduper
    print("training deduper...")
    deduper.train()

    # save out the training data
    print ("saving out training file...")
    with open(training_file, 'w') as tf:
        deduper.writeTraining(tf)

    # save out the settings
    print ("saving out settings file...")
    with open(settings_file, 'w') as sf:
        deduper.writeSettings(sf)

    return

def custom_dedupe(deduper, data_d):
    """dedupes the dictionary data_d using deduper. Returns the clustered dupes"""
    print ('blocking...')
    threshold = deduper.threshold(data_d, recall_weight=2)

    print ('clustering...')
    clustered_dupes = deduper.match(data_d, threshold) # returns tuples

    # return number of duplicated sets
    print ('# duplicate sets', len(clustered_dupes))

    return clustered_dupes

def create_table(table_name, column_names):
    """creates table for holding the deduped data"""

    print ('connecting to db...')
    con = psy.connect(host=host_remote, dbname=dbname_remote, user=user_remote, password=password_remote)
    c = con.cursor()

    print ('creating results table {}...'.format(table_name))
    c.execute('DROP TABLE IF EXISTS ocds.{}'.format(table_name))  # get rid of the table (so we can make a new one)
    field_string = ','.join('%s varchar(500000)' % name for name in column_names)
    c.execute('CREATE TABLE ocds.{} ({})'.format(table_name, field_string))
    con.commit()

    con.close()
    return

def add_data_to_table(table_name, query, column_names, clustered_dupes, clusters_index_start=0):
    """adds the deduped data to table"""

    # start connection
    con = psy.connect(host=host_remote, dbname=dbname_remote, user=user_remote, password=password_remote)
    c = con.cursor()

    # get data from db that we will mix with
    c.execute(query)
    data = c.fetchall()

    con.close()

    full_data = []  # list to be inserted into the deduped table

    cluster_membership = collections.defaultdict(lambda: 'x')  # This variable doesn't seem to be used anywhere else?
    for cluster_id, (cluster, score) in enumerate(clustered_dupes, clusters_index_start):  # cycle through the clustered dupes, start clusters_id where the last one left off
        for record_id in cluster:
            for row in data:  # data is a list, but isn't each row a dict? NO because data was redefined above
                # so I think this assumes that the the first value in each row from the table is an id
                # so if the row id matches the record_id from the clustered_dupes...
                if record_id == int(row[0]):
                    row = list(row)  # turn the tuple into a list
                    row.insert(0, cluster_id)  # put the cluster_id at the beginning of the list
                    row = tuple(row)  # make it a tuple again
                    full_data.append(row)  # add the new row to a new list called fulldata

    # Input the data into the dedupe table
    # make new connection
    con2 = psy.connect(host=host_remote, dbname=dbname_remote, user=user_remote, password=password_remote)
    c2 = con2.cursor()

    num_cols = len(column_names)
    mog = "(" + ("%s," * (num_cols - 1)) + "%s)"
    args_str = ','.join(c2.mogrify(mog, x) for x in full_data)  # mogrify is used to make query strings
    values = "(" + ','.join(x for x in column_names) + ")"
    c2.execute("INSERT INTO ocds.{} {} VALUES {}".format(table_name, values, args_str))
    con2.commit()
    con2.close()

    return



if __name__ == '__main__':

    start_time = time.time()
    debugger_setup()

    # if settings file exists
    if os.path.exists(settings_file):
        # load from settings file
        print ('reading from', settings_file)
        with open(settings_file) as sf:
            deduper = dedupe.StaticDedupe(sf)

        # construct results table
        create_table(results_table, output_fields)
        # start the cluster_id at 0
        current_index = 0

        # loop over different dates
        for date_range in select_date_ranges:

            # construct the query for getting data to be deduped
            select_query = construct_query(input_fields, select_source, date_range, select_country)

            # get data from the database using the query
            selected_data = fetch_data(select_query)

            # clean the data
            cleaned_data = clean_data(selected_data)

            # proceed to deduping:
            clusters = custom_dedupe(deduper, cleaned_data)

            # add the results to the table
            # make query to get the rest of the data we want in the table
            output_query = construct_query(output_fields[1:], select_source, date_range, select_country)
            add_data_to_table(results_table, output_query, output_fields, clusters, clusters_index_start=current_index)

            # update the index for clusters_id (for the next set of clusters to be added to the results table)
            current_index = len(clusters)

    # if the settings file doesn't exist, this means we need to train a new deduping model
    else:
        # import data for training
        select_query = construct_query(input_fields, training_source, training_date_range, training_country)
        training_data = fetch_data(select_query)
        cleaned_training_data = clean_data(training_data)

        # get labelled training examples and train a new model
        collect_labelled_data(cleaned_training_data)
        print("training complete. Run script again to begin deduping with the newly trained model")


    print ('ran in', time.time() - start_time, 'seconds')