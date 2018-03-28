"""
script for deduping tender data.

Works as follows

1) Pulls data from the tenders table/view
2) Looks for training data/settings, if none found then asks for training data and trains model
3) Dedupes the data using that model/settings
4) Uploads deduped table to database

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
settings_file = 'tender_settings_new_structure'
training_file = 'tender_training_new_structure.json'

# select the country for deduping, and the "enddate" range
search_source = "ocds.ocds_tenders_view"
search_country = "United Kingdom"
search_date_range = ['2017-01-01', '2017-01-10']

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


# specify fields to pull from the tenders table (make sure to pull all the fields you need for deduping later)
input_fields = ["id",
                "title",
                "value",
                "description",
                "buyer",
                "postcode",
                "email"
                ]

# column names that you want in the results table
# make sure to have the cluster_id field first
output_fields = ["cluster_id"] + input_fields + ["startdate", "enddate"]

# name of results table
results_table = "ocds_tenders_deduped_NEW"

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

    return deduper

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

def add_data_to_table(table_name, query, column_names, clustered_dupes):
    """adds the deduped data to table"""

    print ('connecting to db...')
    con = psy.connect(host=host_remote, dbname=dbname_remote, user=user_remote, password=password_remote)
    c = con.cursor()

    # get data from db that we will mix with
    c.execute(query)
    data = c.fetchall()

    con.close()

    full_data = []  # list to be inserted into the deduped table

    cluster_membership = collections.defaultdict(lambda: 'x')  # This variable doesn't seem to be used anywhere else?
    for cluster_id, (cluster, score) in enumerate(clustered_dupes):  # cycle through the clustered dupes
        for record_id in cluster:
            for row in data:  # data is a list, but isn't each row a dict? NO because data was redefined above
                # so I think this assumes that the the first value in each row from the table is an id
                # so if the row id matches the record_id from the clustered_dupes...
                # this next section pulls the
                if record_id == int(row[0]):
                    row = list(row)  # turn the tuple into a list
                    row.insert(0, cluster_id)  # put the cluster_id at the beginning of the list
                    row = tuple(row)  # make it a tuple again
                    full_data.append(row)  # add the new row to a new list called fulldata

    # Input the data into the dedupe table
    # make new connection (not sure I need this?)
    con3 = psy.connect(host=host_remote, dbname=dbname_remote, user=user_remote, password=password_remote)
    c3 = con3.cursor()

    num_cols = len(column_names)
    mog = "(" + ("%s," * (num_cols - 1)) + "%s)"
    args_str = ','.join(c3.mogrify(mog, x) for x in full_data)  # mogrify is used to make query strings
    values = "(" + ','.join(x for x in column_names) + ")"
    c3.execute("INSERT INTO ocds.{} {} VALUES {}".format(table_name, values, args_str))
    con3.commit()
    con3.close()


#
#
#
#
#
#
# # BEGIN CONSTRUCTING THE TABLE TO STORE THE DEDUPED DATA
# # connect to ocds again to get the data and to get the columns to make the deduped table
# con2 = psy.connect(host=host_remote, dbname=dbname_remote, user=user_remote, password=password_remote)
# c2 = con2.cursor()
#
# # use restricted number of columns for now, to avoid the JSON problem
# # c2.execute("SELECT id, source, source_id, ocid, language, title FROM ocds.ocds_tenders_view where countryname = 'United Kingdom' limit 500")
#
# # use the SELECT_DATA for now, get the data which will ultimately be put into the _deduped table (along with the cluster ids)
# input_fields += ["startdate", "enddate"]
# DATA_SELECT2 = "select {} from ocds.ocds_tenders_view where countryname = '{}' and enddate between '{}' and '{}'".format(", ".join(input_fields), country, date_range_start, date_range_end)
#
# c2.execute(DATA_SELECT2)
# data = c2.fetchall() # returns a list of tuples
#
# full_data = [] # list to be inserted into the deduped table
#
# cluster_membership = collections.defaultdict(lambda: 'x') # This variable doesn't seem to be used anywhere else?
# for cluster_id, (cluster, score) in enumerate(clustered_dupes): # cycle through the clustered dupes
#     for record_id in cluster:
#         for row in data: # data is a list, but isn't each row a dict? NO because data was redefined above
#             # so I think this assumes that the the first value in each row from the table is an id
#             # so if the row id matches the record_id from the clustered_dupes...
#             # this next section pulls the
#             if record_id == int(row[0]):
#                 row = list(row) # turn the tuple into a list
#                 row.insert(0, cluster_id) # put the cluster_id at the beginning of the list
#                 row = tuple(row) # make it a tuple again
#                 full_data.append(row) # add the new row to a new list called fulldata
#
# # specify column names to have in the deduped table, for now use same ones as the input fields
# column_names = input_fields
#
# # NOTE: having problemw wuth code below because the json format is interfering with the column name retrieval for some reason...
#
# # columns = "SELECT column_name FROM information_schema.columns WHERE table_name = 'ocds_tenders_view'"
# # c2.execute(columns)
# # column_names = c2.fetchall()
# # column_names = [x[0] for x in column_names]
# # # for now, only use the first seven columns
# # column_names = column_names[:7]
# column_names.insert(0, 'cluster_id') # add cluster_id to the column names
#
# # close con2
# con2.close()
#
# # new connection to make the table for the deduped data
# con3 = psy.connect(host=host_remote, dbname=dbname_remote, user=user_remote, password=password_remote)
# c3 = con3.cursor()
#
# # create the dedupe table
# c3.execute('DROP TABLE IF EXISTS ocds.ocds_tenders_deduped8') # get rid of the table (so we can make a new one)
# field_string = ','.join('%s varchar(500000)' % name for name in column_names)
# c3.execute('CREATE TABLE ocds.ocds_tenders_deduped8 (%s)' % field_string)
# con3.commit()
#
# # Input the data into the dedupe table
# num_cols = len(column_names)
# mog = "(" + ("%s," * (num_cols - 1)) + "%s)"
# args_str = ','.join(c3.mogrify(mog, x) for x in full_data) # mogrify is used to make query strings
# values = "(" + ','.join(x for x in column_names) + ")"
# c3.execute("INSERT INTO ocds.ocds_tenders_deduped8 %s VALUES %s" % (values, args_str))
# con3.commit()
# con3.close()
#
# print ('ran in', time.time() - start_time, 'seconds')

if __name__ == '__main__':

    start_time = time.time()
    debugger_setup()

    # construct the query for getting data to be deduped
    select_query = construct_query(input_fields, search_source, search_date_range, search_country)

    # get data
    select_data = fetch_data(select_query)

    # clean the data
    select_dict = clean_data(select_data)

    # get the deduper it it exists
    if os.path.exists(settings_file):
        # load from settings file if that's possible
        print ('reading from', settings_file)
        with open(settings_file) as sf:
            deduper = dedupe.StaticDedupe(sf)

    # otherwise collect labelled data and train it
    else:
        deduper = collect_labelled_data(select_dict)

    # now we have deduper, proceed to deduping:
    clusters = custom_dedupe(deduper, select_dict)

    # construct results table
    create_table(results_table, output_fields)

    # add the results to the table
    output_query = construct_query(output_fields[1:], search_source, search_date_range, search_country)
    add_data_to_table(results_table, output_query, output_fields, clusters)

    print ('ran in', time.time() - start_time, 'seconds')