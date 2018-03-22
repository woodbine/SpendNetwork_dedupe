"""
Script for deduping a single postgres table. Set up to dedupe usm3
Works as follows

1) Pulls data from the tenders table/view
2) Looks for training data/settings, if none found then asks for training data and trains model
3) Dedupes the data using that model/settings
4) Uploads deduped table to database

Only outputs the clustered records (i.e. doesn't show records that didn't end up in a cluster)
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

# right now: only outputs the clustered records (i.e. doesn't show records that didn't end up in the cluster)
# NOTE: relies on the "id" field being in a specific place in order to match (see line 135)
# haven't yet put score in after it was causing some bugs

# parser for debugging
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

# settings and training files
settings_file = 'usm3_10k_learned_settings'
training_file = 'usm3_10k_example_training.json'

# select alphabetic segment of table you want to dedupe
alphabetic_filter = "AB%"

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

start_time = time.time()

# connect to remote database
con = psy.connect(host=host_remote, dbname=dbname_remote, user=user_remote, password=password_remote)
c = con.cursor(cursor_factory=psy.extras.RealDictCursor)

# specify fields to pull from the tenders table (make sure to pull all the fields you need for deduping later)
input_fields = ["id",
                "sss"
                ]

# create query for pulling data from usm3. Pull the unmatched records (with no sid)
DATA_SELECT = "SELECT {} FROM blue.usm3 WHERE (sss LIKE '{}') AND (sid IS NULL)".format( ", ".join(input_fields), alphabetic_filter ) # select id to use as record_id for deduping


def preProcess(column):
    # takes in the key, value pair from data_select - then processes them for deduping later
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
    return column


print 'importing data ...'
c.execute(DATA_SELECT) # returns list, each item in list being a dictionary of the corresponding row
data = c.fetchall()# returns list, each item in list being a dictionary of the corresponding row
data_d = {} # this is the dictionary I will use for deduping.
# data_d is actually a dictionary of dictionaries. The keys are ids, with the values being another dictionary (containing fields and values)

for row in data:
    # clean each row in data using the preprocess function
    clean_row = [(k, preProcess(v)) for (k, v) in row.items()]
    row_id = int(row['id'])
    data_d[row_id] = dict(clean_row)

# close conection
con.close()


# check if settings_file exists and if it does then read the dedupe settings
if os.path.exists(settings_file):
    print 'reading from', settings_file
    with open(settings_file) as sf:
        deduper = dedupe.StaticDedupe(sf)

# otherwise, time to train a new dedupe model
else:
    fields = [
        {'field': 'sss', 'type': 'String'}
    ]

    deduper = dedupe.Dedupe(fields)

    deduper.sample(data_d, 150000)

    # check if a training file exists. If it does load it.
    if os.path.exists(training_file):
        print 'reading labeled examples from ', training_file
        with open(training_file) as tf:
            deduper.readTraining(tf)

    # active labelling phase
    print 'starting active labeling...'

    dedupe.consoleLabel(deduper)

    # train the deduper using the training data (this part is the most time consuming I think)
    deduper.train()

    # save out training and settings files
    with open(training_file, 'w') as tf:
        deduper.writeTraining(tf)

    with open(settings_file, 'w') as sf:
        deduper.writeSettings(sf)

print 'blocking...'
threshold = deduper.threshold(data_d, recall_weight=2)

print 'clustering...'
clustered_dupes = deduper.match(data_d, threshold) # returns tuples

# return number of duplicated sets
print '# duplicate sets', len(clustered_dupes)

# BEGIN CONSTRUCTING THE TABLE TO STORE THE DEDUPED DATA
# connect to ocds again to get the data and to get the columns to make the deduped table
con2 = psy.connect(host=host_remote, dbname=dbname_remote, user=user_remote, password=password_remote)
c2 = con2.cursor()

# get the data which will ultimately be put into the _deduped table (along with the cluster ids)
c2.execute("SELECT * FROM blue.usm3 WHERE (sss LIKE '{}') AND (sid IS NULL)".format(alphabetic_filter))
data = c2.fetchall() # returns a list of tuples

full_data = [] # list to be inserted into the deduped table

cluster_membership = collections.defaultdict(lambda: 'x') # This variable doesn't seem to be used anywhere else?
for cluster_id, (cluster, score) in enumerate(clustered_dupes): # cycle through the clustered dupes
    for record_id in cluster:
        for row in data: # data is a list, but isn't each row a dict? NO because data was redefined above
            # so I think this assumes that the the first value in each row from the table is an id
            # so if the row id matches the record_id from the clustered_dupes...
            if record_id == int(row[10]): # note: row[10] is the id field
                row = list(row) # turn the tuple into a list
                row.insert(0, cluster_id) # put the cluster_id at the beginning of the list
                row = tuple(row) # make it a tuple again
                full_data.append(row) # add the new row to a new list

# retrieve column names to have in the deduped table
columns = "SELECT column_name FROM information_schema.columns WHERE table_name = 'usm3' AND table_schema = 'blue'"
c2.execute(columns)
column_names = c2.fetchall()
column_names = [x[0] for x in column_names]
column_names.insert(0, 'cluster_id') # add cluster_id to the column names

# create the dedupe table
c2.execute('DROP TABLE IF EXISTS blue.usm3_deduped') # get rid of the table (so we can make a new one)
field_string = ','.join('%s varchar(500)' % name for name in column_names) # maybe improve the data types...
c2.execute('CREATE TABLE blue.usm3_deduped (%s)' % field_string)
con2.commit()

# Input the data into the dedupe table
num_cols = len(column_names)
mog = "(" + ("%s," * (num_cols - 1)) + "%s)"
args_str = ','.join(c2.mogrify(mog, x) for x in full_data) # mogrify is used to make query strings
values = "(" + ','.join(x for x in column_names) + ")"
c2.execute("INSERT INTO blue.usm3_deduped %s VALUES %s" % (values, args_str))
con2.commit()
con2.close()

print 'ran in', time.time() - start_time, 'seconds'