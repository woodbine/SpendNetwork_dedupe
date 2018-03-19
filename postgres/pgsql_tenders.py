"""
script for pulling tender data, deduping to find duplicates within the data, and then uploading the clusters to a (newly created) table called
deduped_table_TENDERS

NOTE: remember to specify where the deduped table is generated

Currently only outputs the clustered records (i.e. doesn't show records that didn't end up in a cluster)
relies on the "id" field being the first field read
haven't yet put score in after it was causing some bugs
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
settings_file = 'tender_settings_v2'
training_file = 'tender_training_v2.json'

# select the country for deduping, and the "enddate" range
country = "United Kingdom"
date_range_start ='2017-01-01'
date_range_end = '2017-01-10'

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

con = psy.connect(host=host_remote, dbname=dbname_remote, user=user_remote, password=password_remote)
# con2 = psy.connect(dbname=dbname, user=user, password=password)

# con2 = psy.connect(database='database', user='user', host='host', password='password')

c = con.cursor(cursor_factory=psy.extras.RealDictCursor)

input_fields = ["id",
                "title",
                "description",
                "buyer",
                "postcode",
                "email"
                ]



# load from the tenders view
# select id to use as record_id for deduping
DATA_SELECT = "select {} from ocds.ocds_tenders_view where countryname = '{}' and enddate between '{}' and '{}'".format(", ".join(input_fields), country, date_range_start, date_range_end)


def preProcess(column):
    try:  # python 2/3 string differences
        column = column.decode('utf8')
    except AttributeError:
        pass

    # column = unidecode(column)
    # column = re.sub('  +', ' ', column)
    # column = re.sub('\n', ' ', column)
    # column = column.strip().strip('"').strip("'").lower().strip()
    if not column:
        column = None
    return column

    # try to turn into float if possible (e.g. for price field)
    # try:
    #     float(column)
    # except (ValueError, TypeError):
    #     pass


    return column


print 'importing data ...'
c.execute(DATA_SELECT) # returns list, each item in list being a dictionary of the corresponding row
data = c.fetchall()# returns list, each item in list being a dictionary of the corresponding row
data_d = {} # this is the dictionary I will use for deduping.
# the keys will be record_ids and the values will be dictionaries with the keys being field names

# I think it's possible to use enumerate here but I'm going to stick with using the "id" field as keys in the dict
for row in data:
    clean_row = [(k, preProcess(v)) for (k, v) in row.items()]
    row_id = int(row['id'])
    data_d[row_id] = dict(clean_row)

# try closing up here now
con.close()

if os.path.exists(settings_file):
    print 'reading from', settings_file
    with open(settings_file) as sf:
        deduper = dedupe.StaticDedupe(sf)

else:
    fields = [
        {'field': 'title', 'type': 'String'},
        {'field': 'description', 'type': 'Text'},
        {'field': 'buyer', 'type': 'String'},
        {'field': 'postcode', 'type': 'String'},
        {'field': 'email', 'type': 'String'}
    ]

    deduper = dedupe.Dedupe(fields)

    deduper.sample(data_d, 150000)

    if os.path.exists(training_file):
        print 'reading labeled examples from ', training_file
        with open(training_file) as tf:
            deduper.readTraining(tf)

    print 'starting active labeling...'

    dedupe.consoleLabel(deduper)

    deduper.train()

    with open(training_file, 'w') as tf:
        deduper.writeTraining(tf)

    with open(settings_file, 'w') as sf:
        deduper.writeSettings(sf)

print 'blocking...'

threshold = deduper.threshold(data_d, recall_weight=2)

print 'clustering...'
clustered_dupes = deduper.match(data_d, threshold) # returns tuples

print '# duplicate sets', len(clustered_dupes)

# connect to ocds again to get the data and to get the columns to make the deduped table
con2 = psy.connect(host=host_remote, dbname=dbname_remote, user=user_remote, password=password_remote)

c2 = con2.cursor()

# use restricted number of columns for now, to avoid the JSON problem
# c2.execute("SELECT id, source, source_id, ocid, language, title FROM ocds.ocds_tenders_view where countryname = 'United Kingdom' limit 500")

# use the SELECT_DATA for now
c2.execute(DATA_SELECT)

data = c2.fetchall() # returns a list of tuples

full_data = []

cluster_membership = collections.defaultdict(lambda: 'x') # This variable doesn't seem to be used anywhere else?
for cluster_id, (cluster, score) in enumerate(clustered_dupes): # cycle through the clustered dupes
    for record_id in cluster:
        for row in data: # data is a list, but isn't each row a dict? NO because data was redefined above
            # so I think this assumes that the the first value in each row from the table is an id
            # so if the row id matches the record_id from the clustered_dupes...
            if record_id == int(row[0]): # NEED TO CHANGE THIS TO row[(whatever index matches id)]
                row = list(row) # turn the tuple into a list
                row.insert(0, cluster_id) # put the cluster_id at the beginning of the list
                row = tuple(row) # make it a tuple again
                full_data.append(row) # add the new row to a new list

# # FOR NOW: create manual list of column names
# column_names = ["id",
#                 "source",
#                 "source_id",
#                 "ocid",
#                 "language",
#                 "title"]

# use the same output column_names as the input fields

column_names = input_fields

# NOTE: having problemw wuth code below because the json format is interfering with the column name retrieval for some reason...

# columns = "SELECT column_name FROM information_schema.columns WHERE table_name = 'ocds_tenders_view'"
# c2.execute(columns)
# column_names = c2.fetchall()
# column_names = [x[0] for x in column_names]
# # for now, only use the first seven columns
# column_names = column_names[:7]
column_names.insert(0, 'cluster_id') # add cluster_id to the column names

# for column_name in column_names:
#     print column_name

con2.close()

# make the table for the deduped data
con3 = psy.connect(host=host_remote, dbname=dbname_remote, user=user_remote, password=password_remote)
# con3 = psy.connect(dbname=dbname, user=user, password=password)
c3 = con3.cursor()

# comment out DROP statement for now...
c3.execute('DROP TABLE IF EXISTS ocds.ocds_tenders_deduped2') # get rid of the table (so we can make a new one)
field_string = ','.join('%s varchar(500000)' % name for name in column_names) # maybe improve the data types...
c3.execute('CREATE TABLE ocds.ocds_tenders_deduped2 (%s)' % field_string)
con3.commit()

#This is the input
num_cols = len(column_names)
mog = "(" + ("%s," * (num_cols - 1)) + "%s)"
args_str = ','.join(c3.mogrify(mog, x) for x in full_data) # mogrify is used to make query strings
values = "(" + ','.join(x for x in column_names) + ")"
c3.execute("INSERT INTO ocds.ocds_tenders_deduped2 %s VALUES %s" % (values, args_str))
con3.commit()
con3.close()

# con.close()

print 'ran in', time.time() - start_time, 'seconds'