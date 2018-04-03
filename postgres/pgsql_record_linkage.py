# -*- coding: utf-8 -*-
"""
pgswl_record_linkage pulls data from usm3 and suppliers from the database,
uses dedupes record_linkage to find matches between usm3 and suppliers,
uploads the matches (clusters) to table
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
settings_file = 'data_matching_learned_settings'
training_file = 'data_matching_training.json'

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

# TWO SEPARATE CONNECTION SESSIONS
# the first to be used for fetching the data (or rather the fields that are used for dedupe)
con1 = psy.connect(host=host_remote, dbname=dbname_remote, user=user_remote, password=password_remote)

# dictionary cursor, allows data retrieval using dicts
c1 = con1.cursor(cursor_factory=psy.extras.RealDictCursor)

SELECT_DATA_0 = "SELECT id, sss FROM blue.usm3 WHERE (sss LIKE '{}') AND (sid IS NULL)".format(alphabetic_filter)
SELECT_DATA_1 = "SELECT rid AS id, supplier_name AS sss FROM blue.supplier WHERE (supplier_name LIKE '{}') AND (supplier_id IS NOT NULL)".format(alphabetic_filter)

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

data_d0 = {}
data_d1 = {}
data_list = [data_d0, data_d1] # list to hold both dictionaries needed for deduping

for i, query in enumerate([SELECT_DATA_0, SELECT_DATA_1]):
    print "importing dataset {}...".format(str(i))
    c1.execute(query)
    data = c1.fetchall()
    for row in data:
        # each row is a dictionary
        clean_row = [(k, preProcess(v)) for (k, v) in row.items()]  # what are the keys and values here? are the keys each field name
        row_id = row['id']  # think i'd need to edit this if we don't have id
        data_list[i][row_id] = dict(clean_row)  # not sure exactly why we undictionaried and then dictionaried cleanrow...

# close conection
con1.close()

if os.path.exists(settings_file):
    print 'reading from', settings_file
    with open(settings_file) as sf :
        linker = dedupe.StaticRecordLink(sf)

else:
    fields = [
        {'field' : 'sss', 'type': 'String'}
        ]

    linker = dedupe.RecordLink(fields)
    # deduper = dedupe.Dedupe(fields)

    linker.sample(data_list[0], data_list[1], 15000)

    if os.path.exists(training_file):
        print 'reading labeled examples from ', training_file
        with open(training_file) as tf :
            linker.readTraining(tf)

    print 'starting active labeling...'

    dedupe.consoleLabel(linker)

    linker.train()
    
    with open(training_file, 'w') as tf :
        linker.writeTraining(tf)

    with open(settings_file, 'w') as sf :
        linker.writeSettings(sf)


print 'clustering...'
clustered_dupes = linker.match(data_list[0], data_list[1], threshold=0.5)

print '# duplicate sets', len(clustered_dupes)


# Select all the rows for some reason
c2 = con2.cursor()
c2.execute('SELECT * FROM csv_messy_data')
data = c2.fetchall()

full_data = []

cluster_membership = collections.defaultdict(lambda : 'x') #?
for cluster_id, (cluster, score) in enumerate(clustered_dupes):
    for record_id in cluster:
        for row in data:
            if record_id == int(row[0]):
                row = list(row)
                row.insert(0,cluster_id)
                row = tuple(row)
                full_data.append(row)

columns = "SELECT column_name FROM public.columns WHERE table_name = 'deduped_table'"
c2.execute(columns) # as part of the same session, find the column_names
column_names = c2.fetchall()
column_names = [x[0] for x in column_names]
column_names.insert(0,'cluster_id')

c2.execute('DROP TABLE IF EXISTS deduped_table')
field_string = ','.join('%s varchar(200)' % name for name in column_names)
c2.execute('CREATE TABLE deduped_table (%s)' % field_string)
con2.commit()

num_cols = len(column_names)

# mogrify if just a way of putting variables into

mog = "(" + ("%s,"*(num_cols -1)) + "%s)"
args_str = ','.join(c2.mogrify(mog,x) for x in full_data) # This is the actual data that goes in the table
values = "("+ ','.join(x for x in column_names) +")"
c2.execute("INSERT INTO deduped_table %s VALUES %s" % (values, args_str))
con2.commit()
con2.close()
con.close()

print 'ran in', time.time() - start_time, 'seconds'
