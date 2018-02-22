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

settings_file = 'postgres_settings'
training_file = 'postgres_training.json'

start_time = time.time()

# TWO SEPARATE CONNECTION SESSIONS
# the first to be used for fetching the data (or rather the fields that are used for dedupe)

con = psy.connect(database='database', user = 'user', host='host', password='password')

con2 = psy.connect(database='database', user = 'user', host='host', password='password')

# dictionary cursor, allows data retrieval using dicts
c1 = con.cursor(cursor_factory=psy.extras.RealDictCursor)
c2 = con.cursor(cursor_factory=psy.extras.RealDictCursor)

SELECT_DATA_1 = 'SELECT sss FROM public.usm3 WHERE (sss LIKE 'AB%') AND (sid IS NULL)'
SELECT_DATA_0 = 'SELECT AS FROM public.supplier WHERE (supplier_name LIKE 'AB%') AND (supplier_id IS NOT NULL)'

def preProcess(column):
    try : # python 2/3 string differences
        column = column.decode('utf8')
    except AttributeError:
        pass
    column = unidecode(column)
    column = re.sub('  +', ' ', column)
    column = re.sub('\n', ' ', column)
    column = column.strip().strip('"').strip("'").lower().strip()
    if not column :
        column = None
    return column

print 'importing first data ...'
c1.execute(SELECT_DATA_1)
data= c1.fetchall() # what does this do exactly?

# maybe do the below twice for each dataset
data_d1 = {} #Think this is the cleaned data (in dictionary form)

for row in data:
    # each row is a dictionary
    clean_row = [(k, preProcess(v)) for (k, v) in row.items()] # what are the keys and values here? are the keys each field name
    row_id = int(row['id']) # think i'd need to edit this if we don't have id
    data_d1[row_id] = dict(clean_row) # not sure exactly why we undictionaried and then dictionaried cleanrow...

print 'importing second data...'


if os.path.exists(settings_file):
    print 'reading from', settings_file
    with open(settings_file) as sf :
        deduper = dedupe.StaticDedupe(sf)

else:
    fields = [
        {'field' : 'sss', 'type': 'String'}
        ]

    linker = dedupe.RecordLink(fields)
    # deduper = dedupe.Dedupe(fields)

    linker.sample(data_d1, data_d2, 15000)
    # deduper.sample(data_d, 150000)

    if os.path.exists(training_file):
        print 'reading labeled examples from ', training_file
        with open(training_file) as tf :
            deduper.readTraining(tf)

    print 'starting active labeling...'

    # dedupe.consoleLable(liker)
    dedupe.consoleLabel(deduper)

    # linker.train()
    deduper.train()
    
    with open(training_file, 'w') as tf :
        deduper.writeTraining(tf)

    with open(settings_file, 'w') as sf :
        deduper.writeSettings(sf)

print 'blocking...'

threshold = deduper.threshold(data_d, recall_weight=2)

print 'clustering...'
clustered_dupes = deduper.match(data_d, threshold)

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
