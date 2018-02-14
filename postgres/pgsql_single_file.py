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

# right now: only outputs the clustered records (i.e. doesn't show records that didn't end up in the cluster)
# NOTE: relies on the "id" field being in a specific place in order to match (see line 135)
# haven't yet put score in after it was causing some bugs

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

settings_file = 'usm3_10k_learned_settings'
training_file = 'usm3_10k_example_training.json'

dbname = "postgres"
user = "postgres"
password = "postgres"
host = "host"


start_time = time.time()

con = psy.connect(dbname=dbname, user=user, password=password)
con2 = psy.connect(dbname=dbname, user=user, password=password)

# con2 = psy.connect(database='database', user='user', host='host', password='password')

c = con.cursor(cursor_factory=psy.extras.RealDictCursor)

# test by trying to load from public.usm3
DATA_SELECT = "SELECT id, sss FROM public.usm3 WHERE (sss LIKE 'AB%') AND (sid IS NULL)" # select id to use as record_id for deduping


def preProcess(column):
    try:  # python 2/3 string differences
        column = column.decode('utf8')
    except AttributeError:
        pass
    if not isinstance(column, int):
        column = unidecode(column)
        column = re.sub('  +', ' ', column)
        column = re.sub('\n', ' ', column)
        column = column.strip().strip('"').strip("'").lower().strip()
        if not column:
            column = None
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
        {'field': 'sss', 'type': 'String'}
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


c2 = con2.cursor()
c2.execute("SELECT * FROM public.usm3 WHERE (sss LIKE 'AB%') AND (sid IS NULL)")
data = c2.fetchall() # returns a list of tuples

full_data = []

cluster_membership = collections.defaultdict(lambda: 'x') # This variable doesn't seem to be used anywhere else?
for cluster_id, (cluster, score) in enumerate(clustered_dupes): # cycle through the clustered dupes
    for record_id in cluster:
        for row in data: # data is a list, but isn't each row a dict? NO because data was redefined above
            # so I think this assumes that the the first value in each row from the table is an id
            # so if the row id matches the record_id from the clustered_dupes...
            if record_id == int(row[10]): # NEED TO CHANGE THIS TO row[(whatever index matches id)]
                row = list(row) # turn the tuple into a list
                row.insert(0, cluster_id) # put the cluster_id at the beginning of the list
                row = tuple(row) # make it a tuple again
                full_data.append(row) # add the new row to a new list

columns = "SELECT column_name FROM information_schema.columns WHERE table_name = 'usm3'"
c2.execute(columns)
column_names = c2.fetchall()
column_names = [x[0] for x in column_names]
column_names.insert(0, 'cluster_id') # add cluster_id to the column names

c2.execute('DROP TABLE IF EXISTS public.deduped_table') # get rid of the table (so we can make a new one)
field_string = ','.join('%s varchar(500)' % name for name in column_names) # maybe improve the data types...
c2.execute('CREATE TABLE public.deduped_table (%s)' % field_string)
con2.commit()

#This is the input
num_cols = len(column_names)
mog = "(" + ("%s," * (num_cols - 1)) + "%s)"
args_str = ','.join(c2.mogrify(mog, x) for x in full_data) # mogrify is used to make query strings
values = "(" + ','.join(x for x in column_names) + ")"
c2.execute("INSERT INTO deduped_table %s VALUES %s" % (values, args_str))
con2.commit()
con2.close()

# con.close()

print 'ran in', time.time() - start_time, 'seconds'