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
# the first one seems to be used for fetching the data (or rather the fields that are used for dedupe

con = psy.connect(database='database', user = 'user', host='host', password='password')

con2 = psy.connect(database='database', user = 'user', host='host', password='password')

c = con.cursor(cursor_factory=psy.extras.RealDictCursor) #not sure what this cursor factory thing is

USM3_SELECT = 'SELECT sss FROM public.usm3'

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

print 'importing data ...'
c.execute(USM3_SELECT)
data= c.fetchall()
data_d = {} #Think this is the cleaned data (in dictionary form)
for row in data:
    clean_row = [(k, preProcess(v)) for (k, v) in row.items()]
    row_id = int(row['id']) # think i'd need to edit this if we don't have id
    data_d[row_id] = dict(clean_row)
