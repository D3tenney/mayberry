import pandas
import os

pull_date = '20200217'

csv_dir = 'helper_tables_'+pull_date+'/'

list_dir = os.listdir(csv_dir)

for file in list_dir:
