"""
1) create table bosch.train_numeric_temp (col_value string);
2) load data inpath '/user/hdfs/bosch/train_numeric' overwrite into table bosch.train_numeric_temp;
3) take the resulting columns from 2) and assign to the following cols-variable
"""
import os
import re

db = 'bosch'
table = 'train_numeric'
table_temp = 'train_numeric_temp'

# Step 1. Create a temp table
create_table_temp = """
hive -e "create table {0}.{1} (col_value STRING)"
""".strip().format(db, table_temp)
os.system(create_table_temp);

# Step 2. Load data into temp table
load_data_temp = """
hive -e "LOAD DATA INPATH '/user/hdfs/{0}/{1}.csv' OVERWRITE INTO TABLE {0}.{2}"
""".strip().format(db, table, table_temp)
os.system(load_data_temp)

# Step 3.
work_dir = os.getcwd()
input_dir = '/'.join(work_dir.split('/')[:-1]) + '/input/'
f = [e for e in os.listdir(input_dir) if re.match('^train_numeric\.csv$', e)].pop()
h = [e for e in os.listdir(input_dir) if re.match('^train_numeric_header\.csv$', e)].pop()

cols = open(input_dir + h).read().split(',')

header = "create table bosch.train_numeric ("
for n, c in enumerate(cols):

    if c == 'Id':
        c += ' bigint, '
    elif n != len(cols) - 1:
        c += ' decimal(4,4), '
    else:
        c += ' decimal(4,4));'
    header += c
print(header)

header1 = "insert overwrite table bosch.train_numeric select "
insertion = """
regexp_extract(col_value, '^(?:([^,]*),?){CNT}',
""".strip()
footer1 = " from bosch.train_numeric_temp;"
for n, c in enumerate(cols, start = 1):
    if n != len(cols):
        c = insertion.replace('CNT', str(n)) + ' 1) ' + c + ', '
    else:
        c = insertion.replace('CNT', str(n)) + ' 1) ' + c
    header1 += c
header1 += footer1

print(header1)
