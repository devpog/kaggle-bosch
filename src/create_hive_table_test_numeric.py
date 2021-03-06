"""
1) create table bosch.train_numeric_temp (col_value string);
2) load data inpath '/user/hdfs/bosch/train_numeric' overwrite into table bosch.train_numeric_temp;
3) take the resulting columns from 2) and assign to the following cols-variable
"""
import os
import re

step1 = False
step2 = True
step3 = True
step4 = True

db = 'bosch'
table = 'test_numeric'
table_temp = 'test_numeric_temp'

# Step 1. Create temp table
create_table_temp = '''
/usr/bin/hive -e "create table {0}.{1} (col_value STRING)"
'''.strip().format(db, table_temp)
print(create_table_temp)

if step1:
    os.system(create_table_temp)

# Step 2. Load data into temp table
load_data_temp = '''
/usr/bin/hive -e "LOAD DATA INPATH '/user/hdfs/{0}/{1}.csv' OVERWRITE INTO TABLE {0}.{2}"
'''.strip().format(db, table, table_temp)
print(load_data_temp)

if step2:
    os.system(load_data_temp)

# Step 3. Create permanent table
work_dir = os.getcwd()
input_dir = '/'.join(work_dir.split('/')[:-1]) + '/input/'
f = [e for e in os.listdir(input_dir) if re.match('^test_numeric\.csv$', e)].pop()
h = [e for e in os.listdir(input_dir) if re.match('^test_numeric_header\.csv$', e)].pop()
cols = open(input_dir + h).read().split(',')

header = "create table bosch.test_numeric ("
for n, c in enumerate(cols):

    if c == 'Id':
        c += ' bigint, '
    elif n != len(cols) - 1:
        c += ' decimal(4,4), '
    else:
        c += ' decimal(4,4));'
    header += c
print(header)

create_table_perm = '''
/usr/bin/hive -e "{0}"
'''.strip().format(header)
print(create_table_perm)

if step3:
    os.system(create_table_perm)

# Step 4. Update the table with the values taken from the temp table
header1 = "insert overwrite table bosch.test_numeric select "
insertion = """
regexp_extract(col_value, '^(?:([^,]*),?){CNT}',
""".strip()
footer1 = " from bosch.test_numeric_temp;"
for n, c in enumerate(cols, start = 1):
    if n != len(cols):
        c = insertion.replace('CNT', str(n)) + ' 1) ' + c + ', '
    else:
        c = insertion.replace('CNT', str(n)) + ' 1) ' + c
    header1 += c
header1 += footer1

update_table_perm = '''
/usr/bin/hive -e "{0}"
'''.strip().format(header1)
print(update_table_perm)

if step4:
    os.system(update_table_perm)