import os
import re

work_dir = os.getcwd()
input_dir = '/'.join(work_dir.split('/')[:-1]) + '/input/'

train_files = [input_dir + f for f in os.listdir(input_dir) if re.match(r'^train.*\.csv$', f)]
train_keys = [re.match(r'^train_(.*)\.csv$', f).group(1) for f in os.listdir(input_dir) if re.match(r'^train_(.*)\.csv$', f)]
train = dict(zip(train_keys, train_files))

test_files = [input_dir + f for f in os.listdir(input_dir) if re.match(r'^test.*\.csv$', f)]
test_keys = [re.match(r'^test_(.*)\.csv$', f).group(1) for f in os.listdir(input_dir) if re.match(r'^test_(.*)\.csv$', f)]
test = dict(zip(test_keys, test_files))

