"""
Reads an .csv file with format:
device,os,benchmark,size,threads,seconds,mops
and sums up all seconds and/or mops values for
combination of "device,os,benchmark,size,threads"
that match, stores this in data variable.
If a combination is not in data variable it will not
stored.

"""

import csv
import logging
import os
import sys

from optparse import OptionParser

logging.basicConfig(
    level=logging.INFO,
    # filename='thread-flask-pminfo.log',
    format='[%(process)d][%(asctime)s.%(msecs)03d][%(name)s][%(levelname)s]%(message)s',
    datefmt='%Y/%m/%d-%H:%M:%S'
)
logger = logging.getLogger('SPEEDUPS')


HIKEY970 = 'hikey970'
ODROIDXU4 = 'odroidxu4'
ROCK960 = 'rock960'

ANDROID = 'android'
LINUX = 'linux'

IS = 'is'
MG = 'mg'
BT = 'bt'

B = 'b'
W = 'w'

THREADS1 = '1'
THREADS2 = '2'
THREADS4 = '4'
THREADS8 = '8'


# ANDROID
hikey970_android_is_b_1 = 'hikey970_android_is_b_1'
hikey970_android_is_b_2 = 'hikey970_android_is_b_2'
hikey970_android_is_b_4 = 'hikey970_android_is_b_4'
hikey970_android_mg_b_1 = 'hikey970_android_mg_b_1'
hikey970_android_mg_b_2 = 'hikey970_android_mg_b_2'
hikey970_android_mg_b_4 = 'hikey970_android_mg_b_4'
hikey970_android_bt_w_1 = 'hikey970_android_bt_w_1'
hikey970_android_bt_w_2 = 'hikey970_android_bt_w_2'
hikey970_android_bt_w_4 = 'hikey970_android_bt_w_4'

odroidxu4_android_is_b_1 = 'odroidxu4_android_is_b_1'
odroidxu4_android_is_b_2 = 'odroidxu4_android_is_b_2'
odroidxu4_android_is_b_4 = 'odroidxu4_android_is_b_4'
odroidxu4_android_mg_b_1 = 'odroidxu4_android_mg_b_1'
odroidxu4_android_mg_b_2 = 'odroidxu4_android_mg_b_2'
odroidxu4_android_mg_b_4 = 'odroidxu4_android_mg_b_4'
odroidxu4_android_bt_w_1 = 'odroidxu4_android_bt_w_1'
odroidxu4_android_bt_w_2 = 'odroidxu4_android_bt_w_2'
odroidxu4_android_bt_w_4 = 'odroidxu4_android_bt_w_4'

rock960_android_is_b_1 = 'rock960_android_is_b_1'
rock960_android_is_b_2 = 'rock960_android_is_b_2'
rock960_android_is_b_4 = 'rock960_android_is_b_4'
rock960_android_mg_b_1 = 'rock960_android_mg_b_1'
rock960_android_mg_b_2 = 'rock960_android_mg_b_2'
rock960_android_mg_b_4 = 'rock960_android_mg_b_4'
rock960_android_bt_w_1 = 'rock960_android_bt_w_1'
rock960_android_bt_w_2 = 'rock960_android_bt_w_2'
rock960_android_bt_w_4 = 'rock960_android_bt_w_4'


# LINUX
hikey970_linux_is_b_1 = 'hikey970_linux_is_b_1'
hikey970_linux_is_b_2 = 'hikey970_linux_is_b_2'
hikey970_linux_is_b_4 = 'hikey970_linux_is_b_4'
hikey970_linux_is_b_8 = 'hikey970_linux_is_b_8'
hikey970_linux_mg_b_1 = 'hikey970_linux_mg_b_1'
hikey970_linux_mg_b_2 = 'hikey970_linux_mg_b_2'
hikey970_linux_mg_b_4 = 'hikey970_linux_mg_b_4'
hikey970_linux_mg_b_8 = 'hikey970_linux_mg_b_8'
hikey970_linux_bt_w_1 = 'hikey970_linux_bt_w_1'
hikey970_linux_bt_w_2 = 'hikey970_linux_bt_w_2'
hikey970_linux_bt_w_4 = 'hikey970_linux_bt_w_4'
hikey970_linux_bt_w_8 = 'hikey970_linux_bt_w_8'

odroidxu4_linux_is_b_1 = 'odroidxu4_linux_is_b_1'
odroidxu4_linux_is_b_2 = 'odroidxu4_linux_is_b_2'
odroidxu4_linux_is_b_4 = 'odroidxu4_linux_is_b_4'
odroidxu4_linux_is_b_8 = 'odroidxu4_linux_is_b_8'
odroidxu4_linux_mg_b_1 = 'odroidxu4_linux_mg_b_1'
odroidxu4_linux_mg_b_2 = 'odroidxu4_linux_mg_b_2'
odroidxu4_linux_mg_b_4 = 'odroidxu4_linux_mg_b_4'
odroidxu4_linux_mg_b_8 = 'odroidxu4_linux_mg_b_8'
odroidxu4_linux_bt_w_1 = 'odroidxu4_linux_bt_w_1'
odroidxu4_linux_bt_w_2 = 'odroidxu4_linux_bt_w_2'
odroidxu4_linux_bt_w_4 = 'odroidxu4_linux_bt_w_4'
odroidxu4_linux_bt_w_8 = 'odroidxu4_linux_bt_w_8'

rock960_linux_is_b_1 = 'rock960_linux_is_b_1'
rock960_linux_is_b_2 = 'rock960_linux_is_b_2'
rock960_linux_is_b_4 = 'rock960_linux_is_b_4'
rock960_linux_is_b_6 = 'rock960_linux_is_b_6'
rock960_linux_mg_b_1 = 'rock960_linux_mg_b_1'
rock960_linux_mg_b_2 = 'rock960_linux_mg_b_2'
rock960_linux_mg_b_4 = 'rock960_linux_mg_b_4'
rock960_linux_mg_b_6 = 'rock960_linux_mg_b_6'
rock960_linux_bt_w_1 = 'rock960_linux_bt_w_1'
rock960_linux_bt_w_2 = 'rock960_linux_bt_w_2'
rock960_linux_bt_w_4 = 'rock960_linux_bt_w_4'
rock960_linux_bt_w_6 = 'rock960_linux_bt_w_6'

data = {
    hikey970_android_is_b_1: [],
    hikey970_android_is_b_2: [],
    hikey970_android_is_b_4: [],
    hikey970_android_mg_b_1: [],
    hikey970_android_mg_b_2: [],
    hikey970_android_mg_b_4: [],
    hikey970_android_bt_w_1: [],
    hikey970_android_bt_w_2: [],
    hikey970_android_bt_w_4: [],
    odroidxu4_android_is_b_1: [],
    odroidxu4_android_is_b_2: [],
    odroidxu4_android_is_b_4: [],
    odroidxu4_android_mg_b_1: [],
    odroidxu4_android_mg_b_2: [],
    odroidxu4_android_mg_b_4: [],
    odroidxu4_android_bt_w_1: [],
    odroidxu4_android_bt_w_2: [],
    odroidxu4_android_bt_w_4: [],
    rock960_android_is_b_1: [],
    rock960_android_is_b_2: [],
    rock960_android_is_b_4: [],
    rock960_android_mg_b_1: [],
    rock960_android_mg_b_2: [],
    rock960_android_mg_b_4: [],
    rock960_android_bt_w_1: [],
    rock960_android_bt_w_2: [],
    rock960_android_bt_w_4: [],
    hikey970_linux_is_b_1: [],
    hikey970_linux_is_b_2: [],
    hikey970_linux_is_b_4: [],
    hikey970_linux_is_b_8: [],
    hikey970_linux_mg_b_1: [],
    hikey970_linux_mg_b_2: [],
    hikey970_linux_mg_b_4: [],
    hikey970_linux_mg_b_8: [],
    hikey970_linux_bt_w_1: [],
    hikey970_linux_bt_w_2: [],
    hikey970_linux_bt_w_4: [],
    hikey970_linux_bt_w_8: [],
    odroidxu4_linux_is_b_1: [],
    odroidxu4_linux_is_b_2: [],
    odroidxu4_linux_is_b_4: [],
    odroidxu4_linux_is_b_8: [],
    odroidxu4_linux_mg_b_1: [],
    odroidxu4_linux_mg_b_2: [],
    odroidxu4_linux_mg_b_4: [],
    odroidxu4_linux_mg_b_8: [],
    odroidxu4_linux_bt_w_1: [],
    odroidxu4_linux_bt_w_2: [],
    odroidxu4_linux_bt_w_4: [],
    odroidxu4_linux_bt_w_8: [],
    rock960_linux_is_b_1: [],
    rock960_linux_is_b_2: [],
    rock960_linux_is_b_4: [],
    rock960_linux_is_b_6: [],
    rock960_linux_mg_b_1: [],
    rock960_linux_mg_b_2: [],
    rock960_linux_mg_b_4: [],
    rock960_linux_mg_b_6: [],
    rock960_linux_bt_w_1: [],
    rock960_linux_bt_w_2: [],
    rock960_linux_bt_w_4: [],
    rock960_linux_bt_w_6: []
}


def open_csv(file, data_type):
    f = open(file)
    reader = csv.DictReader(f)

    if data_type == 'energy':
        csv_type = 'joules'
    elif data_type == 'time':
        csv_type = 'seconds'
    else:
        logger.error(f'[Unknown type: {data_type}]')
        sys.exit(1)

    for row in reader:
        item = return_func(row)
        if data.get(item) is not None:
            data.get(item).append(float(row.get(csv_type)))

    for item, values in data.items():
        if len(values) != 0:
            avg = media(values)
            max_value = max(values)
            min_value = min(values)
            plus_variation = "%.2f" % ((max_value - avg) / avg * 100)
            minus_variation = "%.2f" % ((min_value - avg) / avg * 100)
            logger.info(
                f'[{data_type}][{item}][AVG: {avg}][MAX:{max_value}]'
                f'[MIN: {min_value}][VAR: {plus_variation}/{minus_variation}]'
            )


def cond_func(row, device, os, benchmark, size, threads):
    if row.get('device') == device:
        if row.get('os') == os:
            if row.get('benchmark') == benchmark:
                if row.get('size') == size:
                    if row.get('threads') == threads:
                        return float(row.get('seconds'))


def return_func(row):
    device = row.get('device')
    os = row.get('os')
    benchmark = row.get('benchmark')
    size = row.get('size')
    threads = row.get('threads')
    return f'{device}_{os}_{benchmark}_{size}_{threads}'


def media(data):
    sum = 0.0
    for item in data:
        sum += item
    return sum / len(data)


def main():
    # Parsear linea de comandos
    parser = OptionParser("usage: %prog -f|--file FILE -t|--type TYPE")
    parser.add_option("-f", "--file", action="store", type="string", dest="file")
    parser.add_option("-t", "--type", action="store", type="string", dest="type")

    (options, args) = parser.parse_args()

    if not options.file:
        logger.error('[You must specify a .csv file]')
        parser.print_help()
        sys.exit(-1)
    if not options.file:
        logger.error('[You must specify a type: time or energy]')
        parser.print_help()
        sys.exit(-1)

    os.chdir('/'.join(options.file.split('/')[0:-1]))

    file_log = logging.FileHandler('speedups.log', mode='w')
    file_log.setLevel(logging.INFO)
    file_log.setFormatter(
        logging.Formatter(
            '[%(process)d][%(asctime)s.%(msecs)03d][%(name)s][%(levelname)s]%(message)s',
            datefmt='%Y/%m/%d-%H:%M:%S'
        )
    )

    logger.addHandler(
        file_log
    )

    open_csv(options.file, options.type)


if __name__ == "__main__":
    main()
