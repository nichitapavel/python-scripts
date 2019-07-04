import csv
import logging
import os
import sys
import datetime
from optparse import OptionParser

from common import read_timestamp

logging.basicConfig(
    level=logging.INFO,
    # filename='thread-flask-pminfo.log',
    format='[%(process)d][%(asctime)s.%(msecs)03d][%(name)s][%(levelname)s]%(message)s',
    datefmt='%Y/%m/%d-%H:%M:%S'
)
logger = logging.getLogger('XSXF')


def open_csv(file):
    f = open(file)
    reader = csv.DictReader(f)
    data = {'mw': [], 'time': [], 'micro_s': [datetime.timedelta(0)]}
    is_xs = False
    is_xf = False

    for row in reader:
        if not is_xs:
            is_xs = set_xs(row)
        elif not is_xf:
            is_xf = set_xf(row)
            data['mw'].append( row.get('Power(mWatt)') )
            data['time'].append( row.get('Transformed Time - 00') )
        else:
            return data

    logger.error(f'[{file}][XS or XF operation not found, skipping...]')
    return None


def calculate_time(data):
    for i in range(1, len(data.get('time'))):
        post = read_timestamp(data.get('time')[i])
        pre = read_timestamp(data.get('time')[i-1])
        data.get('micro_s').append(post - pre)


def write_to_file(data, filepath):
    file = open(filepath + '.xsxf', 'w')
    file.write('miliWatt,micro_seconds\n')
    for i in range(0, len(data.get('mw'))):
        file.write(f'{data.get("mw")[i]},{data.get("micro_s")[i].microseconds}\n')
    file.close()
    

def set_xs(row):
    op = row.get('Operation')
    if op is not None:
        if op == 'XS':
            return True
    return False


def set_xf(row):
    op = row.get('Operation')
    if op is not None:
        if op == 'XF':
            return True
    return False


def xsxf_files(path):
    files = os.listdir(f'{path}/xsxf/')
    for i in range(0, len(files)):
        files[i] = files[i][0:-5]
    return files


def total_time(data):
    energy = 0
    size = data.get('time')
    for i in range(0, size):
        energy += float(data.get('mw')[i]) * 1000 * data.get('micro_s')[i].microseconds


def main():
    # Parsear linea de comandos
    parser = OptionParser("usage: %prog -d|--directory DIRECTORY")
    parser.add_option("-d", "--directory", action="store", type="string", dest="directory")

    (options, args) = parser.parse_args()

    if not options.directory:
        logger.error('[You must specify a working directory]')
        parser.print_help()
        sys.exit(-1)

    os.chdir(options.directory)
    xsxf_folder = '../xsxf'

    xsxf_list = []
    try:
        xsxf_list = os.listdir(xsxf_folder)
    except FileNotFoundError:
        logger.info('[XSXF folder not found, creating one]')
        os.mkdir(xsxf_folder)

    file_log = logging.FileHandler(f'{xsxf_folder}/xsxf.log', mode='w')
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

    files = os.listdir(os.curdir)
    files.sort()
    for f in files:
        # if f.startswith('transformed') and f.endswith('.csv'):
        if 'transformed' in f and f.endswith('.csv'):
            if f'{f}.xsxf' not in xsxf_list:
                logger.info(f'[{os.getcwd()}][{f}]')
                data = open_csv(options.directory + f)
                if data is not None:
                    calculate_time(data)
                    write_to_file(data, f'{xsxf_folder}/{f}')
            else:
                logger.info(f'[{os.getcwd()}][{f}][File exists, skipping...]')


if __name__ == "__main__":
    main()
