import csv
import datetime
import logging
import os
from multiprocessing import Manager
from multiprocessing.pool import Pool

from common import read_timestamp, CSV_TIME, CSV_POWER, CSV_OP, check_last_row, \
    first_timestamp, csv_name_parsing, log_to_file, profile, write_csv_dict_with_lists, \
    write_csv_list_of_dict, parse_args, sort_list_of_dict
from plotters import power_plot


def csv_shortcuts(data):
    data_time = data.get('time')
    data_mw = data.get('mw')
    data_op = data.get('op')
    data_time_xs = data.get('time_xs')
    data_time_00 = data.get('time_00')
    data_us = data.get('us')

    return data_time, data_mw, data_op, data_time_xs, data_time_00, data_us


# TODO a line can contain NULL byte, this script does not control this use case
def pre_compute_checks(file):
    """
    Checks in file:
    1. return the first timestamp that appears in file
    2. checks if last row is valid (complete data), if no deletes it
    3. searches and returns timestamp of XS and XF marks
    :param file: csv file with raw data
    :return: xs, xf and first timestamps
    """
    ts_first = first_timestamp(file)
    check_last_row(file)
    ts_xs, ts_xf = None, None
    reader = csv.DictReader(file)
    for row in reader:
        op = row.get(CSV_OP)
        if op == 'XS':
            ts_xs = read_timestamp(row.get(CSV_TIME))
        if op == 'XF':
            ts_xf = read_timestamp(row.get(CSV_TIME))

    return ts_xs, ts_xf, ts_first


def calculate_energy(power, power_prev, us):
    """
    Calculate energy in joules for the zone defined by Xn, X(n-1) and { [Tn - T(n-1)] => us }
    :param power: Xn (current power)
    :param power_prev: X(n-1) (previous power)
    :param us: Tn-T(n-1) in microseconds
    :return: float, computed energy in joules
    """
    return (float(power) + float(power_prev)) / 2 * us


def backwards_xs_time_compute(data_time, ts_xs):
    time_xs = []
    for ts in data_time:
        time_xs.append(
            (ts - ts_xs).total_seconds()
        )
    return time_xs


def csv_process(data: dict, file: str, ts_first, ts_xs, ts_xf):
    """
    Opens 'file' in csv form, applies a series of transformations for each csv row
    :param data: a dict,
    :param file: string, file name
    :param ts_first: timestamp, from first row of 'file'
    :param ts_xs: timestamp, when XS marks appeared in 'file
    :param ts_xf: timestamp, when XF marks appeared in 'file
    :return: timestamp x2: XS and XF if found, floats 2x: computed energy (joules) and time (seconds)
    """
    reader = csv.DictReader(file)
    energy = 0
    time_us = 0
    td_dt_ref = datetime.datetime.min
    # TODO a line can contain NULL byte, this script does not control this use case
    for row in reader:
        time_str = row.get(CSV_TIME)
        power_current = row.get(CSV_POWER)
        op = row.get(CSV_OP)
        us = ''  # microseconds
        ts_current = read_timestamp(time_str)
        # Order of these conditions is important
        if ts_xs and not ts_xf:
            # When inside this condition we are at Tn and Xn with n = 1 to XF mark
            # XS is n = 0, Xn defines power in milliwatt, Tn defines time
            # energy of this zone is equal to (Xn + X(n-1))/2 * (Tn-T(n-1))
            # X(n-1) is "data['mw'][-1]", Xn is "power_current"
            # T(n-1) is "data['time'][-1]", Tn is "ts_current"
            us = (ts_current - data['time'][-1]).microseconds
            energy += calculate_energy(power_current, data['mw'][-1], us)
            time_us += us
        if op != '':
            if op == 'XS':
                ts_xs = ts_current
                data['time_xs'] = backwards_xs_time_compute(data['time'], ts_xs)
            if op == 'XF':
                ts_xf = ts_current

            # Here we save the position in data dict, since all lists in data have
            # the same length, the position will always show data from the same row
            # position = reader.line_num - 2, because reader.line_num tells us the
            # position in the csv file, it starts with 1 (not 0) and the line nº 1
            # is the header, the first line with data is nº 2, but in our dict with lists
            # is 0, hence the '- 2'
            # op is the operation, we have it in our data dict already, but is big and
            # mostly empty, and this will only have the values to be marked in the plot
            data['pos_and_marks'].append((reader.line_num - 2, op))
        if ts_xs:
            data['time_xs'].append(
                (ts_current - ts_xs).total_seconds()
            )

        data['time_str'].append(time_str)
        data['time'].append(ts_current)
        data['mw'].append(float(power_current))
        data['op'].append(op)
        data['time_00'].append(ts_current - ts_first)
        data['td_dt_00'].append(td_dt_ref + data['time_00'][-1])
        data['us'].append(us)

    return ts_xs, ts_xf, energy / 1000000000, time_us / 1000000


def data_file_process(cwd, file):
    logger.info(f'[{cwd}][{file}]')
    data = {
        'time_str': [], 'time': [], 'mw': [], 'op': [], 'time_xs': [],
        'time_00': [], 'us': [], 'td_dt_00': [], 'pos_and_marks': []
    }
    ts_xs = None
    ts_xf = None
    with open(file, 'r+') as f:
        energy_dict = csv_name_parsing(file)
        ts_first = first_timestamp(f)
        check_last_row(f, logger)
        ts_xs, ts_xf, energy_dict['joules'], energy_dict['time'] = \
            csv_process(data, f, ts_first, ts_xs, ts_xf)
    if ts_xs and ts_xf:
        power_plot(file, data['td_dt_00'], data['mw'], data['pos_and_marks'])
        del data['time'], data['td_dt_00'], data['pos_and_marks']
        write_csv_dict_with_lists(f'transformed-{file}', data)
    else:
        logger.warning(f'[{cwd}][{file}][XS operation not found, skip this file]')
        # Meta cache system: we use the EXISTENCE of file that starts with 'transformed-' (empty or with data)
        # to NOT re-analyze older files, for cases where new files are added and we run the script
        # again it will prevent adding repeated lines in processed_data.csv.
        open(f'transformed-{file}', 'w').close()
        energy_dict['joules'], energy_dict['time'] = '', ''

    return energy_dict


def get_files(filter):
    """
    Returns a list of files reverse sorted by size.
    Bigger files are first processed to try and maximize the efficiency.
    This does not guaranty that bigger files will always take more time
    to process then smaller files.
    :return: a list of string representing files in current directory
    """
    files = os.listdir(os.curdir)
    size_file = []
    for filename in files:
        if filename.startswith(filter) and filename.endswith('.csv'):
            if f'transformed-{filename}' not in files:
                size = os.path.getsize(filename)
                size_file.append((size, filename))
    size_file.sort(key=lambda s: s[0], reverse=True)
    return [file[1] for file in size_file]


def main():
    options = parse_args(logger)
    os.chdir(options.directory)
    logger.addHandler(log_to_file('transform.log'))

    cwd = os.getcwd()
    files = get_files(options.starts_with)
    processed_data = []

    # TODO mem profiling not working with mp
    with Pool(options.cores) as p:
        results = [p.apply_async(data_file_process, (cwd, file)) for file in files]
        for result in results:
            processed_data.append(result.get())

    sort_list_of_dict(processed_data)
    write_csv_list_of_dict('processed_data.csv', processed_data, logger)


if __name__ == "__main__":
    logger = logging.getLogger('TRANSFORM_CSV')
    with Manager() as manager:
        mem = manager.list()
        profile(mem, 'global', main)

        mem.sort()
        for item in mem:
            print(item)
