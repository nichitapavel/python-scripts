import logging
import os
import signal
import socket
import struct
import sys
import threading
import urllib
from datetime import datetime
from enum import Enum
from optparse import OptionParser
from time import sleep

import urlparse
from Queue import Queue
from flask import request, Flask

logging.basicConfig(
    level=logging.INFO,
    format='[%(process)d][%(asctime)s.%(msecs)03d][%(name)s][%(levelname)s]%(message)s',
    datefmt='%Y/%m/%d-%H:%M:%S'
)


# TODO SHIT CODE: refactor all as soon as possible


class Operation(Enum):
    AS = 'matrix A fill start'
    AF = 'matrix A fill finish'
    BS = 'matrix B fill start'
    BF = 'matrix B fill finish'
    XS = 'matrix compute start'
    XF = 'matrix compute finish'


# Create a single input and a single output queue for all threads.
marks = Queue()
app = Flask("server")


@app.route('/message')
def message():
    try:
        query = urllib.unquote(request.query_string)
        params = urlparse.parse_qs(query)
        timestamp = datetime.fromtimestamp(
            long(params.get('timestamp')[0]) / 1000.0
        )

        msg = {
            'device': params.get('device')[0],
            'device timestamp': params.get('timestamp')[0],
            'local timestamp': timestamp,
            'operation': params.get('operation')[0]
        }
        marks.put(msg)
        logger.info('[{0}][{1}][DEVICE:{2}][DEVICE_TIMESTAMP:{3}][OPERATION:{4}]'.format(
            os.getcwd(),
            file,
            params.get('device')[0],
            datetime.strftime(timestamp, '%Y/%m/%d-%H:%M:%S.%f'),
            params.get('operation')[0]
        ))
    except TypeError:
        logger.exception('[The http query is malformed]')
    return ''


class FlaskThread(threading.Thread):
    def __init__(self, marks):
        super(FlaskThread, self).__init__()
        self.name = 'FlaskThread'
        self.marks = marks
        self.stop_request = threading.Event()
        self.logger = logging.getLogger(__name__)
        log = logging.getLogger('werkzeug')
        log.setLevel(logging.ERROR)

    def run(self):
        try:
            app.run(host='0.0.0.0', port=port)
        except Exception:
            logger.exception('[Fatal error when starting flask server]')
            for thread in threading.enumerate():
                if thread.name == 'PMInfoThread':
                    thread.join()
            sys.exit(1)

    def join(self, timeout=None):
        self.stop_request.set()
        super(FlaskThread, self).join(timeout)


class PMInfoThread(threading.Thread):
    def __init__(self, marks):
        super(PMInfoThread, self).__init__()
        self.name = type(self).__name__
        self.marks = marks
        self.stop_request = threading.Event()
        self.logger = logging.getLogger(type(self).__name__)

    def run(self):
        dev_name = 'APCape8L'
        send_all_data(client, struct.pack("i", 9))
        msg = struct.pack("i", len(dev_name))
        msg += dev_name
        msg += struct.pack("i", 0)
        send_all_data(client, msg)

        f = open(file, 'w')
        f.write('Time,Power(mWatt),Operation\n')

        while not self.stop_request.isSet():
            try:
                lines = receive_data(client, "i")
                lines_array = []

                for lin in xrange(lines):
                    line_power = receive_data(client, "d")
                    lines_array.append("%.2f" % (line_power))

                if not self.marks.empty():
                    mark = self.marks.get()
                    self.marks.task_done()
                    f.writelines(
                        "{0},{1},{2}\n".format(
                            datetime.now().strftime('%Y/%m/%d-%H:%M:%S.%f')[:-2],
                            lines_array[line],
                            mark.get('operation')
                        )
                    )
                elif len(lines_array) != 0:
                    f.writelines(
                        "{0},{1},\n".format(
                            datetime.now().strftime('%Y/%m/%d-%H:%M:%S.%f')[:-2],
                            lines_array[line]
                        )
                    )
            except:
                logger.exception('[Error found! Here are the details:]')
                pass

    def join(self, timeout=None):
        self.stop_request.set()
        super(PMInfoThread, self).join(timeout)


def handler():
    if mode == "read":
        send_all_data(client, struct.pack("i", 0))
        client.close()


def receive_data(client, datatype):
    try:
        msg = client.recv(struct.calcsize(datatype))
        if len(msg) == 0:
            client.close()
            sys.exit(1)
    except Exception:
        client.close()
        sys.exit(1)

    try:
        return struct.unpack(datatype, msg)[0]
    except struct.error:
        logger.exception('[Error found! Here are the details:]')
        pass


def send_all_data(client, msg):
    totalsent = 0
    while totalsent < len(msg):
        sent = client.send(msg[totalsent:])
        if sent == 0:
            raise RuntimeError
        totalsent = totalsent + sent


def main():
    global logger
    logger = logging.getLogger('FLASK_PM_INFO')

    # Parsear linea de comandos
    parser = OptionParser("usage: %prog -s|--server SERVER:PORT\n"
                          "       %prog -r|--read DEVNAME [-f|--freq FREQ]")
    parser.add_option("-s", "--server", action="store", type="string", dest="server")
    parser.add_option("-r", "--read", action="store", type="string", dest="device")
    parser.add_option("-f", "--file", action="store", type="string", dest="file")
    parser.add_option("-d", "--directory", action="store", type="string", dest="directory")
    parser.add_option("-p", "--port", action="store", type="string", dest="port")
    parser.add_option("-l", "--line", action="store", type="int", dest="line")

    (options, args) = parser.parse_args()

    exit_script = False
    if not options.server or len(options.server.split(":")) != 2:
        logger.error('[You must specify a pm_server SERVER:PORT!]')
        exit_script = True
    elif not options.directory:
        logger.error('[You must specify a working directory]')
        exit_script = True
    elif not options.file:
        logger.error('[You must specify a file to save data]')
        exit_script = True
    elif not options.port:
        logger.error('[You must specify a local port]')
        exit_script = True
    elif not options.line:
        logger.error('[You must specify a line to save power metrics]')
        exit_script = True

    if exit_script:
        parser.print_help()
        sys.exit(-1)

    if not os.path.exists(options.directory):
        os.makedirs(options.directory)
    os.chdir(options.directory)

    global client, mode, pool, file, port, line
    mode = 'read'
    file = options.file
    port = options.port
    line = options.line - 1

    file_log = logging.FileHandler(os.getcwd()+'/'+file[:-4]+'.log', mode='w')
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

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((options.server.split(":")[0], int(options.server.split(":")[1])))
    signal.signal(signal.SIGINT, handler)

    pool = [PMInfoThread(marks=marks), FlaskThread(marks=marks)]

    for thread in pool:
        thread.daemon = True
        thread.start()

    while pool[0].is_alive() and pool[1].is_alive():
        sleep(10)
    sys.exit(0)


if __name__ == '__main__':
    main()
