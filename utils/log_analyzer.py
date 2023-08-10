#!/path/to/bin/python
import math
import signal
import subprocess
import timeit

import dateutil.parser as d_parser

from mysql_connector import *


def multi_insert(mysql_impl_: MySQLConnector, table_name_: str, entire_payload_: list, commit_=True):
    def real_multi_insert(real_entire_payload_, commit__=commit_):
        if mysql_impl_.health_check():
            if mysql_impl_.cursor is None:
                mysql_impl_.cursor = mysql_impl_.connection.cursor()
            mysql_impl_.cursor.executemany(mysql_stmt_, list(map(lambda r: list(r.values()), real_entire_payload_)))
            if commit__:
                mysql_impl_.connection.commit()

    mysql_stmt_ = f"INSERT INTO %s (%s) VALUES (%s)" \
                  % (table_name_, ','.join(entire_payload_[0].keys()),
                     ','.join(['%s' for _ in range(entire_payload_[0].keys().__len__())]))

    dispatch_num = math.ceil(entire_payload_.__str__().__sizeof__() / (mysql_impl_.max_allowed_packet * 0.9))
    single_part = math.floor(entire_payload_.__len__() / dispatch_num)
    if dispatch_num > 1:
        print(f"Sizeof STMT exceeds maximum allowed packet, dispatched to %d jobs." % dispatch_num)

    for i in range(dispatch_num):
        print(f"Submitting No.%d job..." % i)
        timer_ = timeit.default_timer()
        real_multi_insert(real_entire_payload_=entire_payload_[i * single_part:
                                                               ((i + 1) * single_part) +
                                                               ((entire_payload_.__len__() % dispatch_num)
                                                                if (i == (dispatch_num - 1)) else 0)])
        print(f"Submitted No.%d job, time elapse: %f" % (i, timeit.default_timer() - timer_))


if __name__ == '__main__':
    mysql_impl = normal_call()
    if mysql_impl is None:
        sys.exit()


    def handle_quit(signum, frame):
        if signum is not None:
            print("Quit log submission with KeyboardInterrupt.")
        if mysql_impl is not None:
            if mysql_impl.connection.is_connected():
                mysql_impl.connection.commit()
            mysql_impl.shutdown()
            sys.exit()

    for file_ in os.listdir("./logs/"):
        if "log" in file_:
            f = subprocess.Popen([f"grep \"payload\" ./logs/%s" % file_],
                                 shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            print(f"Analyzing log file: ./logs/%s" % file_)
            signal.signal(signal.SIGINT, handle_quit)
            signal.signal(signal.SIGTERM, handle_quit)

            entire_result = []
            while True:
                line = f.stdout.readline().decode('utf-8').strip()
                if line.__len__() == 0:
                    break
                cursor_st, time_sub, time_pub, thread_id, namespace, message_id, payload, pub_host, sub_host = line.split("|")
                if "go" not in line:
                    cursor_st_u = cursor_st.replace("DEBUG:root:", "")
                else:
                    cursor_st_u = cursor_st.split("DEBUG:root:")[-1]

                sub_host = sub_host.split("\"")[0]
                time_pub = d_parser.isoparse(time_pub)
                time_sub = d_parser.isoparse(time_sub)

                delta = time_sub - time_pub
                result = {
                    "PULSAR_PUB_TIMESTAMP": f"%s" % time_pub,
                    "PULSAR_SUB_TIMESTAMP": f"%s" % time_sub,
                    "PAYLOAD_SPEC": f"%d" % payload.__len__(),
                    "COMMENTS": f"%s|%s|%s|%s" % (cursor_st_u, thread_id, namespace, message_id),
                    "PUB_HOST": f"%s" % pub_host,
                    "SUB_HOST": f"%s" % sub_host
                }
                entire_result.append(result)

            if entire_result.__len__():
                multi_insert(mysql_impl_=mysql_impl,
                             table_name_="TABLE_PULSAR_SPECS",
                             entire_payload_=entire_result,
                             commit_=True)

            print(f"Done log %s submission.\nTotal inserts: %d" % (file_, entire_result.__len__()))

    handle_quit(None, None)
