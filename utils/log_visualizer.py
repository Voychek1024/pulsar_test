#!/path/to/bin/python
import csv
from datetime import datetime, timedelta
import numpy as np
from mysql_connector import *


def single_query(mysql_impl_: MySQLConnector, start_: datetime, end_: datetime) -> list:
    parsed_result = []
    if mysql_impl_ is not None:
        if mysql_impl_.health_check():
            mysql_impl_.cursor.execute(
                "select microsecond(timediff(PULSAR_SUB_TIMESTAMP, PULSAR_PUB_TIMESTAMP)) as LATENCY "
                "from TABLE_PULSAR_SPECS where PULSAR_SUB_TIMESTAMP>=%s and PULSAR_SUB_TIMESTAMP<%s",
                (start_, end_))
            result_ = mysql_impl_.cursor.fetchall()
            for row_ in result_:
                parsed_result.append(row_[0])
    return parsed_result


def check_unique(mysql_impl_: MySQLConnector, start_: datetime, end_: datetime) -> bool:
    payload_spec = []
    comments = []

    if mysql_impl_ is not None:
        if mysql_impl_.health_check():
            mysql_impl_.cursor.execute(
                "select PAYLOAD_SPEC, COMMENTS from pulsar.TABLE_PULSAR_SPECS "
                "where PULSAR_SUB_TIMESTAMP>=%s and PULSAR_SUB_TIMESTAMP<=%s",
                (start_, end_))
            result_ = mysql_impl_.cursor.fetchall()
            for row_ in result_:
                payload_spec.append(row_[0])
                comments.append(row_[1])
            if set(payload_spec).__len__() == 1:
                if set(comments).__len__() == result_.__len__():
                    print(f"CHECKED %d RECORDS" % result_.__len__())
                    return True
                print("COMMENTS not unique!")
                return False
            print("PAYLOAD_SPEC not stable!")
            return False
    print("Unexpected err.")
    return False


def calc_result(list_: list):
    try:
        return list_.__len__(), np.percentile(list_, 50), np.percentile(list_, 99)
    except IndexError:
        return list_.__len__(), -1, -1


if __name__ == '__main__':
    mysql_impl = normal_call()
    if (mysql_impl is None) or (not mysql_impl.health_check()):
        print("Cannot instantiate mysql_impl, exit.")
        sys.exit()


    def handle_quit():
        if mysql_impl is not None:
            mysql_impl.shutdown()
            sys.exit()

    start_time_raw = str(input("Enter start_sub_time:"))
    end_time_raw = str(input("Enter end_sub_time:"))
    start_time = datetime.strptime(start_time_raw, "%Y-%m-%d-%H.%M.%S")
    end_time = datetime.strptime(end_time_raw, "%Y-%m-%d-%H.%M.%S")

    if not check_unique(mysql_impl_=mysql_impl, start_=start_time, end_=end_time):
        # sys.exit(0)
        print("UNIQUE CHECKLIST FAILED...")
    else:
        print("UNIQUE CHECKLIST SUCCESSFUL...")

    total_tps = {}
    total_p50 = {}
    total_p99 = {}
    ignore_ = True

    while start_time != end_time:
        start_time += timedelta(seconds=1)
        single_tps, single_p50, single_p99 = calc_result(single_query(mysql_impl_=mysql_impl,
                                                                      start_=start_time,
                                                                      end_=start_time + timedelta(seconds=1)))

        if single_tps == 0 and single_p50 == -1 and single_p99 == -1 and ignore_:
            pass
        else:
            ignore_ = False
            total_tps[start_time] = single_tps
            total_p50[start_time] = single_p50
            total_p99[start_time] = single_p99

    with open(f"./result/query_result-%s-%s.csv" % (start_time_raw, end_time_raw), 'w', newline='\n', encoding='utf-8') as out_file:
        writer = csv.writer(out_file)
        writer.writerow(("TIMESTAMP", "TPS", "P50(ms)", "P99(ms)"))
        writer.writerows(list(zip(total_tps.keys(), list(total_tps.values()), list(map(lambda r: r/1000, total_p50.values())), list(map(lambda r: r/1000, total_p99.values())))))

    handle_quit()
