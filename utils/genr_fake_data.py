# -*- coding: UTF-8 -*-
# 向MySQL中插入假数据用于练习PySpark

from time import time
from datetime import datetime
from random import random, randint, shuffle
from hashlib import md5

from pymysql import Connection, connect

_SF = 'AB'
_S = list("abcdefghijklmnopqrstuvwxyz0123456789")


def _genr_fake_log(i: int) -> list:
    hsh = md5()
    acq_num = randint(0, 43)
    deq_num = randint(1, 20)
    acq_share = random()
    shuffle(_S)
    item_code = "".join(_S[:randint(6, 31)])
    status_flag = _SF[randint(0, 1)]
    logtime = datetime(
        2019, randint(1, 12), randint(1, 28), randint(0, 23), randint(0, 59), randint(0, 59)
    )
    logid = "%d%d%g%s%s%s" % (acq_num, deq_num, acq_share, item_code, status_flag, logtime) + str(i)
    hsh.update(logid.encode("utf-8"))
    logid = hsh.hexdigest()
    return [logid, acq_num, deq_num, acq_share, item_code, status_flag, logtime]


def main():
    conn = connect(
        host="localhost",
        user="root",
        password="z2026419",
        port=3306
    )
    num = 2564235
    batch_size = 50000
    table = "test.fakelog"
    columns = "LOGID, ACQ_NUM, DEQ_NUM, ACQ_SHARE, ITEM_CODE, STATUS_FLAG, LOGTIME"
    values_str = ", ".join(["%s" for s in columns.split(", ")])

    cursor = conn.cursor()
    insert_sql = "insert into %s (%s) values (%s);" % (table, columns, values_str)
    print("Insert SQL:\n%s\n num: %d" % (insert_sql, num))
    batch = []
    size = 0
    num_inserted = 0
    tic = time()
    for i in range(num):
        batch.append(_genr_fake_log(i))
        size += 1
        if size == batch_size:
            row_count = cursor.executemany(insert_sql, batch)
            num_inserted += row_count
            conn.commit()
            print("%d items inserted." % num_inserted)
            batch = []
            size = 0
    row_count = cursor.executemany(insert_sql, batch)
    num_inserted += row_count
    toc = time()
    print("Total %d items inserted.\n Done in %.3f secs" % (num_inserted, (toc - tic)))


if __name__ == '__main__':
    main()
