import pymysql

from pymysqlpool.pool import Pool
from typing import Dict, List, Tuple

import config


class MysqlClient:
    def __init__(self,
                 db_host: str,
                 db_user: str,
                 db_pw: str):
        self.conn_pool = Pool(
            user=db_user,
            password=db_pw,
            host=db_host,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
            max_size=10
        )

    def exec_sql(self, sql_text: str, *args) -> Tuple[List[Dict], int]:
        conn = self.conn_pool.get_conn()
        with conn.cursor() as cur:
            rows_affected = cur.execute(sql_text, *args)
            res = cur.fetchall()
        # release connection back to unused pool
        self.conn_pool.release(conn)
        return res, rows_affected


if __name__ == '__main__':
    client = MysqlClient(config.DB_HOST,
                         config.DB_USER,
                         config.DB_PASSWORD)
    # test read
    sql1 = 'select * from social_media_post.test where col1 = %s'
    res1 = client.exec_sql(sql1, [3])
    print(res1)
    # test write
    sql2 = 'insert into social_media_post.test (col1, col2) values (%s, %s);'
    a = ['23', 23]
    res2 = client.exec_sql(sql2, a)
