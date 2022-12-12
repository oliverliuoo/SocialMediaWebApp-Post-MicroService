import logging
from datetime import datetime, timedelta
from typing import Dict, List

import config
import utils
from database import MysqlClient

# config logger
logging.basicConfig(filename=config.log_file, level=logging.INFO)
logger = logging.getLogger(__name__)


class PostsDAO:

    def __init__(self, db_client: MysqlClient):
        self.db_client = db_client

    def get_post_by_post_id(self, post_id) -> List[Dict]:
        sql = 'select * from social_media_post.posts where post_id = %s'
        results = []
        try:
            results, rows_affected = self.db_client.exec_sql(sql, post_id)
            if rows_affected > 0:
                logger.info('successfully retrieved record with post id %s.', post_id)
            else:
                logger.warning('record with post id %s does not exist.', post_id)
        except Exception as e:
            logger.exception('failed to retrieve post record.', exc_info=e)
        finally:
            return results

    def get_post_by_user_id(self, user_id) -> List[Dict]:
        sql = 'select * from social_media_post.posts ' \
              'where user_id = %s ' \
              'order by time_stamp desc'
        results = []
        try:
            results, rows_affected = self.db_client.exec_sql(sql, user_id)
            if rows_affected > 0:
                logger.info('successfully retrieved %s records with user id %s.', rows_affected, user_id)
            else:
                logger.warning('failed to retrieve records for user id %s.', user_id)
        except Exception as e:
            logger.exception('failed to retrieve post record.', exc_info=e)
        finally:
            return results

    def get_post_by_user_id_within_window(self, user_id, within_hours: int):
        after_ts = datetime.now() - timedelta(hours=within_hours)
        sql = 'select * from social_media_post.posts ' \
              'where user_id = %s and time_stamp > \"{}\" ' \
              'order by time_stamp desc'.format(after_ts.strftime('%Y-%m-%d %H:%M:%S'))
        results = []
        try:
            results, rows_affected = self.db_client.exec_sql(sql,
                                                             user_id)
            if rows_affected > 0:
                logger.info('successfully retrieved %s records with user id %s.', rows_affected, user_id)
            else:
                logger.warning('failed to retrieve records for user id %s.', user_id)
        except Exception as e:
            logger.exception('failed to retrieve post record.', exc_info=e)
        finally:
            return results

    def get_post_by_user_list_within_window(self, user_list, within_hours: int = 24):
        results = []
        for user in user_list:
            results += self.get_post_by_user_id_within_window(user, within_hours)
        results.sort(key=lambda record: record['time_stamp'], reverse=True)
        return results

    def push_data(self, record: Dict) -> int:
        rows_affected = 0
        # record time_stamp
        record['time_stamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            sql = "insert into social_media_post.posts ({col_names}) values ({values_holder})" \
                .format(col_names=','.join(record.keys()),
                        values_holder=','.join(['%s'] * len(record)))
            _, rows_affected = self.db_client.exec_sql(sql, list(record.values()))
            if rows_affected > 0:
                logger.info('successfully uploaded %s record.', rows_affected)
            else:
                logger.warning('failed to upload record.')
        except Exception as e:
            print(e)
        finally:
            return rows_affected

    def delete_post_by_post_id(self, post_id) -> int:
        sql = 'delete from social_media_post.posts where post_id = %s'
        rows_affected = 0
        try:
            _, rows_affected = self.db_client.exec_sql(sql, post_id)
            if rows_affected > 0:
                utils.delete_s3_object(config.s3_bucket_name, post_id)
                logger.info('successfully deleted record with post id %s.', post_id)
            else:
                logger.warning('record with post id %s does not exist.', post_id)
        except Exception as e:
            logger.exception('failed to delete post record.', exc_info=e)
        finally:
            return rows_affected


if __name__ == '__main__':
    mysql_client = MysqlClient(config.DB_HOST,
                               config.DB_USER,
                               config.DB_PASSWORD)
    posts_dao = PostsDAO(mysql_client)
    data = {
        'user_id': 'test_user_id_1',
        'post_id': 'test_post_id_1',
        'photo_url': 'test_url_1',
        'post_text': 'sample_post_text',
        'time_stamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    # test upload data
    r = posts_dao.push_data(data)
    print(r)
    # test delete data
    r = posts_dao.delete_post_by_post_id('test_post_id_1')
    # test get data
    res = posts_dao.get_post_by_post_id('test_post_id_1')
    print(res)
    # test get data by user id
    res = posts_dao.get_post_by_user_id_within_window('hl3518', 24)
    print(res)
