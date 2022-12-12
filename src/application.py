import uuid

from flask import Flask, Response, jsonify, request
from flask_cors import CORS

import config
from utils import create_presigned_put_url
from database import MysqlClient
from posts_dao import PostsDAO

application = Flask(__name__)
CORS(application)

# set up database client and dao
db_client = MysqlClient(config.DB_HOST,
                        config.DB_USER,
                        config.DB_PASSWORD)
dao = PostsDAO(db_client)


@application.route('/', methods=['GET'])
def host():
    return {'Info': 'Social Media Post MicroService'}


@application.route('/health', methods=['GET'])
def check_health():
    return {'msg': 'Instance is healthy!'}


@application.route('/post/<post_id>', methods=['GET', 'DELETE'])
def get_post_by_post_id(post_id):
    if request.method == 'GET':
        res = dao.get_post_by_post_id(post_id)
        return jsonify({'data': res})
    else:
        rows_affected = dao.delete_post_by_post_id(post_id)
        if rows_affected > 0:
            return {'msg': 'successfully deleted post!'}
        else:
            return {'msg': 'failed to delete post.'}


@application.route('/post/<user_id>/user', methods=['GET'])
def get_post_by_user_id(user_id):
    res = dao.get_post_by_user_id(user_id)
    return jsonify({'data': res})


@application.route('/post/users', methods=['GET'])
def get_post_by_user_ids_within_window():
    req_args = request.args
    if 'user_list' not in req_args:
        return {}
    user_list = request.args.get('user_list').split(',')
    within_hours = int(request.args.get('window', default=72))
    res = dao.get_post_by_user_list_within_window(user_list, within_hours=within_hours)
    return jsonify({'data': res})


@application.route('/post', methods=['POST'])
def post():
    data = request.json
    r = dao.push_data(data)
    if r > 0:
        return {'msg': 'successfully uploaded data.'}
    else:
        return {'msg': 'Failed to upload data.'}


@application.route('/post/<post_id>', methods=['DELETE'])
def delete_post_by_post_id(post_id):
    r = dao.delete_post_by_post_id(post_id)
    if r > 0:
        return {'msg': 'successfully deleted data.'}
    else:
        return {'msg': 'Failed to delete data.'}


@application.route('/post/s3url', methods=['get'])
def get_s3_url():
    s3_url = create_presigned_put_url(bucket_name=config.s3_bucket_name,
                                      object_name=request.args.get('object_name'))
    if s3_url is not None:
        return {'s3Url': s3_url}
    else:
        return Response('Failed to get a s3 put url.', status=502, mimetype='text/plain')


@application.route('/post/generate_id', methods=['get'])
def generate_post_id():
    return {'id': str(uuid.uuid4())}


if __name__ == '__main__':
    application.run()
