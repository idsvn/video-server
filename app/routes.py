import os

from bson import ObjectId
from flask import request, Response, Blueprint, jsonify
from media import get_collection, validate_json
from bson import json_util
from .errors import bad_request
from media.video.video_editor import get_video_editor_tool
from flask import current_app as app
from pymongo import ReturnDocument

bp = Blueprint('projects', __name__)

SCHEMA_UPLOAD = {'media': {'type': 'binary'}}


@bp.route('/projects', methods=['POST'])
def create_video_editor():
    if request.method == 'POST':
        files = request.files.to_dict(flat=False)['media']
        user_agent = request.headers.environ['HTTP_USER_AGENT']
        return create_video(files, user_agent)


@bp.route('/projects/<path:video_id>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def process_video_editor(video_id):
    """Keep previous url for backward compatibility"""
    if request.method == 'GET':
        return get_video(video_id)
    if request.method == 'PUT':
        return update_video(video_id, request.form)
    if request.method == 'DELETE':
        return delete_video(video_id)


def delete_video(video_id):
    video = get_collection('video')
    video.remove({'_id': ObjectId(video_id)})
    return 'delete successfully'


def update_video(video_id, updates):
    video = get_collection('video')
    updated_video = video.find_one_and_update(
        {'_id': ObjectId(video_id)},
        {'$set': {'processing': True}},
        return_document=ReturnDocument.AFTER
    )
    req = request.get_json()
    if not req:
        return bad_request("invalid request")

    cut_request = req.get('cut')
    if not cut_request:
        return bad_request("action is not supported")

    video_editor = get_video_editor_tool('ffmpeg')
    current_path = os.path.dirname(os.path.abspath(__file__))
    fs_path = os.path.join(current_path, '..', 'media', 'video', 'fs')

    video_editor.edit_video(
        os.path.join(fs_path, 'sample.mp4'),
        os.path.join(fs_path, 'output.mp4'),
        ['-ss', cut_request['start'], '-t', cut_request['end']]
    )

    return Response(json_util.dumps(updated_video), status=200, mimetype='application/json')


def create_video(files, agent):
    """Save video to storage and create records to databases"""
    #: validate incoming data is a binary file
    if validate_json(SCHEMA_UPLOAD, files):
        return bad_request("file not found")
    client_name = agent[0].split('/')
    #: validate the user agent must be in a list support
    if client_name in app.config.get('AGENT_ALLOW'):
        return bad_request("client is not allow to edit")
    docs = []
    video_editor = get_video_editor_tool('ffmpeg')
    for file in files:
        metadata = video_editor.get_meta(file)
        #: validate codec must be support
        if metadata.get('codec_name') not in app.config.get('CODEC_SUPPORT'):
            return bad_request("codec is not support")
        video_editor.create_video(file)
        doc = {
            'metadata': None,
            'client_info': agent,
            'version': 1,
            'processing': False,
            "parent": None,
            'thumbnails': {}
        }
        docs.append(doc)
    video = get_collection('video')
    video.insert_many(docs)
    return Response(json_util.dumps(docs), status=200, mimetype='application/json')


def get_video(video_id):
    """Keep previous url for backward compatibility"""
    video = get_collection('video')
    items = list(video.find())
    for item in items:
        item['_id'] = str(item['_id'])
    return items
