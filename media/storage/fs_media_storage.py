import logging
import tempfile
import bson
import os.path
from media import get_collection
from pymongo import ReturnDocument

logger = logging.getLogger(__name__)
PATH_FS = os.path.dirname(__file__) + '/fs'


def format_id(_id):
    try:
        return bson.ObjectId(_id)
    except bson.errors.InvalidId:
        return _id


class MediaStorageFS():
    pass


class FileSystemMediaStorage(MediaStorageFS):
    def get(self, _id):
        """
        Get a stream file in storage
        :param _id:
        :return:
        """
        logger.debug('Getting media file with id= %s' % _id)
        _id = format_id(_id)
        try:
            file_name = get_collection('video').find_one({"_id": _id}).get('file_name')
            media_file = (open("%s/%s" % (PATH_FS, file_name), 'r+')).read()
        except Exception:
            media_file = None
        return media_file

    def put(self, content, filename, version=1, client_info=None, parent=None, metadata=None, folder=None, **kwargs):
        """
        Put a file into storage
        Create record for this file

        :param content:
        :param filename:
        :param version:
        :param client_info:
        :param parent:
        :param metadata:
        :param folder:
        :param kwargs:
        :return:
        """
        if folder:
            if folder[-1] == '/':
                folder = folder[:-1]
            if filename:
                filename = '{}/{}'.format(folder, filename)
        try:
            with open("%s/%s" % (PATH_FS, filename), "wb") as f:
                f.write(content.read())
            doc = {
                'filename': filename,
                'metadata': metadata,
                'client_info': client_info,
                'version': version,
                'processing': False,
                "parent": parent,
                'thumbnails': {}
            }
            for k, v in kwargs:
                doc[k] = v
            get_collection('video').insert_one(doc)
            return doc
        except Exception as ex:
            logger.info('File filename=%s error ex:' % (filename, ex))

    def edit(self, _id, stream, client_info=None, metadata=None, **kwargs):
        _id = format_id(_id)
        logger.debug('Getting media file with id= %s' % _id)
        video_collection = get_collection('video')
        updated_video = video_collection.find_one_and_update(
            {'_id': _id},
            {'$set': {
                'metadata': metadata,
                'processing': True,
                'client_info': client_info,
            }},
            return_document=ReturnDocument.AFTER
        )

        path_file = os.path.join(PATH_FS, updated_video.filename)
        if os.path.exists(path_file):
            os.remove(path_file)

        with open(path_file, 'wr') as f:
            f.write(stream.read())

        return updated_video

    def delete(self, _id):
        logger.debug('Getting media file with id= %s' % _id)
        _id = format_id(_id)
        try:
            video_collection = get_collection('video')
            file_name = video_collection.find_one({"_id": _id}).get('file_name')
            os.remove("%s/%s" % (PATH_FS, file_name))
            video_collection.remove({'id': _id})
        except Exception:
            media_file = None
        return media_file
