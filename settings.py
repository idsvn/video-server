import json
import os
from distutils.util import strtobool as _strtobool


def strtobool(value):
    try:
        return bool(_strtobool(value))
    except ValueError:
        return False


def env(variable, fallback_value=None):
    if os.environ.get('VIDEOSERVER_USE_DEFAULTS'):
        return fallback_value

    env_value = os.environ.get(variable)
    if env_value is None:
        return fallback_value
    # Next isn't needed anymore
    elif env_value == "__EMPTY__":
        return ''
    else:
        return env_value


def celery_queue(name):
    """Get celery queue name with optional prefix set in environment.

    If you want to use multiple workers in Procfile you have to use the prefix::

        work_publish: celery -A worker -Q "${SUPERDESK_CELERY_PREFIX}publish" worker
        work_default: celery -A worker worker

    :param name: queue name
    """
    return "{}{}".format(os.environ.get('VIDEOSERVER_CELERY_PREFIX', ''), name)


# base path
BASE_PATH = os.path.dirname(__file__)

#: logging
LOG_CONFIG_FILE = env('LOG_CONFIG_FILE', 'logging_config.yml')

CORE_APPS = [
    'apps.swagger',
    'apps.projects',
]

PORT = env('PORT', '5050')

#: Mongo host port
MONGO_HOST = env('MONGO_HOST', 'localhost')
MONGO_PORT = env('MONGO_PORT', 27017)
MONGO_DBNAME = env('MONGO_DBNAME', 'sd_video_editor')
MONGO_URI = "mongodb://{host}:{port}/{dbname}".format(
    host=MONGO_HOST, port=MONGO_PORT, dbname=MONGO_DBNAME
)

#: rabbit-mq url
RABBIT_MQ_URL = env('RABBIT_MQ_URL', 'pyamqp://guest@localhost//')

#: celery broker
BROKER_URL = env('CELERY_BROKER_URL', RABBIT_MQ_URL)
CELERY_BROKER_URL = BROKER_URL
#: number retry when task fail
NUMBER_RETRY = env('NUMBER_RETRY', 3)
BROKER_CONNECTION_MAX_RETRIES = NUMBER_RETRY

#: allow agent
AGENT_ALLOW = json.loads(env('AGENT_ALLOW', '["superdesk", "python-requests" ,"postmanruntime", "mozilla"]'))
#: Codec support
CODEC_SUPPORT = json.loads(env('CODEC_SUPPORT', '["vp8", "vp9", "h264", "theora", "av1"]'))

#: media storage
MEDIA_STORAGE = env('MEDIA_STORAGE', 'filesystem')
default_path = os.path.join(BASE_PATH, 'media', 'projects')
FS_MEDIA_STORAGE_PATH = env('FS_MEDIA_STORAGE_PATH', default_path)

#: media tool
DEFAULT_MEDIA_TOOL = env('DEFAULT_MEDIA_TOOL', 'ffmpeg')

VIDEO_SERVER_URL = env('VIDEO_SERVER_URL', 'http://localhost:5050/projects')
VIDEO_MEDIA_PREFIX = env('VIDEO_MEDIA_PREFIX', '%s/url_raw' % VIDEO_SERVER_URL.rstrip('/'))

#: pagination, items per page
ITEMS_PER_PAGE = env('ITEMS_PER_PAGE', 25)
