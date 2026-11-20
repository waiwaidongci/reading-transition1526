from redis import Redis
from ..config.settings import REDIS_URL
queue=Redis.from_url(REDIS_URL)
def ping():
    return queue.ping()
