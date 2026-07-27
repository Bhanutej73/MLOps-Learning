import time

import redis
from flask import Flask

app = Flask(__name__)
cache = redis.Redis(host='redis', port=6379)

def get_hit_count():
    retries = 5
    while True:
        try:
            return cache.incr('hits')
        except redis.exceptions.ConnectionError as exc:
            retries -= 1
            if retries == 0:
                raise exc
            time.sleep(0.5)

@app.route('/')
def hello():
    count = get_hit_count()
    return 'Hello Bhanu! I have been seen {} times. \n'.format(count)