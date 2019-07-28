import os


class Cache(object):
    def __init__(self, cache_dir='/tmp'):
        if not os.path.exists(cache_dir):
            os.mkdir(cache_dir)

        self.cache_dir = cache_dir

    def put(self, md5, data):
        with open(f'{self.cache_dir}/{md5}', 'wb') as f:
            f.write(data)

    def get(self, md5):
        path = f'{self.cache_dir}/{md5}'
        if not os.path.exists(path):
            return

        with open(path, 'rb') as f:
            return f.read()


if __name__ == '__main__':
    c = Cache(cache_dir='/tmp/sia-s3-proxy-cache')
    c.put('iwjef', b'testtest')
    print(c.get('iwjef'))
