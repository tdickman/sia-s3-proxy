class Bucket(object):
    def __init__(self, name, creation_date):
        self.name = name
        self.creation_date = creation_date


class BucketQuery(object):
    def __init__(self, bucket, matches=[], is_truncated=False, common_prefixes=[], **kwargs):
        self.bucket = bucket
        self.matches = matches
        self.common_prefixes = common_prefixes
        self.is_truncated = is_truncated
        self.marker = kwargs['marker']
        self.prefix = kwargs['prefix']
        self.max_keys = kwargs['max_keys']
        self.delimiter = kwargs['delimiter']


class S3Item(object):
    def __init__(self, key, **kwargs):
        self.key = key
        if 'content_type' in kwargs:
            self.content_type = kwargs['content_type']
        self.md5 = kwargs['md5']
        if 'size' in kwargs:
            self.size = kwargs['size']
        if 'modified_date' in kwargs:
            self.modified_date = kwargs['modified_date']
