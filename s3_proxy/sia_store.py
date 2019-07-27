from datetime import datetime
import hashlib
import io

from .errors import BucketNotEmpty, NoSuchBucket, NoSuchKey
from .models import Bucket, BucketQuery, S3Item
from .sia import Sia


class SiaStore(object):
    def __init__(self, base_dir, sia_password=None):
        self.sia = Sia(sia_password=sia_password)
        self.base_dir = base_dir
        self.buckets = self.get_all_buckets()

    def get_all_buckets(self):
        buckets = []

        for directory in self.sia.list(self.base_dir)['directories']:

            # Skip parent directory
            if directory['siapath'] == self.base_dir:
                continue

            # Use modified time since created isn't available
            create_date = datetime.strptime(directory['mostrecentmodtime'][:-4], '%Y-%m-%dT%H:%M:%S.%f')
            path = directory['siapath'].lstrip(f'{self.base_dir}/')
            buckets.append(Bucket(path, create_date))

        return buckets

    def get_bucket(self, bucket_name):
        for bucket in self.buckets:
            if bucket.name == bucket_name:
                return bucket

    def create_bucket(self, bucket_name):
        if bucket_name not in [bucket.name for bucket in self.buckets]:
            self.sia.create_folder(f'{self.base_dir}/{bucket_name}')
            self.buckets = self.get_all_buckets()

        return self.get_bucket(bucket_name)

    def delete_bucket(self, bucket_name):
        bucket = self.get_bucket(bucket_name)
        if not bucket:
            raise NoSuchBucket

        try:
            self.sia.delete_folder(f'{self.base_dir}/{bucket_name}')
        except:
            raise BucketNotEmpty

    def store_data(self, bucket, item_name, headers, data):
        m = hashlib.md5()
        m.update(data)
        key = f'{self.base_dir}/{bucket.name}/{item_name}'
        self.sia.upload_file(key, data)
        return S3Item(key, md5=m.hexdigest())

    def store_item(self, bucket, item_name, handler):
        size = int(handler.headers['content-length'])
        data = handler.rfile.read(size)
        return self.store_data(bucket, item_name, {}, data)

    def get_item(self, bucket_name, item_name):
        key = f'{bucket_name}/{item_name}'
        (details, data) = self.sia.get_file(f'{self.base_dir}/{key}')

        if not details['available']:
            raise NoSuchKey()

        m = hashlib.md5()
        m.update(data)
        item = S3Item(
            key,
            md5=m.hexdigest(),
            size=details['filesize'],
            modified_date=details['modtime'].rsplit('.')[0] + '.000Z',
            # Make up a content_type - this may break some clients
            content_type='unknown',
        )
        item.io = io.BytesIO(data)

        return item

    def delete_item(self, bucket_name, item_name):
        self.sia.delete_file(f'{self.base_dir}/{bucket_name}/{item_name}')

    def get_all_keys(self, bucket, **kwargs):
        max_keys = int(kwargs['max_keys'])
        prefix = kwargs.get('prefix')
        delimiter = kwargs.get('delimiter')
        if delimiter not in set(['/', '']):
            raise Exception('Delimiter only supports / or `` currently')

        is_truncated = False
        matches = []
        common_prefixes = []
        directories_to_walk = [f'{prefix}']
        walked_directories = set(directories_to_walk)
        
        while len(directories_to_walk) > 0:
            path = directories_to_walk.pop(-1)

            results = self.sia.list(f'{self.base_dir}/{bucket.name}/{path}')

            for file_details in results['files']:
                key = file_details['siapath'].lstrip(f'{self.base_dir}/{bucket.name}')

                matches.append(S3Item(
                    key,
                    # TODO: Fake md5 for now
                    md5=b'098f6bcd4621d373cade4e832627b4f6',
                    creation_date=file_details['createtime'][:-10] + '000Z',
                    size=file_details['filesize'],
                ))

            for dir_details in results['directories']:
                directory = dir_details['siapath']
                print(directory)
                path = directory.lstrip(f'{self.base_dir}/{bucket.name}') + '/'
                print("path: " + path)

                if path in walked_directories or path == '/':
                    continue

                # Check for common prefixes
                if delimiter == '/':
                    common_prefixes.append(path)
                elif path not in walked_directories:
                    directories_to_walk.append(path)
                    walked_directories.add(path)

            if len(matches) >= max_keys:
                is_truncated = True
                break

        return BucketQuery(bucket, matches, is_truncated, common_prefixes, **kwargs)
