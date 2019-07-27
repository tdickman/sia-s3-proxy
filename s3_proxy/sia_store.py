from datetime import datetime
import hashlib
import io
import pickledb
import time

from .errors import BucketNotEmpty, NoSuchBucket, NoSuchKey
from .models import Bucket, BucketQuery, S3Item
from .sia import Sia


class SiaStore(object):
    def __init__(self, base_dir, sia_password=None):
        self.sia = Sia(sia_password=sia_password)
        self.base_dir = base_dir
        self.buckets = self.get_all_buckets()
        self.md5_cache = pickledb.load('md5-cache.db', False)

    def _pre_exit(self):
        self.md5_cache.dump()

    def _md5(self, bucket_name, key):
        """Get md5 from cache, otherwise retrieve file and recalculate."""
        md5 = self.md5_cache.get(f'{bucket_name}/{key}')

        if not md5:
            md5 = self.get_item(bucket_name, key).md5

        return md5

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

    def _block_until_uploaded(self, bucket_name, item_name, timeout_seconds=60):
        uploaded = False
        attempts = 0
        key = f'{self.base_dir}/{bucket_name}/{item_name}'
        while not uploaded:
            uploaded = self.sia.get_file_status(key)['available']
            time.sleep(1)
            attempts += 1
            if attempts > timeout_seconds:
                raise Exception("File failed to fully upload")

    def store_data(self, bucket, item_name, headers, data):
        m = hashlib.md5()
        m.update(data)
        key = f'{self.base_dir}/{bucket.name}/{item_name}'

        # Treat 0 byte files as folders (since that's how s3 treats folders)
        if len(data) == 0:
            self.sia.create_folder(key)
        else:
            self.sia.upload_file(key, data)
            md5 = m.hexdigest()
            self.md5_cache.set(f'{bucket.name}/{item_name}', md5)
        return S3Item(item_name, md5=md5)

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
        md5 = m.hexdigest()
        self.md5_cache.set(key, md5)

        item = S3Item(
            key,
            md5=md5,
            size=details['filesize'],
            modified_date=details['modtime'].rsplit('.')[0] + '.000Z',
            # Make up a content_type - this may break some clients
            content_type='unknown',
        )
        item.io = io.BytesIO(data)

        return item

    def delete_item(self, bucket_name, item_name):
        # s3 doesn't differentiate between files and folders, but sia does. If
        # file deletion fails, assume it was a folder, and delete that. Side
        # note: If you create files and folders with the same name within Sia,
        # this can cause weird situations in s3.
        path = f'{self.base_dir}/{bucket_name}/{item_name}'
        try:
            self.sia.delete_file(path)
            self.md5_cache.rem(f'{bucket_name}/{item_name}')
        except Exception:
            self.sia.delete_folder(path)

    def get_all_keys(self, bucket, **kwargs):
        max_keys = int(kwargs['max_keys'])
        prefix = kwargs.get('prefix')
        delimiter = kwargs.get('delimiter', '')
        if delimiter not in set(['/', '']):
            raise Exception('Delimiter only supports / or `` currently')

        is_truncated = False
        matches = []
        common_prefixes = []
        directories_to_walk = [f'{prefix}']
        walked_directories = set(directories_to_walk)
        
        while len(directories_to_walk) > 0:
            path = directories_to_walk.pop(-1)

            try:
                results = self.sia.list(f'{self.base_dir}/{bucket.name}/{path}')
            except Exception:
                raise NoSuchKey()

            for file_details in results['files']:
                key = file_details['siapath'].lstrip(f'{self.base_dir}/{bucket.name}')

                matches.append(S3Item(
                    key,
                    md5=self._md5(bucket.name, key),
                    modified_date=file_details['modtime'][:-10] + '000Z',
                    size=file_details['filesize'],
                ))

            for dir_details in results['directories']:
                directory = dir_details['siapath']
                path = directory.lstrip(f'{self.base_dir}/{bucket.name}') + '/'

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
