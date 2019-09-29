# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import codecs
import tempfile
import io
import logging

from contextlib import contextmanager

import boto3

from botocore.exceptions import ClientError

from ustorage.exceptions import FileNotFound
from ustorage.bases import BaseStorage
from ustorage.utils import files
from ustorage.utils import drop_none_values
from ustorage.utils import CaseInsensitiveDict

log = logging.getLogger(__name__)


def _is_no_such_key_error(e):
    if isinstance(e, ClientError):
        if e.__class__.__name__ == 'NoSuchKey':
            return True
        if 'not found' in e.message.lower():
            return True
    return False


def _extract_metadata(obj):
    '''Extract metadata from s3 object'''
    try:
        checksum = 'md5:{0}'.format(obj.e_tag[1:-1])
        mime = obj.content_type.split(';', 1)[0] if obj.content_type else None
    except ClientError as e:
        if _is_no_such_key_error(e):
            raise FileNotFound("{} not found.".format(obj.key))
        raise
    meta = {
        'checksum': checksum,
        'size': obj.content_length,
        'mime': mime,
        'modified': obj.last_modified,
    }
    meta.update(obj.metadata)
    return meta


class S3Storage(BaseStorage):
    '''
    An Amazon S3 Backend (compatible with any S3-like API)

    Expect the following settings:

    - `endpoint`: The S3 API endpoint
    - `region`: The region to work on.
    - `access_key`: The AWS credential access key
    - `secret_key`: The AWS credential secret key
    '''
    DEFAULT_CONFIG = dict(
        endpoint=None,
        region=None,
        access_key=None,
        secret_key=None,
    )

    def __init__(self, config):
        super(S3Storage, self).__init__(config)
        config = self.config

        self._aws_session = aws_session = boto3.session.Session()
        self._s3_config = s3_config = boto3.session.Config(
            signature_version='s3v4'
        )

        self.s3 = aws_session.resource('s3',
                                       config=s3_config,
                                       endpoint_url=config.endpoint,
                                       region_name=config.region,
                                       aws_access_key_id=config.access_key,
                                       aws_secret_access_key=config.secret_key)
        self.bucket = self.s3.Bucket(config.bucket)
        self.backend_exceptions = self.s3.meta.client.exceptions

    def create_bucket(self):
        try:
            self.bucket.create()
        except self.s3.meta.client.exceptions.BucketAlreadyOwnedByYou:
            pass

    def exists(self, name):
        try:
            self.bucket.Object(name).load()
        except ClientError as e:
            if _is_no_such_key_error(e):
                return False
            raise
        return True

    def get(self, name):
        """Return a pair of body+metadata"""
        obj = self.bucket.Object(name)
        try:
            return (obj.get()['Body'].read(), _extract_metadata(obj))
        except ClientError as e:
            if _is_no_such_key_error(e):
                raise FileNotFound("{} not found.".format(name))
            raise

    @contextmanager
    def open(self, name, mode='r', encoding='utf8'):
        """
        Returns a file-like object.

        Example:

            with storage.open('test.json', 'w') as f:
                json.dump({'a': 1}, f)

            with storage.open('test.json') as f:
                print(json.load(f))

        :param mode: gives "b" flag if of binary object,
                     gives "w" for writting.
        """
        obj = self.bucket.Object(name)
        if 'r' in mode:
            try:
                f = obj.get()['Body']
            except ClientError as e:
                if _is_no_such_key_error(e):
                    raise FileNotFound("{} not found.".format(name))
                raise
            yield f if 'b' in mode else codecs.getreader(encoding)(f)
        else:  # mode == 'w'
            f = tempfile.SpooledTemporaryFile()
            yield f
            f.seek(0)
            obj.put(Body=f.read())
            f.close()

    def read(self, name):
        try:
            obj = self.bucket.Object(name).get()
        except ClientError as e:
            if _is_no_such_key_error(e):
                raise FileNotFound("{} not found.".format(name))
            raise
        return obj['Body'].read()

    def write(self, name, content, metadata=None):
        metadata = CaseInsensitiveDict(metadata)

        text_type = type('')
        for k, v in metadata.items():
            metadata[k] = text_type(v)

        if 'Content-Type' not in metadata:
            metadata.update({
                'Content-Type': files.mime(name)
            })

        return self.bucket.put_object(**drop_none_values(dict(
            Key=name,
            Body=self.as_binary(content),
            ContentType=metadata.pop('Content-Type', None),
            Metadata=dict(drop_none_values(metadata)),
        )))

    def delete(self, name):
        for obj in self.bucket.objects.filter(Prefix=name):
            obj.delete()

    def copy(self, name, target):
        src = {
            'Bucket': self.bucket.name,
            'Key': name,
        }
        self.bucket.copy(src, target)

    def list_files(self, prefix=''):
        for f in self.bucket.objects.filter(Prefix=prefix):
            yield f.key

    def get_metadata(self, name):
        '''Fetch all availabe metadata'''
        obj = self.bucket.Object(name)
        return _extract_metadata(obj)

    def get_url(self, name, parameters=None, expire=3600):
        params = parameters.copy() if parameters else {}
        params['Bucket'] = self.bucket.name
        params['Key'] = name
        return self.s3.meta.client.generate_presigned_url(
            'get_object', Params=params, ExpiresIn=expire
        )
