# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import codecs
import io
import logging

from contextlib import contextmanager

import boto3

from botocore.exceptions import ClientError

from ustorage import BaseStorage
from ustorage.utils import files

log = logging.getLogger(__name__)


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

    def create_bucket(self):
        try:
            self.bucket.create()
        except self.s3.meta.client.exceptions.BucketAlreadyOwnedByYou:
            pass

    def exists(self, filename):
        try:
            self.bucket.Object(filename).load()
        except ClientError:
            return False
        return True

    @contextmanager
    def open(self, filename, mode='r', encoding='utf8'):
        obj = self.bucket.Object(filename)
        if 'r' in mode:
            f = obj.get()['Body']
            yield f if 'b' in mode else codecs.getreader(encoding)(f)
        else:  # mode == 'w'
            f = io.BytesIO() if 'b' in mode else io.StringIO()
            yield f
            obj.put(Body=f.getvalue())

    def read(self, filename):
        obj = self.bucket.Object(filename).get()
        return obj['Body'].read()

    def write(self, filename, content):
        return self.bucket.put_object(
            Key=filename,
            Body=self.as_binary(content),
            ContentType=files.mime(filename),
        )

    def delete(self, filename):
        for obj in self.bucket.objects.filter(Prefix=filename):
            obj.delete()

    def copy(self, filename, target):
        src = {
            'Bucket': self.bucket.name,
            'Key': filename,
        }
        self.bucket.copy(src, target)

    def list_files(self):
        for f in self.bucket.objects.all():
            yield f.key

    def get_metadata(self, filename):
        '''Fetch all availabe metadata'''
        obj = self.bucket.Object(filename)
        checksum = 'md5:{0}'.format(obj.e_tag[1:-1])
        mime = obj.content_type.split(';', 1)[0] if obj.content_type else None
        return {
            'checksum': checksum,
            'size': obj.content_length,
            'mime': mime,
            'modified': obj.last_modified,
        }
