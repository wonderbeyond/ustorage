# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import codecs
import io
import logging

from contextlib import contextmanager

import boto3

from botocore.exceptions import ClientError

from ustorage.bases import BaseStorage
from ustorage.utils import files
from ustorage.utils import drop_none_values

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

    def exists(self, name):
        try:
            self.bucket.Object(name).load()
        except ClientError:
            return False
        return True

    @contextmanager
    def open(self, name, mode='r', encoding='utf8'):
        obj = self.bucket.Object(name)
        if 'r' in mode:
            f = obj.get()['Body']
            yield f if 'b' in mode else codecs.getreader(encoding)(f)
        else:  # mode == 'w'
            f = io.BytesIO() if 'b' in mode else io.StringIO()
            yield f
            obj.put(Body=f.getvalue())

    def read(self, name):
        obj = self.bucket.Object(name).get()
        return obj['Body'].read()

    def write(self, name, content):
        return self.bucket.put_object(**drop_none_values(dict(
            Key=name,
            Body=self.as_binary(content),
            ContentType=files.mime(name),
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

    def list_files(self):
        for f in self.bucket.objects.all():
            yield f.key

    def get_metadata(self, name):
        '''Fetch all availabe metadata'''
        obj = self.bucket.Object(name)
        checksum = 'md5:{0}'.format(obj.e_tag[1:-1])
        mime = obj.content_type.split(';', 1)[0] if obj.content_type else None
        return {
            'checksum': checksum,
            'size': obj.content_length,
            'mime': mime,
            'modified': obj.last_modified,
        }
