# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import six
import os.path

from ustorage.utils import get_random_string
from ustorage.utils import files


class Config(dict):
    '''
    Wrap the configuration for a single :class:`Storage`.
    Basically, it's an ObjectDict
    '''
    def __getattr__(self, name):
        if name in self:
            return self[name]
        else:
            raise AttributeError('Unknown attribute: ' + name)

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        if name in self:
            del self[name]
        else:
            raise AttributeError('Unknown attribute: ' + name)


class BaseStorage(object):
    '''
    Abstract class to implement a storage backend.
    '''
    root = None
    DEFAULT_MIME = 'application/octet-stream'
    DEFAULT_CONFIG = {}

    def __init__(self, config):
        """
        Initialize a storage

        :param config: A dict-like configuration container.
        """
        self.config = Config(self.DEFAULT_CONFIG, **config)

    def exists(self, name):
        '''Test wether a file exists or not given its name in the storage'''
        raise NotImplementedError('Existance checking is not implemented')

    def open(self, name, *args, **kwargs):
        '''Open a file given its name relative to the storage root'''
        raise NotImplementedError('Open operation is not implemented')

    def read(self, name):
        '''Read a file content given its name in the storage'''
        raise NotImplementedError('Read operation is not implemented')

    def write(self, name, content):
        '''Write content into a file given its name in the storage'''
        raise NotImplementedError('Write operation is not implemented')

    def delete(self, name):
        '''Delete a file given its name in the storage'''
        raise NotImplementedError('Delete operation is not implemented')

    def copy(self, name, target):
        '''Copy a file given its name to another path in the storage'''
        raise NotImplementedError('Copy operation is not implemented')

    def move(self, name, target):
        '''
        Move a file given its name to another path in the storage

        Default implementation perform a copy then a delete.
        Backends should overwrite it if there is a better way.
        '''
        self.copy(name, target)
        self.delete(name)

    def save(self, file_or_wfs, name, overwrite=False):
        '''
        Save a file-like object or a `werkzeug.FileStorage` with the specified name.

        :param storage: The file or the storage to be saved.
        :param name: The destination in the storage.
        :param overwrite: if `False`, raise an exception if file exists in storage

        :raises FileExists: when file exists and overwrite is `False`
        '''
        self.write(name, file_or_wfs.read())
        return name

    def metadata(self, name):
        '''
        Fetch all available metadata for a given file
        '''
        meta = self.get_metadata(name)
        # Fix backend mime misdetection
        meta['mime'] = meta.get('mime') or files.mime(name, self.DEFAULT_MIME)
        return meta

    def get_metadata(self, name):
        '''
        Backend specific method to retrieve metadata for a given file
        '''
        raise NotImplementedError('Copy operation is not implemented')

    def as_binary(self, content, encoding='utf8'):
        '''Perform content encoding for binary write'''
        if hasattr(content, 'read'):
            return content.read()
        elif isinstance(content, six.text_type):
            return content.encode(encoding)
        else:
            return content

    def get_available_name(self, name):
        """
        Returns a name that's free on the target storage system, and
        available for new content to be written to.
        """
        dir_name, file_name = os.path.split(name)
        file_root, file_ext = os.path.splitext(file_name)
        # If the name already exists, add an underscore and a random 7
        # character alphanumeric string (before the file extension, if one
        # exists) to the name until the generated name doesn't exist.
        while self.exists(name):
            # file_ext includes the dot.
            name = os.path.join(
                dir_name,
                '{}_{}{}'.format(file_root, get_random_string(7), file_ext))
        return name
