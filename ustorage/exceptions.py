# -*- coding: utf-8 -*-
from __future__ import unicode_literals


class UStorageException(Exception):
    '''Base class for all UStorage Exceptions'''
    pass


class FileNotFound(UStorageException):
    pass
