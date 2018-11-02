UStorage
========

Unified Storage Interface for Python.
Copied and decoupled from [Flask-FS](https://github.com/noirbizarre/flask-fs) for working anywhere.

Thanks to [Flask-FS](https://github.com/noirbizarre/flask-fs)

See also [django-storages](https://github.com/jschneier/django-storages)

## Installation

```
$ pip install ustorage
```

## Quick start

```python
from ustorage.s3 import S3Storage

options = {
    'endpoint': 'https://s3.cn-north-1.amazonaws.com.cn',
    'access_key': '<AccessKey>',
    'secret_key': '<SecretKey>',
    'bucket': 'my-bucket',
    'region': 'cn-north-1',
}
fs = S3Storage(options)

fs.write('hello.txt', 'Hello, World!')
fs.exists('hello.txt')
fs.read('hello.txt')
fs.delete('hello.txt')
```
