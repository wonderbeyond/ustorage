import os
import re

from setuptools import setup, find_packages


def get_version():
    here = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(here, 'ustorage', '__init__.py')) as f:
        return re.findall(r"^__version__ = '([^']+)'\r?$", f.read(), re.M)[0]


setup(
    name='ustorage',
    version=get_version(),
    description='Unified Storage Interface for Python.',
    license='Apache License 2.0',
    url='https://github.com/wonderbeyond/ustorage',
    author='wonderbeyond',
    author_email='wonderbeyond@gmail.com',
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    install_requires=[
        'six',
    ],
    extras_require={
        's3': 'boto3==1.*',
    },
    zip_safe=False,
)
