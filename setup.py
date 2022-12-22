#!/usr/bin/env python

from distutils.core import setup
from setuptools import find_packages

setup(
    name='housing-data-model',
    version='1.0',
    description='Python Distribution Utilities',
    author='Majid Laali',
    author_email='mjlaali@gmail.com',
    package_dir={"": "src"},
    packages=find_packages(where="src"),
)
