#!/usr/bin/env python

from setuptools import setup, find_packages
import fito

setup(
    name='fondos',
    packages=find_packages(),
    version=fito.__version__,
    description='fondos',
    author='Pablo Zivic',
    author_email='elsonidoq@gmail.com',
    url='https://github.com/elsonidoq/fondos',
    zip_safe=False,
    install_requires=[
    ],
)
