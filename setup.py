#!/usr/bin/env python

from setuptools import setup

setup(
    name='sstable_tools',
    version='1.0.0',
    description='Scylla SStable Tools',
    packages=['sstable_tools'],
    scripts=['sstable-compressioninfo.py', 'sstable-index.py', 'sstable-statistics.py', 'sstable-summary.py'],
)
