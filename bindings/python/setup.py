#!/usr/bin/python
from distutils.core import setup
from Cython.Build import cythonize
from distutils.extension import Extension

sourcefiles = ['client.pyx']

extensions = [Extension('client',
                sourcefiles,
                include_dirs = ['../../client/', '../../'],
                libraries = ['busybee', 'e', 'pthread', 'rt', 'chronos', 'hyperdex-client'],
                extra_compile_args = ['-std=c++0x']
             )]

setup(
    ext_modules = cythonize(extensions)
)
