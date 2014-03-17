#!/usr/bin/python
from distutils.core import setup
from Cython.Build import cythonize
from distutils.extension import Extension

sourcefiles = ['client.pyx','../../common/ids.cc','../../common/serialization.cc','../../common/server.cc','../../common/configuration.cc','../../common/comm_wrapper.cc']

extensions = [Extension('client',
                sourcefiles,
                include_dirs = ['../../','../../../'],
                libraries = ['busybee', 'e', 'pthread', 'rt', 'chronos', 'hyperdex-client'],
                extra_compile_args = ['-std=c++0x']
             )]

setup(
    ext_modules = cythonize(extensions)
)
