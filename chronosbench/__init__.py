# Copyright (c) 2012, Cornell University
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 
#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of HyperDex nor the names of its contributors may be
#       used to endorse or promote products derived from this software without
#       specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
# ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


'''Python scripts used to evaluate Chronos'''


import errno
import os
import random
import subprocess
import time


class ChronosDaemon(object):

    def __init__(self, chronosd='chronosd', host='127.0.0.1', port=None, valgrind=True):
        if not port:
            port = random.randint(1025, 65535)
        args = [chronosd, '-h', host, '-p', str(port)]
        if valgrind:
            args = ['valgrind', '--tool=callgrind'] + args
        self._daemon = subprocess.Popen(args,
                           stdout=open('/dev/null', 'w'),
                           stderr=open('/dev/null', 'w'))
        self._host = host
        self._port = port
        time.sleep(0.5)
        if valgrind:
            time.sleep(1)

    def host(self):
        return self._host

    def port(self):
        return self._port

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._daemon.terminate()
        time.sleep(1)
        self._daemon.kill()


class DataOut(object):

    def __init__(self, filename):
        self._filename = filename
        self._file = None

    def __enter__(self):
        if os.path.exists(self._filename):
            return None
        try:
            tmp = os.open(self._filename + '.part', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0600)
            self._file = os.fdopen(tmp, 'w')
        except OSError as e:
            if e.errno == errno.EEXIST:
                return None
            raise
        return self._file

    def __exit__(self, exc_type, exc_value, traceback):
        if self._file:
            self._file.flush()
            self._file.close()
            if exc_type == None:
                os.rename(self._filename + '.part', self._filename)
            else:
                os.unlink(self._filename + '.part')
