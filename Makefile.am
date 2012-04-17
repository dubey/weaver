## Copyright (c) 2012, Cornell University
## All rights reserved.
##
## Redistribution and use in source and binary forms, with or without
## modification, are permitted provided that the following conditions are met:
##
##     * Redistributions of source code must retain the above copyright notice,
##       this list of conditions and the following disclaimer.
##     * Redistributions in binary form must reproduce the above copyright
##       notice, this list of conditions and the following disclaimer in the
##       documentation and/or other materials provided with the distribution.
##     * Neither the name of HyperDex nor the names of its contributors may be
##       used to endorse or promote products derived from this software without
##       specific prior written permission.
##
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
## AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
## IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
## ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
## LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
## CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
## SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
## INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
## CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
## ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
## POSSIBILITY OF SUCH DAMAGE.

ACLOCAL_AMFLAGS = -I m4 ${ACLOCAL_FLAGS}
AM_CPPFLAGS = $(E_CFLAGS)
AM_CXXFLAGS = $(COVERAGE_CXXFLAGS)

EXTRA_DIST = LICENSE eos.pyx

include_HEADERS = eos.h
noinst_HEADERS = eos_cmp_encode.h network_constants.h

bin_PROGRAMS = event-orderer
lib_LTLIBRARIES = libeos.la
pyexec_LTLIBRARIES = eos.la

event_orderer_SOURCES = event-orderer.cc eos_cmp_encode.cc
event_orderer_LDADD = $(E_LIBS)
# This NOP seems useless.  See:
# http://www.gnu.org/software/automake/manual/html_node/Objects-created-both-with-libtool-and-without.html
event_orderer_CXXFLAGS = $(AM_CXXFLAGS)

libeos_la_SOURCES = eos.cc eos_cmp_encode.cc
libeos_la_LIBADD = $(E_LIBS)

eos_la_SOURCES = python-eos.c
eos_la_CPPFLAGS = $(PYTHON_CPPFLAGS) $(CPPFLAGS)
eos_la_LIBADD = libeos.la $(PYTHON_LDFLAGS)
eos_la_LDFLAGS = -module -avoid-version -export-symbols-regex initeos

python-eos.c: eos.pyx
	cython -o $@ eos.pyx
