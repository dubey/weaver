#! /bin/bash
#
# line_properties.sh
# Copyright (C) 2014 Ayush Dubey <dubey@cs.cornell.edu>
#
# See the LICENSE file for licensing agreement
#

tests/sh/setup.sh
python tests/python/correctness/line_properties.py
status=$?
tests/sh/clean.sh

exit $status
