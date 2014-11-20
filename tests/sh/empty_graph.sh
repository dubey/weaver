#! /bin/bash
#
# empty_graph.sh
# Copyright (C) 2014 Ayush Dubey <dubey@cs.cornell.edu>
#
# See the LICENSE file for licensing agreement
#

tests/sh/setup.sh
echo 'execing empty_graph_sanity_checks.py'
python tests/python/correctness/empty_graph_sanity_checks.py
echo 'done execing py test'
tests/sh/clean.sh
