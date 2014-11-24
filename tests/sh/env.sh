#! /bin/bash
#
# env.sh
# Copyright (C) 2014 Ayush Dubey <dubey@cs.cornell.edu>
#
# See the LICENSE file for licensing agreement
#

export WEAVER_SRCDIR="$1"
export WEAVER_BUILDDIR="$2"

export PATH="$2"/:$PATH:
export PYTHONPATH="$2"/bindings/python:"$2"/bindings/python/.libs:
export LD_LIBRARY_PATH="$2"/.libs:$LD_LIBRARY_PATH:
export LD_RUN_PATH="$2"/.libs:$LD_RUN_PATH:
