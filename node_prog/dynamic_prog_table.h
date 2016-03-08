/*
 * ===============================================================
 *    Description:  Function pointers for dynamically linked
 *                  node program
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2015, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_common_dynamic_prog_table_h_
#define weaver_common_dynamic_prog_table_h_

#include "node_prog/base_classes.h"

using node_prog::np_param_ptr_t;
using node_prog::np_state_ptr_t;
using node_prog::param_ctor_func_t;
using node_prog::param_size_func_t;
using node_prog::param_pack_func_t;
using node_prog::param_unpack_func_t;
using node_prog::state_ctor_func_t;
using node_prog::state_size_func_t;
using node_prog::state_pack_func_t;
using node_prog::state_unpack_func_t;
using node_prog::prog_ptr_t;

struct dynamic_prog_table
{
    param_ctor_func_t   param_ctor;
    param_size_func_t   param_size;
    param_pack_func_t   param_pack;
    param_unpack_func_t param_unpack;

    state_ctor_func_t   state_ctor;
    state_size_func_t   state_size;
    state_pack_func_t   state_pack;
    state_unpack_func_t state_unpack;

    prog_ptr_t node_program;

    dynamic_prog_table(void *prog_handle);
};

#endif
