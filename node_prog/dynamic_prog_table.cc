/*
 * ===============================================================
 *    Description:  Implement dynamic prog table
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2015, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <dlfcn.h>

#include "node_prog/dynamic_prog_table.h"

dynamic_prog_table :: dynamic_prog_table(void *prog_handle)
{
    param_ctor = (param_ctor_func_t)dlsym(prog_handle, "param_ctor");
    param_size = (param_size_func_t)dlsym(prog_handle, "param_size");
    param_pack = (param_pack_func_t)dlsym(prog_handle, "param_pack");
    param_unpack = (param_unpack_func_t)dlsym(prog_handle, "param_unpack");
    state_ctor = (state_ctor_func_t)dlsym(prog_handle, "state_ctor");
    state_size = (state_size_func_t)dlsym(prog_handle, "state_size");
    state_pack = (state_pack_func_t)dlsym(prog_handle, "state_pack");
    state_unpack = (state_unpack_func_t)dlsym(prog_handle, "state_unpack");
    node_program = (prog_ptr_t)dlsym(prog_handle, "node_program");
}
