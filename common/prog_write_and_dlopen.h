/*
 * ===============================================================
 *    Description:  Write a vector of bytes that represents an so
 *                  file to a tmp file, and then dlopen that file.
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2015, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "node_prog/dynamic_prog_table.h"

std::shared_ptr<dynamic_prog_table>
write_and_dlopen(std::vector<uint8_t> &buf);
