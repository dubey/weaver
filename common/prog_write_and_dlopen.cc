/*
 * ===============================================================
 *    Description:  Implement write_and_dlopen
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2015, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <fstream>
#include <sys/stat.h>
#include <wordexp.h>
#include <dlfcn.h>

#include "common/weaver_constants.h"
#include "common/prog_write_and_dlopen.h"

std::shared_ptr<dynamic_prog_table>
write_and_dlopen(std::vector<uint8_t> &buf)
{
    std::string dir_unexp = "~/weaver_runtime";
    wordexp_t dir_exp;
    wordexp(dir_unexp.c_str(), &dir_exp, 0);
    std::string dir = dir_exp.we_wordv[0];
    mkdir(dir.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    std::string so_file_name;
    std::ofstream write_so;
    uint16_t prog_ctr = 0;
    while (!write_so.is_open()) {
        so_file_name = dir + "/weaver_prog_" + std::to_string(prog_ctr++);
        FILE *test_file = fopen(so_file_name.c_str(), "r");
        if (test_file) {
            fclose(test_file);
        } else {
            write_so.open(so_file_name, std::ofstream::out | std::ofstream::binary);
        }
    }

    write_so.write((const char*)&buf[0], buf.size());
    write_so.close();

    void *prog_ptr = dlopen(so_file_name.c_str(), RTLD_NOW);
    if (prog_ptr == NULL) {
        WDEBUG << "dlopen error: " << dlerror() << std::endl;
        WDEBUG << "failed registering node prog" << std::endl;
        return nullptr;
    } else {
        auto prog_table = std::make_shared<dynamic_prog_table>(prog_ptr);
        return prog_table;
    }
}
