/*
 * ===============================================================
 *    Description:  Common methods for setting up Busybee.
 *
 *        Created:  05/22/2013 04:23:55 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __BB_INFRA__
#define __BB_INFRA__

#include <fstream>
#include <unordered_map>
#include <busybee_constants.h>
#include <busybee_mapper.h>
#include <busybee_mta.h>

#include "weaver_constants.h"

// map from server ids -> po6 locs
class weaver_mapper : public busybee_mapper
{
    private:
        std::unordered_map<uint64_t, po6::net::location> mlist;

    public:
        weaver_mapper(std::unordered_map<uint64_t, po6::net::location> &ml) : mlist(ml) { }

        virtual ~weaver_mapper() throw () {}

        virtual bool lookup(uint64_t server_id, po6::net::location *loc)
        {
            uint64_t incr_id = ID_INCR + server_id;
            if (mlist.find(incr_id) != mlist.end()) {
                *loc = mlist.at(incr_id);
                return true;
            } else {
                std::cerr << "returning false from mapper lookup for id " << server_id << std::endl;
                return false;
            }
        }

    private:
        weaver_mapper(const weaver_mapper&);
        weaver_mapper& operator=(const weaver_mapper&);
};

inline void
initialize_busybee(busybee_mta *&bb, uint64_t sid, std::shared_ptr<po6::net::location> myloc, int num_threads=1)
{
    int inport;
    uint64_t server_id;
    std::string ipaddr;
    std::unordered_map<uint64_t, po6::net::location> member_list;
    std::ifstream file(SHARDS_DESC_FILE);
    if (file != NULL) {
        while (file >> server_id >> ipaddr >> inport) {
            uint64_t incr_id = ID_INCR + server_id;
            member_list.emplace(incr_id, po6::net::location(ipaddr.c_str(), inport));
            if (server_id == sid) {
                myloc.reset(new po6::net::location(ipaddr.c_str(), inport));
            }
        }
    } else {
        std::cerr << "File " << SHARDS_DESC_FILE << " not found." << std::endl;
    }
    file.close();
    weaver_mapper *wmap = new weaver_mapper(member_list);
    bb = new busybee_mta(wmap, *myloc, sid+ID_INCR, num_threads);
}

#endif
