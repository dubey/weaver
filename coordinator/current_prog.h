/*
 * ===============================================================
 *    Description:  Metadata for a currently executing node prog.
 *
 *        Created:  2014-02-24 14:07:52
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013-2014, Cornell University, see the LICENSE
 *                     file for licensing agreement
 * ===============================================================
 */

#ifndef weaver_coordinator_current_prog_h_
#define weaver_coordinator_current_prog_h_

namespace coordinator
{
    struct current_prog
    {
        uint64_t req_id, client;
        std::unique_ptr<vc::vclock> vclk;

        current_prog(uint64_t rid, uint64_t cl, const vc::vclock &vc)
            : req_id(rid)
            , client(cl)
            , vclk(new vc::vclock(vc))
        { }
        
        current_prog() : req_id(UINT64_MAX), client(UINT64_MAX) { }
    };
}

#endif
