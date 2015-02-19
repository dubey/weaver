/*
 * ===============================================================
 *    Description:  Graph query cachine classes
 *
 *        Created:  Tuesday 18 November 2013 02:28:29  EDT
 *
 *         Author:  Gregory D. Hill, gdh39@cornell.edu
 * 
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_db_cache_entry_h_
#define weaver_db_cache_entry_h_

#include "db/remote_node.h"
#include "node_prog/base_classes.h"

namespace db
{
    struct cache_entry
    {
        std::shared_ptr<node_prog::Cache_Value_Base> val;
        std::shared_ptr<vc::vclock> clk;
        std::shared_ptr<std::vector<db::remote_node>> watch_set;

        cache_entry() { }

        cache_entry(std::shared_ptr<node_prog::Cache_Value_Base> v,
            std::shared_ptr<vc::vclock> c,
            std::shared_ptr<std::vector<db::remote_node>> ws)
            : val(v)
            , clk(c)
            , watch_set(ws)
        { }
    };
}

#endif
