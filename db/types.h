/*
 * ===============================================================
 *    Description:  Shard data types.
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2014, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_db_types_h
#define weaver_db_types_h

#include <google/sparse_hash_map>
#include <google/sparse_hash_set>

#include "common/utils.h"
#include "common/vclock.h"

namespace db
{
    template<class V> using data_map = google::sparse_hash_map<std::string,
                                                               V,
                                                               weaver_util::murmur_hasher<std::string>,
                                                               weaver_util::eqstr>;

    using string_set = google::sparse_hash_set<std::string,
                                               weaver_util::murmur_hasher<std::string>,
                                               weaver_util::eqstr>;
}

#endif
