/*
 * ===============================================================
 *    Description:  Test for tx msg packing, unpacking, and mapper
 *                  lookup/insertion.
 *
 *        Created:  09/05/2013 03:01:35 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "coordinator/timestamper.h"

coordinator::nmap_stub nmap_client;

void
basic_put_get()
{
    std::vector<std::pair<uint64_t, uint64_t>> toPut;
    toPut.emplace_back(13,14);
    nmap_client.put_mappings(toPut);
    std::unordered_set<uint64_t> toGet;
    toGet.emplace(13);
    auto results = nmap_client.get_mappings(toGet);
    assert(results.size() == 1);
    assert(results.at(0).first == 13);
    assert(results.at(0).second == 14);
}

void
large_put_get_delete()
{
    size_t numPut = 100;
    std::vector<std::pair<uint64_t, uint64_t>> toPut;
    std::unordered_set<uint64_t> toGet;
    std::vector<uint64_t> toDel;
    for (size_t i = 0; i < numPut; i++) {
        toPut.emplace_back(i, 2*i);
        toGet.emplace(i);
        toDel.emplace_back(i);
    }
    nmap_client.put_mappings(toPut);

    auto results = nmap_client.get_mappings(toGet);
    assert(results.size() == numPut);
    for (auto &result : results) {
        assert(result.first * 2 == result.second);
    }

    nmap_client.del_mappings(toDel);
    auto delResults = nmap_client.get_mappings(toGet);
    assert(delResults.size() == 0);
}

void
nmap_unit_tests()
{
    basic_put_get();
    large_put_get_delete();
}
