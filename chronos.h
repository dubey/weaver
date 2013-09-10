/* Copyright (c) 2012, Cornell University
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of Chronos nor the names of its contributors may be
 *       used to endorse or promote products derived from this software without
 *       specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef chronos_h_
#define chronos_h_

/* C */
#include <stdlib.h>
#include <stdint.h>

#ifdef __cplusplus

// STL
#include <map>
#include <memory>
#include <vector>

// po6
#include <po6/net/location.h>
#include <po6/net/socket.h>

// e
#include <e/intrusive_ptr.h>

// Replicant
#include <replicant.h>

// Weaver
#define NUM_SHARDS 4

extern "C" {
#endif

struct chronos_client;

enum chronos_returncode
{
    CHRONOS_SUCCESS,
    CHRONOS_ERROR,
    CHRONOS_GARBAGE
};

enum chronos_cmp
{
    CHRONOS_HAPPENS_BEFORE,
    CHRONOS_HAPPENS_AFTER,
    CHRONOS_CONCURRENT,
    CHRONOS_WOULDLOOP,
    CHRONOS_NOEXIST
};

struct chronos_pair
{
    uint64_t lhs;
    uint64_t rhs;
    uint32_t flags;
    enum chronos_cmp order;
};

struct weaver_pair
{
    uint64_t *lhs;
    uint64_t *rhs;
    uint32_t flags;
    enum chronos_cmp order;
};

struct chronos_stats
{
    uint64_t time;
    uint64_t utime;
    uint64_t stime;
    uint32_t maxrss;
    uint64_t events;
    uint64_t count_create_event;
    uint64_t count_acquire_references;
    uint64_t count_release_references;
    uint64_t count_query_order;
    uint64_t count_assign_order;
    uint64_t count_weaver_order;
};

#define CHRONOS_SOFT_FAIL 1

/* Create a new client.
 *
 * This will establish a connection to the specified host and server, and
 * maintain state necessary for the chronos_client.
 */
struct chronos_client*
chronos_client_create(const char* host, uint16_t port, uint64_t num_shards);

/* Destroy an existing client instance.
 */
void
chronos_client_destroy(struct chronos_client* client);

/* Create a new event.
 *
 * Creates a new event identifier and immediately acquires a reference to it.
 *
 * Returns: A unique, non-zero identifier for the event, or 0 on error.
 * Error:   errno will indicate the error.
 */
int64_t
chronos_create_event(struct chronos_client* client,
                     enum chronos_returncode* status,
                     uint64_t* event);

/* Acquire references to all events pointed to by "events".
 *
 * If any events do not exist, the call will fail, and no leases will be
 * acquired.
 *
 * Returns: 0 on success, -1 on error, or index of non-existent event + 1.
 * Error:   errno will indicate the error.
 */
int64_t
chronos_acquire_references(struct chronos_client* client,
                           uint64_t* events, size_t events_sz,
                           enum chronos_returncode* status, ssize_t* ret);

/* Release references to all events pointed to by "events".
 *
 * If any events do not exist, the remaining events will have their references
 * released.
 *
 * Returns: 0 on success, -1 on error.
 * Error:   errno will indicate the error.
 */
int64_t
chronos_release_references(struct chronos_client* client,
                           uint64_t* events, size_t events_sz,
                           enum chronos_returncode* status, ssize_t* ret);

/* For each chronos_pair, compute the relationship between the two events.
 *
 * In the normal case, pairs[x].order will be set to EOS_HAPPENS_BEFORE if
 * pairs[x].lhs happens before pairs[x].rhs.  Similarly, EOS_HAPPENS_AFTER
 * indicates that pairs[x].lhs happens after pairs[x].rhs.  If there is no order
 * between the events, then pairs[x].order will be EOS_CONCURRENT.  If either
 * event does not exist, pairs[x].order will be EOS_NOEXIST.  Note that
 * EOS_NOEXIST will not cause an error return.  The field chronos_pair.flags is
 * ignored.
 *
 * Return:  0 on success, -1 on error.
 * Error:   errno will indicate the error.
 */
int64_t
chronos_query_order(struct chronos_client* client, struct chronos_pair* pairs, size_t pairs_sz,
                    enum chronos_returncode* status, ssize_t* ret);

/* For each chronos_pair, create a relationship between the two events.
 *
 * If there is no happens-before relationship between pairs[x].lhs and
 * pairs[x].rhs, then a relationship between pairs[x].lhs and pairs[x].rhs will
 * be created according to pairs[x].order.  If there is a preexisting
 * happens-before relationship that conflicts with the one specified by
 * pairs[x].order, then the operation will fail.
 *
 * If EOS_SOFT_FAIL is set in pairs[x].flags, then a preexisting happens-before
 * relationship will not cause failure.  Instead, pairs[x].order will be set to
 * indicate the existing relationship.
 *
 * If without EOS_SOFT_FAIL result in failure, no relationships will be created.
 *
 * Returns: 0 on success, -1 on error, or index of non-existent event + 1.
 * Error:   errno will indicate the error.
 */
int64_t
chronos_assign_order(struct chronos_client* client, struct chronos_pair* pairs, size_t pairs_sz,
                     enum chronos_returncode* status, ssize_t* ret);

int64_t
chronos_weaver_order(struct chronos_client* client, struct weaver_pair* pairs, size_t pairs_sz,
                     enum chronos_returncode* status, ssize_t* ret);
/* Get statistics from the server.
 *
 * The chronos_stats instance will be filled in.  The fields have the following
 * meaning:
 *
 * time:
 *     The wall-clock time on the server.
 *
 * utime:
 *     The amount of time spent in userspace according to getrusage.
 *
 * stime:
 *     The amount of time spent in kernel according to getrusage.
 *
 * maxrss:
 *     The maximum resident set size according to getrusage.
 *
 * events:
 *     The number of events in the event dependency graph.
 *
 * count_create_event:
 *     The number of calls to "create_event".
 *
 * count_acquire_references:
 *     The number of calls to "acquire_references".
 *
 * count_release_references:
 *     The number of calls to "release_references".
 *
 * count_query_order:
 *     The number of calls to "query_order".
 *
 * count_assign_order:
 *     The number of calls to "assign_order".
 *
 * Return:  0 on success, -1 on error.
 * Error:   errno will indicate the error.
 */
int64_t
chronos_get_stats(struct chronos_client* client, enum chronos_returncode* status, struct chronos_stats* st, ssize_t* ret);

int64_t
chronos_loop(struct chronos_client* client, int timeout, enum chronos_returncode* status);

int64_t
chronos_wait(struct chronos_client* client, int64_t id, int timeout, enum chronos_returncode* status);

#ifdef __cplusplus
} // extern "C"

// Each of these public methods corresponds to a C call above.
class chronos_client
{
    public:
        chronos_client(const char* host, uint16_t port, uint64_t num_shards);
        ~chronos_client() throw ();

    public:
        int64_t create_event(chronos_returncode* status, uint64_t* event);
        int64_t acquire_references(uint64_t* events, size_t events_sz,
                                   chronos_returncode* status, ssize_t* ret);
        int64_t release_references(uint64_t* events, size_t events_sz,
                                   chronos_returncode* status, ssize_t* ret);
        int64_t query_order(chronos_pair* pairs, size_t pairs_sz,
                            chronos_returncode* status, ssize_t* ret);
        int64_t assign_order(chronos_pair* pairs, size_t pairs_sz,
                             chronos_returncode* status, ssize_t* ret);
        int64_t weaver_order(weaver_pair* pairs, size_t pairs_sz,
                             chronos_returncode* status, ssize_t* ret);
        int64_t get_stats(chronos_returncode* status, chronos_stats* st, ssize_t* ret);
        int64_t loop(int timeout, chronos_returncode* status);
        int64_t wait(int64_t id, int timeout, chronos_returncode* status);

    private:
        class pending;
        class pending_create_event;
        class pending_references;
        class pending_order;
        class pending_weaver_order;
        class pending_get_stats;
        typedef std::map<uint64_t, e::intrusive_ptr<pending> > pending_map;

    private:
        int64_t send(e::intrusive_ptr<pending> pend, chronos_returncode* status,
                     const char* func, const char* data, size_t data_sz);

    private:
        std::auto_ptr<replicant_client> m_replicant;
        pending_map m_pending;
        uint64_t m_shards; // number of shards, used as dimension for vector clocks
};
#endif // __cplusplus

#endif // chronos_h_
