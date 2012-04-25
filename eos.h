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
 *     * Neither the name of HyperDex nor the names of its contributors may be
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

#ifndef eos_h_
#define eos_h_

/* C */
#include <stdlib.h>
#include <stdint.h>

#ifdef __cplusplus

// po6
#include <po6/net/location.h>
#include <po6/net/socket.h>

extern "C" {
#endif

struct eos_client;

enum eos_cmp
{
    EOS_HAPPENS_BEFORE,
    EOS_HAPPENS_AFTER,
    EOS_CONCURRENT,
    EOS_NOEXIST
};

struct eos_pair
{
    uint64_t lhs;
    uint64_t rhs;
    uint32_t flags;
    enum eos_cmp order;
};

#define EOS_SOFT_FAIL 1

/* Create a new client.
 *
 * This will establish a connection to the specified host and server, and
 * maintain state necessary for the eos_client.
 */
struct eos_client*
eos_client_create(const char* host, uint16_t port);

/* Destroy an existing client instance.
 */
void
eos_client_destroy(struct eos_client* client);

/* Create a new event.
 *
 * Creates a new event identifier and immediately acquires a reference to it.
 *
 * Returns: A unique, non-zero identifier for the event, or 0 on error.
 * Error:   errno will indicate the error.
 */
uint64_t
eos_create_event(struct eos_client* client);

/* Acquire references to all events pointed to by "events".
 *
 * References on events last for the system-configured timeout value (default
 * 1h).  If the client calls "eos_acquire_references" again before the timeout,
 * the timeout will be reset.
 *
 * If any events do not exist, the call will fail, and no leases will be
 * acquired.
 *
 * Returns: 0 on success, -1 on error, or index of non-existent event + 1.
 * Error:   errno will indicate the error.
 */
int
eos_acquire_references(struct eos_client* client, uint64_t* events, size_t events_sz);

/* Release references to all events pointed to by "events".
 *
 * By voluntarily relinquishing reference, the application can enable more
 * efficient behavior.
 *
 * If any events do not exist, the remaining events will have their references
 * released.
 *
 * Returns: 0 on success, -1 on error.
 * Error:   errno will indicate the error.
 */
int
eos_release_references(struct eos_client* client, uint64_t* events, size_t events_sz);

/* For each eos_pair, compute the relationship between the two events.
 *
 * In the normal case, pairs[x].order will be set to EOS_HAPPENS_BEFORE if
 * pairs[x].lhs happens before pairs[x].rhs.  Similarly, EOS_HAPPENS_AFTER
 * indicates that pairs[x].lhs happens after pairs[x].rhs.  If there is no order
 * between the events, then pairs[x].order will be EOS_CONCURRENT.  If either
 * event does not exist, pairs[x].order will be EOS_NOEXIST.  Note that
 * EOS_NOEXIST will not cause an error return.  The field eos_pair.flags is
 * ignored.
 *
 * Return:  0 on success, -1 on error.
 * Error:   errno will indicate the error.
 */
int
eos_query_order(struct eos_client* client, struct eos_pair* pairs, size_t pairs_sz);

/* For each eos_pair, create a relationship between the two events.
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
int
eos_assign_order(struct eos_client* client, struct eos_pair* pairs, size_t pairs_sz);

#ifdef __cplusplus
} // extern "C"

// Each of these public methods corresponds to a C call above.
class eos_client
{
    public:
        eos_client(const char* host, uint16_t port);
        ~eos_client() throw ();

    public:
        uint64_t create_event();
        int acquire_references(uint64_t* events, size_t events_sz);
        int release_references(uint64_t* events, size_t events_sz);
        int query_order(eos_pair* pairs, size_t pairs_sz);
        int assign_order(eos_pair* pairs, size_t pairs_sz);

    private:
        po6::net::location m_where;
        po6::net::socket m_sock;
        uint64_t m_nonce;
};

#endif // __cplusplus

#endif // eos_h_
