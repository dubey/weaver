// Copyright (c) 2012, Cornell University
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of HyperDex nor the names of its contributors may be
//       used to endorse or promote products derived from this software without
//       specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

#define __STDC_LIMIT_MACROS

// C
#include <cstdio>
#include <stdint.h>

// Linux
#include <sys/epoll.h>

// STL
#include <algorithm>
#include <memory>
#include <queue>
#include <vector>

// po6
#include <po6/net/location.h>
#include <po6/net/socket.h>

// e
#include <e/buffer.h>
#include <e/endian.h>

// eos
#include "eos.h"
#include "eos_cmp_encode.h"
#include "event_dependency_graph.h"
#include "network_constants.h"

#define xtostr(X) #X
#define tostr(X) xtostr(X)
#define ERRORMSG(X) fprintf(stderr, "%s:%i:  " X "\n", __FILE__, __LINE__)
#define ERRORMSG1(X, Y) fprintf(stderr, "%s:%i:  " X "\n", __FILE__, __LINE__, Y)
#define ERRORMSG2(X, Y1, Y2) fprintf(stderr, "%s:%i:  " X "\n", __FILE__, __LINE__, Y1, Y2)
#define ERRNOMSG(CALL) ERRORMSG2(tostr(CALL) " failed:  %s  [ERRNO=%i]", strerror(errno), errno)

class event_orderer
{
    public:
        event_orderer();
        ~event_orderer() throw ();

    public:
        uint64_t create_event();
        size_t acquire_references(uint64_t* events, size_t events_sz);
        size_t release_references(uint64_t* events, size_t events_sz);
        size_t query_order(eos_pair* pairs, size_t pairs_sz);
        size_t assign_order(eos_pair* pairs, size_t pairs_sz);

    private:
        event_dependency_graph m_graph;
        uint64_t m_count_create;
        uint64_t m_count_acquire;
        uint64_t m_count_release;
        uint64_t m_count_query;
        uint64_t m_count_assign;
};

event_orderer :: event_orderer()
    : m_graph()
{
}

event_orderer :: ~event_orderer() throw ()
{
}

uint64_t
event_orderer :: create_event()
{
    return m_graph.add_vertex();
}

size_t
event_orderer :: acquire_references(uint64_t* events, size_t events_sz)
{
    size_t i;

    // Increment reference counts
    for (i = 0; i < events_sz; ++i)
    {
        if (!m_graph.incref(events[i]))
        {
            break;
        }
    }

    // Inside this conditional lies the normal exit point.
    if (i == events_sz)
    {
        return events_sz;
    }

    // Decrement reference counts we incremented in error
    for (size_t j = 0; j < i; ++j)
    {
        bool decr = m_graph.decref(events[i]);
        assert(decr);
    }

    return i;
}

size_t
event_orderer :: release_references(uint64_t* events, size_t events_sz)
{
    for (size_t i = 0; i < events_sz; ++i)
    {
        m_graph.decref(events[i]);
    }

    return events_sz;
}

size_t
event_orderer :: query_order(eos_pair* pairs, size_t pairs_sz)
{
    for (size_t i = 0; i < pairs_sz; ++i)
    {
        if (!m_graph.exists(pairs[i].lhs) ||
            !m_graph.exists(pairs[i].rhs))
        {
            pairs[i].order = EOS_NOEXIST;
            continue;
        }

        int resolve = m_graph.compute_order(pairs[i].lhs, pairs[i].rhs);

        if (resolve < 0)
        {
            pairs[i].order = EOS_HAPPENS_BEFORE;
        }
        else if (resolve > 0)
        {
            pairs[i].order = EOS_HAPPENS_AFTER;
        }
        else
        {
            pairs[i].order = EOS_CONCURRENT;
        }
    }

    return pairs_sz;
}

size_t
event_orderer :: assign_order(eos_pair* pairs, size_t pairs_sz)
{
    std::vector<std::pair<uint64_t, uint64_t> > created_edges;
    created_edges.reserve(pairs_sz);
    size_t i;

    for (i = 0; i < pairs_sz; ++i)
    {
        if (!m_graph.exists(pairs[i].lhs) ||
            !m_graph.exists(pairs[i].rhs))
        {
            break;
        }

        int resolve = m_graph.compute_order(pairs[i].lhs, pairs[i].rhs);

        if (resolve < 0)
        {
            if (pairs[i].order != EOS_HAPPENS_BEFORE)
            {
                if ((pairs[i].flags & EOS_SOFT_FAIL))
                {
                    pairs[i].order = EOS_HAPPENS_BEFORE;
                }
                else
                {
                    break;
                }
            }
        }
        else if (resolve > 0)
        {
            if (pairs[i].order != EOS_HAPPENS_AFTER)
            {
                if ((pairs[i].flags & EOS_SOFT_FAIL))
                {
                    pairs[i].order = EOS_HAPPENS_AFTER;
                }
                else
                {
                    break;
                }
            }
        }
        else
        {
            switch (pairs[i].order)
            {
                case EOS_HAPPENS_BEFORE:
                    created_edges.push_back(std::make_pair(pairs[i].lhs, pairs[i].rhs));
                    break;
                case EOS_HAPPENS_AFTER:
                    created_edges.push_back(std::make_pair(pairs[i].rhs, pairs[i].lhs));
                    break;
                case EOS_CONCURRENT:
                case EOS_NOEXIST:
                default:
                    break;
            }
        }
    }

    if (i != pairs_sz)
    {
        return i;
    }

    for (size_t j = 0; j < created_edges.size(); ++j)
    {
        m_graph.add_edge(created_edges[j].first, created_edges[j].second);
    }

    return pairs_sz;
}

class channel
{
    public:
        channel();
        ~channel() throw ();

    public:
        void reset();
        void reset(po6::net::socket* sock);

    public:
        po6::net::socket sock;
        po6::net::location loc;
        std::auto_ptr<e::buffer> outbuffer;
        e::slice outprogress;
        std::auto_ptr<e::buffer> inbuffer;
        std::vector<uint64_t> references;

    private:
        channel(const channel&);

    private:
        channel& operator = (const channel&);
};

channel :: channel()
    : sock()
    , loc()
    , outbuffer()
    , outprogress()
    , inbuffer()
    , references()
{
}

channel :: ~channel() throw()
{
}

void
channel :: reset()
{
    try
    {
        sock.shutdown(SHUT_RDWR);
    }
    catch (...)
    {
    }

    try
    {
        sock.close();
    }
    catch (...)
    {
    }

    loc = po6::net::location();
    outbuffer.reset();
    outprogress = e::slice();
    inbuffer.reset();
    references.clear();
}

void
channel :: reset(po6::net::socket* newsock)
{
    reset();
    sock.swap(newsock);
}

static bool
process_events(int epfd, po6::net::socket* listenfd, channel* channels, int* msgfd, std::auto_ptr<e::buffer>* msg)
{
    while (true)
    {
        epoll_event ee;
        int polled = epoll_wait(epfd, &ee, 1, 50);

        if (polled < 0 && errno != EAGAIN && errno != EINTR && errno != EWOULDBLOCK)
        {
            ERRNOMSG("epoll_wait");
            return false;
        }
        else if (polled <= 0)
        {
            continue;
        }

        int fd = ee.data.fd;
        *msgfd = fd;

        if (fd == listenfd->get())
        {
            po6::net::socket soc;
            listenfd->accept(&soc);
            *msgfd = soc.get();
            channels[*msgfd].reset(&soc);
            ee.data.fd = *msgfd;
            ee.events = EPOLLIN;

            if (epoll_ctl(epfd, EPOLL_CTL_ADD, *msgfd, &ee) < 0)
            {
                ERRNOMSG("epoll_ctl");
                msg->reset();
                return true;
            }

            continue;
        }

        channel& chan(channels[fd]);

        // Handle read I/O.
        if ((ee.events & EPOLLIN))
        {
            if (chan.outbuffer.get())
            {
                ee.events &= ~EPOLLIN;

                if (epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ee) < 0)
                {
                    ERRNOMSG("epoll_ctl");
                    msg->reset();
                    return true;
                }

                continue;
            }

            if (chan.inbuffer.get())
            {
                ssize_t ret = chan.sock.recv(chan.inbuffer->end(), chan.inbuffer->remain(), MSG_DONTWAIT);

                if (ret < 0 && errno != EAGAIN && errno != EINTR && errno != EWOULDBLOCK)
                {
                    ERRNOMSG("recv");
                    msg->reset();
                    return true;
                }
                else if (ret == 0)
                {
                    msg->reset();
                    return true;
                }
                else if (ret < 0)
                {
                    continue;
                }

                chan.inbuffer->extend(ret);

                if (chan.inbuffer->remain() == 0)
                {
                    *msg = chan.inbuffer;
                    return true;
                }
            }
            else
            {
                uint8_t buffer[sizeof(uint32_t)];
                ssize_t ret = chan.sock.recv(buffer, sizeof(uint32_t), MSG_DONTWAIT|MSG_PEEK);
                uint32_t size = 0;
                e::unpack32be(buffer, &size);

                if (ret < 0 && errno != EAGAIN && errno != EINTR && errno != EWOULDBLOCK)
                {
                    ERRNOMSG("recv");
                    msg->reset();
                    return true;
                }
                else if (ret == 0)
                {
                    msg->reset();
                    return true;
                }
                else if (ret < 0)
                {
                    continue;
                }

                chan.inbuffer.reset(e::buffer::create(size));
            }
        }

        // Handle write I/O.
        if ((ee.events & EPOLLOUT))
        {
            if (!chan.outbuffer.get())
            {
                ee.events = EPOLLIN;

                if (epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ee) < 0)
                {
                    ERRNOMSG("epoll_ctl");
                    msg->reset();
                    return true;
                }

                continue;
            }

            ssize_t ret = chan.sock.send(chan.outprogress.data(), chan.outprogress.size(), 0);

            if (ret < 0 && errno != EAGAIN && errno != EINTR && errno != EWOULDBLOCK)
            {
                ERRNOMSG("recv");
                msg->reset();
                return true;
            }
            else if (ret == 0)
            {
                msg->reset();
                return true;
            }
            else if (ret < 0)
            {
                continue;
            }

            chan.outprogress.advance(ret);

            if (chan.outprogress.empty())
            {
                chan.outbuffer.reset();
                ee.events = EPOLLIN;

                if (epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ee) < 0)
                {
                    ERRNOMSG("epoll_ctl");
                    msg->reset();
                    return true;
                }
            }
        }

        // Close the connection on error or hangup.
        if ((ee.events & EPOLLERR) || (ee.events & EPOLLHUP))
        {
            msg->reset();
            return true;
        }
    }
}

static std::auto_ptr<e::buffer>
eosnc_create_event(channel* chan, std::auto_ptr<e::buffer> msg, uint64_t nonce, event_orderer* eo)
{
    const size_t SEND_MSG_SIZE = sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t); 
    chan->references.reserve(chan->references.size() + 1);
    uint64_t event = eo->create_event();
    chan->references.push_back(event);
    msg.reset(e::buffer::create(SEND_MSG_SIZE));
    e::buffer::packer pa = msg->pack();
    pa = pa << static_cast<uint32_t>(SEND_MSG_SIZE) << nonce << event;
    assert(!pa.error());
    return msg;
}

static std::auto_ptr<e::buffer>
eosnc_acquire_ref(channel* chan, std::auto_ptr<e::buffer> msg, uint64_t nonce, event_orderer* eo)
{
    const size_t NUM_EVENTS = (msg->size() - (sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint64_t)))
                            / sizeof(uint64_t);
    const size_t RESP_MSG_SIZE = sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t);

    std::vector<uint64_t> events;
    events.reserve(NUM_EVENTS);
    std::vector<uint64_t> events_offset;
    events_offset.reserve(NUM_EVENTS);
    e::buffer::unpacker up = msg->unpack_from(sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint64_t));

    for (size_t i = 0; i < NUM_EVENTS; ++i)
    {
        uint64_t event = 0;
        up = up >> event;

        if (std::find(chan->references.begin(), chan->references.end(), event)
            == chan->references.end())
        {
            events.push_back(event);
            events_offset.push_back(i);
        }
    }

    assert(!up.error());
    size_t success_to = eo->acquire_references(&events.front(), events.size());

    msg->resize(0);
    e::buffer::packer pa = msg->pack();
    pa = pa << static_cast<uint32_t>(RESP_MSG_SIZE) << nonce;

    if (success_to == events.size())
    {
        pa = pa << NUM_EVENTS;

        for (size_t i = 0; i < events.size(); ++i)
        {
            chan->references.push_back(events[i]);
        }
    }
    else
    {
        pa = pa << events_offset[success_to];
    }

    assert(!pa.error());
    return msg;
}

static std::auto_ptr<e::buffer>
eosnc_release_ref(channel* chan, std::auto_ptr<e::buffer> msg, uint64_t nonce, event_orderer* eo)
{
    const size_t NUM_EVENTS = (msg->size() - (sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint64_t)))
                            / sizeof(uint64_t);
    const size_t RESP_MSG_SIZE = sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t);

    std::vector<uint64_t> events;
    events.reserve(NUM_EVENTS);
    std::vector<uint64_t> events_offset;
    events_offset.reserve(NUM_EVENTS);
    e::buffer::unpacker up = msg->unpack_from(sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint64_t));

    for (size_t i = 0; i < NUM_EVENTS; ++i)
    {
        uint64_t event = 0;
        up = up >> event;

        if (std::find(chan->references.begin(), chan->references.end(), event)
            != chan->references.end())
        {
            events.push_back(event);
            events_offset.push_back(i);
        }
    }

    assert(!up.error());
    size_t success_to = eo->release_references(&events.front(), events.size());
    size_t i = 0;

    while (i < chan->references.size())
    {
        if (std::find(events.begin(), events.end(), chan->references[i]) != events.end())
        {
            std::swap(chan->references[i], chan->references.back());
            chan->references.pop_back();
        }
        else
        {
            ++i;
        }
    }

    msg->resize(0);
    e::buffer::packer pa = msg->pack();
    pa = pa << static_cast<uint32_t>(RESP_MSG_SIZE) << nonce;

    if (success_to == events.size())
    {
        pa = pa << NUM_EVENTS;
    }
    else
    {
        pa = pa << events_offset[success_to];
    }

    assert(!pa.error());
    return msg;
}

static std::auto_ptr<e::buffer>
eosnc_query_order(channel* /*chan*/, std::auto_ptr<e::buffer> msg, uint64_t nonce, event_orderer* eo)
{
    const size_t NUM_PAIRS = (msg->size() - (sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint64_t)))
                           / (sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t));
    const size_t RESP_MSG_SIZE = sizeof(uint32_t) + sizeof(uint64_t)
                               + sizeof(uint8_t) * NUM_PAIRS;
    std::vector<eos_pair> pairs(NUM_PAIRS);
    e::buffer::unpacker up = msg->unpack_from(sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint64_t));

    for (size_t i = 0; i < NUM_PAIRS; ++i)
    {
        up = up >> pairs[i].lhs >> pairs[i].rhs >> pairs[i].flags;
        pairs[i].order = EOS_CONCURRENT;
    }

    assert(!up.error());
    eo->query_order(&pairs.front(), NUM_PAIRS);
    msg->resize(0);
    e::buffer::packer pa = msg->pack();
    pa = pa << static_cast<uint32_t>(RESP_MSG_SIZE) << nonce;

    for (size_t i = 0; i < NUM_PAIRS; ++i)
    {
        pa = pa << eos_cmp_to_byte(pairs[i].order);
    }

    assert(!pa.error());
    return msg;
}

static std::auto_ptr<e::buffer>
eosnc_assign_order(channel* /*chan*/, std::auto_ptr<e::buffer> msg, uint64_t nonce, event_orderer* eo)
{
    const size_t NUM_PAIRS = (msg->size() - (sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint64_t)))
                           / (sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint8_t));
    const size_t RESP_MSG_SIZE = sizeof(uint32_t) + sizeof(uint64_t)
                               + sizeof(uint8_t) * NUM_PAIRS;
    std::vector<eos_pair> pairs(NUM_PAIRS);
    e::buffer::unpacker up = msg->unpack_from(sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint64_t));

    for (size_t i = 0; i < NUM_PAIRS; ++i)
    {
        uint8_t order;
        up = up >> pairs[i].lhs >> pairs[i].rhs >> pairs[i].flags >> order;
        pairs[i].order = byte_to_eos_cmp(order);
    }

    assert(!up.error());
    size_t ret = eo->assign_order(&pairs.front(), NUM_PAIRS);

    if (ret != NUM_PAIRS)
    {
        for (size_t i = 0; i < ret; ++i)
        {
            pairs[i].order = EOS_HAPPENS_BEFORE;
        }

        pairs[ret].order = EOS_NOEXIST;
    }

    msg->resize(0);
    e::buffer::packer pa = msg->pack();
    pa = pa << static_cast<uint32_t>(RESP_MSG_SIZE) << nonce;

    for (size_t i = 0; i < NUM_PAIRS; ++i)
    {
        pa = pa << eos_cmp_to_byte(pairs[i].order);
    }

    assert(!pa.error());
    return msg;
}

static void
close_channel(channel* chan, event_orderer* eo)
{
    ERRORMSG1("closing connection [%i]", chan->sock.get()); // say which
    eo->release_references(&chan->references.front(), chan->references.size());
    chan->reset();
}

int
eos_daemon(po6::net::location loc)
{
    long max_fds = sysconf(_SC_OPEN_MAX);
    event_orderer eo;
    po6::io::fd epollfd(epoll_create(1 << 16));
    po6::net::socket listenfd(loc.address.family(), SOCK_STREAM, IPPROTO_TCP);
    channel* channels = new channel[max_fds];
    int fd;
    std::auto_ptr<e::buffer> msg;

    listenfd.set_reuseaddr();
    listenfd.bind(loc);
    listenfd.listen(16);
    epoll_event ee;
    ee.data.fd = listenfd.get();
    ee.events = EPOLLIN;

    if (epoll_ctl(epollfd.get(), EPOLL_CTL_ADD, listenfd.get(), &ee) < 0)
    {
        ERRNOMSG("epoll_ctl");
        return -1;
    }

    while (process_events(epollfd.get(), &listenfd, channels, &fd, &msg))
    {
        if (msg.get())
        {
            uint64_t nonce;
            uint16_t msg_type;
            e::buffer::unpacker up = msg->unpack_from(sizeof(uint32_t));
            up = up >> msg_type >> nonce;

            switch (static_cast<network_constant>(msg_type))
            {
                case EOSNC_CREATE_EVENT:
                    msg = eosnc_create_event(channels + fd, msg, nonce, &eo);
                    break;
                case EOSNC_ACQUIRE_REF:
                    msg = eosnc_acquire_ref(channels + fd, msg, nonce, &eo);
                    break;
                case EOSNC_RELEASE_REF:
                    msg = eosnc_release_ref(channels + fd, msg, nonce, &eo);
                    break;
                case EOSNC_QUERY_ORDER:
                    msg = eosnc_query_order(channels + fd, msg, nonce, &eo);
                    break;
                case EOSNC_ASSIGN_ORDER:
                    msg = eosnc_assign_order(channels + fd, msg, nonce, &eo);
                    break;
                default:
                    msg.reset();
            }

            channels[fd].outbuffer = msg;

            if (channels[fd].outbuffer.get())
            {
                channels[fd].outprogress = channels[fd].outbuffer->as_slice();
                ee.data.fd = fd;
                ee.events = EPOLLOUT;

                if (epoll_ctl(epollfd.get(), EPOLL_CTL_MOD, fd, &ee) < 0)
                {
                    close_channel(channels + fd, &eo);
                }
            }
        }
        else
        {
            close_channel(channels + fd, &eo);
        }
    }

    return 0;
}

int
main(int argc, const char* argv[])
{
    argc = 0;
    argv = NULL;
    return eos_daemon(po6::net::location("127.0.0.1", 7890));
}
