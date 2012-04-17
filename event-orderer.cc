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
#include <queue>
#include <vector>

// Boost
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/dag_shortest_paths.hpp>
#include <boost/graph/two_bit_color_map.hpp>

// po6
#include <po6/net/location.h>
#include <po6/net/socket.h>

// e
#include <e/buffer.h>
#include <e/endian.h>

// eos
#include "eos.h"
#include "eos_cmp_encode.h"
#include "network_constants.h"

#define xtostr(X) #X
#define tostr(X) xtostr(X)
#define ERRORMSG(X) fprintf(stderr, "%s:%i:  " X "\n", __FILE__, __LINE__)
#define ERRORMSG1(X, Y) fprintf(stderr, "%s:%i:  " X "\n", __FILE__, __LINE__, Y)
#define ERRORMSG2(X, Y1, Y2) fprintf(stderr, "%s:%i:  " X "\n", __FILE__, __LINE__, Y1, Y2)
#define ERRNOMSG(CALL) ERRORMSG2(tostr(CALL) " failed:  %s  [ERRNO=%i]", strerror(errno), errno)

template <typename TM>
class dfs_seen_visitor : public boost::default_dfs_visitor
{
    public:
        dfs_seen_visitor(TM seenmap)
            : m_seenmap(seenmap)
        {
        }
        
        template <typename V, typename G>
        void discover_vertex(V u, const G& g) const
        {
            boost::put(m_seenmap, u, 1);
        }

    private:
        TM m_seenmap;
};

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
        typedef boost::adjacency_list<boost::setS, boost::listS, boost::directedS,
                                      boost::property<boost::vertex_name_t, uint64_t,
                                      boost::property<boost::vertex_index_t, uint64_t,
                                      boost::property<boost::vertex_color_t, uint64_t> > >,
                                      boost::property<boost::edge_weight_t, int> > graph;
        typedef boost::graph_traits<graph>::vertex_descriptor vertex;
        typedef boost::graph_traits<graph>::edge_descriptor edge;

    private:
        void incref(vertex v);
        void decref(vertex v);
        int compute_order(vertex lhs, vertex rhs);

    private:
        uint64_t m_nextid;
        graph m_graph;
        std::map<uint64_t, vertex> m_ids;
};

event_orderer :: event_orderer()
    : m_nextid(1)
    , m_graph()
    , m_ids()
{
}

event_orderer :: ~event_orderer() throw ()
{
}

uint64_t
event_orderer :: create_event()
{
    uint64_t event_id = m_nextid;
    vertex v = boost::add_vertex(m_graph);
    boost::put(boost::vertex_name, m_graph, v, 1);
    boost::put(boost::vertex_index, m_graph, v, event_id); 
    m_ids[event_id] = v;
    ++m_nextid;
    return event_id;
}

size_t
event_orderer :: acquire_references(uint64_t* events, size_t events_sz)
{
    size_t i;

    // Increment reference counts
    for (i = 0; i < events_sz; ++i)
    {
        std::map<uint64_t, vertex>::iterator viter = m_ids.find(events[i]);

        if (viter == m_ids.end())
        {
            break;
        }

        incref(viter->second);
    }

    // Inside this conditional lies the normal exit point.
    if (i == events_sz)
    {
        return events_sz;
    }

    // Decrement reference counts we incremented in error
    for (size_t j = 0; j < i; ++j)
    {
        std::map<uint64_t, vertex>::iterator viter = m_ids.find(events[j]);
        assert(viter != m_ids.end());
        decref(viter->second);
    }

    return i;
}

size_t
event_orderer :: release_references(uint64_t* events, size_t events_sz)
{
    for (size_t i = 0; i < events_sz; ++i)
    {
        std::map<uint64_t, vertex>::iterator viter = m_ids.find(events[i]);

        if (viter == m_ids.end())
        {
            continue;
        }

        decref(viter->second);
    }

    return events_sz;
}

size_t
event_orderer :: query_order(eos_pair* pairs, size_t pairs_sz)
{
    for (size_t i = 0; i < pairs_sz; ++i)
    {
        std::map<uint64_t, vertex>::iterator lhsiter = m_ids.find(pairs[i].lhs);
        std::map<uint64_t, vertex>::iterator rhsiter = m_ids.find(pairs[i].rhs);

        if (lhsiter == m_ids.end() || rhsiter == m_ids.end())
        {
            pairs[i].order = EOS_NOEXIST;
            continue;
        }

        int resolve = compute_order(lhsiter->second, rhsiter->second);

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
    std::vector<std::pair<vertex, vertex> > created_edges;
    size_t i;

    for (i = 0; i < pairs_sz; ++i)
    {
        std::map<uint64_t, vertex>::iterator lhsiter = m_ids.find(pairs[i].lhs);
        std::map<uint64_t, vertex>::iterator rhsiter = m_ids.find(pairs[i].rhs);

        if (lhsiter == m_ids.end() || rhsiter == m_ids.end())
        {
            break;
        }

        vertex lhs = lhsiter->second;
        vertex rhs = rhsiter->second;
        int resolve = compute_order(lhsiter->second, rhsiter->second);

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
                    boost::add_edge(lhs, rhs, m_graph);
                    created_edges.push_back(std::make_pair(lhs, rhs));
                    incref(rhs);
                    break;
                case EOS_HAPPENS_AFTER:
                    boost::add_edge(rhs, lhs, m_graph);
                    created_edges.push_back(std::make_pair(rhs, lhs));
                    incref(lhs);
                    break;
                case EOS_CONCURRENT:
                case EOS_NOEXIST:
                default:
                    break;
            }
        }
    }

    // Inside this conditional lies the normal exit point.
    if (i == pairs_sz)
    {
        return pairs_sz;
    }

    for (size_t j = 0; j < created_edges.size(); ++j)
    {
        boost::remove_edge(created_edges[j].first, created_edges[j].first, m_graph);
        decref(created_edges[j].first);
    }

    return i;
}

void
event_orderer :: incref(vertex v)
{
    uint64_t count = boost::get(boost::vertex_name, m_graph, v);
    boost::put(boost::vertex_name, m_graph, v, count + 1);
}

void
event_orderer :: decref(vertex seed)
{
    std::queue<vertex> toremove;
    toremove.push(seed);

    while (!toremove.empty())
    {
        vertex v = toremove.front();
        toremove.pop();

        uint64_t count = boost::get(boost::vertex_name, m_graph, v);
        boost::put(boost::vertex_name, m_graph, v, count - 1);

        if (count > 1)
        {
            continue;
        }

        boost::graph_traits<graph>::out_edge_iterator os;
        boost::graph_traits<graph>::out_edge_iterator oe;

        for (boost::tie(os, oe) = boost::out_edges(v, m_graph); os != oe; ++os)
        {
            vertex u = boost::target(*os, m_graph);
            toremove.push(u);
        }
    }
}

int
event_orderer :: compute_order(vertex lhs, vertex rhs)
{
    boost::property_map<graph, boost::vertex_color_t>::type colorslhs;
    colorslhs = boost::get(boost::vertex_color, m_graph);
    breadth_first_search(m_graph, lhs, color_map(colorslhs));

    if (colorslhs[rhs] != 0)
    {
        return -1;
    }

    boost::property_map<graph, boost::vertex_color_t>::type colorsrhs;
    colorsrhs = boost::get(boost::vertex_color, m_graph);
    breadth_first_search(m_graph, rhs, color_map(colorsrhs));

    if (colorsrhs[lhs] != 0)
    {
        return 1;
    }

    return 0;
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
        sock.close();
    }
    catch (...)
    {
    }

    loc = po6::net::location();
    outbuffer.reset();
    outprogress = e::slice();
    inbuffer.reset();
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
eosnc_create_event(channel* /*chan*/, std::auto_ptr<e::buffer> msg, uint64_t nonce, event_orderer* eo)
{
    const size_t SEND_MSG_SIZE = sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t); 
    uint64_t event = eo->create_event();
    msg.reset(e::buffer::create(SEND_MSG_SIZE));
    e::buffer::packer pa = msg->pack();
    pa = pa << static_cast<uint32_t>(SEND_MSG_SIZE) << nonce << event;
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
                    ERRORMSG1("closing connection [%i]", fd); // say which
                    channels[fd].reset();
                }
            }
        }
        else
        {
            ERRORMSG1("closing connection [%i]", fd); // say which
            channels[fd].reset();
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
