/*
 * =====================================================================================
 *
 *    Description:  Central server responsible for handling all queries
 *
 *        Created:  10/27/2012 05:20:01 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 *
 * =====================================================================================
 */

#ifndef __CENTRAL__
#define __CENTRAL__

//C++
#include <vector>

//Busybee
#include <busybee_sta.h>

//po6
#include <po6/net/location.h>

//Weaver
#include "graph_elem.h"

//Constants
#define CENTRAL_PORT 5200
#define CENTRAL_REC_PORT 4200
#define LOCAL_IPADDR "127.0.0.1"
/* This also defines the number of physical servers
 * = MAX_PORT - CENTRAL_PORT
 */
#define MAX_PORT 5201

namespace coordinator
{
    class central
    {
        public:
            central ();
        private:
            po6::net::location myloc;

        public:
            busybee_sta bb;
            busybee_sta rec_bb;
            uint32_t time;
            //A list of graph elements, which would probably be some server,
            //id/address pair
            int port_ctr;
            std::vector<coordinator::graph_elem *> elements;
    };

    inline
    central :: central ()
        : myloc (LOCAL_IPADDR, CENTRAL_PORT)
        , bb (myloc.address, myloc.port, 0)
        , rec_bb (myloc.address, CENTRAL_REC_PORT, 0)
    {
        time = 0;
        port_ctr = CENTRAL_PORT;
    }

} //namespace coordinator

#endif // __CENTRAL__
