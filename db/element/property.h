/*
 * ===============================================================
 *    Description:  Each graph element (node or edge) can have
 *                  properties, which are key-value pairs 
 *
 *        Created:  Friday 12 October 2012 01:28:02  EDT
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __PROPERTY__
#define __PROPERTY__

#include <stdlib.h>
#include <string.h>

namespace db 
{
namespace element
{
    class property
    {
        public:
            property ();
            property (uint32_t, uint32_t);
        
        public:
            char* __key;
            char* __value;
            uint32_t key;
            uint32_t value;
            bool operator==(property p2) const;
    };

    inline
    property :: property ()
    {
    }

    inline
    property :: property (uint32_t _key, uint32_t _value)
    {
        key = _key;
        value = _value;
    }

    inline bool
    property :: operator==(property p2) const
    {
        return ((key == p2.key) && (value == p2.value));
    }
}
}

#endif //__PROPERTY__
