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

#ifndef weaver_db_property_h_
#define weaver_db_property_h_

#include <string>
#include <memory>

#include "common/weaver_constants.h"
#include "common/vclock.h"

#include "node_prog/property.h"

namespace db
{
    class property : public node_prog::property
    {
        private:
            vc::vclock creat_time;
            std::unique_ptr<vc::vclock> del_time;

        public:
            property();
            property(const std::string&, const std::string&);
            property(const std::string&, const std::string&, const vc::vclock&);
            property(const property &other);

            bool operator==(property const &p2) const;

            const vc::vclock& get_creat_time() const;
            const std::unique_ptr<vc::vclock>& get_del_time() const;
            bool is_deleted() const;
            void update_creat_time(const vc::vclock&);
            void update_del_time(const vc::vclock&);
    };

    class property_key_hasher
    {
        size_t operator() (const property &p) const;
    };
}

#endif
