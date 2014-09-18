/*
 * ===============================================================
 *    Description:  Graph element base for edges and vertices
 *
 *        Created:  Thursday 11 October 2012 11:15:20  EDT
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_db_element_element_h_
#define weaver_db_element_element_h_

#include <limits.h>
#include <stdint.h>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <string.h>

#include "common/weaver_constants.h"
#include "common/event_order.h"
#include "db/property.h"

namespace db
{
namespace element
{
    class element
    {
        public:
            element() { }
            element(const std::string &handle, vc::vclock &vclk);

        protected:
            std::string handle;
            vc::vclock creat_time;
            vc::vclock del_time;

        public:
            std::unordered_map<std::string, property> properties;
            std::shared_ptr<vc::vclock> view_time;
            order::oracle *time_oracle;

        public:
            void add_property(const property &prop);
            void add_property(const std::string &key, const std::string &value, const vc::vclock &vclk);
            void delete_property(std::string &key, vc::vclock &tdel);
            void remove_property(std::string &key);
            bool has_property(const std::string &key, const std::string &value);
            bool has_property(const std::pair<std::string, std::string> &p);
            bool has_all_properties(const std::vector<std::pair<std::string, std::string>> &props);
            void set_properties(std::unordered_map<std::string, property> &props);
            const std::unordered_map<std::string, property>* get_props() const;
            void update_del_time(vc::vclock &del_time);
            const vc::vclock& get_del_time() const;
            void update_creat_time(vc::vclock &creat_time);
            const vc::vclock& get_creat_time() const;
            void set_handle(const std::string &handle);
            std::string get_handle() const;
    };

}
}

#endif
