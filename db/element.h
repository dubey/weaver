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
#include <algorithm>
#include <string.h>

#include "common/weaver_constants.h"
#include "db/property.h"

namespace db
{
namespace element
{
    class element
    {
        public:
            element() { }
            element(uint64_t id, vc::vclock &vclk);

        protected:
            uint64_t id;
            vc::vclock creat_time;
            vc::vclock del_time;

        public:
            std::vector<property> properties;
            std::shared_ptr<vc::vclock> view_time;

        public:
            void add_property(property prop);
            void delete_property(std::string &key, vc::vclock &tdel);
            void remove_property(std::string &key, vc::vclock &vclk);
            bool has_property(std::string &key, std::string &value, vc::vclock &vclk);
            bool has_property(property &prop, vc::vclock &vclk);
            bool has_property(std::pair<std::string, std::string> &p, vc::vclock &vclk);
            bool has_all_properties(std::vector<std::pair<std::string, std::string>> &props, vc::vclock &vclk);
            bool check_and_add_property(property prop);
            void set_properties(std::vector<property> &props);
            const std::vector<property>* get_props() const;
            void update_del_time(vc::vclock &del_time);
            const vc::vclock& get_del_time() const;
            void update_creat_time(vc::vclock &creat_time);
            const vc::vclock& get_creat_time() const;
            std::pair<bool, std::string> get_property_value(std::string prop_key, vc::vclock &at_time);
            void set_id(uint64_t id);
            uint64_t get_id() const;
    };

}
}

#endif
