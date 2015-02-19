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

#ifndef weaver_db_element_h_
#define weaver_db_element_h_

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
    struct property_hasher
    {
        private:
            std::function<size_t(const std::string&)> string_hasher;

        public:
            property_hasher() : string_hasher(std::hash<std::string>()) { }

            size_t
            operator()(const property &p) const
            {
                size_t hkey = string_hasher(p.key);
                size_t hvalue = string_hasher(p.value);
                return ((hkey + 0x9e3779b9 + (hvalue<<6) + (hvalue>>2)) ^ hvalue);
            }
    };
    
    class element
    {
        public:
            element() { }
            element(const std::string &handle, const vc::vclock &vclk);

        protected:
            std::string handle;
            vc::vclock creat_time;
            vc::vclock del_time;

        public:
            std::unordered_map<std::string, std::vector<std::shared_ptr<property>>> properties;
            std::shared_ptr<vc::vclock> view_time;
            order::oracle *time_oracle;

        public:
            bool add_property(const property &prop);
            bool add_property(const std::string &key, const std::string &value, const vc::vclock &vclk);
            bool delete_property(const std::string &key, const vc::vclock &tdel);
            bool delete_property(const std::string &key, const std::string &value, const vc::vclock &tdel);
            void remove_property(const std::string &key);
            bool has_property(const std::string &key, const std::string &value);
            bool has_property(const std::pair<std::string, std::string> &p);
            bool has_all_properties(const std::vector<std::pair<std::string, std::string>> &props);
            void set_properties(std::unordered_map<std::string, std::vector<std::shared_ptr<property>>> &props);
            const std::unordered_map<std::string, std::vector<std::shared_ptr<property>>>* get_props() const;
            void update_del_time(const vc::vclock &del_time);
            const vc::vclock& get_del_time() const;
            void update_creat_time(const vc::vclock &creat_time);
            const vc::vclock& get_creat_time() const;
            void set_handle(const std::string &handle);
            std::string get_handle() const;
    };

}

#endif
