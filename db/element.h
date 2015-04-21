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
#include "common/property_predicate.h"
#include "db/property.h"

using vc::vclock_ptr_t;

namespace db
{
    struct property_hasher
    {
        private:
            std::function<size_t(const std::string&)> string_hasher;

        public:
            property_hasher() : string_hasher(weaver_util::murmur_hasher<std::string>()) { }

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
            element(const std::string &handle, const vclock_ptr_t &vclk);

        protected:
            std::string handle;
            vclock_ptr_t creat_time;
            vclock_ptr_t del_time;

        public:
#ifdef weaver_large_property_maps_
            std::unordered_map<std::string, std::vector<std::shared_ptr<property>>> properties;
#else
            std::vector<std::shared_ptr<property>> properties;
#endif
            vclock_ptr_t view_time;
            order::oracle *time_oracle;

        public:
            bool add_property(const property &prop);
            bool add_property(const std::string &key, const std::string &value, const vclock_ptr_t &vclk);
            bool delete_property(const std::string &key, const vclock_ptr_t &tdel);
            bool delete_property(const std::string &key, const std::string &value, const vclock_ptr_t &tdel);
            void remove_property(const std::string &key);
            bool has_property(const std::string &key, const std::string &value);
            bool has_property(const std::pair<std::string, std::string> &p);
            bool has_predicate(const predicate::prop_predicate &p);
            bool has_all_properties(const std::vector<std::pair<std::string, std::string>> &props);
            bool has_all_predicates(const std::vector<predicate::prop_predicate> &preds);
            void update_del_time(const vclock_ptr_t &del_time);
            const vclock_ptr_t& get_del_time() const;
            void update_creat_time(const vclock_ptr_t &creat_time);
            const vclock_ptr_t& get_creat_time() const;
            void set_handle(const std::string &handle);
            const std::string& get_handle() const;
#ifdef weaver_large_property_maps_
            void set_properties(std::unordered_map<std::string, std::vector<std::shared_ptr<property>>> &props) { properties = props; }
            const std::unordered_map<std::string, std::vector<std::shared_ptr<property>>>* get_properties() const { return properties; }
#else
            void set_properties(std::vector<std::shared_ptr<property>> &props) { properties = props; }
            const std::vector<std::shared_ptr<property>>* get_properties() const { return &properties; }
#endif
    };

}

#endif
