#ifndef __DIJKSTRA_PROG__
#define __DIJKSTRA_PROG__

#include <vector>

#include "element/node.h"
#include "element/remote_node.h"
#include "db/node_prog_type.h"
#include "db/node_program.h"
#include "db/element/remote_node.h"

namespace db
{
    struct dijkstra_params {
        enum mode {START, PROP};
        db::element::remote_node source;
        db::element::remote_node dest;
        uint32_t edge_weight_name; // they key of the property which holds the weight of an an edge
        std::vector<common::property> edge_props;
        bool is_widest_path;

        // for returning from a prop
        std::pair<uint64_t, uint64_t> tentative_map_value;
    };

    struct dijkstra_node_state : Deletable {
        std::priority_queue<dijkstra_queue_elem, std::vector<dijkstra_queue_elem>, std::less<dijkstra_queue_elem>> next_nodes_shortest; 
        std::priority_queue<dijkstra_queue_elem, std::vector<dijkstra_queue_elem>, std::greater<dijkstra_queue_elem>> next_nodes_widest; 
        // map from a node (by its create time) to its cost and the req_id of the node that came before it in the shortest path
        std::unordered_map<uint64_t, std::pair<uint64_t, uint64_t>> visited_map; 

        virtual ~dijkstra_node_state(){
            /* implement me? XXX */
        }
    };

    struct dijkstra_cache_value : Deletable {
        int dummy;

        virtual ~dijkstra_cache_value(){
        }
    };

    std::vector<std::pair<element::remote_node, dijkstra_params>> 
    dijkstra_node_program(element::node &n,
            dijkstra_params &params,
            dijkstra_node_state &state,
            dijkstra_cache_value &cache){
        // empty response
        return std::vector<std::pair<element::remote_node, dijkstra_params>>();
    }
}

#endif //__DIKJSTRA_PROG__
