#ifndef __DIJKSTRA_PROG__
#define __DIJKSTRA_PROG__

#include <vector>

#include "element/node.h"
#include "element/remote_node.h"

namespace db
{
    class dijkstra_params{
        
    };
    class dijkstra_node_state{

    };

    class dijkstra_cache_value{

    };

    std::vector<std::pair<element::remote_node, dijkstra_params>> 
    dijkstra_node_program(element::node &n,
            dijkstra_params &params,
            dijkstra_node_state &state,
            dijkstra_cache_value &cache){

        return std::vector<std::pair<element::remote_node, dijkstra_params>>();
    }
}

#endif //__DIKJSTRA_PROG__
