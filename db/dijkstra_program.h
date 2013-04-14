namespace db
{
    class disjktra_params{

    }
    class disjktra_node_state{

    }
    class disjktra_cache_value{

    }

    std::vector<std::pair<element::remote_node, dijkstra_params>> 
    dijkstra_node_program(element::node &n,
            dijkstra_params &params,
            dijkstra_node_state &state,
            dijkstra_cache_value &cache){

        return std::vector<std::pair<element::remote_node, dijkstra_params>>();
    }
}
