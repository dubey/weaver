/*
 * ===============================================================
 *    Description:  Node program that implements deep learning
 *                  inference using a feed forward neural network
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *                  Siddhant Manocha, siddhantmanocha1994@gmail
 *
 * Copyright (C) 2015, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_node_prog_neural_net_infer_h_
#define weaver_node_prog_neural_net_infer_h_

#include "node_prog/boilerplate.h"

namespace node_prog
{
    struct nn_params: public virtual Node_Parameters_Base
    {
        // handles of first and last node
        std::pair<std::string, std::string> network_description;
        // input vector or value
        std::vector<double> input;
        // for first node, order defines which index of input to be propagated to next layer
        // for last node, order defines which index of the value to store input
        int order;
        // activation function
        std::string act_func;
        std::string layer_type;
        std::string layer_op;

        nn_params();
        ~nn_params() { }
        uint64_t size(void*) const;
        void pack(e::packer &packer, void*) const;
        void unpack(e::unpacker &unpacker, void*);

        // no caching
        bool search_cache() { return false; }
        cache_key_t cache_key() { return cache_key_t(); }
    };

    struct nn_state: public virtual Node_State_Base
    {
        bool visited;
        uint32_t in_count;
        std::vector<double> value;

        nn_state();
        ~nn_state() { }
        uint64_t size(void*) const;
        void pack(e::packer &packer, void*) const;
        void unpack(e::unpacker &unpacker, void*);
    };

    extern "C" {
        PROG_FUNC_DECLARE;
    }
}

#endif
