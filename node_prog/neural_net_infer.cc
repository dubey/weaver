/*
 * ===============================================================
 *    Description:  Neural net inference implementation
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *                  Siddhant Manocha, siddhantmanocha1994@gmail
 *
 * Copyright (C) 2015, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <math.h>

#define weaver_debug_
#include "common/stl_serialization.h"
#include "node_prog/edge.h"
#include "node_prog/neural_net_infer.h"

using node_prog::Node_Parameters_Base;
using node_prog::Node_State_Base;
using node_prog::search_type;
using node_prog::nn_params;
using node_prog::nn_state;
using node_prog::node;
using node_prog::edge;

// params
nn_params :: nn_params()
    : order(-1)
    , act_func("sigmoid")
{ }

uint64_t
nn_params :: size(void *aux_args) const
{
    return message::size(aux_args, network_description)
         + message::size(aux_args, input)
         + message::size(aux_args, order)
         + message::size(aux_args, act_func)
         + message::size(aux_args, layer_type)
         + message::size(aux_args, layer_op);
}

void
nn_params :: pack(e::packer &packer, void *aux_args) const
{
    message::pack_buffer(packer, aux_args, network_description);
    message::pack_buffer(packer, aux_args, input);
    message::pack_buffer(packer, aux_args, order);
    message::pack_buffer(packer, aux_args, act_func);
    message::pack_buffer(packer, aux_args, layer_type);
    message::pack_buffer(packer, aux_args, layer_op);
}

void
nn_params :: unpack(e::unpacker &unpacker, void *aux_args)
{
    message::unpack_buffer(unpacker, aux_args, network_description);
    message::unpack_buffer(unpacker, aux_args, input);
    message::unpack_buffer(unpacker, aux_args, order);
    message::unpack_buffer(unpacker, aux_args, act_func);
    message::unpack_buffer(unpacker, aux_args, layer_type);
    message::unpack_buffer(unpacker, aux_args, layer_op);
}

// state
nn_state :: nn_state()
    : visited(false)
    , in_count(0)
{ }

uint64_t
nn_state :: size(void *aux_args) const
{
    return message::size(aux_args, visited)
         + message::size(aux_args, in_count)
         + message::size(aux_args, value);
}

void
nn_state :: pack(e::packer &packer, void *aux_args) const
{
    message::pack_buffer(packer, aux_args, visited);
    message::pack_buffer(packer, aux_args, in_count);
    message::pack_buffer(packer, aux_args, value);
}

void
nn_state :: unpack(e::unpacker &unpacker, void *aux_args)
{
    message::unpack_buffer(unpacker, aux_args, visited);
    message::unpack_buffer(unpacker, aux_args, in_count);
    message::unpack_buffer(unpacker, aux_args, value);
}

// forward and backward edge props
static std::pair<std::string, std::string> fwd_prop = std::make_pair("dir", "f");
static std::pair<std::string, std::string> rev_prop = std::make_pair("dir", "b");

uint32_t
calculate_in_count(node &n)
{
    uint32_t count = 0;
    for (edge &e: n.get_edges()) {
        if (e.has_property(rev_prop)) {
            count++;
        }
    }

    return count;
}

bool
to_double(const std::string &str, double &v)
{
    try {
        v = std::stod(str);
    } catch (std::invalid_argument &e) {
        WDEBUG << "invalid arg " << e.what() << std::endl;
        WDEBUG << "prop=" << str << std::endl;
        return false;
    }
    return true;
}

bool
get_edge_weight(edge &e, double &weight)
{
    return to_double(e.get_property("weight"), weight);
}

bool
get_edge_order(edge &e, int &order)
{
    try {
        order = std::stoi(e.get_property("order"));
    } catch (std::invalid_argument &ia) {
        WDEBUG << "invalid arg " << ia.what() << std::endl;
        WDEBUG << "prop=" << e.get_property("order") << std::endl;
        return false;
    }
    return true;
}

// activation functions
// f(x) = x
double
id(double x)
{
    return x;
}

// f(x) = 0 if x<0, 1 otherwise
double
step(double x)
{
    return x>=0? 1 : 0;
}

// f(x) = 1 / (1 + e^(-x))
double
sigmoid(double x)
{
    double denominator = 1 + exp(-1 * x);
    return 1 / denominator;
}

// f(x) = 0 if x<0, x otherwise
double
relu(double x)
{
    return x>=0? x : 0;
}

// f(x) = e^(-(x^2))
double
gaussian(double x)
{
    double exponent = -1 * x * x;
    return exp(exponent);
}

double
apply_activation_function(double val, const std::string &func)
{
    if (func == "id") {
        return id(val);
    } else if (func == "step") {
        return step(val);
    } else if (func == "sigmoid") {
        return sigmoid(val);
    } else if (func == "relu") {
        return relu(val);
    } else if (func == "gaussian") {
        return gaussian(val);
    } else {
        // unknown func type, apply sigmoid
        return sigmoid(val);
    }
}

extern "C" {

PROG_FUNC_DEFINE(nn);

std::pair<search_type, std::vector<std::pair<db::remote_node, std::shared_ptr<Node_Parameters_Base>>>>
node_prog :: node_program(node &n,
   db::remote_node&,
   std::shared_ptr<Node_Parameters_Base> param_ptr,
   std::function<Node_State_Base&()> state_getter)
{
    //WDEBUG << "neural net prog execing at node=" << n.get_handle() << std::endl;

    Node_State_Base &state_base = state_getter();
    nn_state& state = dynamic_cast<nn_state&>(state_base);

    Node_Parameters_Base &param_base = *param_ptr;
    nn_params &params = dynamic_cast<nn_params&>(param_base);

    std::vector<std::pair<db::remote_node, std::shared_ptr<Node_Parameters_Base>>> next;

    if (n.get_handle() == params.network_description.first) {
        // first node/layer
        state.visited = true;
        auto input = params.input;
        params.input.clear();
        params.input.emplace_back(0.0);

        for (edge &e: n.get_edges()) {
            if (e.has_property(fwd_prop)) {
                int order;
                double weight;
                if (!get_edge_weight(e, weight)
                 || !get_edge_order(e, order)) {
                    continue;
                }

                params.input[0] = weight*input[order];
                params.order = -1;
                params.layer_type = "init";
                params.layer_op = "";
                next.emplace_back(std::make_pair(e.get_neighbor(),
                                                 std::make_shared<nn_params>(params)));
            }
        }
    } else if (n.get_handle() == params.network_description.second) {
        // last node/layer
        if (!state.visited) {
            state.visited = true;
            state.in_count = calculate_in_count(n);
            state.value.assign(state.in_count, 0);
        }

        state.value[params.order] = params.input[0];

        if (--state.in_count == 0) {
            params.input = state.value;
            next.emplace_back(std::make_pair(db::coordinator,
                                             std::make_shared<nn_params>(params)));
        }
    } else {
        // middle layer
        if (!state.visited) {
            state.visited = true;
            state.in_count = calculate_in_count(n);
            //WDEBUG << "in count=" << state.in_count << std::endl;

            //double bias;
            //if (!to_double(n.get_property("bias"), bias)) {
            //    WDEBUG << "bias fail" << std::endl;
            //    state.value.emplace_back(bias);
            //} else {
            //    state.value.emplace_back(0.0);
            //}
            //WDEBUG << "value=" << state.value[0] << std::endl;
            state.value.emplace_back(0.0);
        }

        state.value[0] += params.input[0];

        if (--state.in_count == 0) {
            //WDEBUG << "in count zero" << std::endl;
            state.value[0] = apply_activation_function(state.value[0], params.act_func);
            //WDEBUG << "val=" << state.value[0]
            //       << " val sz=" << state.value.size()
            //       << std::endl;

            for (edge &e: n.get_edges()) {
                if (e.has_property(fwd_prop)) {
                    double weight;
                    if (!get_edge_weight(e, weight)
                     || !get_edge_order(e, params.order)) {
                        WDEBUG << "fail weight/order" << std::endl;
                        continue;
                    }

                    params.layer_type = e.get_property("type");
                    params.layer_op   = e.get_property("op");
                    //if (params.layer_type.empty()
                    // || params.layer_op.empty()) {
                    //    continue;
                    //}

                    params.input[0] = weight*state.value[0];
                    //WDEBUG << "params.input=" << params.input[0] << std::endl;

                    next.emplace_back(std::make_pair(e.get_neighbor(),
                                                     std::make_shared<nn_params>(params)));
                }
            }
        }
    }

    return std::make_pair(search_type::BREADTH_FIRST, next);
}

} // extern C
