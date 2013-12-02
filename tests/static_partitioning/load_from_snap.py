import sys
import networkx as nx
import matplotlib.pyplot as plt

def load(argv):
    assert(len(argv) == 2)
    print 'loading graph from file'
    G=nx.Graph()
    inputfile = open(argv[1])
    for line in inputfile:
        edge = line.split()
        if edge[0] is '#': # ignore comments
            continue
        assert(len(edge) == 2) 
        G.add_edge(int(edge[0]),int(edge[1]))
    return G
