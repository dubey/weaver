import sys
import networkx as nx
import matplotlib.pyplot as plt

def load(argv):
    assert(len(argv) == 2)
    G=nx.Graph()
    inputfile = open(argv[1])
    for line in inputfile:
        edge = line.split()
        if edge[0] is '#': # ignore comments
            continue
        assert(len(edge) == 2) 
        print(edge)
        G.add_edge(int(edge[0]),int(edge[1]))
    return G

G = load(sys.argv)
nx.draw(G)
plt.show()
