import sys
from random import randint


def make_matrix(n, m=None):
    if not m:
        m = n
    return [[0 for c in range(n)] for r in range(m)]


class Node(object):
    
    def __init__(self, id, neighbors):
        self.id = id
        self.neighbors = neighbors


class Graph(object):

    def __init__(self, v, e):
        self.cliques = []
        self.v = v
        self.e = e
        self.order = len(v)
        self.size = len(e)
        self.matrix = make_matrix(len(v))
        for r, c in e:
            self.matrix[r][c] = 1
            self.matrix[c][r] = 1

    def adjacent(self, v1, v2):
        if v1 == v2:
            return 0
        return self.matrix[v1][v2]

    def neighbors(self, nid):
        return [idx for idx,edge in enumerate(self.matrix[nid]) if edge == 1]

    def bron_kerbosch_1(self, R, P, X):
        if not P and not X:
            self.cliques.append(R)
        while P:
            vert = P.pop()
            N = set(self.neighbors(vert))
            self.bron_kerbosch_1(R.union({vert}), P.intersection(N), X.intersection(N))
            X.add(vert)

    def max_clique(self):
        if self.cliques:
            return max(self.cliques)
        else:
            return []

    def __str__(self):
        rv = '\n'.join([', '.join([str(r) for r in row]) for row in self.matrix]) 
        return rv


class AssocGraph(Graph):

    def __init__(self, g1, g2):
        self.cliques = []
        self.order = g1.order * g2.order
        self.v = [(a, b) for a in range(g1.order) for b in range(g2.order)]
        self.matrix = make_matrix(self.order)
        for i1, v1 in enumerate(self.v):
            for i2, v2 in enumerate(self.v):
                if v1[0] == v2[0] or v1[1] == v2[1]:
                    continue
                if g1.adjacent(v1[0], v2[0]) and g2.adjacent(v1[1], v2[1]):
                    self.matrix[i1][i2] = 1
                elif not g1.adjacent(v1[0], v2[0]) and not g2.adjacent(v1[1], v2[1]):
                    self.matrix[i1][i2] = 1
                else:
                    self.matrix[i1][i2] = 0


def similarity(mcs, g1, g2):
    return 1.0*mcs/(g1+g2-mcs)


if __name__ == "__main__":
    nodes = 3
    edges = 2

    g1 = Graph([0 for x in range(nodes)], [(randint(0,nodes-1), randint(0, nodes-1)) for x in range(edges)])
    g2 = Graph([0 for x in range(nodes)], [(randint(0,nodes-1), randint(0, nodes-1)) for x in range(edges)])

    g1 = Graph([0, 1, 2], [(0,1), (1,2)])
    g2 = Graph([0, 1, 2], [(0,1), (1,2)])

    ag = AssocGraph(g1, g2)
   
    ag.bron_kerbosch_1(R=set(), P=set([idx for idx in range(len(ag.v))]), X=set())

    sys.stdout.write("%f\n"% similarity(len(ag.max_clique()), g1.order, g2.order))

    # sys.stdout.write("%s\n\n%s\n\n%s\n" % (g1, g2, ag))
