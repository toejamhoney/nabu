import sys
from random import randint


def make_matrix(n, m=None):
    if not m:
        m = n
    return [[0 for c in range(n)] for r in range(m)]


class Graph(object):

    def __init__(self, v, e):
        self.v = []
        self.v_map = dict()
        for idx, (vname, vattrs) in enumerate(v):
            self.v.append(vname)
            self.v_map[vname] = {"idx": idx, "attrs": vattrs}
        self.e = e
        self.order = len(v)
        self.size = len(e)
        self.cliques = []
        self.matrix = make_matrix(self.order)

    def build(self):
        for src, dst in self.e:
            a = self.v_map.get(src)["idx"]
            b = self.v_map.get(dst)["idx"]
            self.matrix[a][b] = 1
            self.matrix[b][a] = 1

    def adjacent(self, v1, v2):
        if v1 == v2:
            return 0
        return self.matrix[v1][v2]

    def neighbors(self, nid):
        return [idx for idx, edge in enumerate(self.matrix[nid]) if edge == 1]

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

    def get_attr(self, v):
        return self.v_map.get(v)["attrs"]

    def __str__(self):
        rv = '\n'.join([', '.join([str(r) for r in row]) for row in self.matrix]) 
        return rv


class AssocGraph(Graph):

    def __init__(self, g1, g2):
        self.g1 = g1
        self.g2 = g2
        self.v_weights = dict()
        v = [((a, b), frozenset([])) for a in range(g1.order) for b in range(g2.order)]
        super(AssocGraph, self).__init__(v, set())

    def build(self):
        for i1, v1 in enumerate(self.v):
            self.calculate_weight(v1[0], v1[1])
            for i2, v2 in enumerate(self.v):
                if v1[0] == v2[0] or v1[1] == v2[1]:
                    continue
                if self.g1.adjacent(v1[0], v2[0]) and self.g2.adjacent(v1[1], v2[1]):
                    self.matrix[i1][i2] = 1
                elif not self.g1.adjacent(v1[0], v2[0]) and not self.g2.adjacent(v1[1], v2[1]):
                    self.matrix[i1][i2] = 1
                else:
                    self.matrix[i1][i2] = 0

    def calculate_weight(self, v1, v2):
        v1attr = self.g1.get_attr(v1)
        v2attr = self.g2.get_attr(v2)
        print v1attr, v2attr
        print len(v1attr.intersection(v2attr))
        print len(v1attr.union(v2attr))
        self.v_weights[(v1, v2)] = 1.0 * len(v1attr.intersection(v2attr))/len(v1attr.union(v2attr))

    def clique_weight(self, clique):
        return sum([self.v_weights.get(self.v[n]) for n in clique])

    def max_clique(self):
        weights = [self.clique_weight(c) for c in self.cliques]
        print weights
        maxw = max(weights)
        clique_idx = weights.index(maxw)
        return self.cliques[clique_idx]


def similarity(mcs, g1, g2):
    return 1.0*mcs/(g1+g2-mcs)


def main():
    nodes = 3
    edges = 2

    #g1 = Graph([0 for x in range(nodes)], [(randint(0,nodes-1), randint(0, nodes-1)) for x in range(edges)])
    #g2 = Graph([0 for x in range(nodes)], [(randint(0,nodes-1), randint(0, nodes-1)) for x in range(edges)])

    g1v = [(0, frozenset(['a'])), (1, frozenset(['b','a'])), (2, frozenset(['a','c']))]
    g2v = [(0, frozenset(['a'])), (1, frozenset(['d','a'])), (2, frozenset(['x','y']))]
    g3v = [(0, frozenset(['a'])), (1, frozenset(['d','a'])), (2, frozenset(['a','c']))]

    g1 = Graph(g1v, [(0, 1), (1, 2)])
    g2 = Graph(g2v, [(0, 1), (1, 2)])
    g3 = Graph(g3v, [(0, 1), (1, 2)])

    ag = AssocGraph(g1, g2)
    ag2 = AssocGraph(g1, g3)

    ag.build()
    ag2.build()
   
    ag.bron_kerbosch_1(R=set(), P=set([idx for idx in range(len(ag.v))]), X=set())
    ag2.bron_kerbosch_1(R=set(), P=set([idx for idx in range(len(ag2.v))]), X=set())

    mcl = ag.max_clique()
    mcl2 = ag2.max_clique()
    print mcl
    print mcl2

    sys.stdout.write("%f\n" % similarity(len(mcl), g1.order, g2.order))
    sys.stdout.write("%f\n" % similarity(len(mcl2), g1.order, g3.order))

    # sys.stdout.write("%s\n\n%s\n\n%s\n" % (g1, g2, ag))


if __name__ == "__main__":
    main()
