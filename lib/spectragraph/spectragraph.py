import sys
from collections import namedtuple


def make_matrix(n, m=None):
    if not m:
        m = n
    return [[0 for col in range(n)] for row in range(m)]


def build_assoc_graph(self, g1, g2):
    v = build_v(g1.v, g2.v)
    e = build_e(v, g1, g2)
    ag = AssocGraph(order=g1.order * g2.order)
    self.init_vertices(g1.v, g2.v)


def build_v(self, verts1, verts2):
    v = []
    for v1 in verts1:
        for v2 in verts2:
            weight = self.calc_weight(v1, v2)
            attrs = []
            v.append(AgVertex(len(self.v), weight, (v1.label, v2.label), attrs, v1.label, v2.label))
    return v


def build_e(self, v, g1, g2):
    for vertex in v:


'''
def build_e(self, v, g1, g2):
    print("AG.build G1.map: %s" % self.g1.v_map)
    print("AG.build G2.map: %s" % self.g2.v_map)
    for i1, v1 in enumerate(self.v):
        self.calc_weight(v1[0], v1[1])
        for i2, v2 in enumerate(self.v):
            if v1[0] == v2[0] or v1[1] == v2[1]:
                continue
            if self.g1.adjacent(v1[0], v2[0]) and self.g2.adjacent(v1[1], v2[1]):
                self.matrix[i1][i2] = 1

            elif not self.g1.adjacent(v1[0], v2[0]) and not self.g2.adjacent(v1[1], v2[1]):
                self.matrix[i1][i2] = 1
            else:
                self.matrix[i1][i2] = 0
'''


def calc_weight(self, v1, v2):
    return float(len(v1.attrs.intersection(v2.attrs)))/len(v1.attrs.union(v2.attrs))

Vertex = namedtuple('Vertex', 'idx weight label attrs')
AgVertex = namedtuple('AgVertex', Vertex._fields + 'v1 v2')


class Graph(object):

    def __init__(self, v=None, e=None, order=None):
        self.v = v
        if not v:
            self.v = []
        self.e = e
        if not e:
            self.e = {}
        self.order = len(v)
        self.size = len(e)
        self.cliques = []
        self.matrix = None

    def build(self):
        self.matrix = [[0 for col in range(self.order)] for row in range(self.order)]
        self.init_vertices()
        self.init_edges()

    def init_vertices(self):
        for idx, vert in enumerate(self.v):
            vertex = Vertex(idx, *vert)
            self.v[idx] = vertex

    def init_edges(self):
        vmap = dict()
        for src, dst in self.e:
            sidx = vmap.get(src)
            if not sidx:
                sidx = self.v_by_label(src)
                vmap[src] = sidx
            didx = vmap.get(dst)
            if not didx:
                didx = self.v_by_label(dst)
                vmap[dst] = didx
            self.matrix[sidx][didx] = 1
            self.matrix[didx][sidx] = 1

    def v_by_label(self, label):
        for vertex in self.v:
            if vertex.label == label:
                return vertex.idx
        raise KeyError("Unknown vertex label")

    def adjacent(self, v1, v2):
        if v1 == v2:
            return 0
        return self.matrix[v1][v2]

    def neighbors(self, nid):
        return [idx for idx, edge in enumerate(self.matrix[nid]) if edge == 1]

    def bron_kerbosch_1(self, r, p, x):
        if not p and not x:
            self.cliques.append(r)
        while p:
            vert = p.pop()
            n = set(self.neighbors(vert))
            self.bron_kerbosch_1(r.union({vert}), p.intersection(n), x.intersection(n))
            x.add(vert)

    def max_clique(self):
        if self.cliques:
            return max(self.cliques)
        else:
            return []

    def v_attr(self, v):
        idx = self.v_map.get(v)["idx"]
        return self.v_map.get(idx)["attrs"]

    def __str__(self):
        rv = '\n'.join([', '.join([str(r) for r in row]) for row in self.matrix]) 
        return rv


class AssocGraph(Graph):

    def __init__(self, order):
        super(AssocGraph, self).__init__(order=order)



    def clique_weight(self, clique):
        """
        return sum([self.v_weights.get(self.v[n]) for n in clique])
        """
        return sum([self.v])

    def max_clique(self):
        weights = [self.clique_weight(c) for c in self.cliques]
        print weights
        maxw = max(weights)
        clique_idx = weights.index(maxw)
        return self.cliques[clique_idx]

    def __str__(self):
        rv = ""
        for node in self.v:
            rv += "[%d/%d] " % (node[0], node[1])
        return rv


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
