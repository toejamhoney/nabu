from collections import namedtuple

from matrix import SimpleMatrix


def build_assoc_graph(g1, g2):
    print g1.matrix
    print g2.matrix
    v = build_v(g1.v, g2.v)
    ag = AssocGraph(v)
    e = build_e(ag, g1, g2)
    ag.init_edges(e)
    print ag.matrix
    return ag


def calc_weight(v1, v2):
    return float(len(v1.attrs.intersection(v2.attrs)))/len(v1.attrs.union(v2.attrs))


def build_v(verts1, verts2):
    v = []
    attrs = []
    for v1 in verts1:
        for v2 in verts2:
            weight = calc_weight(v1, v2)
            v.append(((v1.label, v2.label), attrs, weight, v1.label, v2.label))
    return v


def build_e(ag, g1, g2):
    edges = set()
    for vert1 in ag.v:
        for vert2 in ag.v:
            if vert1.v1 == vert1.v2 or vert2.v1 == vert2.v2:
                continue
            if g1.adjacent(vert1.v1, vert2.v1) and g2.adjacent(vert1.v2, vert2.v2):
                edges.add((vert1.label, vert2.label))
            elif not g1.adjacent(vert1.v1, vert2.v1) and not g2.adjacent(vert1.v2, vert2.v2):
                edges.add((vert1.label, vert2.label))
    return edges


Vertex = namedtuple('Vertex', 'idx label attrs')
WeightedVertex = namedtuple('WeightedVertex', Vertex._fields + ('weight',))
AssocVertex = namedtuple('AgVertex', WeightedVertex._fields + ('v1', 'v2'))


class Graph(object):

    def __init__(self, v=None, e=None):
        if not e:
            e = []
        if not v:
            v = []
        self.v = []
        self.matrix = None
        self.order = 0
        self.size = 0
        self.cliques = []
        self.init_vertices(v)
        self.init_matrix(self.order)
        self.init_edges(e)

    def init_matrix(self, order):
        self.matrix = SimpleMatrix(order)

    def init_vertices(self, v):
        self.v = []
        for idx, vert in enumerate(v):
            self.v.append(self.make_vertex(idx, vert))
        self.order = len(self.v)

    def init_edges(self, e):
        if not e:
            return
        self.size = len(e)
        vmap = dict()
        for src, dst in e:
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

    def make_vertex(self, idx, vert):
        vertex = None
        try:
            vertex = Vertex(idx, *vert)
        except TypeError:
            vertex = Vertex(idx, vert, [])
        finally:
            return vertex

    def v_by_label(self, label):
        for vertex in self.v:
            if vertex.label == label:
                return vertex.idx
        raise KeyError("Unknown vertex label")

    def adjacent(self, v1, v2):
        v1 = self.v_by_label(v1)
        v2 = self.v_by_label(v2)
        if v1 == v2:
            return 0
        return self.matrix[v1][v2]

    def neighbors(self, nid):
        return [idx for idx, edge in enumerate(self.matrix[nid]) if edge]

    def bron_kerbosch_1(self, r, p, x):
        if not p and not x:
            self.cliques.append(r)
        while p:
            vert = p.pop()
            n = set(self.neighbors(vert))
            print 'Neighbors:',vert,n
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
        rv = '\n'.join([' '.join([str(col) for col in row]) for row in self.matrix])
        return rv


class AssocGraph(Graph):

    @staticmethod
    def jaccard_coef(mcs, g1, g2):
        print mcs,g2,g2
        return float(mcs)/(g1+g2-mcs)

    def make_vertex(self, idx, vert):
        vertex = None
        try:
            vertex = AssocVertex(idx, *vert)
        finally:
            return vertex

    def clique_weight(self, clique):
        print clique
        return sum([self.v[n].weight for n in clique])

    def find_max_clique(self):
        weights = [self.clique_weight(c) for c in self.cliques]
        maxw = max(weights)
        return weights.index(maxw)

    def sim_score(self, g1order, g2order):
        self.bron_kerbosch_1(r=set(), p=set([i for i in range(len(self.v))]), x=set())
        mcl_idx = self.find_max_clique()
        mcs = set([self.v[i].v1 for i in self.cliques[mcl_idx]])
        print 'MCS',mcs
        return self.jaccard_coef(len(mcs), g1order, g2order)

    def __str__(self):
        rv = ""
        for idx, node in enumerate(self.v):
            rv += "[%d](%s, %s) " % (idx, node.v1, node.v2)
        return rv


def main():
    import sys

    g = Graph([i for i in range(10)])
    print g

    g1v = [('A1', frozenset(['a'])), ('A2', frozenset(['b','a'])), ('A3', frozenset(['a','c']))]
    g2v = [('B1', frozenset(['a'])), ('B2', frozenset(['d','a'])), ('B3', frozenset(['x','y']))]
    g3v = [('C1', frozenset(['a'])), ('C2', frozenset(['d','a'])), ('C3', frozenset(['a','c']))]

    g1 = Graph(g1v, [('A1', 'A2'), ('A2', 'A3')])
    g2 = Graph(g2v, [('B1', 'B2'), ('B2', 'B3')])
    g3 = Graph(g3v, [('C1', 'C2'), ('C2', 'C3')])

    ag = build_assoc_graph(g1, g2)
    ag2 = build_assoc_graph(g1, g3)

    print ag
    print ag2

    sys.stdout.write("%f\n" % ag.sim_score(g1.order, g2.order))
    sys.stdout.write("%f\n" % ag2.sim_score(g1.order, g3.order))


if __name__ == "__main__":
    main()
