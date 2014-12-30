from matrix import SimpleMatrix


"""

Vertex = (idx, label, attrs, weight)

"""
V_IDX = 0
V_LABEL = 1
V_ATTRS = 2
V_WEIGHT = 3


class Graph(object):

    def __init__(self):
        self.v = []
        self.matrix = None
        self.order = 0
        self.size = 0
        self.cliques = []
        self.clique_weights = []

    @staticmethod
    def calc_weight(v1, v2):
        vattrs = V_ATTRS
        return float(len(set(v1[vattrs]).intersection(v2[vattrs])))/len(set(v1[vattrs]).union(v2[vattrs]))

    @staticmethod
    def make_vertex(idx, label, attrs=None, weight=0.0):
        if not attrs:
            attrs = []
        return idx, label, attrs, weight

    def init(self, v, e):
        self.init_vertices(v)
        self.init_matrix(self.order)
        self.init_edges(e)

    def init_vertices(self, v):
        self.v = []
        for idx, vert in enumerate(v):
            self.v.append(self.make_vertex(idx, *vert))
        self.order = len(self.v)

    def init_matrix(self, order):
        self.matrix = SimpleMatrix(order)

    def init_edges(self, e):
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

    def v_by_label(self, label):
        vlabel = V_LABEL
        for vertex in self.v:
            if vertex[vlabel] == label:
                return vertex[V_IDX]
        raise KeyError("Unknown vertex label")

    def adjacent(self, v1, v2):
        v1 = self.v_by_label(v1)
        v2 = self.v_by_label(v2)
        if v1 == v2:
            return 0
        return self.matrix[v1][v2]

    def neighbors(self, v_idx):
        return [idx for idx, edge in enumerate(self.matrix[v_idx]) if edge]

    def bron_kerbosch_1(self, r, p, x):
        if not p and not x:
            self.cliques.append(r)
        while p:
            vert = p.pop()
            n = set(self.neighbors(vert))
            self.bron_kerbosch_1(r.union({vert}), p.intersection(n), x.intersection(n))
            x.add(vert)

    def __str__(self):
        rv = '['
        rv += '\n '.join([' '.join([str(col) for col in row]) for row in self.matrix])
        rv += ']'
        return rv


class AssocGraph(Graph):
    """
    Vertex = (idx, label, attrs, weight)
    """

    def __init__(self):
        self.g1order = 0
        self.g2order = 0
        super(AssocGraph, self).__init__()

    @staticmethod
    def jaccard_coef(intersection, union):
        return float(intersection)/(union-intersection)

    @staticmethod
    def combine_verts(verts1, verts2):
        vlabel = V_LABEL
        v = []
        for v1 in verts1:
            for v2 in verts2:
                weight = AssocGraph.calc_weight(v1, v2)
                v.append(((v1[vlabel], v2[vlabel]), None, weight))
        return v

    @staticmethod
    def find_edges(v, g1, g2):
        vlabel = V_LABEL - 1
        edges = []
        for vert1 in v:
            for vert2 in v:
                ag_v1 = vert1[vlabel]
                ag_v2 = vert2[vlabel]
                a1 = ag_v1[0]
                a2 = ag_v1[1]
                b1 = ag_v2[0]
                b2 = ag_v2[1]
                if a1 == b1 or a2 == b2:
                    continue
                if (g1.adjacent(a1, b1) and g2.adjacent(a2, b2)) \
                        or (not g1.adjacent(a1, b1) and not g2.adjacent(a2, b2)):
                    edges.append((ag_v1, ag_v2))
        return edges

    def associate(self, g1, g2):
        self.g1order = g1.order
        self.g2order = g2.order
        v = self.combine_verts(g1.v, g2.v)
        e = self.find_edges(v, g1, g2)
        self.init(v, e)

    def clique_weight(self, clique):
        vweight = V_WEIGHT
        return sum([self.v[v_idx][vweight] for v_idx in clique])

    def calc_clique_weights(self):
        self.clique_weights = [self.clique_weight(c) for c in self.cliques]

    def sim_score(self):
        vlabel = V_LABEL
        self.bron_kerbosch_1(r=set(), p=set([i for i in range(len(self.v))]), x=set())
        self.calc_clique_weights()
        mcl_weight = max(self.clique_weights)
        mcl_idx = self.clique_weights.index(mcl_weight)
        mcs = set([self.v[i][vlabel][0] for i in self.cliques[mcl_idx]])
        jscore = self.jaccard_coef(len(mcs), self.g1order + self.g2order)
        weightscore = mcl_weight / self.g1order
        return jscore, weightscore

    def __str__(self):
        rv = ""
        for idx, vert in enumerate(self.v):
            rv += "[%d](%s, %s) " % (idx, vert[V_LABEL][0], vert[V_LABEL][1])
        return rv


def main():
    import sys

    g1v = [('A1', ['a']),
           ('A2', ['b', 'a']),
           ('A3', ['a', 'c'])]
    g2v = [('B1', ['a']),
           ('B2', ['b', 'a']),
           ('B3', ['x', 'y'])]
    g3v = [('C1', ['a']),
           ('C2', ['b', 'a']),
           ('C3', ['a', 'c'])]

    g1e = [('A1', 'A2'), ('A2', 'A3')]
    g2e = [('B1', 'B2'), ('B2', 'B3')]
    g3e = [('C1', 'C2'), ('C2', 'C3')]

    g1 = Graph()
    g2 = Graph()
    g3 = Graph()

    g1.init(g1v, g1e)
    g2.init(g2v, g2e)
    g3.init(g3v, g3e)

    ag1 = AssocGraph()
    ag2 = AssocGraph()

    ag1.associate(g1, g2)
    ag2.associate(g1, g3)

    print g1
    print g2
    print g3
    print ag1
    print ag2

    sys.stdout.write("%f, %f\n" % ag1.sim_score())
    sys.stdout.write("%f, %f\n" % ag2.sim_score())


if __name__ == "__main__":
    main()
