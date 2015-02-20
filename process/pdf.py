import logging
import networkx
from scipy.stats import stats
from xml.etree.ElementTree import tostring, ElementTree

NUMFEATURES = 7


class PDF(object):

    def __init__(self, path, name='unnamed'):
        self.name = name
        self.path = path
        self.parsed = False
        self.size = 0
        self.js = ''
        self.swf = ''
        self.graph = None
        self.xml = None
        self.blob = None
        self.errors = None
        self.bytes_read = 0
        self.v = []
        self.e = []
        self.ftr_vec = []

    def set_feature_vector(self):
        verts, edges = self.get_nodes_edges()
        logging.debug("%s: num verts %d\tnum edges %d" % (self.name, len(verts), len(edges)))
        ftr_matrix = self.get_graph_features(verts, edges)
        logging.debug("%s,feature matrix\n%s" % (self.name, '\n'.join(["%d,%s" % (len(f), str(f)) for f in ftr_matrix])))
        self.ftr_vec = self.aggregate_ftr_matrix(ftr_matrix)
        logging.debug("%s,features\n%d,%s" % (self.name, len(self.ftr_vec), self.ftr_vec))

    def get_root(self):
        rootid = None
        if self.xml is not None:
            obj = self.xml.find(".//Root")
            if obj is not None:
                try:
                    rootid = obj.find(".//ref").get("id")
                except AttributeError:
                    logging.warn("PDF.get_root: %s\tRoot missing reference object: %s" % (self.name, tostring(obj)))
            else:
                logging.warn("PDF.get_root: %s\tMissing root node" % self.name)
        return rootid

    def get_nodes_edges(self):
        if not self.v or not self.e:
            self.v.append(("PDF", ["start"]))
            rootid = self.get_root()
            if not rootid:
                rootid = 'missing_root'
                self.v.append((rootid, ["root"]))
            self.e.append(("PDF", rootid))
            visited = {()}
            new_v = []
            if self.xml is not None:
                for obj in self.xml.iterfind("object"):
                    src_id = obj.get("id")
                    while src_id in visited:
                        src_id += '_'
                    visited.add(src_id)
                    self.v.append((src_id, [item.tag for item in obj.iter()]))
                    for ref in obj.iter("ref"):
                        dst_id = ref.get("id")
                        if dst_id not in visited:
                            new_v.append(dst_id)
                        self.e.append((src_id, dst_id))
                for v in new_v:
                    if v not in visited:
                        self.v.append((v, ['missing_target']))
        return self.v, self.e

    def get_graph_features(self, v, e):
        """ Graph features based on NetSimile paper

        :param v: set of vertices (label, [attrib])
        :type v:  list
        :param e: edges in the graph (vertex, vertex)
        :type e: list
        :return: a vector of features
        :rtype: list
        """
        graph = networkx.Graph()
        for label, attrs in v:
            graph.add_node(label, contains=attrs)
        for edge in e:
            graph.add_edge(*edge)

        """
        Transforms matrix from paper, so that each row is a feature, and each col is a node
        """
        features = [[] for i in range(NUMFEATURES)]
        for node in graph.nodes_iter():
            for idx, ftr in enumerate(self.get_node_features(graph, node)):
                features[idx].append(ftr)

        return features

    def get_node_features(self, graph, node):
        """  Node features based on NetSimile paper
        :param node:
        :type node:
        :return:
        :rtype:
        """
        """
        degree of node
        cluserting coef of node
        avg number of node's two-hop away neighbors
        avg clustering coef of Neighbors(node)
        number of edges in node i's egonet
        number of outgoing edges from ego(node)
        number of neighbors(ego(node))
        """
        neighbors = graph.neighbors(node)

        degree = graph.degree(node)

        cl_coef = networkx.clustering(graph, node)

        nbrs_two_hops = 0.0
        nbrs_cl_coef = 0.0
        for neighbor in neighbors:
            nbrs_two_hops += graph.degree(neighbor)
            nbrs_cl_coef += networkx.clustering(graph, neighbor)

        try:
            avg_two_hops = nbrs_two_hops / degree
            avg_cl_coef = nbrs_cl_coef / degree
        except ZeroDivisionError:
            avg_two_hops = 0.0
            avg_cl_coef = 0.0

        egonet = networkx.ego_graph(graph, node)

        ego_size = egonet.size()

        ego_out = 0
        ego_nbrs = set()
        for ego_node in egonet:
            for nbr in graph.neighbors(ego_node):
                if nbr not in neighbors:
                    ego_out += 1
                    ego_nbrs.add(nbr)

        return [degree, cl_coef, avg_two_hops, avg_cl_coef, ego_size, ego_out, len(ego_nbrs)]

    def aggregate_ftr_matrix(self, ftr_matrix):
        sig = []
        for ftr in ftr_matrix:
            median = stats.nanmedian(ftr)
            mean = stats.nanmean(ftr)
            std = stats.nanstd(ftr)
            # Invalid double scalars warning appears here
            skew = stats.skew(ftr) if any(ftr) else 0.0
            kurtosis = stats.kurtosis(ftr)
            sig.extend([median, mean, std, skew, kurtosis])
        return sig

    def get_xml_str(self):
        try:
            rv = tostring(self.xml)
        except AttributeError as e:
            logging.error("PDF xml element object error: %s" % e)
            rv = ''
        except Exception as e:
            logging.error("PDF xml str uncaught exception: %s" % e)
            rv = ''
        return rv

    def save_xml(self, fp):
        try:
            ElementTree(element=self.xml).write(fp)
        except (AttributeError, IOError) as e:
            logging.error("PDF save xml unable to write out xml: %s" % e)
        except Exception as e:
            logging.error("PDF save xml UNCAUGHT EXCEPTION: %s" % e)
