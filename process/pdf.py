import logging
from xml.etree.ElementTree import tostring


class PDF(object):

    def __init__(self, path):
        self.parsed = False
        self.path = path
        self.size = 0
        self.js = ''
        self.swf = ''
        self.graph = None
        self.xml = None
        self.blob = None
        self.errors = None
        self.bytes_read = 0

    def get_root(self):
        rootid = "0"
        if self.xml is not None:
            obj = self.xml.find(".//Root")
            if obj is not None:
                try:
                    rootid = obj.find(".//ref").get("id")
                except AttributeError:
                    logging.warn("PDF.get_root error: %s\nRoot missing reference object: %s" % (self.path, tostring(obj)))
            else:
                logging.warn("PDF.get_root error: %s\nMissing root node" % self.path)
        return rootid

    def get_nodes_edges(self):
        rootid = self.get_root()
        vertices = set()
        edges = {("PDF", rootid)}
        if self.xml is not None:
            for obj in self.xml.iterfind("object"):
                src_id = obj.get("id")
                vertices.add(src_id)
                for ref in obj.iter("ref"):
                    dst_id = ref.get("id")
                    edges.add((src_id, dst_id))
        return vertices, edges

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
