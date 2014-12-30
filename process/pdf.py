import logging
from xml.etree.ElementTree import tostring, ElementTree


class PDF(object):

    def __init__(self, path, name='unnamed'):
        self.name = name
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
        self.v = None
        self.e = None

    def get_root(self):
        rootid = "0"
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
        rootid = self.get_root()
        vertices = [("PDF", ["start"])]
        edges = [("PDF", rootid)]
        if self.xml is not None:
            for obj in self.xml.iterfind("object"):
                src_id = obj.get("id")
                while src_id in vertices:
                    src_id += '_'
                vertices.append((src_id, [item.tag for item in obj.iter()]))
                for ref in obj.iter("ref"):
                    dst_id = ref.get("id")
                    edges.append((src_id, dst_id))
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

    def save_xml(self, fp):
        try:
            ElementTree(element=self.xml).write(fp)
        except (AttributeError, IOError) as e:
            logging.error("PDF save xml unable to write out xml: %s" % e)
        except Exception as e:
            logging.error("PDF save xml UNCAUGHT EXCEPTION: %s" % e)
