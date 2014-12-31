import logging
from xml.etree.ElementTree import tostring, ElementTree


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
