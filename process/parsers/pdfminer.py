import logging
import re
from xml.etree.ElementTree import TreeBuilder, tostring

from lib.parse.pdfminer import pdftypes
from lib.parse.pdfminer.pdfdocument import PDFDocument
from lib.parse.pdfminer.pdfparser import PDFParser
from lib.parse.pdfminer.psparser import PSKeyword, PSLiteral

from process.pdf import PDF

ESC_PAT = re.compile(r'[\000-\037&<>()"\042\047\134\177-\377]')
ENC = 'base64'


def parse_and_hash(pdfpath):
    parser = PDFMinerParser()
    pdf = PDF(pdfpath)
    parser.parse(pdf)
    return pdf


class PDFMinerParser(object):

    def __init__(self):
        self.treebuild = TreeBuilder()

    @staticmethod
    def esc(s):
        return ESC_PAT.sub(lambda m: '&#%d;' % ord(m.group(0)), s)

    def add_xml_node(self, tag, attrs=None, data=''):
        if not attrs:
            attrs = {}
        self.treebuild.start(tag, attrs)
        self.treebuild.data(data)
        self.treebuild.end(tag)

    def dump(self, obj):
        try:
            obj_attrs = {"size": str(len(obj))}
        except TypeError:
            obj_attrs = {}

        if obj is None:
            self.add_xml_node("null")

        elif isinstance(obj, dict):
            self.treebuild.start("dict", obj_attrs)
            for key, val in obj.iteritems():
                # Replace non word characters in key
                key = re.sub(r'\W+', '', key)
                if key.isdigit() or not key:
                    key = 'KEYERROR'
                self.treebuild.start(key, {})
                self.dump(val)
                self.treebuild.end(key)
            self.treebuild.end("dict")

        elif isinstance(obj, list):
            self.treebuild.start("list", obj_attrs)
            for listobj in obj:
                self.dump(listobj)
            self.treebuild.end("list")

        elif isinstance(obj, str):
            self.add_xml_node("string", obj_attrs.update({"enc": "ascii"}), obj.encode('ascii', 'backslashreplace'))
            self.add_xml_node("string", obj_attrs.update({"enc": ENC}), self.esc(obj).encode(ENC))

        elif isinstance(obj, pdftypes.PDFStream):
            self.treebuild.start("stream", obj_attrs)

            self.treebuild.start("props", {})
            self.dump(obj.attrs)
            self.treebuild.end("props")

            try:
                data = obj.get_data()
            except pdftypes.PDFNotImplementedError as e:
                self.add_xml_node("error", {"type": "PDFNotImplementedError"}, e.message)
            except pdftypes.PDFException as e:
                self.add_xml_node("error", {"type": "PDFException"}, e.message)
            except Exception as e:
                self.add_xml_node("error", {"type": "Uncaught"}, e.message)
            else:
                self.add_xml_node("data", attrs={"enc": ENC, "size": str(len(data))}, data=self.esc(data).encode(ENC))
                """
                Check js? swf?
                """

            self.treebuild.end("stream")

        elif isinstance(obj, pdftypes.PDFObjRef):
            self.add_xml_node("ref", {"id": str(obj.objid)})

        elif isinstance(obj, PSKeyword):
            self.add_xml_node("keyword", data=obj.name)

        elif isinstance(obj, PSLiteral):
            self.add_xml_node("literal", data=obj.name)

        elif isinstance(obj, (int, long, float)):
            self.add_xml_node("number", data=str(obj))

        else:
            raise TypeError(obj)

    def parse(self, pdf):
        try:
            fp = open(pdf.path, 'rb')
        except IOError as e:
            logging.error("Parse unable to open PDF: %s" % e)
            return

        parser = PDFParser(fp)
        doc = PDFDocument(parser)

        if doc.found_eof and doc.eof_distance > 3:
            pdf.blob = parser.read_from_end(doc.eof_distance)

        visited = set()

        self.treebuild.start("pdf", {"path": pdf.path})

        for xref in doc.xrefs:
            for objid in xref.get_objids():

                if objid in visited:
                    continue

                visited.add(objid)

                obj_attrs = {"id": str(objid), "type": "normal"}
                obj_data = ''
                obj_xml = self.treebuild.start("object", obj_attrs)

                try:
                    self.dump(doc.getobj(objid))
                except pdftypes.PDFObjectNotFound as e:
                    obj_xml.set("type", "malformed")
                    obj_data = parser.read_n_from(xref.get_pos(objid)[1], 4096)
                    obj_data = obj_data.replace('<', '0x3C')
                except TypeError:
                    obj_xml.set("type", "unknown")
                    obj_data = parser.read_n_from(xref.get_pos(objid)[1], 512)
                except Exception as e:
                    obj_xml.set("type", "exception")
                    obj_data = parser.read_n_from(xref.get_pos(objid)[1], 512)
                    self.add_xml_node("exception", {}, e.message)

                self.treebuild.data(obj_data)

                self.treebuild.end("object")

        """
        Loop xrefs again to maintain compatibily with edge list. Else, the edges
        would be different, thus throwing off the graph hashes already made.
        """
        for xref in doc.xrefs:
            self.treebuild.start("trailer", {})
            self.dump(xref.trailer)
            self.treebuild.end("trailer")

        self.treebuild.end("pdf")

        pdf.xml = self.treebuild.close()

        pdf.errors = doc.errors
        pdf.bytes_read = parser.BYTES
        pdf.parsed = True
        fp.close()
