from process.parsers import parse
from process.pdf import PDF


PTYPE = None


def parse_and_hash(pdfpath):
    parser = parse.get_parser(PTYPE)
    pdf = PDF(pdfpath)
    parser.parse(pdf)
    return pdf
