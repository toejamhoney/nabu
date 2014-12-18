def get_pdfminer():
    import pdfminer
    return pdfminer.parse_and_hash


def get_peepdf():
    pass


PARSER_FACTORY_FUNCS = {'pdfminer': get_pdfminer, 'peepdf': get_peepdf}


def get_parser(type_):
    factory = PARSER_FACTORY_FUNCS.get(type_)
    return factory()
