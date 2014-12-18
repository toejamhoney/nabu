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