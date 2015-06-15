import cPickle
import logging
import sqlite3
import sys


class NabuDb(object):

    table = "unknown"
    cols = []

    def __init__(self, dbpath):
        self.dbpath = dbpath
        self.conn = None

    def init(self, table, cols):
        cmd = "create table if not exists %s(%s)" % (table, ','.join(cols))
        try:
            self.conn = sqlite3.connect(self.dbpath)
            self.conn.execute(cmd)
        except sqlite3.Error as e:
            logging.error("NabuDb.init error (%s): %s\n%s" % (self.dbpath, e, cmd))
            return False
        else:
            self.conn.text_factory = str
            return True

    def connected(self):
        return self.conn is not None

    def query(self, cmd, subs):
        try:
            c = self.conn.cursor()
            c.execute(cmd, subs)
        except sqlite3.Error as e:
            logging.error("NabuDb.query error: %s" % e)
            return []
        else:
            rows = c.fetchall()
            self.conn.commit()
            c.close()
            return rows

    def size(self):
        cmd = "select count(*) from %s" % self.table
        rows = self.query(cmd, ())
        if rows:
            return rows[0][0]
        else:
            return -1

    def get_unique(self, field):
        cmd = "select distinct %s from %s" % (field, self.table)
        rows = self.query(cmd, ())
        return rows

    def close(self):
        self.conn.close()


class JobDb(NabuDb):

    table = "jobs"
    cols = ["job_name text", "sample_path text"]

    def get_completed(self, job_name):
        cmd = "select sample_path from %s where job_name=?" % self.table
        rows = set([row[0] for row in self.query(cmd, (job_name,))])
        return rows

    def mark_complete(self, job_name, sample):
        cmd = "insert into %s values(?, ?)" % self.table
        rv = self.query(cmd, (job_name, sample))
        return rv


class XmlDb(NabuDb):

    table = "xml"
    cols = ["pdf_id primary key", "xml"]

    def save(self, pdf_id, xml_str):
        cmd = "insert or replace into %s values(?, ?)" % self.table
        return self.query(cmd, (pdf_id, xml_str))

    def load(self, pdf_id):
        cmd = "select xml from %s where pdf_id=?" % self.table
        rows = self.query(cmd, (pdf_id,))
        try:
            rv = rows[0][0]
        except Exception:
            rv = ''
        return rv


class GraphDb(NabuDb):

    table = "results"
    cols = ["pdf_id primary key", "v_md5", "e_md5", "vertices", "edges", "features"]

    @staticmethod
    def serialize(data):
        try:
            json_data = cPickle.dumps(data, protocol=2)
        except cPickle.PicklingError as e:
            logging.error("GraphDb serialize error: %s" % e)
            return ''
        else:
            return json_data

    @staticmethod
    def deserialize(jsondata):
        try:
            data = cPickle.loads(jsondata)
        except cPickle.UnpicklingError as e:
            logging.error("GraphDb deserialize error: %s" % e)
            return ''
        else:
            return data

    def save(self, pdf, v_md5, e_md5, v_set, e_set, ftrs):
        cmd = "insert or replace into %s values(?, ?, ?, ?, ?, ?)" % self.table
        v_json = self.serialize(v_set)
        e_json = self.serialize(e_set)
        f_json = self.serialize(ftrs)
        rv = self.query(cmd, (pdf, v_md5, e_md5, v_json, e_json, f_json))
        return rv

    def load_family_features(self, edge_md5):
        cmd = "select pdf_id, features from %s where e_md5=? limit 1" % self.table
        rows = self.query(cmd, (edge_md5,))
        if rows:
            pdf_id, f_json = rows[0]
            f_list = self.deserialize(f_json)
        else:
            pdf_id, f_list = '', ''
        return pdf_id, f_list

    def load_pdf_graph(self, pdf):
        cmd = "select pdf_id, v_md5, e_md5, vertices, edges, features from %s where pdf_id=?" % self.table
        rows = self.query(cmd, (pdf,))
        if rows:
            graph_md5, v_md5, e_md5, v_json, e_json, f_json = rows[0]
            v_set = self.deserialize(v_json)
            e_set = self.deserialize(e_json)
            f_list = self.deserialize(f_json)
            return graph_md5, v_md5, e_md5, v_set, e_set, f_list
        else:
            logging.debug("PDF not found: %s" % pdf)
            return ['' for i in range(6)]

    def chunk(self, limit, offset):
        cmd = "select pdf_id, vertices, edges from %s limit %d offset %d" % (self.table, limit, offset)
        rows = self.query(cmd, ())
        for idx, (pdf, v, e) in enumerate(rows):
            rows[idx] = [pdf, self.deserialize(v), self.deserialize(e)]
        return rows

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "Need database"
        sys.exit(1)

    gdb = GraphDb(sys.argv[1])
    if not gdb.init(gdb.table, gdb.cols):
        print "Database error"
        sys.exit(1)

    families = [row[0] for row in gdb.get_unique('e_md5')]
    for f in families:
        print "%s,%s" % gdb.load_family_features(f)
