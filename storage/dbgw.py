import logging
import sqlite3
import cPickle


class NabuDb(object):

    table = "unknown"
    cols = []

    def __init__(self, dbpath):
        self.dbpath = dbpath
        try:
            self.conn = sqlite3.connect(dbpath)
        except sqlite3.Error as e:
            logging.error(e)
            self.conn = None
        else:
            self.conn.text_factory = str

    def init(self):
        try:
            self.conn.execute("create table if not exists %s(%s)" % (self.table, ','.join(self.cols)))
        except sqlite3.Error as e:
            logging.error("NabuDb.init error: %s" % e)
            return False
        else:
            return True

    def connected(self):
        return self.conn

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
    cols = ["pdf_id primary key", "graph_md5", "vertices", "edges"]

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

    def save(self, pdf, graph_md5, v_set, e_set):
        cmd = "insert or replace into %s values(?, ?, ?, ?)" % self.table
        v_json = self.serialize(v_set)
        e_json = self.serialize(e_set)
        rv = self.query(cmd, (pdf, graph_md5, v_json, e_json))
        return rv

    def load_sample_graph(self, graph_md5):
        cmd = "select pdf_id, vertices, edges from %s where graph_md5=? limit 1" % self.table
        rows = self.query(cmd, (graph_md5,))
        if rows:
            pdf_id, v_json, e_json = rows[0]
            v_set = self.deserialize(v_json)
            e_set = self.deserialize(e_json)
        else:
            pdf_id, v_set, e_set = '', '', ''
        return pdf_id, v_set, e_set

    def load_pdf_graph(self, pdf):
        cmd = "select graph_md5, vertices, edges from %s where pdf_id=?" % self.table
        rows = self.query(cmd, (pdf,))
        if rows:
            graph_md5, v_json, e_json = rows[0]
            v_set = self.deserialize(v_json)
            e_set = self.deserialize(e_json)
        else:
            graph_md5, v_set, e_set = '', '', ''
        return graph_md5, v_set, e_set

    def chunk(self, limit, offset):
        cmd = "select pdf_id, vertices, edges from %s limit %d offset %d" % (self.table, limit, offset)
        rows = self.query(cmd, ())
        for idx, (pdf, v, e) in enumerate(rows):
            rows[idx] = [pdf, self.deserialize(v), self.deserialize(e)]
        return rows
