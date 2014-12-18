import logging
import sqlite3


class NabuDb(object):

    def __init__(self, dbpath):
        try:
            self.conn = sqlite3.connect(dbpath)
        except sqlite3.Error as e:
            logging.error(e)
            self.conn = None

    def connected(self):
        return self.conn

    def query(self, cmd, subs):
        logging.info("%s, %s" % (cmd, subs))

        try:
            c = self.conn.cursor()
            c.execute(cmd, subs)
        except sqlite3.Error as e:
            logging.error(e)
            return []
        else:
            rows = c.fetchall()
            self.conn.commit()
            c.close()
            return rows

    def close(self):
        self.conn.close()


class JobDb(NabuDb):

    table = "jobs"
    cols = ["job_name text", "sample_path text"]

    def init(self):
        self.table = JobDb.table
        try:
            self.conn.execute("create table if not exists %s(%s)" % (self.table, ','.join(JobDb.cols)))
        except sqlite3.Error as e:
            logging.error(e)
            return False
        else:
            return True

    def get_completed(self, job_name):
        cmd = "select sample_path from %s where job_name=?" % self.table
        rows = set([row[0] for row in self.query(cmd, (job_name,))])
        return rows

    def mark_complete(self, job_name, sample):
        cmd = "insert into %s values(?, ?)" % self.table
        rv = self.query(cmd, (job_name, sample))
        return rv
