import hashlib
import logging
import os
import sys
import traceback
from argparse import ArgumentParser
from multiprocessing import Pool, cpu_count

from storage import dbgw
from process.parsers import parse


def print_usage():
    sys.stdout.write("Todo: Usage\n")


def get_hash(data):
    md5 = hashlib.md5()
    md5.update(data)
    return md5.hexdigest()


def parse_file_set(fpath):
    try:
        fin = open(fpath, "r")
    except IOError as e:
        logging.error("%s\nCould not read input, exiting." % e)
        sys.exit(1)
    else:
        lines = fin.readlines()
        fin.close()
        return set([line.rstrip('\n') for line in lines if not line.startswith('#')])


def shutdown(pool, job_db):
    pool.close()
    pool.join()
    job_db.close()


def main(argv):
    todo = parse_file_set(argv.fin)

    job_db = dbgw.JobDb(os.path.join(argv.dbdir, argv.jobdb))
    graph_db = dbgw.GraphDb(os.path.join(argv.dbdir, argv.graphdb))

    if not job_db.connected() or not graph_db.connected():
        logging.error("Could not connect to job database. Jobs will not be saved, and cannot be resumed")
        while True:
            choice = intern(raw_input("Continue? y/n").lower())
            if choice is "y":
                break
            if choice is "n":
                sys.exit(1)

    job_db.init()
    graph_db.init()

    job_id = get_hash(os.path.abspath(argv.fin))

    if not argv.update:
        done = job_db.get_completed(job_id)
        todo.difference_update(done)
        del done

    pool = Pool(argv.procs, maxtasksperchild=argv.chunk)

    pfunc = parse.get_parser(argv.parser)
    if not pfunc:
        logging.error("Main could not find parser: %s" % argv.parser)
        sys.exit(1)

    try:
        for pdf in pool.imap_unordered(pfunc, todo, argv.chunk * argv.procs):
            if pdf.parsed:
                job_db.mark_complete(job_id, pdf.path)
                verts, edges = pdf.get_nodes_edges()
                if argv.action == "build":
                    graph_db.save(os.path.basename(pdf.path), get_hash(str(edges)), verts, edges)
                elif argv.action == "score":
                    pass
    except KeyboardInterrupt:
        pool.terminate()
        pass

    shutdown(pool, job_db)
    sys.exit(0)


if __name__ == "__main__":
    argparser = ArgumentParser()

    argparser.add_argument('action',
                           default="build",
                           help="Action to perform. 'build' | 'score'")
    argparser.add_argument('fin',
                           help="line separated text file of samples to run")
    argparser.add_argument('-b', '--beginning',
                           action='store_true',
                           default=False,
                           help="Start from beginning. Don't resume job file based on completed")
    argparser.add_argument('-c', '--chunk',
                           type=int,
                           default=10,
                           help="Chunk size in jobs. Default is num_procs * 10")
    argparser.add_argument('-d', '--debug',
                           action='store_true',
                           default=True,
                           help="Spam the terminal with debug output")
    argparser.add_argument('--graphdb',
                           default='nabu-graphdb.sqlite',
                           help='Graph database filename. Default is nabu-graphdb.sqlite')
    argparser.add_argument('--jobdb',
                           default='nabu-jobs.sqlite',
                           help='Job database filename. Default is nabu-jobs.sqlite')
    argparser.add_argument('--dbdir',
                           default='db',
                           help="Database directory. Default is .../nabu/db/")
    argparser.add_argument('--logdir',
                           default='logs',
                           help="Logging directory. Default is .../nabu/logs/")
    argparser.add_argument('--parser',
                           default='pdfminer',
                           help="Type of pdf parser to use. Default is pdfminer")
    argparser.add_argument('-p', '--procs',
                           type=int,
                           default=2*cpu_count()/3,
                           help="Number of parallel processes. Default is 2/3 cpu core count")
    argparser.add_argument('-u', '--update',
                           default=False,
                           action='store_true',
                           help="Ignore completed jobs")

    args = argparser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        logging.debug("Debug mode")
        for key in [arg for arg in dir(args) if not arg.startswith('_')]:
            logging.debug("%s: %s" % (key, getattr(args, key)))

    main(args)
