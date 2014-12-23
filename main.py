import hashlib
import logging
import math
import os
import sys
import time
import traceback
from argparse import ArgumentParser
from multiprocessing import pool, Pool, cpu_count, Process, Lock

from storage import dbgw
from process.parsers import parse
from lib.spectragraph.spectragraph import Graph, AssocGraph


GDB_CHUNK = 0
GDB_OFFSETS = []
GRAPH_DB_PATH = None
GRAPH_SUBJECT = None
THRESH = 0.5
SIMSCORES = []
FILES = []


lock = Lock()


def plock(msg):
    with lock:
        sys.stdout.write("%s" % msg)
        sys.stdout.flush()


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


def shutdown(pool_, job_db):
    pool_.close()
    pool_.join()
    job_db.close()


def pscore(pnum):
    global GRAPH_SUBJECT, GDB_OFFSETS, SIMSCORES
    try:
        offset = GDB_OFFSETS[pnum]
        graph_db = dbgw.GraphDb(GRAPH_DB_PATH)
        if not graph_db.connected():
            logging.error("Unable to connect to graphdb")
            sys.exit(1)
        graph_db.init()

        g1 = Graph(GRAPH_SUBJECT.v, GRAPH_SUBJECT.e)
        plock("GRAPH SUBJECT: %s\n%s\n" % (GRAPH_SUBJECT.name, GRAPH_SUBJECT.v))
        plock("G1:\n%s\n\n" % (g1.v))

        local_chunk = 1000
        end = offset + GDB_CHUNK
        while offset < end:
            rows = graph_db.chunk(local_chunk, offset)
            offset += local_chunk
            for pdf, v, e in rows:
                plock("MATCH PDF: %s\n%s\n" % (pdf, v))
                g2 = Graph(v, e)
                plock("G2:\n%s\n\n" % (g2.v))
                ag = AssocGraph(g1, g2)
                ag.build()
                ag.bron_kerbosch_1(R=set(), P=set([idx for idx in range(len(ag.v))]), X=set())
                mcl = ag.max_clique()
                sim = float(len(mcl))/(g1.order + g2.order - len(mcl))
                SIMSCORES[pnum].append((GRAPH_SUBJECT.name, pdf, sim))
    except Exception as e:
        sys.stderr.write("%s\n" % e)
        print traceback.format_exc()
        pnum = 0
        sys.exit(0)
    else:
        save_score(pnum)
    return pnum


def save_score(pnum):
    global FILES, SIMSCORES
    FILES[pnum].write('\n'.join(SIMSCORES[pnum]))


def score_graph(argv):
    global GRAPH_DB_PATH, GDB_CHUNK, GDB_OFFSETS, GRAPH_SUBJECT, FILES, SIMSCORES
    todo = parse_file_set(argv.fin)
    GRAPH_DB_PATH = os.path.join(argv.dbdir, argv.graphdb)
    graph_db = dbgw.GraphDb(GRAPH_DB_PATH)
    job_db = dbgw.JobDb(os.path.join(argv.dbdir, argv.jobdb))
    if not job_db.connected():
        logging.error("Could not connect to job database. Jobs will not be saved, and cannot be resumed")
        while True:
            choice = intern(raw_input("Continue? y/n").lower())
            if choice is "y":
                break
            if choice is "n":
                sys.exit(1)

    job_db.init()
    graph_db.init()

    job_id = get_hash(os.path.abspath(argv.fin) + argv.action)

    if not argv.update:
        done = job_db.get_completed(job_id)
        todo.difference_update(done)
        del done

    p = Pool(argv.procs, maxtasksperchild=argv.chunk)


    pfunc = parse.get_parser(argv.parser)
    if not pfunc:
        logging.error("Main could not find parser: %s" % argv.parser)
        sys.exit(1)

    parsed_pdfs = []
    cnt = 0
    total_jobs = len(todo)
    try:
        for pdf in p.imap_unordered(pfunc, todo, argv.chunk * argv.procs):
            cnt += 1
            sys.stdout.write("%d / %d %s\n" % (cnt, total_jobs, pdf.name))
            if pdf.parsed:
                verts, edges = pdf.get_nodes_edges()
                graph_db.save(pdf.name, get_hash(str(edges)), verts, edges)
                parsed_pdfs.append(pdf)
    except KeyboardInterrupt:
        sys.stdout.write("Terminating pool...\n")
        p.terminate()
        sys.exit(0)

    p.close()
    p.join()

    gdb_size = graph_db.size()
    GDB_CHUNK = int(math.ceil(gdb_size / float(argv.procs)))
    GDB_OFFSETS = [x for x in range(1, gdb_size, GDB_CHUNK)]
    timestamp = time.strftime("%c").replace(" ", "_")
    SIMSCORES = [[] for x in range(argv.procs)]

    simdir = os.path.join(argv.logdir, "simscores")
    try:
        os.makedirs(simdir)
    except OSError as e:
        if e.errno != 17:
            logging.error(e)

    for pdf in parsed_pdfs:
        print '-'*80
        GRAPH_SUBJECT = pdf
        print GRAPH_SUBJECT.name

        try:
            FILES = [open(os.path.join(simdir, "simscore-%s-%d.txt" % (pdf.name, x)), 'w') for x in range(argv.procs)]
        except IOError as e:
            logging.error("Could not open output files: %s" % e)
            continue

        procs = [Process(target=pscore, args=(x,)) for x in range(min(argv.procs, len(parsed_pdfs)))]
        for proc in procs:
            proc.start()

        for pnum in range(len(procs)):
            procs[pnum].join()
            FILES[pnum].close()


def build_graphdb(argv):
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

    job_id = get_hash(os.path.abspath(argv.fin) + argv.action)

    if not argv.update:
        done = job_db.get_completed(job_id)
        todo.difference_update(done)
        del done

    cnt = 0
    total_jobs = len(todo)

    p = Pool(argv.procs, maxtasksperchild=argv.chunk)

    pfunc = parse.get_parser(argv.parser)
    if not pfunc:
        logging.error("Main could not find parser: %s" % argv.parser)
        sys.exit(1)

    try:
        for pdf in p.imap_unordered(pfunc, todo, argv.chunk * argv.procs):
            cnt += 1
            sys.stdout.write("%7d/%7d %s\n" % (cnt, total_jobs, pdf.name))
            if pdf.parsed:
                verts, edges = pdf.get_nodes_edges()
                graph_db.save(pdf.name, get_hash(str(edges)), verts, edges)
                job_db.mark_complete(job_id, pdf.path)
    except KeyboardInterrupt:
        sys.stdout.write("Terminating pool...\n")
        p.terminate()
    except pool.MaybeEncodingError as e:
        logging.error("Main imap error: %s" % e)

    shutdown(p, job_db)


if __name__ == "__main__":
    argparser = ArgumentParser()

    argparser.add_argument('action',
                           help="build | score")
    argparser.add_argument('fin',
                           help="line separated text file of samples to run")
    argparser.add_argument('-b', '--beginning',
                           action='store_true',
                           default=False,
                           help="Start from beginning. Don't resume job file based on completed")
    argparser.add_argument('-c', '--chunk',
                           type=int,
                           default=1,
                           help="Chunk size in jobs. Default is num_procs * 1")
    argparser.add_argument('-d', '--debug',
                           action='store_true',
                           default=False,
                           help="Spam the terminal with debug output")
    argparser.add_argument('--graphdb',
                           default='nabu-graphdb.sqlite',
                           help='Graph database filename. Default is nabu-graphdb.sqlite')
    argparser.add_argument('--jobdb',
                           default='nabu-jobs.sqlite',
                           help='Job database filename. Default is nabu-jobs.sqlite')
    argparser.add_argument('--xmldb',
                           default='nabu-xml.sqlite',
                           help='xml database filename. Default is nabu-xml.sqlite')
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
        logging.basicConfig(level=logging.DEBUG, filename=os.path.join(args.logdir, "nabu-%s.log" % time.strftime("%c").replace(" ", "_")))
        logging.debug("Debug mode")
        for key in [arg for arg in dir(args) if not arg.startswith('_')]:
            logging.debug("%s: %s" % (key, getattr(args, key)))
    else:
        logging.basicConfig(filename=os.path.join(args.logdir, "nabu-%s.log" % time.strftime("%c").replace(" ", "_")))

    if args.action == "build":
        build_graphdb(args)
    elif args.action == "score":
        score_graph(args)
