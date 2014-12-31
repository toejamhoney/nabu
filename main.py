import hashlib
import logging
import math
import os
import sys
import time
from argparse import ArgumentParser
from multiprocessing import pool, Pool, cpu_count, Process, Lock, current_process

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


def pscore(pdf_name, pdf_graph, gdb_path, unique_graphs):
    pid = current_process().pid
    graph_db = dbgw.GraphDb(gdb_path)
    if not graph_db:
        logging.error("%d pscore: could not establish connection to graph database. exiting." % pid)
        return -1

    with lock:
        logging.debug("%d pscore: %s against %s" % (pid, pdf_name, unique_graphs))
        for graph_md5 in unique_graphs:
            pdf_id, v, e = graph_db.load_sample_graph(graph_md5)
            match_graph = Graph()
            match_graph.init(v, e)
            ag = AssocGraph()
            ag.associate(pdf_graph, match_graph)
            plock("%s,%s,%s,%s\n" % (pdf_name, graph_md5, pdf_id, str(ag.sim_score())))


def save_score(pnum):
    global FILES, SIMSCORES
    FILES[pnum].write('\n'.join(SIMSCORES[pnum]))


def calc_workload(num_jobs, num_procs):
    if num_jobs <= num_procs:
        return 1, num_jobs
    chunk_size = int(math.ceil(float(num_jobs) / num_procs))
    return chunk_size, num_procs


def calc_similarities(pdf, graph_db, num_procs):
    unique_graphs = [row[0] for row in graph_db.get_unique('graph_md5')]
    unique_num = len(unique_graphs)
    chunk_size, num_procs = calc_workload(unique_num, num_procs)
    offsets = [x for x in range(0, unique_num, chunk_size)]

    v, e = pdf.get_nodes_edges()
    pdf_graph = Graph()
    pdf_graph.init(v, e)

    procs = [Process(target=pscore, args=(pdf.name, pdf_graph, graph_db.dbpath, unique_graphs[offsets[proc]:offsets[proc]+chunk_size])) for proc in range(num_procs)]

    for proc in procs:
        proc.start()

    for proc in procs:
        proc.join()


def score_pdfs(argv):
    todo = parse_file_set(argv.fin)
    todo_num = len(todo)

    gdb_path = os.path.join(argv.dbdir, argv.graphdb)
    graph_db = dbgw.GraphDb(gdb_path)
    graph_db.init()

    parse_func = parse.get_parser(argv.parser)
    if not parse_func:
        logging.error("main.score_pdfs did not find valid parser: %s" % argv.parser)
        sys.exit(1)

    cnt = 0
    for pdf in todo:
        if not os.path.isfile(pdf):
            logging.warning("main.score_pdfs not a file: %s" % pdf)
            continue
        cnt += 1
        sys.stdout.write("%d / %d %s\n" % (cnt, todo_num, pdf))

        pdf = parse_func(pdf)
        if pdf.parsed:
            verts, edges = pdf.get_nodes_edges()
            graph_db.save(pdf.name, get_hash(str(edges)), verts, edges)

        calc_similarities(pdf, graph_db, argv.procs)


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
        logging.error("main.build_graphdb could not find parser: %s" % argv.parser)
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
        logging.error("main.build_graphdb imap error: %s" % e)

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
        logging.basicConfig(level=logging.DEBUG)
        logging.debug("Debug mode")
        for key in [arg for arg in dir(args) if not arg.startswith('_')]:
            logging.debug("%s: %s" % (key, getattr(args, key)))
    else:
        logging.basicConfig(filename=os.path.join(args.logdir, "nabu-%s.log" % time.strftime("%c").replace(" ", "_")))

    if args.action == "build":
        build_graphdb(args)
    elif args.action == "score":
        score_pdfs(args)
