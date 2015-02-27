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

import networkx as nx
import scipy.stats as stats
from scipy.spatial.distance import canberra
from scipy.cluster.hierarchy import *

import matplotlib.pyplot as plt


NUMFEATURES = 7
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


def pscore(pdf_name, thresh, ftrs, gdb_path, unique_graphs):
    pid = current_process().pid
    graph_db = dbgw.GraphDb(gdb_path)
    if not graph_db.init(graph_db.table, graph_db.cols):
        logging.error("pscore %d could not initialize db. exiting." % pid)
        sys.exit(-1)

    for edge_md5 in unique_graphs:
        pdf_id, ftrs_b = graph_db.load_family_features(edge_md5)
        logging.debug("\t%s: %s" % (pdf_id, ftrs_b))
        try:
            can_dist = canberra(ftrs, ftrs_b)
        except ValueError:
            logging.error("pscore canberra calc error. likely bad features for %s or %s" % (pdf_name, pdf_id))
        else:
            if not thresh or can_dist <= thresh:
                plock("%s,%s,%s,%f\n" % (pdf_name, edge_md5, pdf_id, can_dist))


def save_score(pnum):
    global FILES, SIMSCORES
    FILES[pnum].write('\n'.join(SIMSCORES[pnum]))


def calc_workload(num_jobs, num_procs):
    if num_jobs <= num_procs:
        return 1, num_jobs
    chunk_size = int(math.floor(float(num_jobs) / num_procs))
    return chunk_size, num_procs


def calc_similarities(pdf, graph_db, num_procs, thresh):
    unique_graphs = [row[0] for row in graph_db.get_unique('e_md5')]
    unique_num = len(unique_graphs)
    chunk_size, num_procs = calc_workload(unique_num, num_procs)
    offsets = [x for x in range(0, unique_num, chunk_size)]
    logging.debug("calc_sim: %d procs offsets[%d] unique_graphs[%d]" % (num_procs, len(offsets), unique_num))

    procs = [Process(target=pscore, args=(pdf.name, thresh, pdf.ftr_vec, graph_db.dbpath, unique_graphs[offsets[proc]:offsets[proc]+chunk_size])) for proc in range(num_procs)]

    logging.debug("nabu.calc_simil. starting  children")

    plock("subject,family,candidate,score\n")
    for proc in procs:
        proc.start()

    logging.debug("nabu.calc_simil. waiting on children")
    for proc in procs:
        proc.join()


def score_pdfs(argv, job_db, graph_db):
    todo = parse_file_set(argv.fin)

    parse_func = parse.get_parser(argv.parser)
    if not parse_func:
        logging.error("main.score_pdfs did not find valid parser: %s" % argv.parser)
        sys.exit(1)

    for pdf_id in todo:
        sys.stdout.write("Scoring: %s\n" % pdf_id)
        if not os.path.isfile(pdf_id):
            logging.warning("main.score_pdfs not a file: %s" % pdf_id)
            continue

        pdf = parse_func(pdf_id)
        logging.debug("%s: %s" % (pdf.name, pdf.ftr_vec))

        graph_db.save(pdf.name, get_hash(str(pdf.v)), get_hash(str(pdf.e)), pdf.v, pdf.e, pdf.ftr_vec)
        calc_similarities(pdf, graph_db, argv.procs, argv.thresh)


def get_node_features(graph, node):
    """  Node features based on NetSimile paper
    :param node:
    :type node:
    :return:
    :rtype:
    """
    """
    degree of node
    cluserting coef of node
    avg number of node's two-hop away neighbors
    avg clustering coef of Neighbors(node)
    number of edges in node i's egonet
    number of outgoing edges from ego(node)
    number of neighbors(ego(node))
    """
    neighbors = graph.neighbors(node)

    degree = graph.degree(node)

    cl_coef = nx.clustering(graph, node)

    nbrs_two_hops = 0.0
    nbrs_cl_coef = 0.0
    for neighbor in neighbors:
        nbrs_two_hops += graph.degree(neighbor)
        nbrs_cl_coef += nx.clustering(graph, neighbor)

    try:
        avg_two_hops = nbrs_two_hops / degree
        avg_cl_coef = nbrs_cl_coef / degree
    except ZeroDivisionError:
        avg_two_hops = 0.0
        avg_cl_coef = 0.0

    egonet = nx.ego_graph(graph, node)

    ego_size = egonet.size()

    ego_out = 0
    ego_nbrs = set()
    for ego_node in egonet:
        for nbr in graph.neighbors(ego_node):
            if nbr not in neighbors:
                ego_out += 1
                ego_nbrs.add(nbr)

    return [degree, cl_coef, avg_two_hops, avg_cl_coef, ego_size, ego_out, len(ego_nbrs)]


def get_graph_features(v, e):
    """ Graph features based on NetSimile paper

    :param v: set of vertices (label, [attrib])
    :type v:  list
    :param e: edges in the graph (vertex, vertex)
    :type e: list
    :return: a vector of features
    :rtype: list
    """
    graph = nx.Graph()
    for label, attrs in v:
        graph.add_node(label, contains=attrs)
    for edge in e:
        graph.add_edge(*edge)

    """
    Transforms matrix from paper, so that each row is a feature, and each col is a node
    """
    features = [[] for i in range(NUMFEATURES)]
    for node in graph.nodes_iter():
        for idx, ftr in enumerate(get_node_features(graph, node)):
            features[idx].append(ftr)

    return features


def aggregate_ftr_matrix(ftr_matrix):
    sig = []
    for ftr in ftr_matrix:
        median = stats.nanmedian(ftr)
        mean = stats.nanmean(ftr)
        std = stats.nanstd(ftr)
        # Invalid double scalars warning appears here
        skew = stats.skew(ftr) if any(ftr) else 0.0
        kurtosis = stats.kurtosis(ftr)
        sig.extend([median, mean, std, skew, kurtosis])
    return sig


def build_graphdb(argv, job_db, graph_db):

    if not argv.update:
        done = job_db.get_completed(argv.job_id)
        argv.todo.difference_update(done)
        del done

    cnt = 0
    total_jobs = len(argv.todo)

    p = Pool(argv.procs, maxtasksperchild=argv.chunk)

    pfunc = parse.get_parser(argv.parser)
    if not pfunc:
        logging.error("main.build_graphdb could not find parser: %s" % argv.parser)
        sys.exit(1)

    try:
        for pdf in p.imap_unordered(pfunc, argv.todo, argv.chunk * argv.procs):
            cnt += 1
            sys.stdout.write("%7d/%7d\r" % (cnt, total_jobs))
            #logging.debug("%s: %s" % (pdf.name, pdf.ftr_vec))
            if pdf.ftr_vec:
                graph_db.save(pdf.name, get_hash(str(pdf.v)), get_hash(str(pdf.e)), pdf.v, pdf.e, pdf.ftr_vec)
    except KeyboardInterrupt:
        logging.warning("\nTerminating pool...\n")
        p.terminate()
    except pool.MaybeEncodingError as e:
        logging.error("main.build_graphdb imap error: %s" % e)

    shutdown(p, job_db)


def draw_clusters(argv, graph_db):
    unique_graphs = [row[0] for row in graph_db.get_unique('e_md5')]
    unique_num = len(unique_graphs)

    logging.info('draw_clusters: Clustering %d graphs' % unique_num)
    '''
    tmpf = open('tmp.csv', 'w')

    ftrs = []
    for edge_md5 in unique_graphs:
        pdf_id, ftrs_b = graph_db.load_family_features(edge_md5)
        ftrs.append([str(f) for f in ftrs_b])

    tmpf.write('\n'.join([('%.5f' % f) for f in ftrs]))
    tmpf.close()


    plock("Loading...")
    x = loadtxt('tmp.csv')
    '''
    x = []
    for edge_md5 in unique_graphs:
        pdf_id, ftrs_b = graph_db.load_family_features(edge_md5)
        x.append(ftrs_b)

    plock("Calculating distances...")
    '''
    This line is for hcluster, not scipy cluster
    y = pdist(x, 'canberra')
    print y[:10]
    '''
    plock("Linkage...")
    z = linkage(x, 'single', metric='canberra')
    plock("Drawing dendrogram...")
    dendrogram(z, color_threshold=2)
    plock("Showtime\n")
    plt.show()


def main(args):
    if args.fin:
        args.job_id = get_hash(os.path.abspath(args.fin) + args.action)

        args.todo = parse_file_set(args.fin)

    job_db = dbgw.JobDb(os.path.join(args.dbdir, args.jobdb))
    graph_db = dbgw.GraphDb(os.path.join(args.dbdir, args.graphdb))

    if not job_db.init(job_db.table, job_db.cols) \
            or not graph_db.init(graph_db.table, graph_db.cols):
        logging.error("main.main could not initialize db. exiting.")
        sys.exit(1)

    start = time.clock()
    if args.action == "build":
        logging.info("main.main Building graph database")
        build_graphdb(args, job_db, graph_db)
        logging.info("Build finished in ~ %.3f" % (time.clock() - start))
    elif args.action == "score":
        logging.info("main.main Scoring graphs")
        score_pdfs(args, job_db, graph_db)
        logging.info("Scoring finished in ~ %.3f" % (time.clock() - start))
    elif args.action == "cluster":
        logging.info("main.main Clustering graphs")
        sys.stdout.write("Lets not do that write now\n")
        #draw_clusters(args, graph_db)
        logging.info("Clustering finished in ~ %.3f" % (time.clock() - start))


if __name__ == "__main__":
    argparser = ArgumentParser()

    argparser.add_argument('action',
                           help="build | score")
    argparser.add_argument('--fin',
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
    argparser.add_argument('-g', '--graphdb',
                           default='nabu-graphdb.sqlite',
                           help='Graph database filename. Default is nabu-graphdb.sqlite')
    argparser.add_argument('-j', '--jobdb',
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
                           default=cpu_count(),
                           help="Number of parallel processes. Default is 2/3 cpu core count")
    argparser.add_argument('-t', '--thresh',
                           type=int,
                           default=0,
                           help="Threshold which reports only graphs with similarities at or below this value.")
    argparser.add_argument('-u', '--update',
                           default=False,
                           action='store_true',
                           help="Ignore completed jobs")

    args = argparser.parse_args()

    del argparser

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        logging.debug("Debug mode")
        for key in [arg for arg in dir(args) if not arg.startswith('_')]:
            logging.debug("%s: %s" % (key, getattr(args, key)))
    else:
        logging.basicConfig(filename=os.path.join(args.logdir, "nabu-%s.log" % time.strftime("%c").replace(" ", "_")))

    main(args)
