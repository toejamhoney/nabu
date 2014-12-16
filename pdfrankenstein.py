import io
import os
import re
import sys
import glob
import json
import time
import argparse
import hashlib
import tempfile
import traceback
import subprocess
import multiprocessing
from Queue import Full, Empty

from scandir import scandir
from storage import StorageFactory
from sdhasher import make_sdhash

import huntterp
import xml_creator
from JSAnalysis import analyse as analyse
from util.str_utils import unescapeHTMLEntities as unescapeHTML


LOCK = multiprocessing.Lock()


class ArgParser(object):

    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument('pdf_in', help="PDF input for analysis")
        self.parser.add_argument('-d', '--debug', action='store_true', default=False, help="Print debugging messages, TODO")
        self.parser.add_argument('-o', '--out', default='file', help="Analysis output type. Default to file. Options: 'postgres'||'stdout'||'file'")
        self.parser.add_argument('-n', '--name', default='unnamed-out-'+time.strftime("%Y-%m-%d_%H-%M-%S"), help="Name for output database")
        self.parser.add_argument('--hasher', default='pdfminer', help='Specify which type of hasher to use. PeePDF | PDFMiner (default)') 
        self.parser.add_argument('-v', '--verbose', action='store_true', default=False, help="Spam the terminal, TODO")

    def parse(self):
        '''
        No need to pass anything; defaults to sys.argv (cli)
        '''
        try:
            parsed = self.parser.parse_args()
        except Exception:
            self.parser.exit(status=0, message='Usage: pdfrankenstein.py <input pdf> [-o] [-d] [-v]\n')
        else:
            return parsed


class HasherFactory(object):

    def get_hasher(self, hasher, **kwargs):
        typ = intern(hasher.lower())
        if typ is "peepdf":
            return PeePDFHasher(**kwargs)
        if typ is "pdfminer":
            return PDFMinerHasher(**kwargs)


class Hasher(multiprocessing.Process):
    '''
    Hashers generally make hashes of things
    '''
    def __init__(self, qin, qout, counter, debug):
        multiprocessing.Process.__init__(self)
        self.qin = qin
        self.qout = qout
        self.counter = counter
        self.debug = debug

    '''
    This loop is the main process of the hasher. It is automatically called
    when you call multiprocessing.Process.start()

    All variables should be local to the loop, and returned as strings
    suitable for inserting into the database.
    '''
    def run(self):
        while True:
            pdf = self.qin.get()
            if not pdf:
                '''
                This terminates the process by receiving a poison sentinel, None.
                '''
                self.qout.put(None)
                #self.qin.task_done()
                return 0

            '''
            Reset the values on each pdf.
            '''
            err = []
            urls = ''
            t_hash = ''
            t_str = ''
            graph = ''
            obf_js = ''
            de_js = ''
            obf_js_sdhash = ''
            de_js_sdhash = ''
            swf_sdhash = ''
            swf = ''
            fsize = ''
            pdfsize = ''
            bin_blob = ''
            malformed = {}

            '''
            Arguments are validated when Jobber adds them to the queue based
            on the Validators valid() return value. We can assume these will
            succeed. However, this process must reach the task_done() call,
            and so we try/catch everything
            '''
            try:
                pdf_name = pdf.rstrip(os.path.sep).rpartition(os.path.sep)[2]
            except Exception as e:
                err.append('UNEXPECTED OS ERROR:\n%s' % traceback.format_exc())
                pdf_name = pdf
            write('H\t#%d\t(%d / %d)\t%s\n' % (self.pid, self.counter.value(), self.counter.ceil(), pdf_name))
            '''
            The parse_pdf call will return a value that evaluates to false if it
            did not succeed. Error messages will appended to the err list.
            '''
            parsed_pdf = self.parse_pdf(pdf, err)

            if parsed_pdf:
                try:
                    fsize = self.get_file_size(pdf)
                    pdfsize = self.get_pdf_size(parsed_pdf, err)
                    graph = self.make_graph(parsed_pdf, err)
                    t_str = self.make_tree_string(parsed_pdf, err)
                    t_hash = self.make_tree_hash(graph, err)
                    obf_js = self.get_js(parsed_pdf, err)
                    de_js = self.get_deobf_js(obf_js, parsed_pdf, err)
                    obf_js_sdhash = make_sdhash(obf_js, err)
                    de_js_sdhash = make_sdhash(de_js, err)
                    urls = self.get_urls(obf_js, err)
                    urls += self.get_urls(de_js, err)
                    swf = self.get_swf(parsed_pdf, err)
                    swf_sdhash = make_sdhash(swf, err)
                    bin_blob = parsed_pdf.bin_blob
                    malformed = parsed_pdf.getmalformed()
                    self.get_errors(parsed_pdf, err)
                except Exception as e:
                    err.append('UNCAUGHT PARSING EXCEPTION:\n%s' % traceback.format_exc())

            err = 'Error: '.join(err)
            malformed['skipkeys'] = False
            try:
                json_malformed = json.dumps(malformed)
            except (TypeError, ValueError):
                malformed['skipkeys'] = True
                json_malformed = json.dumps(malformed, skipkeys=True)

            self.qout.put({'fsize':fsize,
                    'pdf_md5':pdf_name,
                    'tree_md5':t_hash,
                    'tree':t_str,
                    'obf_js':obf_js,
                    'de_js':de_js,
                    'swf':swf,
                    'graph':graph,
                    'pdfsize':pdfsize,
                    'urls':urls,
                    'bin_blob':bin_blob,
                    'obf_js_sdhash':obf_js_sdhash,
                    'de_js_sdhash':de_js_sdhash,
                    'swf_sdhash':swf_sdhash,
                    'malformed': json_malformed,
                    'errors':err })
            self.counter.inc()
            #self.qin.task_done()

    def parse_pdf(self, pdf, err=''):
        return None, 'Hasher: Unimplemented method, %s' % sys._getframe().f_code.co_name
    def get_file_size(self, pdf):
        try:
            size = os.path.getsize(pdf)
        except OSError:
            '''
            This should never actually happen if we were able to parse it
            '''
            size = 0
        return str(size)
    def get_pdf_size(self, pdf):
        return 'Hasher: Unimplemented method, %s' % sys._getframe().f_code.co_name
    def make_graph(self, pdf, err=''):
        return 'Hasher: Unimplemented method, %s' % sys._getframe().f_code.co_name
    def make_tree_string(self, pdf, err=''):
        return 'Hasher: Unimplemented method, %s' % sys._getframe().f_code.co_name
    def make_tree_hash(self, t_str, err=''):
        t_hash = ''
        m = hashlib.md5()
        try:
            m.update(t_str)
            t_hash = m.hexdigest()
        except TypeError:
            err.append('<HashException>%s</HashException>' % traceback.format_exc())
        return t_hash
    def get_js(self, pdf, err=''):
        return 'Hasher: Unimplemented method, %s' % sys._getframe().f_code.co_name
    def get_debof_js(self, js, pdf, err=''):
        return 'Hasher: Unimplemented method, %s' % sys._getframe().f_code.co_name
    def get_swf(self, pdf, err=''):
        return 'Hasher: Unimplemented method, %s' % sys._getframe().f_code.co_name
    def get_errors(self, pdf, err=''):
        return 'Hasher: Unimplemented method, %s' % sys._getframe().f_code.co_name
    def get_urls(self, haystack, err='', needle=''):
        urls = ''
        if not needle:
            for needle in huntterp.Test.tests:
                urls = huntterp.find_in_hex(needle, haystack)
                urls += huntterp.find_unicode(needle, haystack)
        else:
            urls = huntterp.find_in_hex(needle, haystack)
            urls += huntterp.find_unicode(haystack)
        return '\n'.join([u[1] for u in urls])


class PDFMinerHasher(Hasher):

    def parse_pdf(self, pdf, err):
        parsed = False
        try:
            parsed = xml_creator.FrankenParser(pdf, self.debug)
        except Exception:
            err.append('<ParseException><pdf="%s">"%s"</ParseException>' % (str(pdf), traceback.format_exc()))
            if self.debug:
                write('\nPDFMinerHasher.parse_pdf():\n\t%s\n' % err[-1])
        return parsed

    def make_tree_string(self, pdf, err):
        if pdf.xml:
            return pdf.xml
        else:
            return '<TreeException>EMPTY TREE</TreeException>'

    def get_js(self, pdf, err):
        js = ''
        try:
            js_list = [ self.comment_out(js) for js in pdf.javascript ]
            js = '\n\n'.join(js_list)
        except Exception as e:
            err.append('<GetJSException>%s</GetJSException>' % traceback.format_exc())
        return js

    def get_deobf_js(self, js, pdf, err):
        de_js = ''
        try:
            if pdf.tree.startswith('TREE_ERROR'):
                err.append('<DeobfuscateJSException>%s</DeobfuscateJSException>' % pdf.tree)
        except AttributeError:
            try:
                #de_js = analyse(js, pdf.tree)
                pass
            except Exception as e:
                err.append('<DeobfuscateJSException>%s</DeobfuscateJSException>' % traceback.format_exc())
        return de_js

    def get_swf(self, pdf, err):
        swf = ''
        if pdf.swf:
            if isinstance(pdf.swf, list):
                swf = ''.join(pdf.swf)
            elif isinstance(pdf.swf, str):
                swf = pdf.swf
        return swf

    def get_pdf_size(self, pdf, err):
        return str(pdf.bytes_read)

    def get_errors(self, pdf, err):
        err.extend(pdf.errors)

    def make_graph(self, pdf, err):
        graph = ''
        try:
            graph = pdf.make_graph(pdf.tree)
            graph = '\n'.join(graph)
        except Exception as e:
            err.append('<GetJSException>%s</GetJSException>' % traceback.format_exc())
        return graph

    def comment_out(self, js):
        return re.sub("^(<)", "//", unescapeHTML(js), flags=re.M)

class PeePDFHasher(Hasher):

    from peepdf.PDFCore import PDFParser

    def parse_pdf(self, pdf, err):
        retval = True
        try:
            _, pdffile = self.PDFParser().parse(pdf, forceMode=True, manualAnalysis=True)
        except Exception as e:
            retval = False
            pdffile = '\n'.join([traceback.format_exc(), repr(e)])
        return pdffile

    def get_swf(self, pdf, err):
        swf = ''
        for version in range(pdf.updates + 1):
            for idx, obj in pdf.body[version].objects.items():
                if obj.object.type == 'stream':
                    stream_ident = obj.object.decodedStream[:3]
                    if stream_ident in ['CWS', 'FWS']:
                        swf += obj.object.decodedStream.strip()
        return swf

    def get_js(self, pdf, err):
        js = ''
        for version in range(pdf.updates+1):
            for obj_id in pdf.body[version].getContainingJS():
                js += self.do_js_code(obj_id, pdf)
        return js

    def make_tree_string(self, pdf, err):
        try:
            t_str = self.do_tree(pdf)
        except Exception as e:
            t_str = 'ERROR: ' + repr(e)
        return t_str

    def do_js_code(self, obj_id, pdf):
        consoleOutput = ''
        obj_id = int(obj_id)
        pdfobject = pdf.getObject(obj_id, None)
        if pdfobject.containsJS():
            jsCode = pdfobject.getJSCode()
            for js in jsCode:
                consoleOutput += js
        return consoleOutput

    def do_tree(self, pdfFile):
        version = None
        treeOutput = ''
        tree = pdfFile.getTree()
        for i in range(len(tree)):
            nodesPrinted = []
            root = tree[i][0]
            objectsInfo = tree[i][1]
            if i != 0:
                treeOutput += os.linesep + ' Version '+str(i)+':' + os.linesep*2
            if root != None:
                nodesPrinted, nodeOutput = self.printTreeNode(root, objectsInfo, nodesPrinted)
                treeOutput += nodeOutput
            for object in objectsInfo:
                nodesPrinted, nodeOutput = self.printTreeNode(object, objectsInfo, nodesPrinted)
                treeOutput += nodeOutput
        return treeOutput

    def printTreeNode(self, node, nodesInfo, expandedNodes = [], depth = 0, recursive = True):
        '''
            Given a tree prints the whole tree and its dependencies

            @param node: Root of the tree
            @param nodesInfo: Information abour the nodes of the tree
            @param expandedNodes: Already expanded nodes
            @param depth: Actual depth of the tree
            @param recursive: Boolean to specify if it's a recursive call or not
            @return: A tuple (expandedNodes,output), where expandedNodes is a list with the distinct nodes and output is the string representation of the tree
        '''
        output = ''
        if nodesInfo.has_key(node):
            if node not in expandedNodes or (node in expandedNodes and depth > 0):
                output += '\t'*depth + nodesInfo[node][0] + ' (' +str(node) + ')' + os.linesep
            if node not in expandedNodes:
                expandedNodes.append(node)
                children = nodesInfo[node][1]
                if children != []:
                    for child in children:
                        if nodesInfo.has_key(child):
                            childType = nodesInfo[child][0]
                        else:
                            childType = 'Unknown'
                        if childType != 'Unknown' and recursive:
                            expChildrenNodes, childrenOutput = self.printTreeNode(child, nodesInfo, expandedNodes, depth+1)
                            output += childrenOutput
                            expandedNodes = expChildrenNodes
                        else:
                            output += '\t'*(depth+1) + childType + ' (' +str(child) + ')' + os.linesep
                else:
                    return expandedNodes, output
        return expandedNodes, output



class Stasher(multiprocessing.Process):
    
    def __init__(self, qin, store_type, store_name, counter, qmsg, nprocs):
        multiprocessing.Process.__init__(self)
        self.qin = qin
        self.counter = counter
        self.qmsg = qmsg
        self.nprocs = nprocs
        self.store_type = store_type
        self.store_name = store_name
        self.storage = None

    def setup(self):
        write("%s" % self.qmsg.get())
        self.storage = StorageFactory().new_storage(self.store_type, name=self.store_name)
        if not self.storage:
            print("Error in storage setup")
            return False
        return self.storage.open()

    def run(self):
        proceed = self.setup()
        write("Proceeding: %s\n" % (str(proceed)))
        nfinished = 0
        while proceed:
            try:
                t_data = self.qin.get()
            except Empty:
                write('S Empty job queue.\n')
                proceed = False
            else:
                if not t_data:
                    nfinished += 1
                    proceed = not nfinished == self.nprocs
                    write('S Received a finished message (%d of %d)\n' % (nfinished, self.nprocs))
                else:
                    write('S\t#%d\t%s\n' % (self.pid, t_data.get('pdf_md5')))
                    try:
                        self.storage.store(t_data)
                    except Exception as e:
                        write('S\t#%d ERROR storing\t%s\t%s\n' % (self.pid, t_data.get('pdf_md5'), str(e)))
                    self.counter.inc()
                #self.qin.task_done()

        self.cleanup()
        self.finish()

    def cleanup(self):
        try:
            self.storage.close()
        except AttributeError:
            pass
        #self.qmsg.task_done()

    def finish(self):
        write('Stasher: Storage closed. Exiting.\n')


class Counter(object):

    def __init__(self, soft_max=0, name='Untitled'):
        self.counter = multiprocessing.RawValue('i', 0)
        self.hard_max = multiprocessing.RawValue('i', 0)
        self.soft_max = soft_max
        self.lock = multiprocessing.Lock()
        self.name = name

    def inc(self):
        with self.lock:
            self.counter.value += 1

    def value(self):
        with self.lock:
            return self.counter.value
    
    def complete(self):
        with self.lock:
            if self.hard_max > 0:
                return self.counter.value == self.hard_max.value

    def ceil(self):
        return self.hard_max.value


class Jobber(multiprocessing.Process):

    def __init__(self, job_list, job_qu, counters, num_procs):
        multiprocessing.Process.__init__(self)
        self.jobs = job_list
        self.qu = job_qu
        self.qu.cancel_join_thread()
        self.counters = counters
        self.validator = validator
        self.num_procs = num_procs

    def run(self):
        write("Jobber started\n")
        job_cnt = 0
        x = 0
        for job in self.jobs:
            #while x < 320000:
            #    x += 1
            #    continue
            if os.path.isfile(job.path):
                self.qu.put(job.path)
                job_cnt += 1
                if job_cnt % 100 == 0:
                    sys.stdout.write("Jobs: %d\n" % job_cnt)
                    sys.stdout.flush()
        for n in range(self.num_procs):
            self.qu.put(None)
        for counter in self.counters:
            counter.soft_max = job_cnt
            counter.hard_max.value = job_cnt
        write("\n-------------------------------------------\nJob queues complete: %d processes. Counters set: %d.\n-----------------------------------------------------\n" % (self.num_procs, job_cnt))


def write(msg):
    with LOCK:
        sys.stdout.write(msg)
        sys.stdout.flush()


if __name__ == '__main__':
    pdfs = []

    args = ArgParser().parse()

    num_procs = multiprocessing.cpu_count()/2
    num_procs = num_procs if num_procs > 0 else 1

    print('Running on %d processes' % num_procs)

    if os.path.isdir(args.pdf_in):
        dir_name = os.path.join(args.pdf_in, '*')
        print('Parsing directory %s' % dir_name)
        pdfs = scandir(args.pdf_in)
    elif os.path.exists(args.pdf_in):
        print('Parsing file: %s' % args.pdf_in)
        try:
            fin = open(args.pdf_in, 'r')
        except IOError as e:
            print("%s" % e)
            sys.exit(0)
        else:
            pdfs = [ line.rstrip() for line in fin.readlines() ]
            fin.close()
        print('Found %d jobs in file' % len(pdfs))
    else:
        print('Unable to find PDF file/directory: %s' % args.pdf_in)
        sys.exit(1)

    '''
    Locks
    '''
    io_lock = multiprocessing.Lock()

    '''
    Queues
    '''
    jobs = multiprocessing.Queue()
    results = multiprocessing.Queue()
    msgs = multiprocessing.Queue()

    '''
    Counters
    '''
    job_counter = Counter('Hashed')
    result_counter = Counter('Stored')
    counters = [job_counter, result_counter]

    '''
    Jobber and Jobs Validator
    '''
    jobber = Jobber(pdfs, jobs, counters, num_procs)

    '''
    Workers
    '''
    hf = HasherFactory()
    print('Creating hashing processings')
    hashers = [ hf.get_hasher(hasher=args.hasher, qin=jobs, qout=results, counter=job_counter, debug=args.debug) for cnt in range(num_procs) ]

    print('Creating stash process')
    stasher = Stasher(qin=results, store_type=args.out, store_name=args.name, counter=result_counter, qmsg=msgs, nprocs=num_procs)

    '''
    Begin processing
    '''
    write("Starting jobber...\n")
    jobber.start()
    msgs.put("Starting stashing job process...\n")
    stasher.start()
    write("Starting hashing job processes...\n")
    for hasher in hashers:
        hasher.start()

    '''
    Wait on processing
    '''
    msgs.join()
    time.sleep(1)

    '''
    End processes
    '''
    results.put(None)

    write("Collecting hashing processes...\n")
    while hashers:
        for h in hashers:
            if h.is_alive():
                h.terminate()
                h.join(1)
            else:
                hashers.remove(h)
    write("PDFrankenstein Exiting\n")
