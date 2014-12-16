import tempfile
from subprocess import Popen, PIPE
from sys import stderr

def make_sdhash(data, log=None):
    if not data or len(data) < 512:
        return ''
    stdout = ''
    try:
        tmpfile = tempfile.NamedTemporaryFile(delete=True)
    except IOError as e:
        if log:
            if isinstance(log, list):
                log.append('sdhash: %s\n' % str(e))
            else:
                logmsg(log, 'sdhash: %s\n'%str(e))
        else:
            stderr.write('sdhash: %s\n'%str(e))
    else:
        tmpfile.write(data)
        tmpfile.flush()
        cmd = ['sdhash', tmpfile.name]
        proc = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        stdout, stderr = proc.communicate()
        proc.wait()
        tmpfile.close()
        if not stdout:
            stdout = ''
        if stderr:
            print stderr
    finally:
        return stdout
