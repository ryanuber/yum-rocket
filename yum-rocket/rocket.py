#!/usr/bin/python -tt
#
# yum-rocket
# Fast, threaded downloads for YUM
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import os
import time
import threading
import Queue
import urllib
from urlparse import urlparse, urljoin
from yum.plugins import TYPE_CORE, PluginYumExit
import tempfile

requires_api_version = '2.5'
plugin_type = (TYPE_CORE,)

spanmirrors = 3
maxthreads  = 5
repo_list   = dict()

def wait_on_threads(threads):
    """ Wait for a list of threads to finish working. """
    while len(threads) > 0:
        for thread in threads:
            if not thread.is_alive():
                threads.remove(thread)

def format_number(number):
    """ Turn numbers into human-readable metric-like numbers

    This function is based on yum-cli/output.py and needed here due to
    the typical post-download CLI callback not getting called.
    """
    symbols = [' ', 'k', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y']
    step = 1024.0
    thresh = 999
    depth = 0
    max_depth = len(symbols) - 1
    while number > thresh and depth < max_depth:
        depth  = depth + 1
        number = number / step
    if type(number) == type(1) or type(number) == type(1L):
        format = '%i %s'
    elif number < 9.95:
        format = '%.1f %s'
    else:
        format = '%.0f %s'
    return(format % (float(number or 0), symbols[depth]))

def init_hook(conduit):
    if hasattr(conduit, 'registerPackageName'):
        conduit.registerPackageName('yum-rocket')

def config_hook(conduit):
    global maxthreads, spanmirrors
    maxthreads = conduit.confInt('main', 'maxthreads', default=5)
    spanmirrors = conduit.confInt('main', 'spanmirrors', default=3)
    parser = conduit.getOptParser()
    if hasattr(parser, 'plugin_option_group'):
        parser = parser.plugin_option_group
    parser.add_option('', '--maxthreads', dest='maxthreads', type='int',
                      action='store', help='Maximum downloader threads')
    parser.add_option('', '--spanmirrors', dest='spanmirrors', type='int',
                      action='store', help='Mirrors to span per repository')

def postreposetup_hook(conduit):
    global maxthreads, spanmirrors
    opts, _ = conduit.getCmdLine()
    if opts.maxthreads:
        maxthreads = opts.maxthreads
    if opts.spanmirrors:
        spanmirrors = opts.spanmirrors

    def getMD(url, thread_id):
        repo_host = urlparse(url).netloc
        remote_path = urlparse(url).path
        conduit.verbose_logger.info('[%s] start: %s [%s]' %
                                    (thread_id, remote_path, repo_host))
        urllib.urlretrieve(url, '/dev/null')
        conduit.verbose_logger.info('[%s] done: %s [%s]' %
                                    (thread_id, remote_path, repo_host))

    class MDDownloadThread(threading.Thread):
        def __init__(self, q, run_event):
            threading.Thread.__init__(self)
            self.q = q
            self.run_event = run_event

        def run(self):
            while self.run_event.is_set() and not self.q.empty():
                url = self.q.get()
                getMD(url, self.name)
                self.q.task_done()
            if not self.run_event.is_set():
                conduit.logger.warn('[%s] Stopping...' % self.name)

    # Threaded metadata download
    q = Queue.Queue()
    cachedir = conduit._base.conf.cachedir

    for reponame in conduit.getRepos().repos:
        repo = conduit.getRepos().getRepo(reponame)
        if not repo.enabled:
            continue
        md_url = urljoin(repo.urls[0], 'repodata/repomd.xml')
        temp = tempfile.NamedTemporaryFile()
        urllib.urlretrieve(md_url, temp.name)
        from yum.repoMDObject import RepoData
        rd = RepoData()
        rd.parse(temp.read())
        #print repo.repoXML.repoData.get('repomd')
        #for ft in repo.repoXML.fileTypes():
        #    location = repo.repoXML.repoData[ft].location[1]
        #    url = urljoin(repo.urls[0], location)
        #    q.put(url)

    run_event = threading.Event()
    run_event.set()

    beg_download = time.time()

    threads = []
    for i in range(0, maxthreads):
        thread = MDDownloadThread(q, run_event)
        thread.start()
        threads.append(thread)

    try:
        wait_on_threads(threads)
    except:
        conduit.logger.warn('\n\nCaught exit signal\nHang tight, '
                            'waiting for threads to exit...\n')
        run_event.clear()
        wait_on_threads(threads)
        raise PluginYumExit, 'Threads terminated'

def predownload_hook(conduit):
    global maxthreads, spanmirrors, repo_list

    def prioritize_po_repos(po):
        """ Organize a repository list based on mirror speed.

        This function will (somewhat) intelligently choose from a number of
        mirrors to download from. It will choose only the fastest set of
        mirrors, and keep track of how many threads are already downloading
        from each of them to help distribute the load over multiple fast
        mirror servers.
        """
        repo_url = None
        load = 1
        urlcount = 0
        for url in po.repo._urls:
            if urlcount < spanmirrors:
                if not po.repo.id in repo_list.keys():
                    repo_list[po.repo.id] = dict()
                if not url in repo_list[po.repo.id].keys():
                    repo_list[po.repo.id][url] = 0
                    urlcount += 1

        while not repo_url:
            for r in repo_list[po.repo.id].keys():
                if repo_list[po.repo.id][r] < load:
                    repo_list[po.repo.id][r] += 1
                    repo_url = r
                    break
            load += 1

        po.repo._urls = repo_list[po.repo.id].keys()
        po.repo._urls.remove(repo_url)
        po.repo._urls = [repo_url] + po.repo._urls

        return repo_url

    def mirror_load(id, url, load):
        """ Update the load value of a given mirror """
        if id in repo_list.keys() and url in repo_list[id].keys():
            repo_list[id][url] += load

    def getPackage(po, thread_id):
        """ Handle downloading a package from a repository. """
        repo_url = prioritize_po_repos(po)
        repo_host = urlparse(repo_url).netloc
        conduit.verbose_logger.info('[%s] start: %s [%s]' %
                                    (thread_id, po, repo_host))
        mirror_load(po.repo.id, repo_url, 1)
        url = urljoin(repo_url, po.remote_path)
        urllib.urlretrieve(url, po.localPkg())
        mirror_load(po.repo.id, repo_url, -1)
        conduit.verbose_logger.info('[%s] done: %s' %
                                    (thread_id, po))

    class PkgDownloadThread(threading.Thread):
        def __init__(self, q, run_event):
            threading.Thread.__init__(self)
            self.q = q
            self.run_event = run_event

        def run(self):
            while self.run_event.is_set() and not self.q.empty():
                po = self.q.get()
                getPackage(po, self.name)
                self.q.task_done()
            if not self.run_event.is_set():
                conduit.logger.warn('[%s] Stopping...' % self.name)

    total_size = 0
    download_po = []
    for po in conduit.getDownloadPackages():
        local = po.localPkg()
        if os.path.exists(local):
            conduit.verbose_logger.debug('using local copy of %s' % po)
        else:
            total_size += po.size
            download_po.append(po)

        dirstat = os.statvfs(po.repo.pkgdir)
        if (dirstat.f_bavail * dirstat.f_bsize) <= long(po.size):
            raise PluginYumExit, ('Insufficient disk space in directory %s' %
                                  po.repo.pkgdir)

    # Let's thread this bitch!
    if len(download_po) < maxthreads:
        maxthreads = len(download_po)
    if (len(download_po) > 0):
        conduit.verbose_logger.info('Spawning %d download threads' %
                                    maxthreads)

    q = Queue.Queue()
    for po in download_po:
        q.put(po)

    run_event = threading.Event()
    run_event.set()

    beg_download = time.time()

    threads = []
    for i in range(0, maxthreads):
        thread = PkgDownloadThread(q, run_event)
        thread.start()
        threads.append(thread)

    try:
        wait_on_threads(threads)
    except:
        conduit.logger.warn('\n\nCaught exit signal\nHang tight, '
                            'waiting for threads to exit...\n')
        run_event.clear()
        wait_on_threads(threads)
        raise PluginYumExit, 'Threads terminated'

    time_delta = time.time() - beg_download
    total_time = '%02d:%02d' % (int(time_delta/60), int(time_delta%60))

    speed = total_size / time_delta
    conduit.verbose_logger.info('Downloaded %sB in %s (%sB/s)' %
                                (format_number(total_size), total_time,
                                format_number(speed).strip()))
