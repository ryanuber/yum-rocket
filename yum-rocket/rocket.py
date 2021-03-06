#!/usr/bin/python -tt

import os
import time
import threading
import Queue
import urllib
from urlparse import urlparse, urljoin
from yum.plugins import TYPE_CORE, PluginYumExit
from yum.repoMDObject import RepoMD

requires_api_version = '2.5'
plugin_type = (TYPE_CORE,)

spanmirrors = 3
maxthreads  = 5
repo_list   = dict()

def wait_on_threads(threads):
    """ Wait for a list of threads to finish working. """
    while len(threads) > 0:
        time.sleep(0.1)
        for thread in threads:
            if not thread.is_alive():
                threads.remove(thread)

def wait_on_queue(q):
    """ Wait for all tasks in a queue to complete.

    This allows us to wait on the queue while still capturing exceptions,
    including KeyboardInterrupt, and signaling early termination if needed.
    """
    while q.unfinished_tasks:
        time.sleep(0.1)

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
    cachedir = conduit._base.conf.cachedir

    if opts.maxthreads:
        maxthreads = opts.maxthreads
    if opts.spanmirrors:
        spanmirrors = opts.spanmirrors

    def getMD(dl_name, url, dest, thread_id):
        conduit.verbose_logger.info('[%s] start: %s' % (thread_id, dl_name))
        urllib.urlretrieve(url, dest)
        conduit.verbose_logger.info('[%s] done: %s' % (thread_id, dl_name))

    class MDDownloadThread(threading.Thread):
        def __init__(self, q, run_event):
            threading.Thread.__init__(self)
            self.q = q
            self.run_event = run_event

        def run(self):
            while self.run_event.is_set() and not self.q.empty():
                (repoid, url, dest, ft) = self.q.get()
                getMD(dl_name, url, dest, self.name)
                self.q.task_done()
            if not self.run_event.is_set():
                conduit.logger.warn('[%s] Stopping...' % self.name)

    md_downloads = []
    for reponame in conduit.getRepos().repos:
        repo = conduit.getRepos().getRepo(reponame)
        if not repo.isEnabled():
            continue

        fname = os.path.join(cachedir, repo.id, 'repomd.xml')

        if not os.path.exists(fname):
            # Just use urls[0] because we are only downloading one file
            md_url = urljoin(repo.urls[0], 'repodata/repomd.xml')
            urllib.urlretrieve(md_url, fname)

        mdo = RepoMD(repo.id, fname)

        for ft in ['primary_db']:
            location = mdo.repoData[ft].location[1]
            fname = os.path.basename(location)
            url = urljoin(repo.urls[0], location)
            dest = os.path.join(cachedir, repo.id, fname)
            if not os.path.exists(dest):
                md_downloads.append(('%s/%s' % (repo.id, ft), url, dest))

    # Threaded metadata download
    parallel = min(len(md_downloads), maxthreads)
    if parallel > 0:
        conduit.verbose_logger.info('Spawning %d metadata download threads' %
                                    parallel)

    q = Queue.Queue()
    for download in md_downloads:
        q.put(download)

    run_event = threading.Event()
    run_event.set()

    beg_download = time.time()

    threads = []
    for i in range(0, parallel):
        thread = MDDownloadThread(q, run_event)
        thread.start()
        threads.append(thread)

    try:
        wait_on_queue(q)
    except:
        conduit.logger.warn('\n\nCaught exit signal\nHang tight, '
                            'waiting for threads to exit...\n')
        run_event.clear()
        wait_on_threads(threads)
        raise PluginYumExit, 'Threads terminated'

    if (parallel > 0):
        time_delta = time.time() - beg_download
        total_time = '%02d:%02d' % (int(time_delta/60), int(time_delta%60))
        conduit.verbose_logger.info('Downloaded metadata in %s' % total_time)

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

    # Threaded package downloads
    parallel = min(len(download_po), maxthreads)
    if (parallel > 0):
        conduit.verbose_logger.info('Spawning %d package download threads' %
                                    parallel)

    q = Queue.Queue()
    for po in download_po:
        q.put(po)

    run_event = threading.Event()
    run_event.set()

    beg_download = time.time()

    threads = []
    for i in range(0, parallel):
        thread = PkgDownloadThread(q, run_event)
        thread.start()
        threads.append(thread)

    try:
        wait_on_queue(q)
    except:
        conduit.logger.warn('\n\nCaught exit signal\nHang tight, '
                            'waiting for threads to exit...\n')
        run_event.clear()
        wait_on_threads(threads)
        raise PluginYumExit, 'Threads terminated'

    if parallel > 0:
        time_delta = time.time() - beg_download
        total_time = '%02d:%02d' % (int(time_delta/60), int(time_delta%60))

        speed = total_size / time_delta
        conduit.verbose_logger.info('Downloaded %sB in %s (%sB/s)' %
                                    (format_number(total_size), total_time,
                                format_number(speed).strip()))
