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

requires_api_version = '2.5'
plugin_type = (TYPE_CORE,)

spanmirrors = 3
threadcount = 5
repo_list   = dict()

def init_hook(conduit):
    global threadcount, spanmirrors
    threadcount = conduit.confInt('main', 'threadcount', default=5)
    spanmirrors = conduit.confInt('main', 'spanmirrors', default=3)
    if hasattr(conduit, 'registerPackageName'):
        conduit.registerPackageName('yum-rocket')

def predownload_hook(conduit):
    global threadcount, spanmirrors, repo_list

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

    def wait_on_threads(threads):
        """ Wait for a list of threads to finish working. """
        while len(threads) > 0:
            for thread in threads:
                if not thread.is_alive():
                    threads.remove(thread)

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
                conduit.logger.warn('Stopping %s...' % self.name)

    download_po = []
    for po in conduit.getDownloadPackages():
        local = po.localPkg()
        if os.path.exists(local):
            conduit.verbose_logger.debug('using local copy of %s' % po)
        else:
            download_po.append(po)

        dirstat = os.statvfs(po.repo.pkgdir)
        if (dirstat.f_bavail * dirstat.f_bsize) <= long(po.size):
            raise PluginYumExit, ('Insufficient disk space in directory %s' %
                                  po.repo.pkgdir)

    # Let's thread this bitch!
    if len(download_po) < threadcount:
        threadcount = len(download_po)
    if (len(download_po) > 0):
        conduit.verbose_logger.info('Spawning %d download threads' %
                                    threadcount)

    q = Queue.Queue()
    for po in download_po:
        q.put(po)

    run_event = threading.Event()
    run_event.set()

    threads = []
    for i in range(0, threadcount):
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
