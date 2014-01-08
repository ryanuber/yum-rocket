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
import random
import Queue
import urllib
from urlparse import urlparse

from yum.plugins import TYPE_CORE
from yum import YumBase
from yum.plugins import PluginYumExit
import yum.packages
import yum.i18n
_ = yum.i18n._

requires_api_version = '2.5'
plugin_type = (TYPE_CORE,)

spanmirrors = 3
threadcount = 5
repo_list   = dict()

class YumRocket(YumBase):
    """ Monkey-patch class for yum.YumBase

    This class simply replaces the `downloadPkgs` method of yum.YumBase. This
    is required because current versions of YUM use URLGrabber for downloads,
    which is good for simplicity and progress indication, but limiting for speed
    and parallel processing.

    This implementation of `downloadPkgs` will use urllib to fetch packages from
    mirrors, and Python's built-in threading capabilities to make it parallel.
    This means we are making a sacrifice of progress meters for speed. This may
    change in time by sending progress data back through the thread queue, but
    for now all this does is print a message when packages start downloading,
    and when they finish.

    Parallel downloads pose new questions though - Since we are downloading in
    multiple threads at the same time, potentially from the same mirror, why not
    take advantage of the fact that a single repo can have multiple URL's
    associated with it?

    This is especially useful in combination with the yum-fastestmirror plugin.
    YumRocket will use the fastest N mirrors for each repository, where N is the
    the configured number of mirrors to span. This means that even if we only
    have a single repository, we could potentially download the packages from a
    slew of different mirrors all in parallel without hitting bandwidth or
    connection limits enforced by any single mirror. This behaviour can be
    disabled by setting spanmirrors to 1.
    """
    def set_conduit(self, conduit):
        """ Pass in the conduit so we can still access the original YumBase. """
        self.conduit = conduit

    def downloadPkgs(self, pkglist, callback=None, callback_total=None):
        global logger, verboselogger, threadcount, spanmirrors, yb_orig
        global repo_list

        def getPackage(po, thread_id):
            """ Handle downloading a package from a repository.

            This function will (somewhat) intelligently choose from a number of
            mirrors to download from. It will choose only the fastest set of
            mirrors, and keep track of how many threads are already downloading
            from each of them to help distribute the load over multiple fast
            mirror servers.
            """
            repo_url = po.repo._urls[0]

            load = 1
            urlcount = 0
            for url in po.repo._urls:
                if not po.repo.id in repo_list.keys():
                    repo_list[po.repo.id] = dict()
                if not url in repo_list[po.repo.id].keys():
                    if spanmirrors < urlcount:
                        break
                    repo_list[po.repo.id][url] = 0
                    urlcount += 1

            for r in repo_list[po.repo.id].keys():
                if repo_list[po.repo.id][r] < load:
                    repo_list[po.repo.id][r] += 1
                    repo_url = r
                    break
                load += 1

            po.repo._urls = repo_list[po.repo.id].keys()
            po.repo._urls.remove(repo_url)
            po.repo._urls = [repo_url] + po.repo._urls

            repo_name = urlparse(po._remote_url()).netloc
            url = po._remote_url()
            verbose_logger.info(_('[%s] %s start: %s' % (thread_id, repo_name, po)))
            urllib.urlretrieve(url, po.localPkg())
            repo_list[po.repo.id][repo_url] -= 1
            verbose_logger.info(_('[%s] %s done: %s' % (thread_id, repo_name, po)))

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
                    logger.warn('Stopping %s...' % self.name)

        def wait_on_threads(threads):
            """ Wait for a list of threads to finish working. """
            while len(threads) > 0:
                for thread in threads:
                    if not thread.is_alive():
                        threads.remove(thread)

        def mediasort(apo, bpo):
            # FIXME: we should probably also use the mediaid; else we
            # could conceivably ping-pong between different disc1's
            a = apo.getDiscNum()
            b = bpo.getDiscNum()
            if a is None and b is None:
                return cmp(apo, bpo)
            if a is None:
                return -1
            if b is None:
                return 1
            if a < b:
                return -1
            elif a > b:
                return 1
            return 0

        """download list of package objects handed to you, output based on
           callback, raise yum.Errors.YumBaseError on problems"""

        errors = {}
        def adderror(po, msg):
            errors.setdefault(po, []).append(msg)

        #  We close the history DB here because some plugins (presto) use
        # threads. And sqlite really doesn't like threads. And while I don't
        # think it should matter, we've had some reports of history DB
        # corruption, and it was implied that it happened just after C-c
        # at download time and this is a safe thing to do.
        #  Note that manual testing shows that history is not connected by
        # this point, from the cli with no plugins. So this really does
        # nothing *sigh*.
        self.conduit._base.history.close()

        self.conduit._base.plugins.run('predownload', pkglist=pkglist)
        repo_cached = False
        remote_pkgs = []
        remote_size = 0
        for po in pkglist:
            if hasattr(po, 'pkgtype') and po.pkgtype == 'local':
                continue

            local = po.localPkg()
            if os.path.exists(local):
                if not self.verifyPkg(local, po, False):
                    if po.repo.cache:
                        repo_cached = True
                        adderror(po, _('package fails checksum but caching is '
                            'enabled for %s') % po.repo.id)
                else:
                    self.verbose_logger.debug(_("using local copy of %s") %(po,))
                    continue

            remote_pkgs.append(po)
            remote_size += po.size

            # caching is enabled and the package
            # just failed to check out there's no
            # way to save this, report the error and return
            if (self.conf.cache or repo_cached) and errors:
                return errors

        remote_pkgs.sort(mediasort)
        beg_download = time.time()
        i = 0
        local_size = 0
        done_repos = set()

        download_po = []
        for po in remote_pkgs:
            #  Recheck if the file is there, works around a couple of weird
            # edge cases.
            local = po.localPkg()
            i += 1
            if os.path.exists(local):
                if self.verifyPkg(local, po, False):
                    self.verbose_logger.debug(_("using local copy of %s") %(po,))
                    remote_size -= po.size
                    continue
                if os.path.getsize(local) >= po.size:
                    os.unlink(local)

            checkfunc = (self.verifyPkg, (po, 1), {})
            dirstat = os.statvfs(po.repo.pkgdir)
            if (dirstat.f_bavail * dirstat.f_bsize) <= long(po.size):
                adderror(po, _('Insufficient space in download directory %s\n'
                        "    * free   %s\n"
                        "    * needed %s") %
                         (po.repo.pkgdir,
                          format_number(dirstat.f_bavail * dirstat.f_bsize),
                          format_number(po.size)))
                continue
            download_po.append(po)

        # Let's thread this bitch!
        if len(download_po) < threadcount:
            threadcount = len(download_po)
        if (len(download_po) > 0):
            self.verbose_logger.info(_("Spawning %d download threads" %
                                       threadcount))

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
            self.logger.warn('\n\nCaught exit signal\nHang tight, '
                             'waiting for threads to exit...\n')
            run_event.clear()
            wait_on_threads(threads)
            raise PluginYumExit, 'Threads terminated'

        if callback_total is not None and not errors:
            callback_total(remote_pkgs, remote_size, beg_download)

        self.conduit._base.plugins.run('postdownload', pkglist=pkglist,
                                       errors=errors)

        return errors

def init_hook(conduit):
    """ Install the monkey-patched yum.YumBase class.

    Since this is literally replacing an entire function of Yum, there is
    definitely a chance that some other plugins might not work with this.
    """
    global threadcount, spanmirrors, logger, verbose_logger, yum_base_orig
    threadcount = conduit.confInt('main', 'threadcount', default=5)
    spanmirrors = conduit.confInt('main', 'spanmirrors', default=3)
    logger = conduit.logger
    verbose_logger = conduit.verbose_logger
    if hasattr(conduit, 'registerPackageName'):
        conduit.registerPackageName('yum-rocket')
    rocket = YumRocket()
    rocket.set_conduit(conduit)
    conduit._base.downloadPkgs = rocket.downloadPkgs
