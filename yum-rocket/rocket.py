from yum.plugins import TYPE_CORE
from yum import YumBase
import yum.packages
import os
import time
import yum.i18n
_ = yum.i18n._
P_ = yum.i18n.P_
import urllib
from urlparse import urlparse
import Queue
import threading
import random

requires_api_version = '2.5'
plugin_type = (TYPE_CORE,)

spanmirrors = 1
threadcount = 5

class _yb(YumBase):

    def downloadPkgs(self, pkglist, callback=None, callback_total=None):

        global logger, verboselogger, threadcount, spanmirrors

        def getPackage(po, thread_id):
            if spanmirrors == 1:
                repo_url = po.repo._urls[random.randint(0,threadcount-1)]
                po.repo._urls.remove(repo_url)
                po.repo._urls = [repo_url] + po.repo._urls
            repo_name = urlparse(po._remote_url()).netloc
            url = po._remote_url()
            verbose_logger.info(_('[%s] %s start: %s' % (thread_id, repo_name, po)))
            urllib.urlretrieve(url, po.localPkg())
            verbose_logger.info(_('[%s] %s done: %s' % (thread_id, repo_name, po)))

        class PkgDownloadThread(threading.Thread):
            def __init__(self, q, run_event):
                threading.Thread.__init__(self)
                self.q = q
                self.run_event = run_event

            def run(self):
                while self.run_event.is_set():
                    po = self.q.get()
                    getPackage(po, self.name)
                    self.q.task_done()
                logger.warn('Stopping %s...' % self.name)

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
        self.history.close()

        self.plugins.run('predownload', pkglist=pkglist)
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

        # Let's thread this bitch
        if (len(download_po) > 0):
            if len(download_po) < threadcount:
                threadcount = len(download_po)
            self.verbose_logger.info(_("yum-rocket => spawn %d threads" % threadcount))

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

        def wait_on_threads():
            while len(threads) > 0:
                for thread in threads:
                    if not thread.is_alive():
                        threads.remove(thread)

        try:
            wait_on_threads()
        except:
            run_event.clear()
            wait_on_threads()
            raise yum.Errors.YumBaseError, 'Caught exit signal'

        if callback_total is not None and not errors:
            callback_total(remote_pkgs, remote_size, beg_download)

        self.plugins.run('postdownload', pkglist=pkglist, errors=errors)

        return errors

def init_hook(conduit):
    global threadcount, spanmirrors, logger, verbose_logger
    threadcount = conduit.confInt('main', 'threadcount', default=5)
    spanmirrors = conduit.confInt('main', 'spanmirrors', default=1)
    logger = conduit.logger
    verbose_logger = conduit.verbose_logger
    if hasattr(conduit, 'registerPackageName'):
        conduit.registerPackageName('yum-rocket')
    conduit._base.downloadPkgs = _yb().downloadPkgs
