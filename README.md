yum-rocket
----------

Fast, parallel downloads for [YUM](http://yum.baseurl.org). It uses good old
[urllib](http://docs.python.org/library/urllib.html) in place of URLGrabber to
enable threading capabilities.

This YUM plugin works by monkey-patching the yum.YumBase.downloadPackages()
method.

Goals (work in progress)
------------------------

- Minimize download time using threads instead of serial downloads
- Utilize more than one of the available mirrors for each repository to
  distribute HTTP server load and avoid connection/bandwidth limits enforced by
  individual mirror servers

This is highly experimental and not even proven viable at this point. At most,
the current functionality distributes HTTP load over many mirrors rather than
repeatedly hammering just one. It might save you a few seconds per transaction.
The downside to this library is the HTTP request library limitations.

- `urllib` and `urllib2` can do http and ftp, but no (solid) keepalive support
- `httplib` can do only http and has no keepalive
- `requests` can do http and keepalive, but can't do ftp (even with
  `requests-ftp` its dodgy at best) and is not part of stdlib.
- `urlgrabber` can do http and ftp and keepalive, but currently has a
  single-operation lock, so not suitable for threading.

This is a work in progress. Use at your own risk.
