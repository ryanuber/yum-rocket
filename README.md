yum-rocket
----------

Fast, parallel downloads for [YUM](http://yum.baseurl.org). It uses good old
[urllib](http://docs.python.org/library/urllib.html) in place of URLGrabber to
enable threading capabilities.

This YUM plugin works by monkey-patching the yum.YumBase.downloadPackages()
method.

This is a work in progress. Use at your own risk.

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

Initial Results
---------------

These are some basic tests I ran using the `downloadonly` YUM plugin with
`yum-rocket`:

### Downloading Puppet:

```
2.3 MB/s | 3.7 MB     00:01  <-- With yum-rocket
534 kB/s | 3.7 MB     00:07  <-- Without yum-rocket
```

### Downloading gnome-terminal:

```
1.9 MB/s |  16 MB     00:08  <-- With yum-rocket
1.2 MB/s |  16 MB     00:13  <-- Without yum-rocket
```

### Downloading thunderbird:

```
2.2 MB/s |  47 MB     00:21  <-- With yum-rocket
972 kB/s |  47 MB     00:49  <-- Without yum-rocket
```

Installing
----------

Build an RPM for installation:

```
git clone https://github.com/ryanuber/yum-rocket
tar czf yum-rocket.tar.gz yum-rocket
rpmbuild -tb yum-rocket.tar.gz
rpm -ivh rpmbuild/RPMS/noarch/yum-plugin-rocket-*.rpm
```

Or just put the two files into the right places:

```
git clone https://github.com/ryanuber/yum-rocket
cp yum-rocket/yum-rocket/rocket.py /usr/lib/yum-plugins
cp yum-rocket/yum-rocket/rocket.conf /etc/yum/pluginconf.d
```
