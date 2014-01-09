yum-rocket
----------

Fast, parallel downloads for [YUM](http://yum.baseurl.org). It uses good old
[urllib](http://docs.python.org/library/urllib.html) in place of URLGrabber to
enable threading capabilities.

Goals
-----

- Minimize download time using threads instead of serial downloads
- Utilize more than one of the available mirrors for each repository to
  distribute HTTP server load and avoid connection/bandwidth limits enforced by
  individual mirror servers

Mirror selection
----------------

As long as `yum-fastestmirror` is enabled, `yum-rocket` will favor the fastest
mirrors. It will use a configurable subset (default fastest 3) of the mirrors
for each repository. Of the subset, the mirror deemed fastest will be favored,
then second fastest, etc.

This is accomplished by tracking the number of threads downloading from each
mirror. If the fastest mirror has 1 download active, then the other mirrors in
the subset will be used in order of fastness for further downloads until either
a faster mirror completes a download and is available for another, or all
mirrors have 1 download active, in which case each mirror will start accepting a
second thread, again in order of fastness until there are 2 downloads active for
each mirror, or a job completes. This will continue until all downloads have
been completed.

This is highly experimental and not even proven viable at this point.
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

### Downloading puppet:

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
