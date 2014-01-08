%define realname yum-rocket
%define _plugin_dir /usr/lib/yum-plugins
%define _conf_dir /etc/yum/pluginconf.d

name: yum-plugin-rocket
summary: Fast, threaded downloads for YUM
version: 0.1
release: 1%{?dist}
buildarch: noarch
license: MIT
source0: %{realname}.tar.gz
requires: python
requires: yum

%description
A threaded downloader plugin for YUM.

%prep
%setup -n %{realname}

%install
%{__mkdir_p} %{buildroot}/%{_plugin_dir} %{buildroot}/%{_conf_dir}
%{__cp} %{realname}/*.py %{buildroot}/%{_plugin_dir}/
%{__cp} %{realname}/*.conf %{buildroot}/%{_conf_dir}/

%clean
%{__rm} -rf %{buildroot}

%files
%defattr(0644,root,root,0755)
%{_plugin_dir}/*.py
%{_conf_dir}/*.conf

%changelog
* %(date "+%a %b %d %Y") %{name} - %{version}-%{release}
- Automatic build
