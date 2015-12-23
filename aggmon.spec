Summary: Advanced monitoring and aggregation infrastructure
Name: aggmon
Version: %{pkgversion}
Release: %{pkgrelease}
BuildArch: x86_64
Group: Application/System
License: GPLv2
Source: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
BuildRequires: python-devel gcc gcc-c++ boost-python boost-devel
Requires: python boost-python

%define debug_package %{nil}

%description
A general purpose monitoring and aggregation infrastructure.

%prep
%setup -q

%build
make -C src/aggmon/module-quantiles
python2 -m compileall .

%install
rm -rf %{buildroot}
install -m 755 -d %{buildroot}%{_bindir}
install -m 755 -d %{buildroot}/%{python_sitelib}/metric_store
install -m 755 -d %{buildroot}/%{python_sitelib}/aggmon
install -m 644 src/metric_store/mongodb_store.py %{buildroot}/%{python_sitelib}/metric_store/
install -m 644 src/metric_store/mongodb_store.pyc %{buildroot}/%{python_sitelib}/metric_store/
install -m 644 src/metric_store/__init__.py %{buildroot}/%{python_sitelib}/metric_store/
install -m 644 src/metric_store/__init__.pyc %{buildroot}/%{python_sitelib}/metric_store/
install -m 644 src/aggmon/module-quantiles/quantiles.so %{buildroot}/%{python_sitelib}/
for P in src/bin/agg_*; do
    install -m 755 "$P" %{buildroot}%{_bindir}
done
for P in src/aggmon/agg_*.py* \
  src/aggmon/basic_aggregators.py* src/aggmon/data_store.py* src/aggmon/msg_tagger.py* \
  src/aggmon/packet_sender.py* src/aggmon/repeat_timer.py*; do
    install -m 644 "$P" %{buildroot}/%{python_sitelib}/aggmon/
done

%clean
rm -rf %{buildroot}

%files
%defattr(-, root, root)
%{python_sitelib}/*.so
%{python_sitelib}/aggmon/*
%{_bindir}/*


%package -n metric-store
Summary: MongoDB abstraction layer
Requires: python pymongo
BuildArch: noarch

%description -n metric-store
Helper classes to store metrics and job data in a MongoDB database.

%files -n metric-store
%defattr(-, root, root)
%{python_sitelib}/metric_store/*


%description -n metric-store
Helper classes to store metrics and job data in a MongoDB database.

%changelog
* Thu Dec 17 2015 NEC AJ 
- added aggmon package (yet containing qunatiles module only)
* Thu Jul 16 2015 NEC AJ
- initial version based on new aggmon
