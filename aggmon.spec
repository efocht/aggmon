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
install -m 755 -d %{buildroot}/%{_bindir}
install -m 755 -d %{buildroot}/%{_unitdir}
install -m 755 -d %{buildroot}/%{python_sitelib}
install -m 644 aggmon.service %{buildroot}/%{_unitdir}/
for P in bin/*; do
    install -m 755 "$P" %{buildroot}/%{_bindir}/
done
for D in aggmon res_mngr metric_store; do
    install -m 755 -d %{buildroot}/%{python_sitelib}/"$D"
    for P in src/"$D"/*.py*; do
        install -m 644 "$P" %{buildroot}/%{python_sitelib}/"$D"/
    done
done
install -m 644 src/aggmon/module-quantiles/quantiles.so %{buildroot}/%{python_sitelib}/aggmon/

%clean
rm -rf %{buildroot}

%files
%defattr(-, root, root)
%{python_sitelib}/aggmon/*
%{python_sitelib}/res_mngr/*
%{_bindir}/*
%{_unitdir}/*

%package -n metric-store
Summary: MetricStore abstraction layer
Requires: python pymongo
BuildArch: noarch

%description -n metric-store
Helper classes to store metrics and job data in a database (MongoDB/TokuMX or InfluxDB).

%files -n metric-store
%defattr(-, root, root)
%{python_sitelib}/metric_store/*


%changelog
* Fri Jun 17 2016 NEC AJ
- added influxdb store 
* Thu Dec 17 2015 NEC AJ 
- added aggmon package (yet containing qunatiles module only)
* Thu Jul 16 2015 NEC AJ
- initial version based on new aggmon
