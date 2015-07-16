Summary: Advanced monitoring and aggregation infrastructure
Name: aggmon
Version: %{pkgversion}
Release: %{pkgrelease}
BuildArch: noarch
Group: Application/System
License: GPLv2
Source: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
Requires: python

%define debug_package %{nil}

%description
A general purpose monitoring and aggregation infrastructure.

%prep
%setup -q

%build
python2 -m compileall .

%install
rm -rf %{buildroot}
install -m 755 -d %{buildroot}%{_bindir}
install -m 755 -d %{buildroot}/%{python_sitelib}/metric_store
install -m 644 src/metric_store/mongodb_store.py %{buildroot}/%{python_sitelib}/metric_store/
install -m 644 src/metric_store/mongodb_store.pyc %{buildroot}/%{python_sitelib}/metric_store/
install -m 644 src/metric_store/__init__.py %{buildroot}/%{python_sitelib}/metric_store/
install -m 644 src/metric_store/__init__.pyc %{buildroot}/%{python_sitelib}/metric_store/

%clean
rm -rf %{buildroot}

%files

%package -n metric-store
Summary: MongoDB abstraction layer
Requires: python pymongo

%description -n metric-store
Helper classes to store metrics and job data in a MongoDB database.

%files -n metric-store
%defattr(-, root, root)
%{python_sitelib}/metric_store/*

%changelog
* Thu Jul 16 2015 NEC AJ
- initial version based on new aggmon
