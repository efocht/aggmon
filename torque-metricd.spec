Summary: Service to retrieve Torque metrics
Name: torque-metricd
Version: 1.6
Release: 1%{?dist}
BuildArch: noarch
Group: System Environment/Base
License: Proprietary
Source: torque-metricd-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
Requires: python metric-store python-psutil

%define upstartdir %{_sysconfdir}/init
%define systemddir %{_prefix}/lib/systemd/system

%description
This service retrieves Torque accounting data and stores them in MongoDB

%prep
%setup -q

%build

%install
install -m 755 -d %{buildroot}%{_sbindir}
install -m 755 -d %{buildroot}%{_sysconfdir}
install -m 755 -d %{buildroot}%{upstartdir}
install -m 755 -d %{buildroot}%{systemddir}
install -m 755 torque-metricd %{buildroot}/%{_sbindir}
install -m 640 torque-metricd.conf %{buildroot}/%{_sysconfdir}
install -m 644 torque-metricd.upstart %{buildroot}%{upstartdir}/torque-metricd.conf
install -m 644 torque-metricd-override.upstart %{buildroot}%{upstartdir}/torque-metricd.override
install -m 644 torque-metricd.service %{buildroot}%{systemddir}/torque-metricd.service

%clean
rm -rf %{buildroot}

%files
%defattr(-, root, root)
%{_sbindir}/*
%{_sysconfdir}/*
%{upstartdir}/*
%{systemddir}/*


%changelog
* Fri Mar 06 2015 NEC EHPCTC AJ -> 1.6
- Fixed some problems with remote tagger invocation.
* Tue Mar 04 2015 NEC EHPCTC AJ -> 1.5
- improvements to cope with thousands of acc files
* Fri Feb 27 2015 NEC EHPCTC AJ -> 1.4
- added CPUs use by job to job metric (config: include_cpus)
- added call of tagger (agg_cmd)
* Tue Dec 11 2014 NEC EHPCTC AJ -> 1.3
- changed types of metrics to be stored in MongoDB
* Wed Dec 10 2014 NEC EHPCTC AJ -> 1.2
- added MongoDB host to connect to, various minor improvments
* Tue Dec 02 2014 NEC EHPCTC AJ -> 1.1
- added missing dependency python-psutil
* Wed Jun 25 2014 NEC EHPCTC AJ -> 1.0
- version 1.0
