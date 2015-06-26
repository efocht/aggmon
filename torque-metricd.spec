Summary: Service to retrieve Torque metrics
Name: torque-metricd
Version: 1.12
Release: 1%{?dist}
BuildArch: noarch
Group: System
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
install -m 755 -d %{buildroot}%{_sysconfdir}/init
install -m 755 -d %{buildroot}%{systemddir}
install -m 755 %{name} %{buildroot}/%{_sbindir}
install -m 640 %{name}.conf %{buildroot}/%{_sysconfdir}
install -m 644 %{name}.upstart %{buildroot}%{upstartdir}/%{name}.conf
install -m 644 %{name}-override.upstart %{buildroot}%{upstartdir}/%{name}.override
install -m 644 %{name}.service %{buildroot}%{systemddir}/%{name}.service

%clean
rm -rf %{buildroot}

%files
%defattr(-, root, root)
%{_sbindir}/*
%config(noreplace) %{_sysconfdir}/%{name}.conf
%{upstartdir}/%{name}*
%{systemddir}/*

%preun
/bin/rm -f %{upstartdir}/%{name}.override

%changelog
* Thu Jun 25 2015 NEC EHPCTC AJ -> 1.12
- changed tagger command to use environment variables
* Thu May 28 2015 NEC EHPCTC AJ -> 1.11
- do not send --reset to tagger on startup
* Tue Apr 28 2015 NEC EHPCTC AJ -> 1.10
- changed def. log dest., fixed problem if tagger cmd is not set, new: create_joblist
* Thu Apr 07 2015 NEC EHPCTC AJ -> 1.9
- Added list of running jobs in database
* Tue Apr 07 2015 NEC EHPCTC EF -> 1.8
- Bumped version
* Mon Mar 09 2015 NEC EHPCTC AJ -> 1.7
- workaround for upstart problem with *.override files
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
