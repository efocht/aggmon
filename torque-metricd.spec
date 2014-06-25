Summary: Service to retrieve Torque metrics
Name: torque-metricd
Version: 1.0
Release: 1%{?dist}
BuildArch: noarch
Group: System Environment/Base
License: Proprietary
Source: torque-metricd-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
Requires: python metric-store mongodb-server

%define upstartdir %{_sysconfdir}/init
%define systemddir %{_prefix}/lib/systemd/system

%description
This service retrieves Torque accounting data and stores them in MongoDB

%prep
%setup -q

%build

%install
install -m 755 -d %{buildroot}%{_sbindir}
install -m 755 -d %{buildroot}%{upstartdir}
install -m 755 -d %{buildroot}%{systemddir}
install -m 755 torque-metricd %{buildroot}/%{_sbindir}
install -m 644 torque-metricd.upstart %{buildroot}%{upstartdir}/torque-metricd.conf
install -m 644 torque-metricd-override.upstart %{buildroot}%{upstartdir}/torque-metricd.override
install -m 644 torque-metricd.service %{buildroot}%{systemddir}/torque-metricd.service

%clean
rm -rf %{buildroot}

%files
%defattr(-, root, root)
%{_sbindir}/*
%{upstartdir}/*
%{systemddir}/*


%changelog
* Wed Jun 25 2014 NEC EHPCTC AJ -> 1.0
- version 1.0
