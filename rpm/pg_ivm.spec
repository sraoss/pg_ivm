# How to build RPM:
#
#   rpmbuild -bb pg_ivm.spec --define "pgmajorversion 14" --define "pginstdir /usr/pgsql-14"

%global sname pg_ivm

%if 0%{?rhel} && 0%{?rhel} >= 7
%global llvm  1
%endif

Summary:	PostgreSQL-based distributed RDBMS
Name:		%{sname}_%{pgmajorversion}
Version:	1.5
Release:	1%{dist}
License:    BSD
Vendor:     IVM Development Group
URL:		https://github.com/sraoss/%{sname}
Source0:	https://github.com/sraoss/%{sname}/archive/v%{version}.tar.gz
BuildRequires:	postgresql%{pgmajorversion}-devel
Requires:	postgresql%{pgmajorversion}-server

%description
pg_ivm provides Incremnetal View Maintenance feature for
PostgreSQL. Incremental View Maintenance (IVM) is a way to make
materialized views up-to-date in which only incremental changes
are computed and applied on views rather than recomputing. 

%prep
%setup -q -n %{sname}-%{version}

%build
PATH=%{pginstdir}/bin:$PATH %{__make} %{?_smp_mflags}

%install
%{__rm} -rf %{buildroot}
PATH=%{pginstdir}/bin:$PATH %{__make} %{?_smp_mflags} INSTALL_PREFIX=%{buildroot} DESTDIR=%{buildroot} install

# Install documentation with a better name:
%{__mkdir} -p %{buildroot}%{pginstdir}/doc/extension
%{__cp} README.md %{buildroot}%{pginstdir}/doc/extension/README-%{sname}.md

%clean
%{__rm} -rf %{buildroot}

%files
%defattr(-,root,root,-)
%license LICENSE
%doc %{pginstdir}/doc/extension/README-%{sname}.md
%{pginstdir}/lib/%{sname}.so
%{pginstdir}/share/extension/%{sname}-*.sql
%{pginstdir}/share/extension/%{sname}.control
%if %llvm
    %{pginstdir}/lib/bitcode/%{sname}*.bc
    %{pginstdir}/lib/bitcode/%{sname}/*.bc
%endif

%changelog
* Mon Jun 30 2023 - Yugo Nagata <nagata@sraoss.co.jp> 1.5-1
- Update to 1.4
* Fri Dec 16 2022 - Yugo Nagata <nagata@sraoss.co.jp> 1.4-1
- Update to 1.4
* Fri Sep 30 2022 - Yugo Nagata <nagata@sraoss.co.jp> 1.3-1
- Update to 1.3
* Mon Jul 25 2022 - Yugo Nagata <nagata@sraoss.co.jp> 1.2-1
- Update to 1.2
* Thu Jun 23 2022 - Yugo Nagata <nagata@sraoss.co.jp> 1.1-1
- Update to 1.1
* Thu Jun 2 2022 - Yugo Nagata <nagata@sraoss.co.jp> 1.0-1
- Initial pg_ivm 1.0 RPM from IVM Development Group
