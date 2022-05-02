# How to build RPM:
#
#   rpmbuild -ba pgpool.spec --define="pgpool_version 3.4.0" --define="pg_version 93" --define="pghome /usr/pgsql-9.3" --define="dist .rhel6" --define="pgsql_ver 93"

%global sname pg_ivm

%if 0%{?rhel} && 0%{?rhel} == 7
%global llvm  1
%endif
%if 0%{?rhel} && 0%{?rhel} >= 8
%global llvm  1
%endif

Summary:	PostgreSQL-based distributed RDBMS
Name:		%{sname}_%{pgmajorversion}
Version:	1.0	
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


%if %llvm
%package llvmjit
Summary:	Just-in-time compilation support for pg_ivm
Requires:	%{name}%{?_isa} = %{version}-%{release}
%if 0%{?rhel} && 0%{?rhel} == 7
# Packages come from EPEL and SCL:
BuildRequires:	llvm5.0-devel >= 5.0 llvm-toolset-7-clang >= 4.0.1
%endif
%if 0%{?rhel} && 0%{?rhel} >= 8
# Packages come from Appstream:
BuildRequires:	llvm-devel >= 8.0.1 clang-devel >= 8.0.1
%endif

%description llvmjit
This packages provides JIT support for pg_ivm 
%endif

%prep
%setup -q -n %{sname}-%{version}

%build
make %{?_smp_mflags} PG_CONFIG=%{pginstdir}/bin/pg_config

%install
%make_install
# Install documentation with a better name:
%{__mkdir} -p %{buildroot}%{pginstdir}/doc/extension
%{__cp} README.md %{buildroot}%{pginstdir}/doc/extension/README-%{sname}.md

%clean
%{__rm} -rf %{buildroot}

%files
%defattr(-,root,root,-)
%if 0%{?rhel} && 0%{?rhel} <= 6
%doc LICENSE
%else
%license LICENSE
%endif
%doc %{pginstdir}/doc/extension/README-%{sname}.md
%{pginstdir}/lib/%{sname}.so
%{pginstdir}/share/extension/%{sname}-*.sql
%{pginstdir}/share/extension/%{sname}.control

%if %llvm
%files llvmjit
    %{pginstdir}/lib/bitcode/%{sname}*.bc
    %{pginstdir}/lib/bitcode/%{sname}/*.bc
    %{pginstdir}/lib/bitcode/%{sname}/*/*.bc
    %{pginstdir}/lib/bitcode/columnar/*.bc
%endif

%changelog
* Fri Mar 25 2016 - Yugo Nagata <nagata@sraoss.co.jp> 1.0-1
- Initial pg_ivm 1.0 RPM
from IVM Development Group
