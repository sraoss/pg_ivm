project('pg_ivm', ['c'])

pg_config = find_program('pg_config')

bindir = run_command(pg_config, '--bindir', check: true).stdout().strip()
includedir_server = run_command(pg_config, '--includedir-server', check: true).stdout().strip()
includedir = run_command(pg_config, '--includedir', check: true).stdout().strip()
pkglibdir = run_command(pg_config, '--pkglibdir', check: true).stdout().strip()
sharedir = run_command(pg_config, '--sharedir', check: true).stdout().strip()
libdir = run_command(pg_config, '--libdir', check: true).stdout().strip()

module_name = meson.project_name()

# ruleutils.c includes ruleutils_13.c or ruleutils_14.c based on PostgreSQL version
# Note: We don't need to explicitly add these files since they're included by ruleutils.c
pg_ivm_sources = files(
  'createas.c',
  'matview.c',
  'pg_ivm.c',
  'ruleutils.c',
  'subselect.c',
)

if meson.get_compiler('c').get_id() == 'msvc'
  incdir = [includedir_server / 'port/win32_msvc',
            includedir_server / 'port/win32',
            includedir_server,
            includedir]
  postgres_lib = meson.get_compiler('c').find_library(
                                           'postgres',
                                            dirs: libdir,
                                            static: true,
                                            required: true
                                         )
else
  incdir = [ includedir_server ]
  postgres_lib = []
endif

shared_module(module_name,
  pg_ivm_sources,
  include_directories: incdir,
  install: true,
  install_dir: pkglibdir,
  name_prefix: '',
  dependencies: postgres_lib,
)

install_data(
  'pg_ivm--1.0.sql',
  'pg_ivm--1.0--1.1.sql',
  'pg_ivm--1.1--1.2.sql',
  'pg_ivm--1.2--1.3.sql',
  'pg_ivm--1.3--1.4.sql',
  'pg_ivm--1.4--1.5.sql',
  'pg_ivm--1.5--1.6.sql',
  'pg_ivm--1.6--1.7.sql',
  'pg_ivm--1.7--1.8.sql', 
  'pg_ivm--1.8--1.9.sql',
  'pg_ivm--1.9--1.10.sql',
  'pg_ivm--1.10.sql',
  'pg_ivm.control',
  install_dir: sharedir / 'extension',
) 

pg_regress = find_program('pg_regress',
  dirs: [pkglibdir / 'pgxs/src/test/regress']
)

regress_tests = ['pg_ivm', 'create_immv', 'refresh_immv']

test('regress',
 pg_regress,
 args: ['--bindir', bindir,
        '--inputdir', meson.current_source_dir(),
       ] + regress_tests,
)

pg_isolation_regress = find_program('pg_isolation_regress',
  dirs: [pkglibdir / 'pgxs/src/test/isolation']
)

isolation_tests = [
 'create_insert', 'refresh_insert', 'insert_insert',
 'create_insert2', 'refresh_insert2', 'insert_insert2',
 'create_insert3', 'refresh_insert3', 'insert_insert3'
]

isolation_opts = [
  '--load-extension','pg_ivm',
]

test('isolation',
 pg_isolation_regress,
 args: ['--bindir', bindir,
        '--inputdir', meson.current_source_dir(),
        '--outputdir', 'output_iso',
       ] + isolation_opts + isolation_tests,
)
