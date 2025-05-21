# contrib/pg_ivm/Makefile

PG_CONFIG ?= pg_config

MODULE_big = pg_ivm
OBJS = \
	$(WIN32RES) \
	createas.o \
	matview.o \
	pg_ivm.o \
	ruleutils.o \
	subselect.o \
	event_trigger.o
PGFILEDESC = "pg_ivm - incremental view maintenance on PostgreSQL"

EXTENSION = pg_ivm
DATA = pg_ivm--1.0.sql \
       pg_ivm--1.0--1.1.sql pg_ivm--1.1--1.2.sql pg_ivm--1.2--1.3.sql \
       pg_ivm--1.3--1.4.sql pg_ivm--1.4--1.5.sql pg_ivm--1.5--1.6.sql \
       pg_ivm--1.6--1.7.sql pg_ivm--1.7--1.8.sql pg_ivm--1.8--1.9.sql \
       pg_ivm--1.9--1.10.sql \
	   pg_ivm--1.10.sql pg_ivm--1.11.sql

REGRESS = pg_ivm create_immv refresh_immv

PGVER = $(shell $(PG_CONFIG) --version | sed "s/^[^ ]* \([0-9]*\).*$$/\1/" 2>/dev/null)

# We assume PG13 is the only version that is supported by pg_ivm but
# missing pg_isolation_regress.

ifneq ($(PGVER),13)
ISOLATION = create_insert  refresh_insert  insert_insert \
            create_insert2 refresh_insert2 insert_insert2 \
            create_insert3 refresh_insert3 insert_insert3
ISOLATION_OPTS = --load-extension=pg_ivm
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
