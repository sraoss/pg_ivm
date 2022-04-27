# contrib/pg_ivm/Makefile

MODULE_big = pg_ivm
OBJS = \
	$(WIN32RES) \
	createas.o \
	matview.o \
	pg_ivm.o
PGFILEDESC = "pg_ivm - incremental view maintenance on PostgreSQL"

EXTENSION = pg_ivm
DATA = pg_ivm--1.0.sql

REGRESS = pg_ivm

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
