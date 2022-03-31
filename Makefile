# contrib/pg_ivm/Makefile

MODULES = pg_ivm
EXTENSION = pg_ivm
DATA = pg_ivm--1.0.sql


REGRESS = pg_ivm

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_ivm
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
