SUBDIRS = module-quantiles

all: $(SUBDIRS) quantiles.so

quantiles.so: module-quantiles/quantiles.so
	cp -p module-quantiles/quantiles.so .

$(SUBDIRS):
	$(MAKE) -C $@ 

.PHONY: $(SUBDIRS)

