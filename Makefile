# $Id: Makefile 39788 2015-09-01 16:19:21Z friedel $
# $Rev:: 39788                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                              $:  # Author of last commit. 
# $LastChangedDate:: 2015-09-01 11:19:21 #$:  # Date of last commit.

SHELL=/bin/sh

build:
	@echo "ArchiveTools: Ready to install"

install:
ifndef INSTALL_ROOT
	@echo "ArchiveTools: Must define INSTALL_ROOT"
	false
endif
	@echo "ArchiveTools: Installing to ${INSTALL_ROOT}"
	-mkdir -p ${INSTALL_ROOT}
	-mkdir -p ${INSTALL_ROOT}/python
	-rsync -Caq python/archivetools ${INSTALL_ROOT}/python
	-rsync -Caq python/backuptools ${INSTALL_ROOT}/python
	-rsync -Caq bin ${INSTALL_ROOT}/
	@echo "Make sure ${INSTALL_ROOT}/python is in PYTHONPATH"

test:
	@echo "ArchiveTools: tests are currently not available"

clean:
	@echo "ArchiveTools: no cleanup defined"
