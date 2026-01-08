# Global variables
# You can set these variables from the command line.
UV            = uv
SPHINXOPTS    = -j auto
SPHINXBUILD   = $(UV) run --frozen sphinx-build
PAPER         =
BUILDDIR      = _build
SOURCEDIR     = _source

# Internal variables
PAPEROPT_a4     = -D latex_paper_size=a4
PAPEROPT_letter = -D latex_paper_size=letter
ALLSPHINXOPTS   = -d $(BUILDDIR)/doctrees $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) $(SOURCEDIR)
TESTSPHINXOPTS  = $(ALLSPHINXOPTS) -W --keep-going

.PHONY: all
all: dirhtml

.PHONY: setup
setup:
	$(UV) sync
	mkdir -p $(SOURCEDIR)
	cp -RL source/* $(SOURCEDIR)
	$(UV) run python _utils/prepare_sphinx_source.py $(SOURCEDIR)

.PHONY: update
update:
	$(UV) lock --upgrade

# Clean commands
.PHONY: pristine
pristine: clean
	git clean -dfX

.PHONY: clean
clean:
	rm -rf $(BUILDDIR)/*
	rm -rf $(SOURCEDIR)/*

# Generate output commands
.PHONY: dirhtml
dirhtml: setup
	$(SPHINXBUILD) -b dirhtml $(ALLSPHINXOPTS) $(BUILDDIR)/dirhtml
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/dirhtml."

.PHONY: singlehtml
singlehtml: setup
	$(SPHINXBUILD) -b singlehtml $(ALLSPHINXOPTS) $(BUILDDIR)/singlehtml
	@echo
	@echo "Build finished. The HTML page is in $(BUILDDIR)/singlehtml."

.PHONY: epub
epub: setup
	$(SPHINXBUILD) -b epub $(ALLSPHINXOPTS) $(BUILDDIR)/epub
	@echo
	@echo "Build finished. The epub file is in $(BUILDDIR)/epub."

.PHONY: epub3
epub3: setup
	$(SPHINXBUILD) -b epub3 $(ALLSPHINXOPTS) $(BUILDDIR)/epub3
	@echo
	@echo "Build finished. The epub3 file is in $(BUILDDIR)/epub3."

.PHONY: multiversion
multiversion: setup
	$(UV) run --frozen _utils/multiversion.sh
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/dirhtml."

.PHONY: redirects
redirects: setup
	$(UV) run --frozen redirects-cli fromfile --yaml-file _utils/redirects.yaml --output-dir $(BUILDDIR)/dirhtml
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/dirhtml."

# Preview commands
.PHONY: preview
preview: setup
	$(UV) run --frozen sphinx-autobuild -b dirhtml $(ALLSPHINXOPTS) $(BUILDDIR)/dirhtml --port 5500

.PHONY: multiversionpreview
multiversionpreview: multiversion
	$(UV) run --frozen python -m http.server 5500 --directory $(BUILDDIR)/dirhtml

# Test commands
.PHONY: test
test: setup
	$(SPHINXBUILD) -b dirhtml $(TESTSPHINXOPTS) $(BUILDDIR)/dirhtml
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/dirhtml."

.PHONY: linkcheck
linkcheck: setup
	$(SPHINXBUILD) -b linkcheck $(SOURCEDIR) $(BUILDDIR)/linkcheck
