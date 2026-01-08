#! /bin/bash

sphinx-multiversion source _build/dirhtml \
    --pre-build 'python _utils/prepare_sphinx_source.py source'
