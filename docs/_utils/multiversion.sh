#! /bin/bash

sphinx-multiversion source _build/dirhtml \
    --pre-build 'python3 _utils/prepare_sphinx_source.py source'
