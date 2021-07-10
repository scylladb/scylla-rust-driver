#!/bin/python3

# A script which removes mdbook's # notation, which sphinx cannot handle.

import os
import shutil
import pathlib

def remove_hashes(md_file_text):
    text_split = md_file_text.split('\n')
    result = ''
    for line in text_split:
        err_idx = line.find('<span class="err">')
        if err_idx == -1:
            result += line + '\n'
        elif err_idx > 0:
            result += line[:err_idx] + '\n'

    return result[:-1] if len(result) > 0 else result

for mdfile_path in pathlib.Path("docs/_build/dirhtml").rglob("*.html"):
    mdfile = open(mdfile_path, "r").read()
    new_mdfile = remove_hashes(mdfile)
    open(mdfile_path, "w").write(new_mdfile)
