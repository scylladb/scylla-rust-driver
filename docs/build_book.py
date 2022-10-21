#!/usr/bin/env python3

# A script which removes sphinx markdown and builds the documentation book
# It copies all files to working directory, modifies the .md files
# to remove all ```eval_rst parts and builds the book
# The generated HTML will be in docs/book/scriptbuild/book

import os
import shutil
import pathlib

def remove_sphinx_markdown(md_file_text):
    text_split = md_file_text.split("```eval_rst")
    
    result = text_split[0]

    for i in range(1, len(text_split)):
        cur_chunk = text_split[i]
        result += cur_chunk[cur_chunk.find("```") + 3:]

    return result

# Working directory, normally book builds in docs/book
# so we can make our a folder in this build directory
work_dir = os.path.join("docs", "book", "scriptbuild")
os.makedirs(work_dir, exist_ok = True)

# All modified sources will be put in work_dir/source
src_dir = os.path.join(work_dir, "source")
os.makedirs(src_dir, exist_ok = True)

# Generated HTML will be put in work_dir/book
build_dir = os.path.join(work_dir, "book")



# Copy all mdbook files to work_dir before modifying

# Copy book.toml
shutil.copyfile(os.path.join("docs", "book.toml"), os.path.join(work_dir, "book.toml"))

# Go over all .md files, remove the ``` sphinx parts and put them in work_dir/source
for mdfile_path in pathlib.Path(os.path.join("docs", "source")).rglob("*.md"):

    # Path in the book structure ex. queries/queries.md instead of docs/source/queries/queries.md
    relative_path = mdfile_path.relative_to(os.path.join("docs", "source"))

    # Read the current file
    mdfile = open(mdfile_path, "r").read()

    # Remove sphinx markdown
    new_mdfile = remove_sphinx_markdown(mdfile)

    # Write the modified file to src_dir
    new_mdfile_path = os.path.join(src_dir, relative_path)
    os.makedirs(os.path.dirname(new_mdfile_path), exist_ok = True)
    open(new_mdfile_path, "w").write(new_mdfile)

build_result = os.system(f"mdbook build {work_dir}")

if build_result == 0:
    print(f"OK Done - rendered HTML is in {build_dir}")
    exit(0)
else:
    print(f"ERROR: mdbook build returned: {build_result}")
    exit(1)
