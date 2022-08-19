import os
import shutil
import pathlib

def prepare_sphinx_md_file(original_md_file):
    return original_md_file

original_source_dir = "../source"
output_dir = "source"

shutil.rmtree(output_dir, ignore_errors = True)
shutil.copytree(original_source_dir, output_dir)

# Go over all .md files and modify them to work with Sphinx
for mdfile_path in pathlib.Path(output_dir).rglob("*.md"):
    mdfile = open(mdfile_path, "r").read()
    new_mdfile = remove_hidden_code_lines(mdfile)
    open(mdfile_path, "w").write(new_mdfile)

shutil.copy("conf.py", "source/conf.py")

print(f"OK Done - prepared sphinx source is in {output_dir}")
