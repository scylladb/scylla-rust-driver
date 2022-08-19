import os
import shutil
import pathlib


"""
In mdbook blocks of rust code can contain hidden lines starting with #
So for example when there is:

```rust
let a: i32 = 0;
# let hidden = "not visible in html"
```

Only the first line would be visible in the generated html.
Sphinx does handle this and just puts these lines in the html,
so to avoid this they are removed using this function.
"""
def remove_hidden_code_lines(md_file_text):
    text_split = md_file_text.split("```")

    result = []

    for i in range(0, len(text_split)):
        cur_chunk = text_split[i]

        if i % 2 == 0 or not cur_chunk.startswith("rust"):
            result.append(cur_chunk)
            continue

        new_chunk_lines = []
        chunk_lines = cur_chunk.split('\n')
        for line in cur_chunk.split('\n'):
            if not line.lstrip().startswith("#"):
                new_chunk_lines.append(line)
        new_chunk = "\n".join(new_chunk_lines)
        result.append(new_chunk)

    return "```".join(result)

def prepare_sphinx_md_file(original_md_file):
    return remove_hidden_code_lines(original_md_file)

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
