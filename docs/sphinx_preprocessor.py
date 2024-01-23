import json
import sys

def remove_sphinx_markdown(md_file_text, name):
    output_lines = []
    skipped_sections = []

    in_eval_rst = False
    rst_section_content = []

    for line in md_file_text.splitlines():
        if in_eval_rst:
            rst_section_content.append(line)
            if line.startswith("```"):
                skipped_sections.append('\n'.join(rst_section_content))
                rst_section_content = []
                in_eval_rst = False
            continue

        if line.startswith("```{eval-rst}"):
            rst_section_content.append(line)
            in_eval_rst = True
            continue

        # mdbook doesn't support other types of admonitions
        if line == ":::{warning}": 
            line = '<div class="warning">'
        elif line == ":::":
            line = '</div>'

        if line.startswith(':::'):
            print(f"Unknown admonition marker in chapter {name}: {line}", file=sys.stderr)
            sys.exit(1)


        output_lines.append(line)
    
    if len(rst_section_content) > 0:
        print(f'Found unclosed rst section in chapter {name}', file=sys.stderr)
        sys.exit(1)
    
    if len(skipped_sections) > 0:
        print(f"Skipped sections in chapter \"{name}\":", file=sys.stderr)
        for section in skipped_sections:
            print(section, file=sys.stderr)

    return '\n'.join(output_lines)

def process_section(section):
    if 'Chapter' in section:
        print(f'Processing chapter {section['Chapter']['name']}', file=sys.stderr)
        section['Chapter']['content'] = remove_sphinx_markdown(section['Chapter']['content'], section['Chapter']['name'])
        for s in section['Chapter']['sub_items']:
            process_section(s)

if __name__ == '__main__':
    if len(sys.argv) > 1: # we check if we received any argument
        if sys.argv[1] == "supports": 
            # then we are good to return an exit status code of 0, since the other argument will just be the renderer's name
            sys.exit(0)
    print('SphinxToMdBook preprocessor running', file=sys.stderr)
    # load both the context and the book representations from stdin
    context, book = json.load(sys.stdin)

    for section in book['sections']:
        process_section(section)

    # we are done with the book's modification, we can just print it to stdout
    print(json.dumps(book))
