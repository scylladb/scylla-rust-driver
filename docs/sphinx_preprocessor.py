import json
import sys

def remove_sphinx_markdown(md_file_text):
    text_split = md_file_text.split("```eval_rst")
    
    result = text_split[0]

    for i in range(1, len(text_split)):
        cur_chunk = text_split[i]
        result += cur_chunk[cur_chunk.find("```") + 3:]

    return result

def process_section(section):
    if 'Chapter' in section:
        section['Chapter']['content'] = remove_sphinx_markdown(section['Chapter']['content'])
        for s in section['Chapter']['sub_items']:
            process_section(s)

if __name__ == '__main__':
    if len(sys.argv) > 1: # we check if we received any argument
        if sys.argv[1] == "supports": 
            # then we are good to return an exit status code of 0, since the other argument will just be the renderer's name
            sys.exit(0)

    # load both the context and the book representations from stdin
    context, book = json.load(sys.stdin)

    for section in book['sections']:
        process_section(section)

    # we are done with the book's modification, we can just print it to stdout
    print(json.dumps(book))
