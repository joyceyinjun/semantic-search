import sys
import wikipediaapi

MAX_LEVEL = 3

def getCategoryMembers(categorymembers, level=0, max_level=MAX_LEVEL):
    members = set([])
    for c in categorymembers.values():
        members.add(c.title)
        if c.ns == wikipediaapi.Namespace.CATEGORY and level < max_level:
            members = members.union(getCategoryMembers(c.categorymembers, level=level+1,max$
    return members

wiki = wikipediaapi.Wikipedia('en')


if __name__ == '__main__':

    _, category, MAX_LEVEL, outfile = sys.argv
    MAX_LEVEL = int(MAX_LEVEL)
    print('category:',category,'to level',MAX_LEVEL)

    cat = wiki.page(category)
    cat_list = sorted([x for x in
               getCategoryMembers(cat.categorymembers,max_level=MAX_LEVEL)
               if 'Category:' not in x and 'File:' not in x
                   and 'Template:' not in x and 'List of' not in x])

    with open(outfile,'w') as f:
        for item in cat_list:
            f.write(item+'\n')

