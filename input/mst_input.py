import sys, json
import wikipediaapi
from util import *

wiki = wikipediaapi.Wikipedia('en')


if __name__ == '__main__':

    _, infile, outfile = sys.argv

    with open(infile,'r') as f:
        titles = f.readlines()


    with open(outfile,'w') as f:
        for title in titles:
            page = wiki.page(title.strip())
            if page.exists():
                record = {'title': page.title,
                          'content': extractMainText(page.text.strip()),
                          'url': page.fullurl
                         }
                json.dump(record,f)
                f.write('\n')


