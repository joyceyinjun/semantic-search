#!/bin/bash
# detailed infor check
# https://pypi.org/project/Wikipedia-API/
# adjust max_level as is needed

CATEGORY_NAME=Category:Music
MAX_LEVEL=1
TITLES=titles_category_${CATEGORY_NAME}.txt
TEXTS=texts_category_${CATEGORY_NAME}.txt


python3 get_title_list.py $CATEGORY_NAME $MAX_LEVEL $TITLES
python3 get_text.py $TITLES $TEXTS
