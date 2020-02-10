#!/bin/bash

CATEGORY_NAME=music
MAX_LEVEL=3
TITLES=titles_category_${CATEGORY_NAME}.txt
TEXTS=texts_category_${CATEGORY_NAME}.txt


python3 get_title_list.py $CATEGORY_NAME $MAX_LEVEL $TITLES
python3 get_text.py $TITLES $TEXTS 
