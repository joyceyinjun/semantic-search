def extractMainText(text):

    text = [x for x in text.split('\n') if x]
    k = 0
    while k < len(text):
        if 'See also' in text[k] or\
           'External links' in text[k] or\
           '== References ==' == text[k].strip() or\
           'Notes' == text[k].strip():
            break
        k += 1
    return ' '.join(text[:k])

