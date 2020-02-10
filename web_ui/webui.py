from time import time
from query import webQuery,faissIndex

from flask import Flask, render_template, request
app = Flask(__name__,template_folder='./',static_url_path='/static')


MAIN_ROUTE = '/wittgenstein'
RESULT_ROUTE = '/wittgenstein/results'
MAIN_HTML = 'main.html'
RESULT_HTML = 'results.html'


@app.route(MAIN_ROUTE)
def main():
    dt_obj = time.asctime(time.localtime(time.time()))
    return render_template(MAIN_HTML,**locals())


@app.route(RESULT_ROUTE,  methods=['POST'])
def results():
    if request.method == 'POST':

        query_sentence = request.form['query']
        results = webQuery(faissIndex,query_sentence)

        return render_template(RESULT_HTML,**locals())

    else:
        pass


if __name__ == "__main__":
   app.run()

