from mst_search import *
from util import *

from flask import Flask, render_template, request

app = Flask(__name__,template_folder='./',static_url_path='/static')


@app.route('/wittgenstein')
def main():
    dt_obj = time.asctime(time.localtime(time.time()))
    return render_template('main.html',**locals())


@app.route('/wittgenstein/results',  methods=['POST'])
def results():
    if request.method == 'POST':

        query_sentence = request.form['query']
        results = webQuery(faissIndex,query_sentence)

        return render_template('results.html',**locals())

    else:
        pass


if __name__ == "__main__":
   app.run()

