from flask import Flask, Response, redirect, url_for, request, session, abort, render_template, flash
from tagger import tagger

app = Flask(__name__)


# some protected url
@app.route('/login')
def home():
    return redirect("/", code=302)

# somewhere to login
@app.route("/", methods=["GET", "POST"])
def login():
    # print dir(request)
    if request.method == 'POST':
        if 'query' in request.form:
            query = request.form['query']
            Tagger = tagger()
            t = Tagger.tag(query)
            return render_template('index.html', title='Quiz Question Tagger',status=t,NL = query, Output='')
        else:
            print "Unknown POST request"
    else:
        return render_template('index.html', title='Query', status='',NL='', Output='')



if __name__ == "__main__":
    app.run()

