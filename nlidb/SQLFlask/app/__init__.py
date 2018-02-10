from flask import Flask, Response, redirect, url_for, request, session, abort, render_template, flash
from ln2sql import ln2sql
import Execution

app = Flask(__name__)

database = "WikiSQL"
path_to_sqlite_db = "C:\\New Software\\nlidb\\data\\WikiSQL\\dev.db"


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
            ln = ln2sql("app/database/JoinTrial.sql", query, "app/lang/english.csv",
                   "app/thesaurus/th_english.dat", './output.json')
            sql_queries = ln.return_SQL_queries()
            # print sql_queries
            return render_template('index.html', title='Query',status=sql_queries,NL = query, Output='')
        elif 'SQL' in request.form:
            exe = Execution.Execution(path_to_sqlite_db)
            query = request.form['SQL']
            output_string = exe.execute(query, database)
            return render_template('index.html', title='Query',status='',NL = query, Output=output_string)
        else:
            print "Unknown POST request"
    else:
        return render_template('index.html', title='Query', status='',NL='', Output='')



if __name__ == "__main__":
    app.run()

