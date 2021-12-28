from flask import Flask
from flask import g
from flask.helpers import url_for
from markupsafe import escape
import sqlite3

app = Flask(__name__)

def get_db():
    if not hasattr(g, 'sqlite_db'):
        g.sqlite_db = sqlite3.connect('raw_data.sqlite')
    return g.sqlite_db

def get_cursor():
    return get_db().cursor()

@app.teardown_appcontext
def close_db(error):
    if hasattr(g, 'sqlite_db'):
        g.sqlite_db.close()

@app.route("/")
def hello():
    return '''
        <p><a href='{}'>Top State for Whatcom County</a></p>
        <p><a href='{}'>Top State for Whatcom County for 2020</a></p>
        <p><a href='{}'>Top State for Washington State for 2020</a></p>
    '''.format(
        url_for('top_state_for_county', county="Whatcom"), 
        url_for('top_state_per_year_for_county', county="Whatcom", year="2020"),
        url_for('top_state_per_year', year="2020"))

@app.route('/top_state_for/<county>')
def top_state_for_county(county):
    cur = get_cursor()
    county = escape(county)

    cur.execute('''
            SELECT COUNT(*) as count_from_state, origin_state, MIN(issue_date), MAX(issue_date)
            FROM card_transfer
            WHERE origin_country = "USA"
                AND county = ?
            GROUP BY origin_state
            ORDER BY origin_state;
    ''', (county,))

    output = '''
        <h1>Top Origin States for {} County</h1>
        <table>
            <tr>
                <th>Count</th>
                <th>Origin State</th>
            </tr>'''.format(county);

    for row in cur:
        count = row[0]
        state = row[1]
        output += "<tr><td>{}</td><td>{}</td></tr>".format(count, state);

    return output + "</table>"

@app.route('/top_state_for/<county>/<year>')
def top_state_per_year_for_county(county, year):
    cur = get_cursor()
    county = escape(county)
    year = escape(year)

    cur.execute('''
            SELECT COUNT(*) as count_from_state, origin_state, MIN(issue_date), MAX(issue_date)
            FROM card_transfer
            WHERE origin_country = "USA"
                AND county = ?
                AND year = ?
            GROUP BY origin_state
            ORDER BY origin_state;
    ''', (county, year))

    output = '''
        <h1>Top Origin States for {} County in {}</h1>
        <table>
            <tr>
                <th>Count</th>
                <th>Origin State</th>
            </tr>'''.format(county, year);

    for row in cur:
        count = row[0]
        state = row[1]
        output += "<tr><td>{}</td><td>{}</td></tr>".format(count, state);

    return output + "</table>"

@app.route('/top_state_per_year/<year>')
def top_state_per_year(year):
    cur = get_cursor()
    year = escape(year)

    cur.execute('''
            SELECT COUNT(*) as count_from_state, origin_state, MIN(issue_date), MAX(issue_date)
            FROM card_transfer
            WHERE origin_country = "USA"
                AND year = ?
            GROUP BY origin_state
            ORDER BY count_from_state DESC;
    ''', (year,))

    output = '''
        <h1>Top Origin States for Washington State in {}</h1>
        <table>
            <tr>
                <th>Count</th>
                <th>Origin State</th>
            </tr>'''.format(year);

    for row in cur:
        count = row[0]
        state = row[1]
        output += "<tr><td>{}</td><td>{}</td></tr>".format(count, state);

    return output + "</table>"