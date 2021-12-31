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
        <p><a href='{}'>Top State Moved From for Whatcom County</a></p>
        <p><a href='{}'>Top State Moved From for Whatcom County in 2020</a></p>
        <p><a href='{}'>Top State Moved From for Washington State</a></p>
        <p><a href='{}'>Top State Moved From for Washington State in 2020</a></p>
        <p><a href='{}'>Top County Moved to from California in 2020</a></p>
        <p><a href='{}'>Top County Moved to from California</a></p>
        <p><a href='{}'>Top Country Moved From for Washington State</a></p>
        <p><a href='{}'>Top Country Moved From for Washington State in 2020</a></p>
        <p><a href='{}'>Top Country Moved From For Whatcom County</a></p>
        <p><a href='{}'>Top Country Moved From For Whatcom County in 2020</a></p>
    '''.format(
        url_for('top_state_for_county', county="Whatcom"), 
        url_for('top_state_per_year_for_county', county="Whatcom", year="2020"),
        url_for('top_states'),
        url_for('top_state_per_year', year="2020"),
        url_for('top_county_per_state_year', state="California", year="2020"),
        url_for('top_county_per_state', state="California"),
        url_for('top_country'),
        url_for('top_country_per_year', year="2020"),
        url_for('top_country_per_county', county="Whatcom"),
        url_for('top_country_per_county_per_year', county="Whatcom", year="2020"))

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
            </tr>'''.format(county)

    for row in cur:
        count = row[0]
        state = row[1]
        output += "<tr><td>{}</td><td>{}</td></tr>".format(count, state)

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
            </tr>'''.format(county, year)

    for row in cur:
        count = row[0]
        state = row[1]
        output += "<tr><td>{}</td><td>{}</td></tr>".format(count, state)

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
            </tr>'''.format(year)

    for row in cur:
        count = row[0]
        state = row[1]
        output += "<tr><td>{}</td><td>{}</td></tr>".format(count, state)

    return output + "</table>"

@app.route('/top_states')
def top_states():
    cur = get_cursor()

    cur.execute('''
            SELECT COUNT(*) as count_from_state, origin_state, MIN(issue_date), MAX(issue_date)
            FROM card_transfer
            WHERE origin_country = "USA"
            GROUP BY origin_state
            ORDER BY count_from_state DESC;
    ''')

    output = '''
        <h1>Top Origin States for Washington State</h1>
        <table>
            <tr>
                <th>Count</th>
                <th>Origin State</th>
            </tr>'''

    for row in cur:
        count = row[0]
        state = row[1]
        output += "<tr><td>{}</td><td>{}</td></tr>".format(count, state)

    return output + "</table>"

@app.route('/top_county_per_state/<state>/<year>')
def top_county_per_state_year(state, year):
    cur = get_cursor()
    state = escape(state)
    year = escape(year)

    cur.execute('''
            SELECT county, COUNT(*) AS total_cards FROM card_transfer
            WHERE year = ?
                AND origin_country = "USA"
                AND origin_state = ?
                AND county <> "Unverified Address"
            GROUP BY county
            ORDER BY total_cards DESC
    ''', (year, state))

    output = '''
        <h1>Top Washington Counties Moved to from {} in {}</h1>
        <table>
            <tr>
                <th>County</th>
                <th>Total</th>
            </tr>'''.format(state, year)

    for row in cur:
        print(row)
        county = row[0]
        total = row[1]
        output += "<tr><td>{}</td><td>{}</td></tr>".format(county, total)

    return output + "</table>"

@app.route('/top_county_per_state/<state>')
def top_county_per_state(state):
    cur = get_cursor()
    state = escape(state)

    cur.execute('''
            SELECT county, COUNT(*) AS total_cards FROM card_transfer
            WHERE origin_country = "USA"
                AND origin_state = ?
                AND county <> "Unverified Address"
            GROUP BY county
            ORDER BY total_cards DESC
    ''', (state,))

    output = '''
        <h1>Top Washington Counties Moved to from {}</h1>
        <table>
            <tr>
                <th>County</th>
                <th>Total</th>
            </tr>'''.format(state)

    for row in cur:
        print(row)
        county = row[0]
        total = row[1]
        output += "<tr><td>{}</td><td>{}</td></tr>".format(county, total)

    return output + "</table>"

@app.route('/top_country')
def top_country():
    cur = get_cursor()

    cur.execute('''
            SELECT origin_country, COUNT(*) AS total_cards FROM card_transfer
            WHERE origin_country <> "USA"
            GROUP BY origin_country
            ORDER BY total_cards DESC;
    ''')

    output = '''
        <h1>Top Origin Countries</h1>
        <table>
            <tr>
                <th>Country</th>
                <th>Total</th>
            </tr>'''

    for row in cur:
        print(row)
        country = row[0]
        total = row[1]
        output += "<tr><td>{}</td><td>{}</td></tr>".format(country, total)

    return output + "</table>"

@app.route('/top_country/<county>')
def top_country_per_county(county):
    cur = get_cursor()
    county = escape(county)

    cur.execute('''
            SELECT origin_country, COUNT(*) AS total_cards FROM card_transfer
            WHERE origin_country <> "USA"
                AND county = ?
            GROUP BY origin_country
            ORDER BY total_cards DESC;
    ''', (county,))

    output = '''
        <h1>Top Origin Countries for {} County</h1>
        <table>
            <tr>
                <th>Country</th>
                <th>Total</th>
            </tr>'''.format(county)

    for row in cur:
        print(row)
        country = row[0]
        total = row[1]
        output += "<tr><td>{}</td><td>{}</td></tr>".format(country, total)

    return output + "</table>"

@app.route('/top_country/<county>/<year>')
def top_country_per_county_per_year(county, year):
    cur = get_cursor()
    county = escape(county)
    year = escape(year)

    cur.execute('''
            SELECT origin_country, COUNT(*) AS total_cards FROM card_transfer
            WHERE origin_country <> "USA"
                AND year = ?
                AND county = ?
            GROUP BY origin_country
            ORDER BY total_cards DESC;
    ''', (year, county))

    output = '''
        <h1>Top Origin Countries for {} County in {}</h1>
        <table>
            <tr>
                <th>Country</th>
                <th>Total</th>
            </tr>'''.format(county, year)

    for row in cur:
        print(row)
        country = row[0]
        total = row[1]
        output += "<tr><td>{}</td><td>{}</td></tr>".format(country, total)

    return output + "</table>"

@app.route('/top_country_per_year/<year>')
def top_country_per_year(year):
    cur = get_cursor()
    year = escape(year)

    cur.execute('''
            SELECT origin_country, COUNT(*) AS total_cards FROM card_transfer
            WHERE origin_country <> "USA"
                AND year = ?
            GROUP BY origin_country
            ORDER BY total_cards DESC;
    ''', (year,))

    output = '''
        <h1>Top Origin Countries in {}</h1>
        <table>
            <tr>
                <th>Country</th>
                <th>Total</th>
            </tr>'''.format(year)

    for row in cur:
        print(row)
        country = row[0]
        total = row[1]
        output += "<tr><td>{}</td><td>{}</td></tr>".format(country, total)

    return output + "</table>"