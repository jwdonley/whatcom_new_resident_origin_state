from dal import DAL
from flask import Flask
from flask import g
from flask.helpers import url_for
from markupsafe import escape

app = Flask(__name__)

@app.before_request
def setup():
    if not hasattr(g, 'db'):
        g.db = DAL(app.root_path)

@app.teardown_appcontext
def close_db(error):
    if hasattr(g, 'db'):
        g.db.close()

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
        url_for('top_states_for_county', county="Whatcom"), 
        url_for('top_states_per_year_for_county', county="Whatcom", year="2020"),
        url_for('top_states'),
        url_for('top_states_per_year', year="2020"),
        url_for('top_county_per_state_year', state="California", year="2020"),
        url_for('top_county_per_state', state="California"),
        url_for('top_country'),
        url_for('top_country_per_year', year="2020"),
        url_for('top_country_per_county', county="Whatcom"),
        url_for('top_country_per_county_per_year', county="Whatcom", year="2020"))

@app.route('/top-states-for/<county>')
def top_states_for_county(county):
    county = escape(county)

    results = g.db.get_top_state_per_county(county)

    output = '''
        <h1>Top Origin States for {} County</h1>
        <table>
            <tr>
                <th>Count</th>
                <th>Origin State</th>
            </tr>'''.format(county)

    for result in results:
        count = result[0]
        state = result[1]
        output += "<tr><td>{}</td><td>{}</td></tr>".format(count, state)

    return output + "</table>"

@app.route('/top-states-for/<county>/<year>')
def top_states_per_year_for_county(county, year):
    county = escape(county)
    year = escape(year)

    results = g.db.get_top_states_per_year_for_county(county, year)

    output = '''
        <h1>Top Origin States for {} County in {}</h1>
        <table>
            <tr>
                <th>Count</th>
                <th>Origin State</th>
            </tr>'''.format(county, year)

    for result in results:
        count = result[0]
        state = result[1]
        output += "<tr><td>{}</td><td>{}</td></tr>".format(count, state)

    return output + "</table>"

@app.route('/top-states-per-year/<year>')
def top_states_per_year(year):
    year = escape(year)

    results = g.db.get_top_states_per_year(year)

    output = '''
        <h1>Top Origin States for Washington State in {}</h1>
        <table>
            <tr>
                <th>Count</th>
                <th>Origin State</th>
            </tr>'''.format(year)

    for result in results:
        count = result[0]
        state = result[1]
        output += "<tr><td>{}</td><td>{}</td></tr>".format(count, state)

    return output + "</table>"

@app.route('/top-states')
def top_states():
    results = g.db.get_top_states()

    output = '''
        <h1>Top Origin States for Washington State</h1>
        <table>
            <tr>
                <th>Count</th>
                <th>Origin State</th>
            </tr>'''

    for result in results:
        count = result[0]
        state = result[1]
        output += "<tr><td>{}</td><td>{}</td></tr>".format(count, state)

    return output + "</table>"

@app.route('/top-county-per-state/<state>/<year>')
def top_county_per_state_year(state, year):
    state = escape(state)
    year = escape(year)

    results = g.db.get_top_county_per_state_year(state, year)

    output = '''
        <h1>Top Washington Counties Moved to from {} in {}</h1>
        <table>
            <tr>
                <th>County</th>
                <th>Total</th>
            </tr>'''.format(state, year)

    for row in results:
        print(row)
        county = row[0]
        total = row[1]
        output += "<tr><td>{}</td><td>{}</td></tr>".format(county, total)

    return output + "</table>"

@app.route('/top-county-per-state/<state>')
def top_county_per_state(state):
    state = escape(state)

    results = g.db.get_top_county_per_state(state)

    output = '''
        <h1>Top Washington Counties Moved to from {}</h1>
        <table>
            <tr>
                <th>County</th>
                <th>Total</th>
            </tr>'''.format(state)

    for row in results:
        print(row)
        county = row[0]
        total = row[1]
        output += "<tr><td>{}</td><td>{}</td></tr>".format(county, total)

    return output + "</table>"

@app.route('/top-country')
def top_country():
    results = g.db.get_top_country()

    output = '''
        <h1>Top Origin Countries</h1>
        <table>
            <tr>
                <th>Country</th>
                <th>Total</th>
            </tr>'''

    for row in results:
        print(row)
        country = row[0]
        total = row[1]
        output += "<tr><td>{}</td><td>{}</td></tr>".format(country, total)

    return output + "</table>"

@app.route('/top-country/<county>')
def top_country_per_county(county):
    county = escape(county)

    results = g.db.get_top_country_per_county(county)

    output = '''
        <h1>Top Origin Countries for {} County</h1>
        <table>
            <tr>
                <th>Country</th>
                <th>Total</th>
            </tr>'''.format(county)

    for row in results:
        print(row)
        country = row[0]
        total = row[1]
        output += "<tr><td>{}</td><td>{}</td></tr>".format(country, total)

    return output + "</table>"

@app.route('/top-country/<county>/<year>')
def top_country_per_county_per_year(county, year):
    county = escape(county)
    year = escape(year)

    results = g.db.get_top_country_per_county_per_year(county, year)

    output = '''
        <h1>Top Origin Countries for {} County in {}</h1>
        <table>
            <tr>
                <th>Country</th>
                <th>Total</th>
            </tr>'''.format(county, year)

    for row in results:
        print(row)
        country = row[0]
        total = row[1]
        output += "<tr><td>{}</td><td>{}</td></tr>".format(country, total)

    return output + "</table>"

@app.route('/top-country-per-year/<year>')
def top_country_per_year(year):
    year = escape(year)

    results = g.db.get_top_country_per_year(year)

    output = '''
        <h1>Top Origin Countries in {}</h1>
        <table>
            <tr>
                <th>Country</th>
                <th>Total</th>
            </tr>'''.format(year)

    for row in results:
        print(row)
        country = row[0]
        total = row[1]
        output += "<tr><td>{}</td><td>{}</td></tr>".format(country, total)

    return output + "</table>"