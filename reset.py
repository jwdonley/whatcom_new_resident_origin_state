import sqlite3

conn = sqlite3.connect('raw_data.sqlite')
cur = conn.cursor()

cur.executescript('''
DROP TABLE IF EXISTS card_transfer;

CREATE TABLE card_transfer(
    id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    count_of_new_credential     TEXT,
    year                        TEXT,
    month                       TEXT,
    issue_date                  TEXT,
    card_type_issued            TEXT,
    county                      TEXT,
    origin_state                TEXT,
    origin_country              TEXT,
    geocode_type                TEXT,
    geocode_x                   NUMBER,
    geocode_y                   NUMBER
);
''')

conn.commit()
conn.close()