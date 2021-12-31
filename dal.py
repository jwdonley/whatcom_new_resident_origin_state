import os
import sqlite3

class DAL:
    connection = None

    def __init__(self, root_path) -> None:
        self.connection = sqlite3.connect(os.path.join(root_path, 'db', 'raw_data.sqlite'))

    def get_cursor(self) ->sqlite3.Cursor:
        return self.connection.cursor()

    def close(self):
        self.connection.close()

    def get_top_state_per_county(self, county):
        return self.__load_tuple_list(self.get_cursor().execute('''
            SELECT COUNT(*) as count_from_state, origin_state, MIN(issue_date), MAX(issue_date)
            FROM card_transfer
            WHERE origin_country = "USA"
                AND county = ?
            GROUP BY origin_state
            ORDER BY origin_state;
        ''', (county,)))
    
    def get_top_states_per_year_for_county(self, county, year):
        return self.__load_tuple_list(self.get_cursor().execute('''
            SELECT COUNT(*) as count_from_state, origin_state, MIN(issue_date), MAX(issue_date)
            FROM card_transfer
            WHERE origin_country = "USA"
                AND county = ?
                AND year = ?
            GROUP BY origin_state
            ORDER BY origin_state;
        ''', (county, year)))

    def get_top_states_per_year(self, year):
        return self.__load_tuple_list(self.get_cursor().execute('''
            SELECT COUNT(*) as count_from_state, origin_state, MIN(issue_date), MAX(issue_date)
            FROM card_transfer
            WHERE origin_country = "USA"
                AND year = ?
            GROUP BY origin_state
            ORDER BY count_from_state DESC;
        ''', (year,)))

    def get_top_states(self):
        return self.__load_tuple_list(self.get_cursor().execute('''
            SELECT COUNT(*) as count_from_state, origin_state, MIN(issue_date), MAX(issue_date)
            FROM card_transfer
            WHERE origin_country = "USA"
            GROUP BY origin_state
            ORDER BY count_from_state DESC;
        '''))

    def get_top_county_per_state_year(self, state, year):
        return self.__load_tuple_list(self.get_cursor().execute('''
            SELECT county, COUNT(*) AS total_cards FROM card_transfer
            WHERE year = ?
                AND origin_country = "USA"
                AND origin_state = ?
                AND county <> "Unverified Address"
            GROUP BY county
            ORDER BY total_cards DESC
        ''', (year, state)))

    def get_top_county_per_state(self, state):
        return self.__load_tuple_list(self.get_cursor().execute('''
            SELECT county, COUNT(*) AS total_cards FROM card_transfer
            WHERE origin_country = "USA"
                AND origin_state = ?
                AND county <> "Unverified Address"
            GROUP BY county
            ORDER BY total_cards DESC
        ''', (state,)))

    def get_top_country(self):
        return self.__load_tuple_list(self.get_cursor().execute('''
            SELECT origin_country, COUNT(*) AS total_cards FROM card_transfer
            WHERE origin_country <> "USA"
            GROUP BY origin_country
            ORDER BY total_cards DESC;
        '''))

    def get_top_country_per_county(self, county):
        return self.__load_tuple_list(self.get_cursor().execute('''
            SELECT origin_country, COUNT(*) AS total_cards FROM card_transfer
            WHERE origin_country <> "USA"
                AND county = ?
            GROUP BY origin_country
            ORDER BY total_cards DESC;
        ''', (county,)))
    
    def get_top_country_per_county_per_year(self, county, year):
        return self.__load_tuple_list(self.get_cursor().execute('''
            SELECT origin_country, COUNT(*) AS total_cards FROM card_transfer
            WHERE origin_country <> "USA"
                AND year = ?
                AND county = ?
            GROUP BY origin_country
            ORDER BY total_cards DESC;
        ''', (year, county)))

    def get_top_country_per_year(self, year):
        return self.__load_tuple_list(self.get_cursor().execute('''
            SELECT origin_country, COUNT(*) AS total_cards FROM card_transfer
            WHERE origin_country <> "USA"
                AND year = ?
            GROUP BY origin_country
            ORDER BY total_cards DESC;
        ''', (year,)))

    def __load_tuple_list(self, cur):
        output = []
        for row in cur:
            output.append((row[0], row[1]))
        return output