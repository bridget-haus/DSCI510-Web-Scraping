import sys
import argparse
import requests
import json
from bs4 import BeautifulSoup
import sqlite3
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import matplotlib.pyplot as plt
import seaborn as sns
import os

rank_stats_url = 'https://publicuniversityhonors.com/2016/09/18/average-u-s-news-rankings-for-126-universities-2010-1017/'
tuition_url = 'https://phillips-scholarship.org/new-applicants/cost-of-college-list/'
demographic_base_url = 'https://api.data.gov/ed/collegescorecard/v1/schools'
demographic_api_key = '6sX5SOPjUqItWIRHi5xxUBOc3Hu0SijTazi2oYxp'

#make surecorrect path for creating db

path = os.getcwd()
path = path.split('/')
last_val = path[-1:]
while last_val[0] != 'inf510_project' :
    path = path[:-1]
    last_val = path[-1:]
    
dbPath = "/".join(path) + '/data/college.db'

#create db

conn = sqlite3.connect(dbPath)
cur = conn.cursor()

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--source', help = 'remote or local')
    args = parser.parse_args()
    print(args)

    try:
        if args.source == 'remote':
            print('Executing scrapers and API requests...')
            print('This should only take 2-3 minutes...')
            setup_tables()
            insert_college_table()
            insert_tuition_table()
            insert_rank_table()
            insert_demographic_table()
            print('Finished building database!')
        elif args.source == 'local':
            print('local')
        else:
            print('please enter arguments --source=remote or –-source=local')
    except Exception as e:
        print(e)

#helper function to parse websites

def make_soup(url):
    try:
        r = requests.get(url)
        r.raise_for_status()
    #catch exceptions
    except requests.exceptions.RequestException as e:
        print(e)
    soup = BeautifulSoup(r.content, 'lxml')
    return soup

#scrapes website with college tuition data. returns zipped list of tuition data

def get_tuition_stats(url):
    soup = make_soup(url)
    college_list = []
    tuition_in_state_list = []
    tuition_out_state_list = []
    room_board_list = []
    main_table = soup.findAll('table', {"id" : "tablepress-25"})[0]
    main_body = main_table.find('tbody')
    for college in main_body.findAll('td', {"class": "column-1"}):
        college = college.text
        college_list.append(college)
    for tuition_in_state in main_body.findAll('td', {"class": "column-2"}):
        tuition_in_state = tuition_in_state.text
        tuition_in_state_list.append(tuition_in_state)
    for tuition_out_state in main_body.findAll('td', {"class": "column-3"}):
        tuition_out_state = tuition_out_state.text
        tuition_out_state_list.append(tuition_out_state)
    for room_board in main_body.findAll('td', {"class": "column-4"}):
        room_board = room_board.text
        room_board_list.append(room_board)
    tuition_zip = [list(a) for a in list(zip(college_list, tuition_in_state_list, tuition_out_state_list, room_board_list))]
    return tuition_zip

#scrapes website with college rank data. returns zipped list of college rank from 2013 - 2020

def get_rank_stats(url):
    soup = make_soup(url)
    college_list = []
    rank_2013_list = []
    rank_2014_list = []
    rank_2015_list = []
    rank_2016_list = []
    rank_2017_list = []
    rank_2018_list = []
    rank_2019_list = []
    main_table = soup.findAll('table', {"id" : "tablepress-105"})[0]
    main_body = main_table.find('tbody')
    for college in main_body.findAll('td', {"class": "column-1"}):
        if college.text != "":
            college_list.append(college.text)
    for rank_2013 in main_body.findAll('td', {"class": "column-2"}):
        if rank_2013.text != "":
            rank_2013_list.append(tuple((2013, rank_2013.text)))
    for rank_2014 in main_body.findAll('td', {"class": "column-3"}):
        if rank_2014.text != "":
            rank_2014_list.append(tuple((2014, rank_2014.text)))
    for rank_2015 in main_body.findAll('td', {"class": "column-4"}):
        if rank_2015.text != "":
            rank_2015_list.append(tuple((2015, rank_2015.text)))
    for rank_2016 in main_body.findAll('td', {"class": "column-5"}):
        if rank_2016.text != "":
            rank_2016_list.append(tuple((2016, rank_2016.text)))
    for rank_2017 in main_body.findAll('td', {"class": "column-6"}):
        if rank_2017.text != "":
            rank_2017_list.append(tuple((2017, rank_2017.text)))
    for rank_2018 in main_body.findAll('td', {"class": "column-7"}):
        if rank_2018.text != "":
            rank_2018_list.append(tuple((2018, rank_2018.text)))
    for rank_2019 in main_body.findAll('td', {"class": "column-8"}):
        if rank_2019.text != "":
            rank_2019_list.append(tuple((2019, rank_2019.text)))
    rank_zip = [list(a) for a in list(zip(college_list, rank_2013_list, rank_2014_list, rank_2015_list, rank_2016_list, rank_2017_list, rank_2018_list, rank_2019_list))]
    return rank_zip

#creates api query parameters and calls for each college. returns list of demographic features

def get_demographic_stats(api_url):

    rank_data = get_rank_stats(rank_stats_url)
    rank_map_data = fuzzy_wuzzy_mapping(rank_data)

    years = list(range(2013, 2018))
    json_response_list = []
    admission_rate = ''
    loan_completion_rate = ''
    sat_score = ''
    percent_black = ''
    percent_hispanic = ''
    median_income = ''
    for year in years:
        admission_rate = str(year) + '.admissions.admission_rate.overall,' + admission_rate
        loan_completion_rate = str(year) + '.completion.completion_rate_4yr_150nt,' + loan_completion_rate
        sat_score = str(year) + '.admissions.sat_scores.average.overall,' + sat_score
        percent_black = str(year) + '.student.demographics.race_ethnicity.black,' + percent_black
        percent_hispanic = str(year) + '.student.demographics.race_ethnicity.hispanic,' + percent_hispanic
        median_income = str(year) + '.student.demographics.median_family_income,' + median_income
    fields_base = '?fields=school.name,' + admission_rate + loan_completion_rate + sat_score + percent_black + percent_hispanic + median_income
    demographic_url = api_url + fields_base
    for college in rank_map_data:
        demographic_params = {'school.name': college[0].replace(',',''), 'api_key': demographic_api_key}
        json_response = requests.get(demographic_url, params=demographic_params)
        json_response_list.append(json_response.json())
    return json_response_list

#maps college name from one data source to another

def fuzzy_wuzzy_mapping(original_data):

    rank_data = get_rank_stats(rank_stats_url)
    tuition_data = get_tuition_stats(tuition_url)

    mapping_list = []
    cleaned_list = []

    for i in range(len(tuition_data)):
        mapping_list.append(tuition_data[i][0])

    for i in range(len(original_data)):
        if f'{original_data[i][0]} University' in mapping_list:
            original_data[i][0] = f'{original_data[i][0]} University'
        elif f'University of {original_data[i][0]}' in mapping_list:
            original_data[i][0] = f'University of {original_data[i][0]}'
        elif 'UC' in original_data[i][0]:
            original_data[i][0] = f'University of California {original_data[i][0]}'
        x = process.extract(original_data[i][0], mapping_list, limit=1)[0][0]
        if x not in cleaned_list:
            cleaned_list.append(x)
            original_data[i][0] = x
        else:
            original_data[i][0] = 'No Match'
    
    return original_data

#creates SQL tables

def setup_tables():

    cur.execute('DROP TABLE IF EXISTS College')
    cur.execute('DROP TABLE IF EXISTS Tuition')
    cur.execute('DROP TABLE IF EXISTS Rank')
    cur.execute('DROP TABLE IF EXISTS Demographics')

    cur.execute('CREATE TABLE College (primary_key INTEGER PRIMARY KEY AUTOINCREMENT, college_name TEXT)')
    cur.execute('CREATE TABLE Tuition (primary_key INTEGER PRIMARY KEY AUTOINCREMENT, in_state_tuition INTEGER, out_state_tuition INTEGER, room_board INTEGER, college_primary_key INTEGER)')
    cur.execute('CREATE TABLE Rank (primary_key INTEGER PRIMARY KEY AUTOINCREMENT, year INTEGER, rank INTEGER, college_primary_key INTEGER)')
    cur.execute('CREATE TABLE Demographics (primary_key INTEGER PRIMARY KEY AUTOINCREMENT, year INTEGER, sat_overall INTEGER, admission_rate INTEGER, loan_completion_rate INTEGER, percent_black INTEGER, percent_hispanic INTEGER, median_income INTEGER, college_primary_key INTEGER)')

    conn.commit()

#inserts data from scrapers and API requests into each table

def insert_college_table():

    tuition_data = get_tuition_stats(tuition_url)

    for i in range(len(tuition_data)):
        college_name = tuition_data[i][0]
        cur.execute("INSERT INTO College (primary_key, college_name) VALUES (?, ?)", (None, college_name))

    conn.commit()

def insert_tuition_table():
    
    tuition_data = get_tuition_stats(tuition_url)

    for i in range(len(tuition_data)):
        cur.execute("SELECT primary_key from College WHERE college_name = ?", (tuition_data[i][0],))
        college_primary_key = cur.fetchall()[0][0]
        in_state_tuition = tuition_data[i][1]
        out_state_tuition = tuition_data[i][2]
        room_board = tuition_data[i][3]
        cur.execute('INSERT INTO Tuition (primary_key, in_state_tuition, out_state_tuition, room_board, college_primary_key) VALUES ( ?, ?, ?, ?, ? )', (None, in_state_tuition, out_state_tuition, room_board, college_primary_key))

    conn.commit()

def insert_rank_table():

    rank_data = get_rank_stats(rank_stats_url)
    rank_map_data = fuzzy_wuzzy_mapping(rank_data)

    for i in range(len(rank_map_data) - 1):
        cur.execute("SELECT primary_key from College WHERE college_name = ?", (rank_map_data[i][0],))
        result = cur.fetchall()
        if len(result) > 0:
            college_primary_key = result[0][0]
            for j in range(len(rank_map_data[0])-1):
                year = rank_map_data[i][j+1][0]
                rank = rank_map_data[i][j+1][1]
                cur.execute('INSERT INTO Rank (primary_key, year, rank, college_primary_key) VALUES ( ?, ?, ?, ? )', (None, year, rank, college_primary_key))
    
    conn.commit()

def insert_demographic_table():

    demographic_data = get_demographic_stats(demographic_base_url)
    cleaned_list = []

    for response in demographic_data:
        for result in response['results']:
            sorted_keys = sorted(list(result.keys()))
            n = 6
            final = [sorted_keys[i * n:(i + 1) * n] for i in range((len(sorted_keys) + n - 1) // n )]
            college_name = result[final[-1][0]]
            cur.execute("SELECT primary_key from College WHERE college_name = ?", (college_name,))
            primary_key = cur.fetchall()
            if len(primary_key) > 0:
                if primary_key[0][0] not in cleaned_list:
                    college_primary_key = primary_key[0][0]
                    cleaned_list.append(college_primary_key)
                    table_year = 2013
                    for year in final[:-1]:
                        admission_rate = result[year[0]]
                        sat_overall = result[year[1]]
                        loan_completion_rate = result[year[2]]
                        median_income = result[year[3]]
                        percent_black = result[year[4]]
                        percent_hispanic = result[year[5]]
                        cur.execute('INSERT INTO Demographics (primary_key, year, admission_rate, sat_overall, loan_completion_rate, percent_black, percent_hispanic, median_income, college_primary_key) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ? )', (None, table_year, admission_rate, sat_overall, loan_completion_rate, percent_black, percent_hispanic, median_income, college_primary_key))
                        table_year += 1   

    conn.commit()

if __name__ == '__main__':
    main()
