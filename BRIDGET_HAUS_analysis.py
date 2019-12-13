import sys
import argparse
import requests
import json
from bs4 import BeautifulSoup
import sqlite3
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import seaborn as sns
import numpy as np
from numpy import mean
from numpy import std
from scipy.stats import pearsonr

#Connect to db
conn = sqlite3.connect('college.db')
cur = conn.cursor()

counter_map = []
user_input = int()
fields = ['admission_rate', 'sat_overall', 'loan_completion_rate', 'percent_black', 'percent_hispanic', 'median_income']
colors = ['blue', 'green', 'red', 'cyan', 'magenta', 'yellow']

def main():
    print('What would you like to do? Please select the number of the options below')
    print('1. Analyze a single school')
    print('2. Analyze all schools together')
    main_input = int(input())
    if main_input == 1:
        input_function()
        rank_trend()
        demographics_trend()
    elif main_input == 2:
        correlation__demographics()
        correlation__tuition()
    else:
        print('Invalid input. Please select the number of the choice above')

def input_function():

    global counter_map
    global user_input

    print('Please select the number of the school would you like to analyze.')
    cur.execute('SELECT college.primary_key, college.college_name FROM Rank JOIN College ON Rank.college_primary_key = College.primary_key JOIN Demographics on rank.college_primary_key = demographics.college_primary_key WHERE Rank.year = 2013 GROUP BY college_name ORDER BY college_name')
    result = cur.fetchall()
    counter = 1
    for college in result:
        print(f'{counter}.', college[1])
        counter_map.append(college[0])
        counter += 1
    while True:
        try:
            user_input = int(input())
            if 0 < user_input < len(counter_map) + 1:
                break
            else:
                print('Invalid input. Please select the number of the school would you like to analyze.')
        except:
            print('Invalid input. Please select the number of the school you would like to analyze.')    

def rank_trend():

    global counter_map
    global user_input

    years = [year for year in range(2013,2020)]
    xs = np.array(years, dtype=np.float64)
    cur.execute('SELECT college_name FROM College WHERE primary_key = ?', (counter_map[user_input - 1],))
    title = cur.fetchall()
    cur.execute('SELECT rank FROM Rank WHERE college_primary_key = ?', (counter_map[user_input - 1],))
    y = cur.fetchall()
    if y[0][0] != None:
        sns.set()
        ax = plt.figure().gca()
        ax.xaxis.set_major_locator(MaxNLocator(integer=True))
        res = [i[0] for i in y]
        ys = np.array(res, dtype=np.float64)
        m = best_fit_slope(xs,ys)
        if round(m, 5) < 0:
            m_sign = 'negative'
            m_interpretation = 'As year increases, the US News Rank generally improves'
        else:
            m_sign = 'positive'
            m_interpretation = 'As year increases, the US News Rank generally worsens'
        plt.plot(xs, ys, color = 'black', linewidth=4)
        plt.plot(np.unique(xs), np.poly1d(np.polyfit(xs, ys, 1))(np.unique(xs)), linestyle='dashed')
        plt.ylim(plt.ylim()[::-1])
        plt.xlabel('Year')
        plt.ylabel('US News Rank')
        plt.title(f'{title[0][0]} US News Rank Trend')
        plt.show(block=True)
        print(f'The slope of the best fit regression line is a {m_sign} value: {round(m, 5)}')
def demographics_trend():

    global counter_map
    global user_input

    years = [year for year in range(2013,2018)]
    cur.execute('SELECT college_name FROM College WHERE primary_key = ?', (counter_map[user_input - 1],))
    title = cur.fetchall()
    xs = np.array(years, dtype=np.float64)
    for i in range(len(fields)):
        select_statement = f'select {fields[i]} from Demographics WHERE college_primary_key = ?'
        cur.execute(select_statement, (counter_map[user_input - 1],))
        y = cur.fetchall()
        if y[0][0] != None:
            field_name = fields[i].title().replace('_', ' ')
            sns.set()
            ax = plt.figure().gca()
            ax.xaxis.set_major_locator(MaxNLocator(integer=True))
            cur.execute('select median_income from Demographics where year = 2016 and college_primary_key = ?', (counter_map[user_input - 1],))
            default = cur.fetchall()[0][0]
            res = [default if i[0] == None else i[0] for i in y]
            ys = np.array(res, dtype=np.float64)
            m = best_fit_slope(xs,ys)
            if m < 0:
                m_sign = 'negative'
            else:
                m_sign = 'positive'
            plt.plot(xs, ys, color = colors[i], linewidth=4)
            plt.plot(np.unique(xs), np.poly1d(np.polyfit(xs, ys, 1))(np.unique(xs)), linestyle='dashed')
            plt.title(f'{title[0][0]} {field_name} Trend')
            plt.xlabel('Year')
            plt.ylabel(field_name)
            plt.show(block=True)
            print(f'The slope of the best fit regression line is a {m_sign} value: {round(m, 5)}')

def correlation__demographics():

    fields = ['admission_rate', 'sat_overall', 'loan_completion_rate', 'percent_black', 'percent_hispanic', 'median_income']
    colors = ['blue', 'green', 'red', 'cyan', 'magenta', 'yellow']
    for i in range(len(fields)):
        rank_select = f'SELECT rank FROM Rank JOIN College ON Rank.college_primary_key = College.primary_key JOIN Demographics on rank.college_primary_key = demographics.college_primary_key AND rank.year = demographics.year WHERE {fields[i]} is not null and rank < 2018'
        cur.execute(rank_select)    
        y = cur.fetchall()
        ys = [float(number[0]) for number in y]
        fields_select = f'SELECT {fields[i]} FROM Rank JOIN College ON Rank.college_primary_key = College.primary_key JOIN Demographics on rank.college_primary_key = demographics.college_primary_key AND rank.year = demographics.year WHERE {fields[i]} is not null and rank < 2018'
        cur.execute(fields_select)
        x = cur.fetchall()
        xs = [float(number[0]) for number in x]
        field_name = fields[i].title().replace('_', ' ')
        plt.scatter(xs, ys, color = colors[i])
        plt.ylim(plt.ylim()[::-1])
        plt.plot(np.unique(xs), np.poly1d(np.polyfit(xs, ys, 1))(np.unique(xs)), linestyle='dashed')
        plt.title(f'{field_name} vs US News Rank')
        plt.xlabel(field_name)
        plt.ylabel("US News Rank")
        plt.show(block=True)
        print(f'US News Rank: mean={round(mean(ys),3)} stdv={round(std(ys),3)}')
        print(f'{field_name}: mean={round(mean(xs),3)} stdv={round(std(xs),3)}')
        corr = pearsonr(xs, ys)
        print(f'Pearson correlational coefficient: {round(corr[0], 3)}')

def correlation__tuition():
    fields = ['in_state_tuition', 'out_state_tuition', 'room_board']
    colors = ['blue', 'green', 'red']
    for i in range(len(fields)):
        rank_select = f'SELECT rank FROM Rank JOIN College ON Rank.college_primary_key = College.primary_key JOIN Tuition on rank.college_primary_key = tuition.college_primary_key WHERE NULLIF({fields[i]}, "") IS NOT NULL and rank IS NOT NULL'
        cur.execute(rank_select)    
        y = cur.fetchall()
        ys = [float(number[0]) for number in y]
        fields_select = f'SELECT {fields[i]} FROM Rank JOIN College ON Rank.college_primary_key = College.primary_key JOIN Tuition on rank.college_primary_key = tuition.college_primary_key WHERE NULLIF({fields[i]}, "") IS NOT NULL and rank IS NOT NULL'
        cur.execute(fields_select)
        x = cur.fetchall()
        xs = [float(number[0][1:].replace(',', '')) for number in x]
        field_name = fields[i].title().replace('_', ' ')
        plt.scatter(xs, ys, color = colors[i])
        plt.ylim(plt.ylim()[::-1])
        plt.plot(np.unique(xs), np.poly1d(np.polyfit(xs, ys, 1))(np.unique(xs)), linestyle='dashed')
        plt.title(f'{field_name} vs US News Rank')
        plt.xlabel(field_name)
        plt.ylabel("US News Rank")
        plt.show(block=True)
        print(f'US News Rank: mean={round(mean(ys),3)} stdv={round(std(ys),3)}')
        print(f'{field_name}: mean={round(mean(xs),3)} stdv={round(std(xs),3)}')
        corr = pearsonr(xs, ys)
        print(f'Pearson correlational coefficient: {round(corr[0], 3)}')


def best_fit_slope(xs,ys):
    m = (((mean(xs)*mean(ys)) - mean(xs*ys)) /
         ((mean(xs)**2) - mean(xs*xs)))
    return m

if __name__ == '__main__':
    main()

