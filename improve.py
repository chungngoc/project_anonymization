import csv
import sys
import argparse
import math
import numpy as np
import pandas as pd
import datetime as dt
separator = '\t'
from timeit import default_timer as timer

#################################
#         Global variables      #
# To know:                      #
# dx =1 means that you allow    #
# a maximum of 111.195km        #
# 0.0001 : cellule au mà¨tre     #
# 0.001 : cellule à  la rue      #
# 0.01 : cellule au quartier    #
# 0.1 : cellule à  la ville      #
# 1 : cellule à  la région       #
# 10 : cellule au pays          #
#                               #
#################################
dx = 0.1

#################################
#         Function              #
#################################
def calcul_utility(diff):
    score = diff*(-1/dx) + 1
    if(score < 0):
        return 0
    return score

#################################
#         Utiliy Function       #
#################################
# fd_nona_file : file original
# fd_anon_file : file anonymised
def main(fd_anon_file,fd_nona_file,parameters={"dx":0.1}):
    #variables
    utilities = {}
    filesize = 0

    s = timer()
    #open the files:
    columns_name = ['id', 'date', 'latitude', 'longitude' ]
    nona_reader = pd.read_csv(fd_nona_file, names = columns_name, sep = '\t')
    anon_reader = pd.read_csv(fd_anon_file, names = columns_name, sep = '\t')
    filesize = nona_reader.shape[0]  #count number of lines with id != "DEL"
    if (filesize != anon_reader.shape[0]):  #check if file original and file anonym have the same size
        raise Exception("Filesize must be the same")
    e = timer()
    print(e - s, "1")

    #read the files and calcul
    #DistanceUtility
    s = timer()
    global dx
    dx = parameters['dx']
    diff_distance = abs(nona_reader.latitude - anon_reader.latitude) + abs(nona_reader.longitude - anon_reader.longitude)
    anon_reader['score'] = diff_distance.apply(calcul_utility)
    distance_utility = anon_reader.loc[anon_reader.id != "DEL", 'score'].sum()/filesize
    e = timer()
    print(e - s)

    #DateUtil
    s = timer()
    nona_reader.date = pd.to_datetime(nona_reader.date)
    anon_reader.date = pd.to_datetime(anon_reader.date)

    if (nona_reader.date.dt.isocalendar().week != nona_reader.date.dt.isocalendar().week).any():  # check if weeks are same
        raise Exception("Weeks must be the same")
    
    anon_reader['score'] = 3 - abs(nona_reader.date.dt.dayofweek - anon_reader.date.dt.dayofweek) # date difference, subtract 1/3 of a point per weekday
    anon_reader.loc[(anon_reader.score < 0) | (anon_reader.id == "DEL"), "score"] = 0  # set score = 0 with lines have id = "DEL" or have score < 0
    date_utility =  anon_reader.score.sum()/(3*filesize)

    e = timer()
    print(e - s)

    #HourUtil : Compute the utility in function of the date gap
    s = timer()
    if ((nona_reader.date.dt.hour > 24).any() or (nona_reader.date.dt.hour < 0).any()):  # check if weeks are same
        raise Exception("Hour must be between 0 and 24")
    hourdec = [1, 0.9, 0.8, 0.6, 0.4, 0.2, 0, 0.1, 0.2, 0.3, 0.4, 0.5,
               0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0, 0.2, 0.4, 0.6, 0.8, 0.9]
    anon_reader['score'] = abs(nona_reader.date.dt.hour - anon_reader.date.dt.hour)
    anon_reader.score = anon_reader.score.apply(lambda x: hourdec[x])
    hour_utility =  anon_reader.loc[anon_reader.id != "DEL", "score"].sum()/(filesize)
    # hourdec = [1, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0, 0, 0]  # Set the amount linked to the hour gap
    # anon_reader['score'] = 10 - abs(nona_reader.date.dt.hour - anon_reader.date.dt.hour) # Subtract 0,1 points per hour (even if days are identical)
    # anon_reader.loc[(anon_reader.score < 0) | (anon_reader.id == "DEL"), "score"] = 0  # set score = 0 with lines have id = "DEL" or have score < 0
    # hour_utility =  anon_reader.score.sum()/(10*filesize)

    e = timer()
    print(e - s)

    utilities = {"distance": distance_utility, "date":date_utility, "hour": hour_utility}
    return utilities


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("anonymized", help="Anonymized Dataframe filename")
    parser.add_argument("original", help="Original Dataframe filename")
    args = parser.parse_args()
    print(main(args.anonymized, args.original))
