import ncaa
import espn
import model

import pandas
import sqlalchemy
import os
import sys
import datetime

dbfile = 'sqlite:///cfb.sqlite3'
cachedir = 'cache'

# Which week do we want?
# Future update: take command line arguments?
season = None
while season is None:
    try:
        season = int(input("Please enter a season : "))
    except:
        season = None
week = None
while week is None:
    week = input("Please enter a week (1-15,B) : ")
    if week in map(str, range(1,16)):
        week = int(week)
    elif str(week).lower() in ['b', 'bowl']:
        week = 'Bowl'
    else:
        week = None

# Get the games, checking for cached copies
def get_games(season, week, cachedir='cache', echo=True):
    # ESPN
    espnfilename = "ESPN-{}-{}.csv".format(season, week)
    if os.path.exists(os.path.join(cachedir, espnfilename)):
        if echo:
            print("Reading cached ESPN games...", end=" ", flush=True)
        espngames = pandas.read_csv(os.path.join(cachedir, espnfilename),
                                    parse_dates=['Date'])
    else:
        if echo:
            print("Fetching ESPN games...", end=" ", flush=True)
        espngames = espn.get_week_games(season, week)
        os.makedirs(cachedir, exist_ok=True)
        espngames.to_csv(os.path.join(cachedir, espnfilename), index=False)
    if echo:
        print(len(espngames), "games.")
    # NCAA
    ncaafilename = "NCAA-{}-{}.csv".format(season, week)
    if os.path.exists(os.path.join(cachedir, ncaafilename)):
        if echo:
            print("Reading cached NCAA games...", end=" ", flush=True)
        ncaagames = pandas.read_csv(os.path.join(cachedir, ncaafilename),
                                    parse_dates=['Date'])
    else:
        if echo:
            print("Fetching NCAA games...", end=" ", flush=True)
        ncaagamelist = []
        for d in list(set(espngames['Date'])):
            ncaagamelist.append(ncaa.get_date_games(season,d))
        ncaagames = pandas.concat(ncaagamelist).reset_index(drop=True)
        os.makedirs(cachedir, exist_ok=True)
        ncaagames.to_csv(os.path.join(cachedir, ncaafilename), index=False)
    if echo:
        print(len(ncaagames), "games.")
    # Return them both
    return espngames, ncaagames

# Get the games
espngames, ncaagames = get_games(season, week, cachedir=cachedir, echo=True)

# Create engine/connection/session
engine = sqlalchemy.create_engine(dbfile)
dbcon = engine.connect()
model.Base.metadata.create_all(dbcon)  # create temp tables
session = model.Session(bind=dbcon)

# Load games into db
for r in espngames.to_dict('records'):
    game = model.TempESPNGame(away=r['Away'],
                              home=r['Home'],
                              date=r['Date'],
                              awaypoints=r['AwayPoints'],
                              homepoints=r['HomePoints'],
                              seasonyear=r['Season'],
                              comments=r['Comments'],
                              overtimes=r['Overtimes'])
    session.add(game)
for r in ncaagames.to_dict('records'):
    game = model.TempNCAAGame(away=r['Away'],
                              home=r['Home'],
                              date=r['Date'],
                              awaypoints=r['AwayPoints'],
                              homepoints=r['HomePoints'],
                              seasonyear=r['Season'],
                              comments=r['Comments'],
                              neutralsite=r['NeutralSite'])
    session.add(game)
session.commit()

# Check for games with unknown names - prompt user and create entries for those names
# Check for games without matches
# Check for games with score disagreements

# Upload games


