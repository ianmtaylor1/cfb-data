import ncaa
import espn
import model

import pandas
import sqlalchemy
import os
import sys

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
        espngames = pandas.read_csv(os.path.join(cachedir, espnfilename))
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
        ncaagames = pandas.read_csv(os.path.join(cachedir, ncaafilename))
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
espngames, ncaagames = get_games(season, week)





###################################################3

# Function to get appropriate id's from the database and add them to the data
def add_ids(games, datasource, dbcon):
    # Get everything we need from the database connection
    teams = pandas.read_sql_table('sourceteamname', dbcon)
    teams = teams[teams['datasource'] == datasource][['teamid', 'name']]
    seasons = pandas.read_sql_table('season', dbcon)[['id', 'start']]
    # Now merge, trim to the appropriate columns
    newgames = games.copy()
    cols = games.columns
    if ('teamid' in cols) or ('name' in cols) or ('id' in cols) or ('start' in cols):
        raise Exception('Overlapping column names.')
    # Home team
    newgames = newgames.merge(teams, how="left", left_on="Home", right_on="name", validate="m:1")
    newgames['HomeID'] = newgames['teamid']
    del newgames['teamid']
    del newgames['name']
    # Away team
    newgames = newgames.merge(teams, how="left", left_on="Away", right_on="name", validate="m:1")
    newgames['AwayID'] = newgames['teamid']
    del newgames['teamid']
    del newgames['name']
    # Season
    newgames = newgames.merge(seasons, how="left", left_on="Season", right_on="start", validate="m:1")
    newgames['SeasonID'] = newgames['id']
    del newgames['id']
    del newgames['start']
    # Return
    return newgames

# Function to merge the games together, matching any 
def match(espngames, ncaagames, dbcon):
    ncaagames = add_ids(ncaagames, 'ncaa.org', dbcon)
    espngames = add_ids(espngames, 'espn.com', dbcon)


# Map names to unique id's
print("Comparing...\n")

# Where is the sqlite db?
dbfile = 'sqlite:///cfb.sqlite3'

engine = sqlalchemy.create_engine(dbfile, echo=False)
conn = engine.connect()
ncaagames = add_ids(ncaagames, 'ncaa.org', conn)
espngames = add_ids(espngames, 'espn.com', conn)
conn.close()

# Are there any unknown teams?
print("Unknown NCAA Teams: ", end="")
if sum(ncaagames['HomeID'].isna()) + sum(ncaagames['AwayID'].isna()) > 0:
    homeunknown = list(ncaagames[ncaagames['HomeID'].isna()]['Home'])
    awayunknown = list(ncaagames[ncaagames['AwayID'].isna()]['Away'])
    unknown = list(set(homeunknown + awayunknown))
    print(", ".join(unknown), end=" ")
    ngames = sum(ncaagames['HomeID'].isna() | ncaagames['AwayID'].isna())
    print("(", ngames, " games affected)", sep="")
else:
    print("None.")
print("Unknown ESPN Teams: ", end="")
if sum(espngames['HomeID'].isna()) + sum(espngames['AwayID'].isna()) > 0:
    homeunknown = list(espngames[espngames['HomeID'].isna()]['Home'])
    awayunknown = list(espngames[espngames['AwayID'].isna()]['Away'])
    unknown = list(set(homeunknown + awayunknown))
    print(", ".join(unknown), end=" ")
    ngames = sum(espngames['HomeID'].isna() | espngames['AwayID'].isna())
    print("(", ngames, " games affected)", sep="")
else:
    print("None.")
print()

# Are there any unmatched NCAA/ESPN games?
regmerged = pandas.merge(ncaagames.reset_index().add_suffix('_ncaa'), espngames.reset_index().add_suffix('_espn'), 
                         how="inner", left_on=["HomeID_ncaa", "AwayID_ncaa", "Date_ncaa"], right_on=["HomeID_espn", "AwayID_espn", "Date_espn"],
                         validate="1:1"
                         )
regmerged['reversed'] = 0
nsmerged = pandas.merge(ncaagames[ncaagames['NeutralSite'] == 1].reset_index().add_suffix('_ncaa'), espngames.reset_index().add_suffix('_espn'),
                        how="inner", left_on=["HomeID_ncaa", "AwayID_ncaa", "Date_ncaa"], right_on=["AwayID_espn", "HomeID_espn", "Date_espn"],
                        validate="1:1"
                        )
nsmerged['reversed'] = 1
mergedgames = pandas.concat([regmerged, nsmerged], sort=True).reset_index(drop=True)
unmatchedncaaidx = [int(x) for x in ncaagames.index if x not in list(mergedgames['index_ncaa'])]
unmatchedespnidx = [int(x) for x in espngames.index if x not in list(mergedgames['index_espn'])]
print("Unmatched NCAA games:")
if len(unmatchedncaaidx) > 0:
    print(ncaagames.loc[unmatchedncaaidx,["Home", "Away", "Date"]])
else:
    print("None.")
print("\nUnmatched ESPN games:")
if len(unmatchedespnidx) > 0:
    print(espngames.loc[unmatchedespnidx,["Home", "Away", "Date"]])
else:
    print("None.")
print()

# Are there any matched games but with disagreement on score?
scoremismatchidx = ((mergedgames['reversed'] == 0) 
            & (
                (mergedgames['HomePoints_ncaa'] != mergedgames['HomePoints_espn']) 
                | (mergedgames['AwayPoints_ncaa'] != mergedgames['AwayPoints_espn'])
            )
        ) | ((mergedgames['reversed'] == 1) 
            & (
                (mergedgames['HomePoints_ncaa'] != mergedgames['AwayPoints_espn']) 
                | (mergedgames['AwayPoints_ncaa'] != mergedgames['HomePoints_espn'])
            )
        )
scoremismatchgames = mergedgames[scoremismatchidx]
print("Games with score disagreements:")
if len(scoremismatchgames) > 0:
    print(scoremismatchgames[['Date_espn','Home_espn','Away_espn','reversed','HomePoints_espn','AwayPoints_espn','HomePoints_ncaa','AwayPoints_ncaa']])
else:
    print("None.")
    
# Ask if the user wants to upload the non-problematic games.
print("\n\nDo you want to upload", len(mergedgames) - sum(scoremismatchidx), "games? (y/n) ", end="")
proceed = ""
while proceed not in ["y","n"]:
    proceed = str(input()).lower()
if proceed == "y":
    print("Here is where I would do the insert.")

