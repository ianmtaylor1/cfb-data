import ncaa
import espn
import pandas
import sqlalchemy

# Which week do we want?
# Future update: prompt user and/or take command line arguments
season = 2018
week = 2

# Where is the sqlite db?
dbfile = 'sqlite:///cfb.sqlite3'

# Fetch the games from ESPN/NCAA
print("Getting ESPN games...")
espngames = espn.get_week_games(season, week)
print("Getting NCAA games...")
ncaagamelist = []
for d in list(set(espngames['Date'])):
    ncaagamelist.append(ncaa.get_date_games(season,d))
ncaagames = pandas.concat(ncaagamelist).reset_index(drop=True)

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
    
# Map names to unique id's
print("Comparing...\n")

engine = sqlalchemy.create_engine(dbfile, echo=False)
conn = engine.connect()
ncaagames = add_ids(ncaagames, 'ncaa.org', conn)
espngames = add_ids(espngames, 'espn.com', conn)
conn.close()

# Are there any unknown teams?
if sum(ncaagames['HomeID'].isna()) + sum(ncaagames['AwayID'].isna()) > 0:
    print("Unknown NCAA Teams: ", end="")
    homeunknown = list(ncaagames[ncaagames['HomeID'].isna()]['Home'])
    awayunknown = list(ncaagames[ncaagames['AwayID'].isna()]['Away'])
    unknown = list(set(homeunknown + awayunknown))
    print(", ".join(unknown), end=" ")
    ngames = sum(ncaagames['HomeID'].isna() | ncaagames['AwayID'].isna())
    print("(", ngames, " games affected)", sep="")
if sum(espngames['HomeID'].isna()) + sum(espngames['AwayID'].isna()) > 0:
    print("Unknown ESPN Teams: ", end="")
    homeunknown = list(espngames[espngames['HomeID'].isna()]['Home'])
    awayunknown = list(espngames[espngames['AwayID'].isna()]['Away'])
    unknown = list(set(homeunknown + awayunknown))
    print(", ".join(unknown), end=" ")
    ngames = sum(espngames['HomeID'].isna() | espngames['AwayID'].isna())
    print("(", ngames, " games affected)", sep="")
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
print("Do you want to upload", len(mergedgames) - sum(scoremismatchidx), "games? (y/n) ", end="")
proceed = ""
while proceed not in ["y","n"]:
    proceed = str(input()).lower()
if proceed == "n":
    quit()

# Do the upload
engine = sqlalchemy.create_engine(dbfile, echo=False)
gameinsert = sqlalchemy.text("insert into game (date, seasonid, hometeamid, awayteamid, neutralsite, comments) values (:date, :seasonid, :hometeamid, :awayteamid, :neutralsite, :comments)")
resultinsert = sqlalchemy.text("insert ito gameresult (id, homepoints, awaypoints, overtimes) values (:id, :homepoints, :awaypoints, :overtimes)")
conn = engine.connect()
for i in mergedgames[~scoremismatchidx].index:
    # Insert the game
    comments = None
    if (mergedgames.loc[i,'Comments_espn'] is not None) and (mergedgames.loc[i,'Comments_ncaa'] is not None):
        comments = mergedgames.loc[i,'Comments_espn'] + ", " + mergedgames.loc[i,'Comments_ncaa']
    elif mergedgames.loc[i,'Comments_espn'] is not None:
        comments = mergedgames.loc[i,'Comments_espn']
    elif mergedgames.loc[i,'Comments_ncaa'] is not None:
        comments = mergedgames.loc[i,'Comments_ncaa']
    gamevalues = {
        'date': mergedgames.loc[i,'Date_espn'],
        'seasonid': mergedgames.loc[i,'SeasonID_espn'],
        'hometeamid': mergedgames.loc[i,'HomeID_espn'],
        'awayteamid': mergedgames.loc[i,'AwayID_espn'],
        'neutralsite': mergedgames.loc[i,'NeutralSite_ncaa'],
        'commets': comments
    }
    qresult = conn.execute(gameinsert, **gamevalues)
    # Insert the result
    resultvalues = {
        'id': qresult.inserted_primary_key[0],
        'homepoints': mergedgames.loc[i,'HomePoints_espn'],
        'awaypoints': mergedgames.loc[i,'AwayPoints_espn'],
        'overtimes': mergedgames.loc[i,'Overtimes_espn']
    }
    conn.execute(resultinsert, **resultvalues)
    
conn.close()

print("Done.")