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

# Flush/Fill the match table in the given session
def create_matches(session):
    # Delete any existing matches
    for m in session.query(model.Match):
        session.delete(m)
    # Create new matches
    for row in session.execute(model.match_query):
        session.add(model.Match(espngameid=row['espngameid'], ncaagameid=row['ncaagameid']))
    # Commit the changes
    session.commit()

# Load games into temporary tables in model for analysis
def load_games(espndf, ncaadf, session):
    # ESPN games
    for r in espndf.to_dict('records'):
        game = model.TempESPNGame(away=r['Away'],
                                  home=r['Home'],
                                  date=r['Date'],
                                  awaypoints=r['AwayPoints'],
                                  homepoints=r['HomePoints'],
                                  seasonyear=r['Season'],
                                  comments=r['Comments'],
                                  overtimes=r['Overtimes'])
        session.add(game)
    # NCAA games
    for r in ncaadf.to_dict('records'):
        game = model.TempNCAAGame(away=r['Away'],
                                  home=r['Home'],
                                  date=r['Date'],
                                  awaypoints=r['AwayPoints'],
                                  homepoints=r['HomePoints'],
                                  seasonyear=r['Season'],
                                  comments=r['Comments'],
                                  neutralsite=r['NeutralSite'])
        session.add(game)
    # Commit these inserts
    session.commit()
    # Now create matches
    create_matches(session)
    # Query and return the results
    return session.query(model.TempESPNGame), session.query(model.TempNCAAGame)



# Get the games
print()
espndf, ncaadf = get_games(season, week, cachedir=cachedir, echo=True)

# Create engine/connection/session
engine = sqlalchemy.create_engine(dbfile)
dbcon = engine.connect()
model.Base.metadata.create_all(dbcon)  # create temp tables
session = model.Session(bind=dbcon)

# Load the games into the db
espngames, ncaagames = load_games(espndf, ncaadf, session)

# Check for games with unknown team names
def find_unknown_teams(games):
    unknown = set()
    for g in games:
        if g.hometeamlink is None:
            unknown.add(g.home)
        if g.awayteamlink is None:
            unknown.add(g.away)
    return list(unknown)

print()
print("Unknown ESPN Teams: ", end="")
unknown_espn_teams = find_unknown_teams(espngames)
if len(unknown_espn_teams) > 0:
    print(", ".join(unknown_espn_teams))
else:
    print("None.")
print("Unknown NCAA Teams: ", end="")
unknown_ncaa_teams = find_unknown_teams(ncaagames)
if len(unknown_ncaa_teams) > 0:
    print(", ".join(unknown_ncaa_teams))
else:
    print("None.")
    
if len(unknown_espn_teams) + len(unknown_ncaa_teams) > 0:
    # To Do: handle this better. Perhaps prompts?
    raise Exception('Please add entries to "sourceteamname" table for the above team(s).')

# Check for ESPN or NCAA games without matches or with more than one match
def get_mismatch(games):
    nomatch = []
    multimatch = []
    for g in games:
        if len(g.matches) == 0:
            nomatch.append(g)
        elif len(g.matches) > 1:
            multimatch.append(g)
    return nomatch, multimatch

def print_no_score(game):
    print("{}: {}, {} ('{}') vs {} ('{}')".format(
        game.id,
        game.date.strftime("%Y-%m-%d"),
        game.hometeamlink.team.shortname,
        game.home,
        game.awayteamlink.team.shortname,
        game.away
    ))
    
print()
espn_nomatch, espn_multi = get_mismatch(espngames)
print("ESPN Games without matches: ", end="")
if len(espn_nomatch) > 0:
    print()
    for g in espn_nomatch:
        print_no_score(g)
else:
    print("None.")
print("ESPN Games with more than one match: ", end="")
if len(espn_multi) > 0:
    print()
    for g in espn_multi:
        print_no_score(g)
else:
    print("None.")
ncaa_nomatch, ncaa_multi = get_mismatch(ncaagames)
print("NCAA Games without matches: ", end="")
if len(ncaa_nomatch) > 0:
    print()
    for g in ncaa_nomatch:
        print_no_score(g)
else:
    print("None.")
print("NCAA Games with more than one match: ", end="")
if len(ncaa_multi) > 0:
    print()
    for g in ncaa_multi:
        print_no_score(g)
else:
    print("None.")

# Check for games with score disagreements
def check_scores_same(espngame, ncaagame):
    # If home matches home, and away matches away...
    if ((espngame.hometeamlink.teamid == ncaagame.hometeamlink.teamid)
            and (espngame.awayteamlink.teamid == ncaagame.awayteamlink.teamid)):
        if ((espngame.homepoints == ncaagame.homepoints) 
                and (espngame.awaypoints == ncaagame.awaypoints)):
            return True
        else:
            return False
    # If home matches away, and away matches home (i.e. neutral site)...
    elif ((espngame.hometeamlink.teamid == ncaagame.awayteamlink.teamid)
            and (espngame.awayteamlink.teamid == ncaagame.hometeamlink.teamid)):
        if ((espngame.homepoints == ncaagame.awaypoints) 
                and (espngame.awaypoints == ncaagame.homepoints)):
            return True
        else:
            return False
    # Otherwise some error happened
    else:
        raise Exception('check_scores_same received unmatched games')

def print_with_score(game):
    print("{}: {}, {} ('{}') vs {} ('{}'): {}-{}".format(
        game.id,
        game.date.strftime("%Y-%m-%d"),
        game.hometeamlink.team.shortname,
        game.home,
        game.awayteamlink.team.shortname,
        game.away,
        game.homepoints,
        game.awaypoints
    ))
    
print()
print("Matched games with score disagreements: ", end="")
numdisagreements = 0
for m in session.query(model.Match):
    if not check_scores_same(m.espngame, m.ncaagame):
        if numdisagreements == 0:
            print()
        numdisagreements += 1
        print("ESPN ", end="")
        print_with_score(m.espngame)
        print("NCAA ", end="")
        print_with_score(m.ncaagame)
if numdisagreements == 0:
    print("None.")

# Upload games if desired.
print("\n")
proceed = str(input("Upload all matched games? (y/n) ")).lower()
while proceed not in ['y','n']:
    proceed = str(input("Upload all matched games? (y/n) ")).lower()

if proceed == 'y':
    print("Uploading games...")
    for m in session.query(model.Match):
        if not check_scores_same(m.espngame, m.ncaagame):
            # Skip score mismatches
            continue
        # Sort out combining comments
        comments = None
        if (m.espngame.comments is not None) and (m.ncaagame.comments is not None):
            comments = m.espngame.comments + ', ' + m.ncaagame.comments
        elif m.espngame.comments is not None:
            comments = m.espngame.comments
        elif m.ncaagame.comments is not None:
            comments = m.ncaagame.comments
        # Create game, result, add to session
        game = model.Game(date=m.espngame.date, seasonid=m.espngame.season.id,
                          hometeamid=m.espngame.hometeamlink.teamid,
                          awayteamid=m.espngame.awayteamlink.teamid,
                          neutralsite=m.ncaagame.neutralsite, comments=comments)
        result = model.GameResult(homepoints = m.espngame.homepoints,
                                  awaypoints = m.espngame.awaypoints,
                                  overtimes = m.espngame.overtimes)
        result.game = game
        session.add(game)
        session.add(result)
        # Delete the temporary rows we used to make this game
        session.delete(m.espngame)
        session.delete(m.ncaagame)
        session.delete(m)
    session.commit()


