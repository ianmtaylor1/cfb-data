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

# How many games are already in the database?
print("Games already existing for these dates:",
        session.query(model.Game).filter(model.Game.date.in_({g.date for g in ncaagames})).count(),
        "games.")

################################################################################

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
# Display unknown ESPN Teams
print("Unknown ESPN Teams: ", end="")
unknown_espn_teams = find_unknown_teams(espngames)
if len(unknown_espn_teams) > 0:
    print(", ".join(unknown_espn_teams))
else:
    print("None.")
# Loop through them and prompt for teamids
for t in unknown_espn_teams:
    team = None
    while team is None:
        id = input("Enter team id for " + t + ": ")
        team = session.query(model.Team).filter(model.Team.id == id).one_or_none()
        print(team)
        if input('Correct? (y/n) ').lower() != 'y':
            team = None
    session.add(model.SourceTeamName(datasource='espn.com', name=t, teamid=id))
session.commit()
    
# Display unknown NCAA Teams
print("Unknown NCAA Teams: ", end="")
unknown_ncaa_teams = find_unknown_teams(ncaagames)
if len(unknown_ncaa_teams) > 0:
    print(", ".join(unknown_ncaa_teams))
else:
    print("None.")
# Loop through them and prompt for teamids
for t in unknown_ncaa_teams:
    team = None
    while team is None:
        id = input("Enter team id for " + t + ": ")
        team = session.query(model.Team).filter(model.Team.id == id).one_or_none()
        print(team)
        if input('Correct? (y/n) ').lower() != 'y':
            team = None
    session.add(model.SourceTeamName(datasource='ncaa.org', name=t, teamid=id))
session.commit()
    
# Create new matches and look for unknown teams again
create_matches(session)
unknown_espn_teams = find_unknown_teams(espngames)
unknown_ncaa_teams = find_unknown_teams(ncaagames)
if len(unknown_espn_teams) + len(unknown_ncaa_teams) > 0:
    # To Do: handle this better. Perhaps prompts?
    raise Exception('Please add entries to "sourceteamname" table for the above team(s).')

################################################################################

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

################################################################################    

# Create a game (model.Game and model.GameResult) from a matched pair
def game_from_match(m):
    # Sort out combining comments
    comments = None
    if (m.espngame.comments is not None) and (m.ncaagame.comments is not None):
        comments = m.espngame.comments + ', ' + m.ncaagame.comments
    elif m.espngame.comments is not None:
        comments = m.espngame.comments
    elif m.ncaagame.comments is not None:
        comments = m.ncaagame.comments
    # Create game, result
    game = model.Game(date=m.espngame.date, seasonid=m.espngame.season.id,
                      hometeamid=m.espngame.hometeamlink.teamid,
                      awayteamid=m.espngame.awayteamlink.teamid,
                      neutralsite=m.ncaagame.neutralsite, comments=comments)
    if check_scores_same(m.espngame, m.ncaagame):
        result = model.GameResult(homepoints = m.espngame.homepoints,
                                  awaypoints = m.espngame.awaypoints,
                                  overtimes = m.espngame.overtimes)
    else:
        print("Score needed for ", end="")
        print_no_score(m.espngame)
        homepoints = input('Home team points: ')
        awaypoints = input('Away team points: ')
        overtimes = input('Overtimes: ')
        result = model.GameResult(homepoints = homepoints,
                                  awaypoints = awaypoints,
                                  overtimes = overtimes)
    result.game = game
    return game, result

# Check if a model.Game already exists in the database before adding it to the session
def game_is_duplicate(testgame, session):
    potentialmatches = session.query(model.Game).filter(
        sqlalchemy.or_(
            sqlalchemy.and_(
                model.Game.hometeamid == testgame.hometeamid, 
                model.Game.awayteamid == testgame.awayteamid,
                model.Game.date == testgame.date),
            sqlalchemy.and_(
                model.Game.hometeamid == testgame.awayteamid, 
                model.Game.awayteamid == testgame.hometeamid,
                model.Game.date == testgame.date,
                testgame.neutralsite == True),
        )
    )
    n = potentialmatches.count()
    if n == 0:
        return False
    elif n == 1:
        return True
    else:
        raise Exception('Duplicate games in database.')

# Upload games if desired.
print("\n")
proceed = str(input("Upload all matched games? (y/n) ")).lower()
while proceed not in ['y','n']:
    proceed = str(input("Upload all matched games? (y/n) ")).lower()

if proceed == 'y':
    print("Uploading games...")
    inserted = 0
    duplicates = 0
    for m in session.query(model.Match):
        game, result = game_from_match(m)
        if not game_is_duplicate(game, session):
            inserted += 1
            session.add(game)
            session.add(result)
        else:
            duplicates += 1
        # Delete the temporary rows we used to make this game
        session.delete(m.espngame)
        session.delete(m.ncaagame)
        session.delete(m)
    session.commit()
    print('Newly inserted:', inserted)
    print('Duplicates not inserted:', duplicates)

################################################################################

# Create a game from an unmatched ESPN game, prompting for info as necessary
def game_from_espn(espn, neutralsite=None, comment='Missing from NCAA.org'):
    # Create full comment
    if espn.comments is not None:
        comment = espn.comments + ', ' + comment
    # Prompt user for neutralsite, if necessary
    while neutralsite is None:
        neutralsite = input('Neutral Site: (y/n) ').lower()
        if neutralsite in ['y','n']:
            neutralsite = (neutralsite == 'y')
        else:
            neutralsite = None
    # Create game, result
    game = model.Game(date=espn.date, seasonid=espn.season.id,
                      hometeamid=espn.hometeamlink.teamid,
                      awayteamid=espn.awayteamlink.teamid,
                      neutralsite=neutralsite, comments=comment)
    result = model.GameResult(homepoints = espn.homepoints,
                              awaypoints = espn.awaypoints,
                              overtimes = espn.overtimes)
    result.game = game
    return game, result

# Create a game from an unmatched NCAA game, prompting for info as necessary
def game_from_ncaa(ncaa, overtimes=None, comment='Missing from ESPN.com'):
    # Create full comment
    if ncaa.comments is not None:
        comment = ncaa.comments + ', ' + comment
    # Prompt user for overtimes, if necessary
    while overtimes is None:
        overtimes = input('Overtimes: ')
        try:
            overtimes = int(overtimes)
        except:
            overtimes = None
    # Create game, result
    game = model.Game(date=ncaa.date, seasonid=ncaa.season.id,
                      hometeamid=ncaa.hometeamlink.teamid,
                      awayteamid=ncaa.awayteamlink.teamid,
                      neutralsite=ncaa.neutralsite, comments=comment)
    result = model.GameResult(homepoints = ncaa.homepoints,
                              awaypoints = ncaa.awaypoints,
                              overtimes = overtimes)
    result.game = game
    return game, result

# Upload ESPN games if desired, prompting for each.
print("\n")
proceed = str(input("Upload unmatched ESPN games? (y/n) ")).lower()
while proceed not in ['y','n']:
    proceed = str(input("Upload unmatched ESPN games? (y/n) ")).lower()

if proceed == 'y':
    inserted = 0
    duplicates = 0
    for espn in espngames:
        print_with_score(espn)
        creategame = 'x'
        while creategame not in ['y','n']:
            creategame = str(input("Create this game? (y/n) ")).lower()
        if creategame == 'y':
            game, result = game_from_espn(espn)
            if not game_is_duplicate(game, session):
                inserted += 1
                session.add(game)
                session.add(result)
            else:
                duplicates += 1
            # Delete the temporary rows we used to make this game
            session.delete(espn)
    session.commit()
    print('Newly inserted:', inserted)
    print('Duplicates not inserted:', duplicates)
    
# Upload NCAA games if desired, prompting for each.
print("\n")
proceed = str(input("Upload unmatched NCAA games? (y/n) ")).lower()
while proceed not in ['y','n']:
    proceed = str(input("Upload unmatched NCAA games? (y/n) ")).lower()

if proceed == 'y':
    inserted = 0
    duplicates = 0
    for ncaa in ncaagames:
        print_with_score(ncaa)
        creategame = 'x'
        while creategame not in ['y','n']:
            creategame = str(input("Create this game? (y/n) ")).lower()
        if creategame == 'y':
            game, result = game_from_ncaa(ncaa)
            if not game_is_duplicate(game, session):
                inserted += 1
                session.add(game)
                session.add(result)
            else:
                duplicates += 1
            # Delete the temporary rows we used to make this game
            session.delete(ncaa)
    session.commit()
    print('Newly inserted:', inserted)
    print('Duplicates not inserted:', duplicates)