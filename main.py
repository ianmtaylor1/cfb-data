import ncaa
import espn
import pandas
import sqlalchemy

# Which week do we want?
# Future update: prompt user and/or take command line arguments
season = 2018
week = 1


# Fetch the games from ESPN/NCAA
print("Getting ESPN games...")
espngames = espn.get_week_games(season, week)
print("Getting NCAA games...")
ncaagamelist = []
for d in list(set(espngames['Date'])):
    ncaagamelist.append(ncaa.get_date_games(season,d))
ncaagames = pandas.concat(ncaagamelist).reset_index(drop=True)


print("NCAA:",len(ncaagames)," ESPN:",len(espngames))