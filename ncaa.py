import requests
import datetime
import bs4
import pandas
import dateutil.parser as dateparser


def _get_date_page(season, division, date):
    """Returns the page for all teams in the given division/year as a requests
    Response object.
    
    season - an integer
    division - one of 'FBS', 'FCS', 'D2', or 'D3'
    date - a date"""
    url = 'http://stats.ncaa.org/contests/scoreboards'
    division_codes = {'FBS':11,'FCS':12,'D2':2,'D3':3}
    params = {'sport_code': 'MFB',
              'conf_id': -1,
              'academic_year': int(season) + 1,
              'division': division_codes[division],
              'game_date':datetime.date.strftime(date, "%m/%d/%Y")}
    return requests.get(url, params=params)

def _get_division_date_games(season, division, date):
    """Returns a pandas DataFrame of all games taking place in that season,
    for teams in that divison, on that date
    
    season - an integer
    division - one of 'FBS', 'FCS', 'D2', or 'D3'
    date - a date."""
    # Get the text from the response
    response = _get_date_page(season, division, date)
    page_text = response.text
    soup = bs4.BeautifulSoup(page_text,'lxml')
    # Get the table/tbody we need to search through.
    contentdiv = soup.find('div', id='contentarea')
    if contentdiv is None:
        return pandas.DataFrame([])
    gametable = contentdiv.find('table', recursive=False)
    if gametable is None:
        return pandas.DataFrame([])
    gametbody = gametable.find('tbody', recursive=False)
    # Go through the rows and parse each game
    rows = gametbody.find_all('tr', recursive=False)
    gamelist = []
    # Each game takes up 3 '<tr>'s: one for away, one for home, one as a separator
    for idx in range(0, len(rows), 3):
        gamelist.append(_parse_game(rows[idx:(idx+3)]))
    allgames = pandas.DataFrame(gamelist)
    allgames['Season'] = season
    return allgames

def _parse_game(tablerows):
    """Returns a dictionary corresponding to a single game.
    
    tablerows - a list of the <tr> elements in the table corresponding to this game"""
    gamedata = {}
    # Separate out <td> elements for home and away
    # The layout of the table is:
    # Date ; Away Picture ; Away Name ; Qtr Scores ; Away Final Score ; Site ; Attendance
    #        Home Picture ; Home Name ;              Home Final Score
    homerow = [x.get_text() for x in tablerows[1].find_all('td', recursive=False)]
    awayrow = [x.get_text() for x in tablerows[0].find_all('td', recursive=False)]
    # Date
    gamedata['Date'] = dateparser.parse(awayrow[0]).date()
    # Home and Away Teams
    gamedata['Home'] = homerow[1].strip()
    gamedata['Away'] = awayrow[2].strip()
    # Neutral site and comments
    if len(awayrow[5].strip()) > 0:
        gamedata['NeutralSite'] = 1
        gamedata['Comments'] = awayrow[5].strip()
    else:
        gamedata['NeutralSite'] = 0
        gamedata['Comments'] = None
    # Final Score
    if len(homerow[2].strip()) > 0:
        gamedata['HomePoints'] = int(homerow[2])
    else:
        gamedata['HomePoints'] = None
    if len(awayrow[4].strip()) > 0:
        gamedata['AwayPoints'] = int(awayrow[4])
    else:
        gamedata['AwayPoints'] = None
    # Return the dictionary of all game attributes
    return gamedata
    
    
def get_date_games(season, date, retries=3):
    """Returns a pandas DataFrame of all games taking place in that season, on that date
    
    season - an integer
    date - a date."""
    divlist = []
    for div in ['FBS', 'FCS', 'D2', 'D3']:
        for i in range(retries):
            try:
                divlist.append(_get_division_date_games(season, div, date))
                break
            except:
                continue
    allgames = pandas.concat(divlist)
    if len(allgames) > 0:
        allgames = allgames[allgames['Date'] == date]
        return allgames.drop_duplicates().reset_index(drop=True)
    else:
        return None

