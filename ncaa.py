import requests
import datetime
import bs4
import re
import urllib
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
    gametbody = contentdiv.find('table', recursive=False).find('tbody', recursive=False) # Do this better? with error checking?
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
    gamedata['HomePoints'] = int(homerow[2])
    gamedata['AwayPoints'] = int(awayrow[4])
    
    return gamedata
    
    
def get_date_games(season, date):
    """Returns a pandas DataFrame of all games taking place in that season, on that date
    
    season - an integer
    date - a date."""
    divlist = []
    for div in ['FBS', 'FCS', 'D2', 'D3']:
        divlist.append(_get_division_date_games(season, div, date))
    allgames = pandas.concat(divlist)
    return allgames.drop_duplicates()

games = get_date_games(2018, datetime.date(2018,9,8))

#####################################################
#####################################################
    
    
    
def _get_team_urls(season,division):
    """Returns a pandas DataFrame of all teams and divisions each year, and
    a URL where to find the games for that team/year.
    
    season - an integer
    division - one of 'FBS', 'FCS', 'D2', or 'D3'"""
    # Get the text and url from the response
    response = _get_season_page(season,division)
    page_text = response.text
    soup = bs4.BeautifulSoup(page_text,'lxml')
    page_url = response.url
    # Create the dataframe of all teams in this division this year
    team_links = soup.find_all('a',href=re.compile('^/team/\d+'))
    teamnames = list(map(lambda x: x.get_text().strip(), team_links))
    teamurls = list(map(lambda x: urllib.parse.urljoin(page_url,x['href']), team_links))
    return pandas.DataFrame({'Team':teamnames,'Season':season,'Division':division,'URL':teamurls})
    # Crawl each team link and extract season data
    #gameslist = []
    #for tl in team_links:
    #    print('{} - {} - '.format(season,tl.get_text()),end='')
    #    linkurl = urllib.parse.urljoin(page_url,tl['href'])
    #    teamgames = _get_team_games(linkurl)
    #    teamgames['Team'] = tl.get_text()
    #    gameslist.append(teamgames)
    #    print('{} games'.format(len(teamgames)))
    #if len(gameslist) == 0:
    #    all_games = pandas.concat(gameslist)
    #else:
    #    all_games = pandas.DataFrame()
    #return teams, all_games.reset_index()[all_games.columns]
        
def _get_team_games(url):
    """Loads the page at url and returns a pandas DataFrame of games found there.
    The dataframe will be missing some information - most notably, the name of
    the team whose page this is. That will be added on by the calling function."""
    # What does an "empty" table look like?
    emptydf = pandas.DataFrame({'Date':[],'HomeAway':[],'Site':[],'Opponent':[],
                                'Result':[],'TeamPts':[],'OppPts':[],'Overtimes':[]})
    response = requests.get(url)
    page_text = response.text
    soup = bs4.BeautifulSoup(page_text,'lxml')
    # Find the appropriate table
    games_table = None
    table_candidates = soup.find_all('table',class_='mytable')
    for table in table_candidates:
        heading = table.find('tr',class_='heading')
        if 'Schedule/Results' in heading.get_text():
            games_table = table
            break
    else:
        # No games found on this page
        return emptydf
    # Now search our table for games
    rows = games_table.find_all('tr')
    if len(rows) < 3:
        # No games found on this page
        return emptydf
    games = []
    for r in rows[2:]: #ignore headers
        data = {}
        cells = r.find_all('td')
        # Check if there's enough data
        if len(cells) < 3:
            continue
        # Date in the first cell
        datestring = cells[0].get_text().strip()
        data['Date'] = datetime.datetime.strptime(datestring,'%m/%d/%Y').date()
        # Opponent and location in second cell
        oppstring = cells[1].get_text().strip()
        oppfilter = re.compile('^(@)?\s*(.+?)\s*(@.+?)?$')
        oppmatch = oppfilter.match(oppstring).groups()
        if oppmatch[0] is None:
            data['HomeAway'] = 'vs.'
        else:
            data['HomeAway'] = '@'
        data['Opponent'] = oppmatch[1]
        if oppmatch[2] is None:
            data['Site'] = ''
        else:
            data['Site'] = oppmatch[2]
        # Score and result in third cell
        scorestring = cells[2].get_text().strip()
        scorefilter = re.compile('^([WLTD])\s*(\d+)\s*-\s*(\d+)(?:\s*\((\d+)OT\))?')
        scorematch = scorefilter.match(scorestring)
        if scorematch is None:
            data['Result'] = None
            data['TeamPts'] = None
            data['OppPts'] = None
            data['Overtimes'] = None
        else:
            data['Result'] = scorematch.group(1)
            data['TeamPts'] = int(scorematch.group(2))
            data['OppPts'] = int(scorematch.group(3))
            data['Overtimes'] = 0 if scorematch.group(4) is None else int(scorematch.group(4))
        # Add this game to our list
        games.append(data)
    return pandas.DataFrame(games)