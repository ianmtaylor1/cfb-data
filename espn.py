from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import datetime
import dateutil.parser as dateparser
import pandas
import contextlib
import time

# Like a different version of contextlib.closing
@contextlib.contextmanager
def _quitting(thing):
    try:
        yield thing
    finally:
        thing.quit()

def _get_division_week_games(season,division,week,waittime):
    """Returns a pandas dataframe of the games from the ESPN page for the given parameters.
    
    season - a year number
    division - one of 'FBS','FCS', or 'D2D3'
    week - a number (1-15), 'Bowl' for bowl weeks and 'A' for all-star weeks.
    waittime - number of seconds to wait for the page to load
    """
    # What URL do we need to get?
    base_url = 'http://www.espn.com/college-football/scoreboard/_/group/{group}/year/{year}/seasontype/{seasontype}/week/{week}'
    if week == 'Bowl': # "Bowls" week
        seasontype,weeknum = 3,1
    elif week == 'A': # "All-star" week
        seasontype,weeknum = 4,1
    else:
        seasontype,weeknum = 2,week
    group_codes = {'FBS':80,'FCS':81,'D2D3':35}
    url = base_url.format(group=group_codes[division],year=season,
                          seasontype=seasontype,week=weeknum)
    # Get the games from that URL                      
    games = None
    with _quitting(webdriver.PhantomJS()) as driver:
        driver.get(url)
        # Wait for a bit so that dynamic things can load
        if _wait_for_load(driver,waittime,2,10):
            # Parse the page for game data
            games = _scrub_week_page(driver)
    # Clean up the data, add extra stuff, return it
    if games is not None:
        def fixdate(d):
            newyear = season if d.month > 1 else (season + 1)
            return datetime.date(newyear,d.month,d.day)
        games['Date'] = games['Date'].map(fixdate)
        games['Season'] = season
    else:
        games = pandas.DataFrame([])
    return games

def _wait_for_load(driver,waittime,poll,maxattempts):
    """Waits for the weekly games page to load and then returns a boolean value
    indicating whether it loaded. Waits waittime seconds for game tables to
    appear, then polls every poll seconds until the count stabilizes or we've
    polled maxattempts times."""
    articles = "//div[@id='events']/article[contains(@class,'scoreboard')]"
    # The initial wait
    wait = WebDriverWait(driver,waittime)
    try:
        elements = wait.until(EC.presence_of_all_elements_located((By.XPATH,articles)))
    except TimeoutException as e:
        return False
    # The poll
    count = len(elements)
    for attempt in range(maxattempts):
        time.sleep(poll)
        prevcount = count
        try:
            count = len(driver.find_elements_by_xpath(articles))
        except NoSuchElementException as e:
            count = 0
        if count==prevcount:
            break
    else:
        return False
    # We broke out, we stabilized
    return True

def _scrub_week_page(driver):
    """Returns a pandas dataframe of all games shown on the given ESPN scoreboard
    page. This is only being called if some games exist on the page."""
    # Find the events div
    events_xpath = '//div[@id="events"]/*'
    current_date = None
    games = []
    events_children = driver.find_elements_by_xpath(events_xpath)
    for child in events_children:
        if child.tag_name == 'h2':
            # Look for date headers
            current_date = dateparser.parse(child.text).date()
        elif child.tag_name == 'article':
            game = _parse_game(child)
            if game is not None:
                game['Date'] = current_date
                games.append(game)
    return pandas.DataFrame(games)
        
def _parse_game(table):
    """Returns a dictionary of game attributes after being passed an element
    on ESPN's scoreboard page that contains them. (Usually an <article> tag."""
    data = {}
    gametime_xpath =  './/th[contains(@class,"date-time")]'
    awayteam_xpath =  './/tr[contains(@class,"away")]//span[contains(@class,"sb-team-short")]'
    awayscore_xpath = './/tr[contains(@class,"away")]/td[contains(@class,"total")]/span'
    hometeam_xpath =  './/tr[contains(@class,"home")]//span[contains(@class,"sb-team-short")]'
    homescore_xpath = './/tr[contains(@class,"home")]/td[contains(@class,"total")]/span'
    comment_xpath =   './/section[contains(@class,"sb-notes")]'
    # Make sure the game has ended.
    gametime = table.find_element_by_xpath(gametime_xpath).text.strip().upper()
    if 'FINAL' not in gametime:
        return None
    # Away team attributes
    data['Away'] = table.find_element_by_xpath(awayteam_xpath).text.strip()
    data['AwayPts'] = int(table.find_element_by_xpath(awayscore_xpath).text.strip())
    # Home team attributes
    data['Home'] = table.find_element_by_xpath(hometeam_xpath).text.strip()
    data['HomePts'] = int(table.find_element_by_xpath(homescore_xpath).text.strip())
    # Winner
    if data['HomePts'] > data['AwayPts']:
        data['Winner'] = data['Home']
    elif data['AwayPts'] > data['HomePts']:
        data['Winner'] = data['Away']
    else:
        data['Winnner'] = ''
    # Overtime(s)
    if 'OT' in gametime:
        if (gametime.find('/') + 1 == gametime.find('OT')):
            data['Overtimes'] = 1
        else:
            data['Overtimes'] = int(gametime[(gametime.find('/')+1):gametime.find('OT')])
    else:
        data['Overtimes'] = 0
    # Game Comments
    comments = table.find_elements_by_xpath(comment_xpath)
    if (len(comments) > 0):
        data['Comments'] = comments[0].text.strip()
    else:
        data['Comments'] = None
    return data

    
def get_week_games(season, week, waittime=30):
    """Returns a pandas dataframe of the games from the ESPN page for the given parameters.
    
    season - a year number
    week - a number (1-15), 'Bowl' for bowl weeks and 'A' for all-star weeks.
    waittime - number of seconds to wait for the page to load
    """
    gameslist = []
    for div in ['FBS', 'FCS', 'D2D3']:
        divgames = _get_division_week_games(season, div, week, waittime)
        gameslist.append(divgames)
    allgames = pandas.concat(gameslist)
    return allgames.drop_duplicates()