from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, ForeignKey, UniqueConstraint
from sqlalchemy.types import Integer, String, Date, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select, and_, or_

from view import View

Base = declarative_base()
Session = sessionmaker()

class Team(Base):
    __tablename__ = 'team'
    
    id = Column(Integer, primary_key=True)
    shortname = Column(String, nullable=False, unique=True)
    longname = Column(String, unique=True)
    mascot = Column(String)
    
    homegames = relationship("Game", foreign_keys="Game.hometeamid", back_populates="hometeam")
    awaygames = relationship("Game", foreign_keys="Game.awayteamid", back_populates="awayteam")
    
    def __repr__(self):
        return "<Team(shortname='{}', longname='{}', mascot='{}')>".format(
                self.shortname, self.longname, self.mascot)


class Conference(Base):
    __tablename__ = 'conference'
    
    id = Column(Integer, primary_key=True)
    shortname = Column(String, nullable=False, unique=True)
    longname = Column(String, unique=True)
    
    def __repr__(self):
        return "<Conference(shortname='{}', longname='{}')>".format(
                self.shortname, self.longname)


class Division(Base):
    __tablename__ = 'division'
    
    id = Column(Integer, primary_key=True)
    shortname = Column(String, nullable=False, unique=True)
    longname = Column(String, unique=True)
    
    def __repr__(self):
        return "<Division(shortname='{}', longname='{}')>".format(
                self.shortname, self.longname)


class Season(Base):
    __tablename__ = 'season'
    
    id = Column(Integer, primary_key=True)
    name = Column(String)
    start = Column(Integer, nullable=False)
    end = Column(Integer)
    
    games = relationship("Game", back_populates="season")
    
    def __repr__(self):
        return "<Season(name='{}', start='{}', end='{}')>".format(
                self.name, self.start, self.end)


class Game(Base):
    __tablename__ = 'game'
    __table_args__ = (
            UniqueConstraint('date', 'hometeamid', 'awayteamid'),
            )
    
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    seasonid = Column(Integer, ForeignKey('season.id'))
    hometeamid = Column(Integer, ForeignKey('team.id'), nullable=False)
    awayteamid = Column(Integer, ForeignKey('team.id'), nullable=False)
    neutralsite = Column(Boolean, nullable=False, default=False)
    comments = Column(String)
    
    season = relationship("Season", back_populates="games")
    hometeam = relationship("Team", foreign_keys=[hometeamid], back_populates="homegames")
    awayteam = relationship("Team", foreign_keys=[awayteamid], back_populates="awaygames")
    result = relationship("GameResult", back_populates="game")
    
    def __repr__(self):
        return "<Game(date='{}', seasonid='{}', hometeamid='{}', awayteamid='{}', neutralsite='{}', comments='{}')>".format(
                self.date, self.seasonid, self.hometeamid, self.awayteamid, self.neutralsite, self.comments)


class GameResult(Base):
    __tablename__ = 'gameresult'
    
    id = Column(Integer, ForeignKey('game.id'), primary_key=True)
    homepoints = Column(Integer, nullable=False)
    awaypoints = Column(Integer, nullable=False)
    overtimes = Column(Integer, nullable=False, default=0)
    comments = Column(String)
    
    game = relationship("Game", back_populates="result")
    
    def __repr__(self):
        return "<GameResult(homepoints='{}', awaypoints='{}', overtimes='{}', comments='{}')>".format(
                self.homepoints, self.awaypoints, self.overtimes, self.comments)


class TeamDivision(Base):
    __tablename__ = 'teamdivision'
    
    teamid = Column(Integer, ForeignKey("team.id"), primary_key=True, nullable=False)
    divisionid = Column(Integer, ForeignKey("division.id"), nullable=False)
    seasonid = Column(Integer, ForeignKey("season.id"), primary_key=True, nullable=False)
    
    team = relationship("Team")
    division = relationship("Division")
    season = relationship("Season")
    

class TeamConference(Base):
    __tablename__ = 'teamconference'
    
    teamid = Column(Integer, ForeignKey("team.id"), primary_key=True, nullable=False)
    conferenceid = Column(Integer, ForeignKey("conference.id"), nullable=False)
    seasonid = Column(Integer, ForeignKey("season.id"), primary_key=True, nullable=False)
    
    team = relationship("Team")
    conference = relationship("Conference")
    season = relationship("Season")
    

class SourceTeamName(Base):
    __tablename__ = 'sourceteamname'
    
    datasource = Column(String, primary_key=True, nullable=False)
    teamid = Column(Integer, ForeignKey("team.id"), nullable=False)
    name = Column(String, primary_key=True, nullable=False)
    
    team = relationship("Team")

################################################################################    

class TempESPNGame(Base):
    __tablename__ = 'espngame'
    __table_args__ = {'prefixes':['TEMPORARY']}
    
    id = Column(Integer, primary_key=True)
    away = Column(String, nullable=False)
    awaypoints = Column(Integer, nullable=False)
    home = Column(String, nullable=False)
    homepoints = Column(Integer, nullable=False)
    date = Column(Date, nullable=False)
    comments = Column(String)
    seasonyear = Column(Integer, nullable=False)
    overtimes = Column(Integer, nullable=False)
    
    # Relationships - joins have to be explicit
    season = relationship("Season", primaryjoin="foreign(TempESPNGame.seasonyear)==remote(Season.start)")
    hometeamlink = relationship("SourceTeamName",
                                primaryjoin="and_(foreign(TempESPNGame.home) == remote(SourceTeamName.name), "
                                            "remote(SourceTeamName.datasource)=='espn.com')"
                                )
    awayteamlink = relationship("SourceTeamName", 
                                primaryjoin="and_(foreign(TempESPNGame.away) == remote(SourceTeamName.name), "
                                            "remote(SourceTeamName.datasource)=='espn.com')"
                                )
    

class TempNCAAGame(Base):
    __tablename__ = 'ncaagame'
    __table_args__ = {'prefixes':['TEMPORARY']}
    
    id = Column(Integer, primary_key=True)
    away = Column(String, nullable=False)
    awaypoints = Column(Integer, nullable=False)
    home = Column(String, nullable=False)
    homepoints = Column(Integer, nullable=False)
    date = Column(Date, nullable=False)
    comments = Column(String)
    seasonyear = Column(Integer, nullable=False)
    neutralsite = Column(Boolean, nullable=False)
    
    # Relationships - joins have to be explicit
    season = relationship("Season", primaryjoin="foreign(TempNCAAGame.seasonyear)==remote(Season.start)")
    hometeamlink = relationship("SourceTeamName",
                                primaryjoin="and_(foreign(TempNCAAGame.home) == remote(SourceTeamName.name), "
                                            "remote(SourceTeamName.datasource)=='ncaa.org')"
                                )
    awayteamlink = relationship("SourceTeamName", 
                                primaryjoin="and_(foreign(TempNCAAGame.away) == remote(SourceTeamName.name), "
                                            "remote(SourceTeamName.datasource)=='ncaa.org')"
                                )


# Create the secondary view for linking temporary NCAA and ESPN games
# First build up the select query
_ncaagame = TempNCAAGame.__table__
_espngame = TempESPNGame.__table__
_homename = SourceTeamName.__table__.alias()
_awayname = SourceTeamName.__table__.alias()
_ncaa_ids = select(
                    [_ncaagame, 
                    _awayname.c.teamid.label('awayteamid'), 
                    _homename.c.teamid.label('hometeamid')]
                  ).select_from(
                    _ncaagame.outerjoin(_homename, 
                        and_(_ncaagame.c.home==_homename.c.name, _homename.c.datasource=='ncaa.org')
                    ).outerjoin(_awayname,
                        and_(_ncaagame.c.away==_awayname.c.name, _awayname.c.datasource=='ncaa.org')
                    )
                  ).alias()
_espn_ids = select(
                    [_espngame, 
                    _awayname.c.teamid.label('awayteamid'), 
                    _homename.c.teamid.label('hometeamid')]
                  ).select_from(
                    _espngame.outerjoin(_homename, 
                        and_(_espngame.c.home==_homename.c.name, _homename.c.datasource=='espn.com')
                    ).outerjoin(_awayname,
                        and_(_espngame.c.away==_awayname.c.name, _awayname.c.datasource=='espn.com')
                    )
                  ).alias()
_matched_ids = select(
                        [_espn_ids.c.id.label('espngameid'), _ncaa_ids.c.id.label('ncaagameid')]
                     ).select_from(
                        _espn_ids.join(_ncaa_ids, 
                            or_(
                                and_(
                                    _espn_ids.c.hometeamid == _ncaa_ids.c.hometeamid,
                                    _espn_ids.c.awayteamid == _ncaa_ids.c.awayteamid,
                                    _espn_ids.c.date == _ncaa_ids.c.date
                                ),
                                and_(
                                    _espn_ids.c.hometeamid == _ncaa_ids.c.awayteamid,
                                    _espn_ids.c.awayteamid == _ncaa_ids.c.hometeamid,
                                    _espn_ids.c.date == _ncaa_ids.c.date,
                                    _ncaa_ids.c.neutralsite == True
                                )
                            )
                        )
                     )

# Now make the view
matches = View('matches', Base.metadata, _matched_ids, prefixes=['TEMPORARY'])
class Match(Base):
    __table__ = matches
    
    espngame = relationship("TempESPNGame", primaryjoin='foreign(Match.espngameid) == remote(TempESPNGame.id)',
                            backref='matches')
    ncaagame = relationship("TempNCAAGame", primaryjoin='foreign(Match.ncaagameid) == remote(TempNCAAGame.id)',
                            backref='matches')
                             