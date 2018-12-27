from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, ForeignKey, UniqueConstraint
from sqlalchemy.types import Integer, String, Date, Boolean
from sqlalchemy.orm import relationship

Base = declarative_base()


class Team(Base):
    __tablename__ = 'team'
    
    id = Column(Integer, primary_key=True)
    shortname = Column(String, nullable=False, unique=True)
    longname = Column(String, unique=True)
    mascot = Column(String)
    
    homegames = relationship("Game", order_by="game.date", back_populates="hometeam")
    awaygames = relationship("Game", order_by="game.date", back_populates="awayteam")
    
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
    
    games = relationship("Game", order_by="game.date", back_populates="season")
    
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