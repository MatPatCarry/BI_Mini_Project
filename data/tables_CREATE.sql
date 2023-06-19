CREATE TABLE IF NOT EXISTS Competitions (
    Id varchar(30) not null PRIMARY KEY,
    Name varchar(60)
)

CREATE TABLE IF NOT EXISTS Localization(
    Id varchar(100) not null PRIMARY KEY,
    City varchar(50) not null,
    Country varchar(50),
    Continent varchar(30)
)

CREATE TABLE IF NOT EXISTS Time(
    Id varchar(30) not null PRIMARY KEY,
    Date_day inteeger not null,
    Date_month inteeger not null,
    Date_year inteeger not null
)

CREATE TABLE IF NOT EXISTS Nationality(
    Id varchar(30) not null PRIMARY KEY,
    Name varchar(50),
    Continent varchar(30) not null
)

CREATE TABLE IF NOT EXISTS Puzzle(
    Id varchar(30) PRIMARY KEY,
    Name varchar(40)
)

CREATE TABLE IF NOT EXISTS Attendance (
    Number_of_participants inteeger not null,
    Competition_id varchar(30) not null, 
    Nationality_id varchar(30) not null, 
    Time_id varchar(30) not null, 
    Localization_id varchar(50) not null,
    Puzzle_id varchar(30) not null, 
    FOREIGN KEY (Competition_id) REFERENCES Competitions(Id),
    FOREIGN KEY (Nationality_id) REFERENCES Nationality(Id),
    FOREIGN KEY (Time_id) REFERENCES Time(Id),
    FOREIGN KEY (Localization_id) REFERENCES Localization(Id),
    FOREIGN KEY (Puzzle_id) REFERENCES Puzzle(Id)
)

