# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This config is for club_1 database from Spider dataset.
# https://github.com/taoyds/spider

@Engine("sqlite");

@AttachDatabase("db", "../spider/club_1/club_1.sqlite");

# Facts table.
Student({age:, fname:, lname:, stuid:, sex:}) :- db.Student(age:, fname:, lname:, stuid:, sex:);

# Dimension tables.
Club(clubid:, clubname:) :- db.Club(clubid:, clubname:);
MemberOfClub(stuid:, clubid:, position:) :- db.Member_of_club(clubid:, stuid:, position:);

# Measures
CountStudents(fact) = Count(fact.stuid);
CountClubs(fact) = Count(fact.clubid);
AverageAge(fact) = Avg(fact.age);
NumRecords(fact) = Sum(1);

CountDistinctPositions(fact) = Count(position) :-
  position = fact.position;

# Dimensions
StudentFirstName(fact) = fact.fname;
StudentLastName(fact) = fact.lname;
ClubName(fact) = clubname :- MemberOfClub(stuid: fact.stuid, clubid:), Club(clubid:, clubname:);
ClubLocation(fact) = fact.clublocation;
ClubDescription(fact) = fact.clubdesc;
Position(fact) = fact.position;

# Filters
ClubNameIs(fact, club_name:) :- 
  ClubName(fact) == club_name;

StudentNameIs(fact, first_name:, last_name:) :-
  fact.fname == first_name,
  fact.lname == last_name;

AgeAbove(fact, age:) :-
  fact.age > age;

AgeBelow(fact, age:) :-
  fact.age < age;

GenderIs(fact, sex:) :-
  fact.sex == sex;

MajorIs(fact, major:) :-
  fact.major == major;

AdvisorIs(fact, advisor:) :-
  fact.advisor == advisor;

CityCodeIs(fact, city_code:) :-
  fact.city_code == city_code;

PositionIs(fact, position:) :-
  fact.position == position;
