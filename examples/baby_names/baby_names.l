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

BabyNames(fact) :-
  `bigquery-public-data.usa_names.usa_1910_current`(..fact);

NumberOfBabies(fact) = Sum(fact.number);
NameFraction(fact, name:) = Sum(is_this_name) / Sum(1.0) :-
  is_this_name = (
    if fact.name == name then 1 else 0
  );
# Rarely name is more than 1 in 1000, so promille is a good scale.
NameFractionPromille(fact, name:) = 1000 * NameFraction(fact, name:);

State(fact) = fact.state;
Name(fact) = fact.name;
Year(fact) = fact.year;
CumulativeYear(fact, to_year:) = year + d :-
  year = fact.year,
  d in Range(to_year - year + 1);
Gender(fact) = fact.gender;

StateIn(fact, states:) :-
  Constraint(fact.state in states);
YearIn(fact, years:) :-
  years_nums List= (ToInt64(y) :- y in years),
  Constraint(fact.year in years_nums);
YearRange(fact, from_year:, to_year:) :-
  fact.year >= from_year,
  fact.year <= to_year;
NameIn(fact, names:) :-
  Constraint(fact.name in names);
GenderIs(fact, gender:) :-
  g == (
    if gender == "Male" then
      "M"
    else if gender == "Female" then
      "F"
    else
      gender
  ),
  fact.gender == g;
  
