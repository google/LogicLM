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


@Engine("sqlite");
@AttachDatabase("db", "examples/reach/synthetic_logs.sqlite");

##########
# Tables
Event({date:, campaign:, device:, age:, gender:, person:}) :-
  db.Event(date:, campaign:, device:, age:, gender:, person:);

GetPopulation(age:, gender:) Aggr= Count(person) * 1000 :-
  db.Event(age:, gender:, person:);

@With(PopulationData);
PopulationData({age:, gender:, population: GetPopulation(age:, gender:)});

############
# Measures
ReachedFraction(fact) = Sum(fact.reach) / ToFloat64(Sum(fact.population));

# This is set to sampling rate used in dataset generation.
SamplingRate() = 1000;

Reach(fact) = SamplingRate() * Count(fact.person);
Impressions(fact) = SamplingRate() * Sum(1);

# Works over PopulationData fact table.
Population(fact) = Sum(fact.population);

Zero(fact) = Sum(0);

###############
# Dimensions
Device(fact) = fact.device;
DeviceAndCrossDevice(fact) = device :-
  device in [fact.device, "cross-device"];

Age(fact) = fact.age;
Gender(fact) = fact.gender;
Demo(fact) = fact.gender ++ fact.age;
Campaign(fact) = fact.campaign;
EventDate(fact) = fact.date;
CumulativeDate(fact, end_date:) = DateAddDay(fact.date, i) :-
  i in Range(DateDiffDay(end_date, fact.date) + 1);
Total(fact) = "total";

############
# Filters
LowerArray(a) = r :-
  r List= (Lower(x) :- x in a);

DeviceIn(fact, devices:) :-
  Constraint(fact.device in LowerArray(devices));
DemoIn(fact, demos:) :-
  Constraint(Demo(fact) in LowerArray(demos));
AgeIn(fact, ages:) :-
  Constraint(Age(fact) in LowerArray(ages));
GenderIn(fact, genders:) :-
  Constraint(Gender(fact) in LowerArray(genders));
CampaignIn(fact, campaigns:) :-
  Constraint(fact.campaign in campaigns);
DateRange(fact, date_from:, date_to:) :-
  EventDate(fact) >= date_from,
  EventDate(fact) <= date_to;
