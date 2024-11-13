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

# Educational example of Star Fleet config.

@Engine("duckdb");

# LogicLM config.
LogicLM(name: "Space exploration",
        title: "Exoplanet missions",
        tagline: "<i>Per Aspera Ad Astra!</i> <br/> Current date: <b>November 12th, 2124</b>.",
        port: 2124,
        fact_tables: ["MissionFact"],
        default_fact_table: "MissionFact",
        dimensions: ["MissionName", "PilotName", "OriginPlanetName",
                     "DestinationPlanetName", "StarshipName", "MissionId",
                     "DestinationStarSystem", "LaunchYear", "LaunchDecade",
                     "PilotAlmaMater", "FlightYear"],
        measures: ["MissionCount", "NumRecords", "MatterAnnihilated", "ShipCount",
                   "FlightDuration"],
        filters: ["MissionIsToPlanets", "MissionIsToStars", "MissonIsByPilots",
                  "PilotIsFromUniversities", "LaunchAfter"],
        suffix_lines: [
            "Use Table() chart type by default.",
            "Do not forget to use parameter names, e.g. MissionIsToPlanets(planet_names: ['Mars'])",
            "If you need to order then give order just after space, like order: [StarhipName() asc].",
            "Measures, filters and dimesions should all be lists of predicate calls, like DestinationStarSystem().",
            "Do not filter unless asked to.",
            "Use -1 for no limit.",
            "Good luck!"]);

# Source of data and predicates with the data.
JsonData(..r) :- `('examples/starfleet/data.jsonl')`(..r);

Mission(
    mission_id:, name:,
    destination_planet:, origin_planet:,
    solar_year_launch:, solar_year_landing:,
    starship_id:, matter_annihilated:) :-
  JsonData(mission_id:, name:,
           destination_planet:, origin_planet:,
           solar_year_launch:, solar_year_landing:,
           starship_id:, matter_annihilated:,
           table_name: "Mission");

Starship(starship_id:, name:, pilot_id:) :-
  JsonData(starship_id:, name:,  pilot_id:, table_name: "Starship");

Planet(planet_id:, name:, star_id:) :-
  JsonData(planet_id:, name:, star_id:, table_name: "Planet");

Star(star_id:, star_name:) :-
  JsonData(star_id:, star_name:, table_name: "Star");

Person(person_id:, name:, alma_mater:) :-
  JsonData(person_id:, name:, alma_mater:, table_name: "Person");

University(university_id:, name:, planet_id:) :-
  JsonData(university_id:, name:, planet_id:, table_name: "University");

# Fact table.
MissionFact(
    {mission_id:, name:,
     destination_planet:, origin_planet:,
     solar_year_launch:, solar_year_landing:,
     starship_id:, matter_annihilated:}) :- 
  Mission(
    mission_id:, name:,
    destination_planet:, origin_planet:,
    solar_year_launch:, solar_year_landing:,
    starship_id:, matter_annihilated:);

# Dimension tables.
MissionName(fact) = fact.name;

PilotName(fact) = person_name :-
  Starship(starship_id: fact.starship_id, pilot_id: person_id),
  Person(person_id:, name: person_name);
OriginPlanetName(fact) = planet_name :-
  Planet(planet_id: fact.origin_planet,
         name: planet_name);
DestinationPlanetName(fact) = planet_name :-
  Planet(planet_id: fact.destination_planet,
         name: planet_name);
StarshipName(fact) = name :-
  Starship(starship_id: fact.starship_id,
           name:);
MissionId(fact) = fact.mission_id;
DestinationStarSystem(fact) = star_name :-
  Star(star_name:, star_id:),
  Planet(planet_id: fact.destination_planet, star_id:);
LaunchYear(fact) = fact.solar_year_launch;
LaunchDecade(fact) = ToInt64(Floor(fact.solar_year_launch / 10)) * 10;

PilotAlmaMater(fact) = name :-
  Starship(starship_id: fact.starship_id, pilot_id: person_id),
  Person(person_id:,
         alma_mater:),
  University(university_id: alma_mater, name:);
FlightYear(fact) = fact.solar_year_launch + i :-
  i in Range(fact.solar_year_landing - fact.solar_year_launch + 1);

Total(fact) = "total";

# Measures.
MissionCount(fact) = Count(fact.mission_id);
NumRecords(fact) = Sum(1);
MatterAnnihilated(fact) = Sum(fact.matter_annihilated);
ShipCount(fact) = Count(fact.starship_id);
FlightDuration(fact) = Sum(fact.solar_year_landing - fact.solar_year_launch);

# Filters.
MissionIsToPlanets(fact, planet_names:) :-
  Planet(planet_id: fact.destination_planet,
         name: planet_name),
  Constraint(planet_name in planet_names);

MissionIsToStars(fact, star_names:) :-
  Star(star_id:, star_name:),
  Planet(planet_id: fact.destination_planet, star_id:),
  Constraint(star_name in star_names);

MissonIsByPilots(fact, pilot_names:) :-
  Constraint(PilotName(fact) in pilot_names);

PilotIsFromUniversities(fact, university_names:) :-
  Constraint(PilotAlmaMater(fact) in university_names);

LaunchAfter(fact, year:) :-
  fact.solar_year_launch > year;