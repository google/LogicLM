
@Engine("duckdb");

LogicLM(name: "Space exploration",
        title: "Exoplanet missions",
        fact_tables: ["MissionFact"],
        default_fact_table: "MissionFact",
        dimensions: ["MissionName", "PilotName", "OriginPlanetName",
                     "DestinationPlanetName"],
        measures: ["MissionCount"],
        filters: ["MissionIsToPlanets"],
        suffix_lines: [
            "Use Table() chart type by default.",
            "Do not forget to use parameter names, e.g. MissionIsToPlanets(planet_names: ['Mars'])",
            "Good luck!"]);

JsonData(..r) :- `('examples/starfleet/data.jsonl')`(..r);

Mission(
    mission_id:, name:,
    destination_planet:, origin_planet:,
    solar_year_launch:, solar_year_landing:,
    starship_id:) :-
  JsonData(mission_id:, name:,
           destination_planet:, origin_planet:,
           solar_year_launch:, solar_year_landing:,
           starship_id:, table_name: "Mission");

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

MissionFact(
    {mission_id:, name:,
     destination_planet:, origin_planet:,
     solar_year_launch:, solar_year_landing:,
     starship_id:}) :- 
  Mission(
    mission_id:, name:,
    destination_planet:, origin_planet:,
    solar_year_launch:, solar_year_landing:,
    starship_id:);

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

MissionCount(fact) = Count(fact.mission_id);

MissionIsToPlanets(fact, planet_names:) :-
  Planet(planet_id: fact.destination_planet,
         name: planet_name),
  Constraint(planet_name in planet_names);

NumRecords(fact) = Sum(1);
Total(fact) = "total";
