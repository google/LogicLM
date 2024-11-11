
@Engine("duckdb");

LogicLM(name: "Space exploration",
        title: "Exoplanet missions",
        fact_tables: ["MissionFact"],
        default_fact_table: "MissionFact",
        dimensions: ["MissionName", "PilotName", "OriginPlanetName",
                     "DestinationPlanetName", "StarshipName", "MissionId"],
        measures: ["MissionCount", "NumRecords"],
        filters: ["MissionIsToPlanets", "MissionIsToStars", "MissonIsByPilots"],
        suffix_lines: [
            "Use Table() chart type by default.",
            "Do not forget to use parameter names, e.g. MissionIsToPlanets(planet_names: ['Mars'])",
            "If you need to order then give order just after space, like order: [StarhipName() asc].",
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
StarshipName(fact) = name :-
  Starship(starship_id: fact.starship_id,
           name:);
MissionId(fact) = fact.mission_id;

MissionCount(fact) = Count(fact.mission_id);

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
 
NumRecords(fact) = Sum(1);
Total(fact) = "total";
