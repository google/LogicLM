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

# Educational example of Food Delivery config.

@Engine("duckdb");

# LogicLM config.
LogicLM(name: "Food delivery",
        title: "Delivery statistics",
        tagline: "<i>Fast, Fresh, Friendly!</i>",
        port: 2124,
        fact_tables: ["FoodOrderFact"],
        default_fact_table: "FoodOrderFact",
        dimensions: ["OrderType", "DriverName", "RestaurantDistrictName",
                     "CustomerDistrictName", "VehicleName", "OrderId",
                     "CustomerCityName", "OrderDay", "TransitDay"],
        measures: ["OrderCount", "NumRecords", "OrderRevenue", "VehicleCount",
                   "DeliveryDuration"],
        filters: ["OrdersToDistricts", "OrdersToCities", "OrdersByDrivers", "OrdersAfter"],
        suffix_lines: [
            "Try using linechart or barchart. If unsure use Table() chart.",
            "Use your judgement to sort by dimensions, or descending by measure.",
            "Do not forget to use parameter names, e.g. OrdersToDistricts(district_names: ['Central Zone'])",
            "If you need to order then give order just after space, like order: [VehicleName() asc].",
            "Measures, filters and dimensions should all be lists of predicate calls, like CustomerCityName().",
            "Do not filter unless asked to.",
            "Use -1 for no limit.",
            "Good luck!"]);

# Source of data and predicates with the data.
JsonData(..r) :- `('examples/foodfleet/data.jsonl')`(..r);

FoodOrder(
    order_id:, order_type:,
    customer_district_id:, restaurant_district_id:,
    order_date:, delivery_date:,
    delivery_vehicle_id:, order_revenue:) :-
  JsonData(order_id:, order_type:,
           customer_district_id:, restaurant_district_id:,
           order_date:, delivery_date:,
           delivery_vehicle_id:, order_revenue:,
           table_name: "Orders");

DeliveryVehicle(vehicle_id:, name:, driver_id:) :-
  JsonData(vehicle_id:, name:, driver_id:, table_name: "Delivery Vehicles");

District(district_id:, name:, city_id:) :-
  JsonData(district_id:, district_name: name, city_id:, table_name: "Districts");

City(city_id:, city_name:) :-
  JsonData(city_id:, city_name:, table_name: "Cities");

Driver(driver_id:, name:) :-
  JsonData(driver_id:, name:, table_name: "Drivers");

# Fact table.
FoodOrderFact(
    {order_id:, order_type:,
     customer_district_id:, restaurant_district_id:,
     order_date:, delivery_date:,
     delivery_vehicle_id:, order_revenue:}) :- 
  FoodOrder(
    order_id:, order_type:,
    customer_district_id:, restaurant_district_id:,
    order_date:, delivery_date:,
    delivery_vehicle_id:, order_revenue:);

# Dimension tables.
OrderType(fact) = fact.order_type;

DriverName(fact) = driver_name :-
  DeliveryVehicle(vehicle_id: fact.delivery_vehicle_id, driver_id: driver_id),
  Driver(driver_id:, name: driver_name);

RestaurantDistrictName(fact) = district_name :-
  District(district_id: fact.restaurant_district_id,
           name: district_name);

CustomerDistrictName(fact) = district_name :-
  District(district_id: fact.customer_district_id,
           name: district_name);

VehicleName(fact) = name :-
  DeliveryVehicle(vehicle_id: fact.delivery_vehicle_id,
                  name:);

OrderId(fact) = fact.order_id;

CustomerCityName(fact) = city_name :-
  City(city_id:, city_name:),
  District(district_id: fact.customer_district_id, city_id: city_id);

OrderDay(fact) = fact.order_date;

TransitDay(fact) = fact.order_date + i :-
  i in Range(fact.delivery_date - fact.order_date + 1);

Total(fact) = "total";

# Measures.
OrderCount(fact) = Count(fact.order_id);
NumRecords(fact) = Sum(1);
OrderRevenue(fact) = Sum(fact.order_revenue);
VehicleCount(fact) = Count(fact.delivery_vehicle_id);
DeliveryDuration(fact) = Sum(fact.delivery_date - fact.order_date);

# Filters.
OrdersToDistricts(fact, district_names:) :-
  District(district_id: fact.customer_district_id,
           name: district_name),
  Constraint(district_name in district_names);

OrdersToCities(fact, city_names:) :-
  District(district_id: fact.customer_district_id, city_id: city_id),
  City(city_id:, city_name:),
  Constraint(city_name in city_names);

OrdersByDrivers(fact, driver_names:) :-
  Constraint(DriverName(fact) in driver_names);

OrdersAfter(fact, day:) :-
  fact.order_date > day;

