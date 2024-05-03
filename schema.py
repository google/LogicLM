#!/usr/bin/python
#
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


import jsonschema


def String():
  return {'type': 'string'}

def Number():
  return {'type': 'number'}

def Object(properties):
  return {'type': 'object',
          'properties': properties}

def FieldDescription():
  return Object({
    'field_name': String(),
    # TODO: Add field type.
  })

def PredicateSignature():
  return Object({
    'predicate_name': String(),
    'parameters': List(FieldDescription())
  })

def List(x):
  return {
    'type': 'array',
    'items': x
  }

def Measure():
  return Object({
    'aggregating_function': PredicateSignature(),
    'fact_table': String()
  })

def Dimension():
  return Object({
    'function': PredicateSignature()
  })

def Filter():
  return Object({
    'predicate': PredicateSignature(),
    'depends_on_dimensions': List(String())
  })

def NamedDimension():
  return Object({
    'name': String(),
    'dimension': String()
  })

def FactTable():
  return Object({
    'fact_table': String(),
    'consolidation': Object({
      'consolidated_fact_table': String(),
      'projected_dimensions': List(NamedDimension()),
      'consolidated_dimensions': List(NamedDimension()),
    }),
    'union': Object({
      'fact_tables': List(String())
    }),
    'ephemeral_dimensions': List(String()),
    'hostile_dimensions': List(String()),
  })

def OlapConfig():
  return Object({
    'name': String(),
    'fact_tables': List(FactTable()),
    'default_fact_table': String(),
    'measures': List(Measure()),
    'dimensions': List(Dimension()),
    'filters': List(Filter())
  })
