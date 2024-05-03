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


import hashlib
import jsonschema
import json
import schema
import sys

from logica.compiler import universe
from logica.compiler import rule_translate
from logica.tools import avatar
from logica.parser_py import parse

class Olap:
  def __init__(self, config, request):
    jsonschema.validate(config, schema.OlapConfig())
    self.config = config
    self.request = request
    self.called_predicate_cache = {}
    self.measures = request.get('measures', [])
    default_fact_table = config['default_fact_table']
    self.fact_table_of_measure = {
      m['aggregating_function']['predicate_name']: m.get('fact_table',
                                                         default_fact_table)
      for m in self.config['measures']}
    
    self.default_fact_table = default_fact_table
    self.dimensions = request.get('dimensions', [])
    self.filters = request.get('filters', [])
    self.limit = request.get('limit', -1)
    self.order = request.get('order', [])
    self.direct_dependency = self.BuildDirectFactualDependencies()
    self.fact_dependencies = self.BuildFactualDependencies()
    self.fact_table_to_measures = self.BuildFactTableToMeasures()
    self.consolidation_info = self.GetConsolidationInfo()
    self.union_info = self.GetUnionInfo()
    self.table_needed_by_measure = {
        m: self.fact_table_of_measure[self.CalledPredicate(m)]
        for m in self.measures}
    self.measures_to_compute_from_table = self.BuildMeasuresToComputeFromTable()
    self.relevant_fact_tables = self.BuildListOfAllNeededTables()
    self.table_to_ephemeral_dimensions = self.BuildEphemeralDimensions()
    self.filter_to_needed_dimensions = self.BuildFilterToNeededDimensions()

  def DimensionsDomainRule(self) -> avatar.Rule:
    dimensions_domain_predicate = avatar.Predicate('DimensionsDomain')
    fact_variable = avatar.Variable('fact')
    dimensions_args = {
        self.ColumnName(d): self.AsPredicateCall(d)(fact_variable)
        for d in self.dimensions
    }
    head = dimensions_domain_predicate(**dimensions_args)
    filters_proposition = avatar.Conjunction([
      self.AsPredicateCall(f)(fact_variable) for f in self.filters])
    body = filters_proposition & avatar.Predicate(self.default_fact_table)(fact_variable)
    return +head << body

  def BuildFilterToNeededDimensions(self):
    result = {}
    for f in self.config['filters']:
      result[f['predicate']['predicate_name']] = f.get('depends_on_dimensions', [])

    return result

  def BuildEphemeralDimensions(self):
    result = {}
    for t in self.config['fact_tables']:
      result[t['fact_table']] = t.get('ephemeral_dimensions', [])
    return result

  def BuildListOfAllNeededTables(self):
    result = set()
    for t in self.measures_to_compute_from_table:
      result |= {t}
      result |= set(self.fact_dependencies[t])
    return list(sorted(result))

  def BuildMeasuresToComputeFromTable(self):
    result = {}
    for measure, table in self.table_needed_by_measure.items():
      result[table] = result.get(table, []) + [measure]
    return result

  def BuildFactTableToMeasures(self):
    result = {}
    for m in self.request['measures']:
      measure = self.CalledPredicate(m)
      table = self.fact_table_of_measure[measure]
      result[table] = result.get(table, []) + [measure]
    return result
  
  def GetConsolidationInfo(self) -> dict[str, tuple[str,
                                                    dict[str, str],
                                                    dict[str, str]]]:
    result = {}
    for f in self.config['fact_tables']:
      name = f['fact_table']
      if consolidation := f.get('consolidation'):
        result[name] = (consolidation.get('consolidated_fact_table'),
                        consolidation.get('consolidated_dimensions', []),
                        consolidation.get('projected_dimensions', []))
    return result

  def GetUnionInfo(self):
    result = {}
    for f in self.config['fact_tables']:
      name = f['fact_table']
      if union := f.get('union'):
        result[name] = (union['fact_tables'],
                        union.get('consolidated_dimensions', []),
                        union.get('projected_dimensions', []))
    return result
                        
  def BuildDirectFactualDependencies(self):
    direct_dependency = {}
    all_fact_tables = []
    for f in self.config['fact_tables']:
      all_fact_tables.append(f['fact_table'])
      if 'consolidation' in f:
        direct_dependency[f['fact_table']] = {
          f['consolidation']['consolidated_fact_table']}
      if 'union' in f:
        direct_dependency[f['fact_table']] = set(f['union']['fact_tables'])
    self.all_fact_tables = all_fact_tables
    for k, v in direct_dependency.items():
      assert k in all_fact_tables, (k, v)
      assert v <= set(all_fact_tables), ((k, v), all_fact_tables)


    return direct_dependency

  def BuildFactualDependencies(self):
    direct_dependency = self.direct_dependency
    all_fact_tables = self.all_fact_tables

    all_dependencies = {}
    def GetAllDependencies(t):
      if t not in direct_dependency:
        return set()
      if t not in all_dependencies:
        all_dependencies[t] = direct_dependency[t]
        for x in direct_dependency[t]:
          all_dependencies[t] = (
            all_dependencies[t] |
            GetAllDependencies(x))
      return all_dependencies[t]
    for t in all_fact_tables:
      all_dependencies[t] = GetAllDependencies(t)
    return {t: sorted(all_dependencies[t])
            for t in all_fact_tables}

  def OldLogicProgram(self, request):
    Report = avatar.Predicate('Report')
    fact = avatar.Variable('fact')

    head = +Report(**{f'`{measure}`': avatar.Predicate(measure)(fact)
                      for measure in request['measures'] })

    fact_table = avatar.Predicate(self.config['fact_tables'][0]['fact_table'])

    body = fact_table(fact)
    rule = head << body
    return str(rule)

  def ColumnName(self, predicate_call):
    p = self.CalledPredicate(predicate_call)
    disambiguation = abs(Hash(predicate_call)) % 1000000
    return '_'.join([p.lower(), str(disambiguation)])

  def UnionFacts(self, unioned_table, component_fact_tables):
    unioned_predicate = avatar.Predicate(unioned_table)

    fact_variable = avatar.Variable('fact')
    head = unioned_predicate(fact_variable)
    disjuncts = [avatar.Predicate(t)(fact_variable)
                 for t in component_fact_tables]
    body = avatar.Disjunction(disjuncts)
    return head << body

  def WrapFacts(self, resulting_fact_table,
                input_rule: avatar.Rule,
                dimensions_domain_rule: avatar.Rule):
    head_args = {}
    domain_call_args = {}
    for domain_arg in dimensions_domain_rule.head.named_args.keys():
      domain_call_args[domain_arg] = head_args[domain_arg] = avatar.Variable(domain_arg)

    for input_arg in input_rule.head.named_args.keys():
      head_args[input_arg] = avatar.Variable(input_arg)

    columns = input_rule.head.named_args.keys()
    input_predicate = input_rule.head.predicate_name
    args = {c: avatar.Variable(c) for c in columns}
    head = avatar.Predicate(resulting_fact_table)(avatar.Literal(head_args))
    body = avatar.Conjunction([
      avatar.Predicate(input_predicate)(**args),
      avatar.Predicate(dimensions_domain_rule.head.predicate_name)(**domain_call_args)
      ])
    return head << body

  def ConsolidateFacts(self, fact_table, measures, dimensions, filters,
                       consolidated_dimensions, projected_dimensions,
                       translucent_dimensions,
                       consolidating_predicate_name=None):
    """Produces rule and predicate that consolidates the fact table."""
    consolidating_predicate_name = consolidating_predicate_name or ('Consolidating' + fact_table)
    consolidating_predicate = avatar.Predicate(consolidating_predicate_name)
    fact_variable = avatar.Variable('fact')
    Aggr = lambda a : avatar.Aggregation('Aggr', a)
    measures_args = {
        self.ColumnName(m): Aggr(self.AsPredicateCall(m)(fact_variable))
        for m in measures}
    consolidated_dimensions_args = {
        k: Aggr(self.AsPredicateCall(v)(fact_variable))
        for k, v in consolidated_dimensions.items()
    }
    dimensions_args = {
        self.ColumnName(d): self.AsPredicateCall(d)(fact_variable)
        for d in dimensions
    }
    projected_dimensions_args = {
        k: self.AsPredicateCall(v)(fact_variable)
        for k, v in projected_dimensions.items()
    }
    translucent_dimensions_args = {
        self.ColumnName(d): avatar.Subscript(fact_variable,
                                             self.ColumnName(d))
        for d in translucent_dimensions
    }
    filters_list = [self.AsPredicateCall(f)(fact_variable) for f in filters]

    fact_table_call = avatar.Predicate(fact_table)(fact_variable)
    body = fact_table_call & avatar.Conjunction(filters_list)
    head_args = {**measures_args,
                 **consolidated_dimensions_args,
                 **dimensions_args,
                 **projected_dimensions_args,
                 **translucent_dimensions_args}
    head = consolidating_predicate(**head_args)
    rule = +head << body
    return consolidating_predicate_name, rule

  def ParseExpression(self, s):
    return parse.ParseExpression(parse.HeritageAwareString(s))

  def CalledPredicate(self, predicate_call):
    if predicate_call not in self.called_predicate_cache:
      parsed_call = self.ParseExpression(predicate_call)
      assert 'call' in parsed_call, parsed_call
      self.called_predicate_cache[predicate_call] = parsed_call['call']['predicate_name']
    return self.called_predicate_cache[predicate_call]

  def AsPredicateCall(self, predicate_call_str):
    parsed_call = self.ParseExpression(predicate_call_str)
    call = avatar.LogicalTerm.FromSyntax(parsed_call)
    return call

  def FactTableDimensions(self, t):
    return [
      d for d in self.dimensions
      if self.CalledPredicate(d) not in self.table_to_ephemeral_dimensions[t]]
  
  def GetLogicProgram(self):
    self.source_of_measure = {}
    needs_building = []
    # Computing each measure.
    rules_for_measures = []
    program = avatar.Program([])
    dimensions_domain_rule = self.DimensionsDomainRule()
    need_dimensions_domain = False
    for i, (fact_table, measures) in enumerate(self.measures_to_compute_from_table.items()):
      dimensions = [
        d for d in self.dimensions
        if self.CalledPredicate(d) not in self.table_to_ephemeral_dimensions[fact_table]]
      filters = [
        f for f in self.filters
        if not set(self.filter_to_needed_dimensions[self.CalledPredicate(f)]) & set(self.table_to_ephemeral_dimensions[fact_table])
      ]  
      if fact_table in self.direct_dependency:
        needs_building += [fact_table]
        translucent_dimensions, dimensions = dimensions, []
        filters = []
      else:
        translucent_dimensions = []

      consolidated_table, rule = self.ConsolidateFacts(
        fact_table, measures, dimensions, filters, {}, {}, translucent_dimensions)

      rules_for_measures.append(rule)
      program.AddRule(rule)
      if i == 0:
        program.rules[i].comment_before_rule = 'Computing all the measures.'
    # Computing auxiliary tables.
    i = 0
    while i < len(needs_building):
      fact_table_to_build = needs_building[i]
      if fact_table_to_build in self.consolidation_info:
        t, c, p = self.consolidation_info[fact_table_to_build]
        if t in self.direct_dependency:
          needs_building += [t]
        dimensions = self.FactTableDimensions(t)
        filters = [
          f for f in self.filters
          if not (set(self.filter_to_needed_dimensions[self.CalledPredicate(f)]) <= set(self.table_to_ephemeral_dimensions[t]))
        ]
        consolidated_table, rule = self.ConsolidateFacts(
          t, [], dimensions, filters, 
          {x['name']: x['dimension'] for x in c},
          {x['name']: x['dimension'] for x in p},
          translucent_dimensions=[],
          consolidating_predicate_name=fact_table_to_build + 'Step1')
        program.AddRule(rule)
        program.AddRule(self.WrapFacts(fact_table_to_build, rule, dimensions_domain_rule))
        need_dimensions_domain = True
      elif fact_table_to_build in self.union_info:
        ts, c, p = self.union_info[fact_table_to_build]
        for t in ts:
          if t in self.direct_dependency:
            needs_building += [t]
        dimensions = [
          d for d in self.dimensions
          if self.CalledPredicate(d) not in self.table_to_ephemeral_dimensions[fact_table_to_build]]
        filters = [
          f for f in self.filters
          if self.filter_to_needed_dimensions[self.CalledPredicate(f)] not in self.table_to_ephemeral_dimensions[fact_table_to_build]
        ]
        program.AddRule(self.UnionFacts(fact_table_to_build, ts))
      i += 1
    if need_dimensions_domain:
      program.AddRule(dimensions_domain_rule
                      )
    def ColumnName(predicate_call_str):
      return '`%s`' % predicate_call_str.replace('(', '<').replace(')', '>').replace('"', "'").replace('"', "'")
    # Assembling all the measures together.
    measures_args = {ColumnName(m): avatar.Variable(self.ColumnName(m))
                     for m in self.measures}
    dimensions_args = {ColumnName(d): avatar.Variable(self.ColumnName(d))
                       for d in self.dimensions}
    if self.limit >= 0:
      program.AddRule(avatar.Predicate('@Limit')(
        avatar.Literal('Report'), avatar.Literal(self.limit)) << None)
    if self.order:
      def DecorateOrder(s):
        for suffix in ['asc', 'desc']:
          if s.endswith(suffix):
            direction = suffix
            s = s.removesuffix(' ' + suffix)
            break
        else:
          direction = 'asc'
        return ColumnName(s) + ' ' + direction
      program.AddRule(avatar.Predicate('@OrderBy')(
        avatar.Literal('Report'), *map(lambda x: avatar.Literal(DecorateOrder(x)),
                                       self.order)) << None)
    head = avatar.Predicate('Report')(**(dimensions_args | measures_args))
    body = avatar.Conjunction([])
    for rule in rules_for_measures:
      p = rule.head.predicate_name
      named_args = {a: avatar.Variable(a) for a in rule.head.named_args}
      body = body & avatar.Predicate(p)(**named_args)
    rule = head << body
    rule.comment_before_rule = 'Assembling all the measures.'
    program.AddRule(rule)
    return program

  def GetFullLogicProgram(self):
    base_program = open(self.config['logica_program']).read()
    incremental_program = self.GetLogicProgram()
    program = base_program + ';\n' + str(incremental_program)
    return program

  def GetSQL(self):
    program = self.GetFullLogicProgram()
    rules = parse.ParseFile(program)['rule']
    logic_program = universe.LogicaProgram(rules)
    sql = logic_program.FormattedPredicateSql('Report')
    return sql

def Hash(s):
  return abs(int(hashlib.md5(str(s).encode()).hexdigest()[:16], 16) - (1 << 63))

if __name__ == '__main__':
  config = json.loads(open('test_data/abstract_model/abstract.json').read())
  request = {"measures": ["NumberOfBabies()", "Fact2Measure(x: 3)", "MeasureOverConsolidated()"],
            "dimensions": ["State()", "Year()", "Dim4()"],
            "filters": ["StateIn(states: [\"NY\", \"WA\"])"]}
  olap = Olap(config, request)
  # p, r = olap.ConsolidateFacts(
  #   'T', ['M1(a:1)', 'M2(b:2)'], ['D1(a:1, b:2)'], ['F1(x:1)', 'F2()'],
  #   {'cd': 'ConsolidatedDim1(a: 1)'},
  #   {'pd': 'ProjectedDim1(x: 5)'})
  # print(p)
  # print(r)
  # print(olap.fact_dependencies)
  program = olap.GetLogicProgram()
  print('Program:')
  print(program)
  # print(olap.LogicProgram(request))