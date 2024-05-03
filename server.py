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


import cgi
import json
from http import server
import socketserver
import time
import os
import tempfile
from urllib import parse
import ai
import olap
from logica.tools import run_in_terminal
from logica.parser_py import parse as parse_logica
from logica.compiler import rule_translate
from logica.type_inference.research import infer
from logica.common import sqlite3_logica

from logica.common import color
import io


class LogicLMServerHeart:
  def __init__(self, config):
    self.request_counter = 0
    self.nous = ai.AI.Get()
    self.prompt_template = ai.GetPromptTemplate(config)
    self.config = config
    color.CHR_ERROR = '<span style="color:red;">'
    color.CHR_END = '</span>'
    color.CHR_WARNING = '<span style="font-weight: bold">'
    color.CHR_UNDERLINE = '<span style="font-weight: bold;">'

  def StaticFilename(self, filename):
    return os.path.dirname(__file__) + '/html/' + filename

  def SpliceIn(self, template, cut_line, content):
    decorated_cut_line = '\n%s\n' % cut_line
    up, unused_cut_out, down = template.split('%s' % decorated_cut_line)
    return decorated_cut_line.join([up, content, down])

  def RawHtml(self):
    with open(self.StaticFilename('index.html')) as html_file:
      return html_file.read()
  
  def Html(self):
    html = self.RawHtml()
    html = self.SpliceIn(
        html,
        '  // < - dashboard cut out line. do not modify - >',
        'let tmpDashboardMappings = ' +
        json.dumps(self.config['dashboard'], indent=2),
    )
    html = self.SpliceIn(
        html,
        '  // < - intelligence config cut out line. do not modify. - >',
        'let intelligenceConfig = ' +
        json.dumps(self.LegacyIntelligenceConfig(), indent=2),
    )
    if 'custom_header' in self.config:
      html = self.SpliceIn(
        html,
        '    <!-- header title cut. do not modify. -->',
        self.config['custom_header']
      )
    if 'sample_questions' in self.config:
      html = html.SpliceIn(
        html,
        '  // < - sample questions cut. do not modify. - >',
        'let '
      )
    if 'tagline' in self.config:
      html = html.replace('The only true wisdom is in knowing you know nothing.',
                          self.config['tagline'])
    if 'example_question' in self.config:
      html = html.replace('Enter your request here.',
                          self.config['example_question'])
    return html

  def LogoPng(self):
    with open(self.StaticFilename('logiclm.png'), 'rb') as logo_file:
      return logo_file.read()

  # TODO: Refactor this.
  def LegacyIntelligenceConfig(self):
    intelligence_config = {}
    intelligence_config['aiDataRequest'] = {}
    intelligence_config['aiDataRequest']['dimensions'] = [
        {'functionName': d['function']['predicate_name']}
        for d in self.config['dimensions']]
    intelligence_config['aiDataRequest']['measures'] = [
        {'aggregatingFunctionName': d['aggregating_function']['predicate_name']}
        for d in self.config['measures']]
    intelligence_config['aiDataRequest']['filters'] = [
        {'predicateName': d['predicate']['predicate_name']}
        for d in self.config['filters']]
    intelligence_config['dashboard'] = bool(self.config['dashboard'])
    intelligence_config['aiVisualizationRequest'] = {
      'chartTypes': [
        {'chartTypeName': p['predicate']['predicate_name']}
        for p in self.config['chart_types']]
    }
    return intelligence_config

  def NaturalLanguageToRequestJson(self, user_request):
    json_request_str = self.nous(self.prompt_template.replace('__USER_REQUEST__',
                                                              user_request))
    print('AI response:', json_request_str)
    json_request = json.loads(json_request_str)
    json_request['exampleQuery'] = user_request
    # TODO: Change HTML to understand raw config.
    json_request['intelligence_config'] = self.LegacyIntelligenceConfig()
    return json_request
  
  def RunJson(self, json_request):
    if len(json_request['dimensions']) < 1 or len(json_request['measures']) < 1:
      json_request['nice_error'] = '<i>Please specify at least one measure and at least one dimension.</i>'
      return 'Fail(true)', "select 'fail'", []

    o = olap.Olap(self.config, json_request)
    charting_call = o.AsPredicateCall(json_request['chartType'])
    json_request['chart_type_predicate_call'] = {
      'predicate_name': charting_call.predicate_name,
      'arguments': {k: v.AsJson() for k, v in charting_call.named_args.items()}
    }
    try:
      logic_program = str(o.GetFullLogicProgram())
    except parse_logica.ParsingException as e:
      print('Failure of parsing:')
      e.ShowMessage()
      s = io.StringIO()
      e.ShowMessage(stream=s)
      json_request['nice_error'] = s.getvalue()
      return 'Fail(true)', "select 'fail'", [] 

    print('Logic program:')
    print(logic_program)

    try:
      sql = o.GetSQL()
    except parse_logica.ParsingException as e:
      print('Failure of parsing when building SQL:')
      e.ShowMessage()
      s = io.StringIO()
      e.ShowMessage(stream=s)
      json_request['nice_error'] = s.getvalue()
      return 'Fail(true)', "select 'fail'", []
    except rule_translate.RuleCompileException as e:
      print('Failure of compilation when building SQL:')
      e.ShowMessage()
      s = io.StringIO()
      e.ShowMessage(stream=s)
      json_request['nice_error'] = s.getvalue()
      return 'Fail(true)', "select 'fail'", []
    except infer.TypeErrorCaughtException as e:
      print('Failure of typing when building SQL:')
      e.ShowMessage()
      s = io.StringIO()
      e.ShowMessage(stream=s)
      json_request['nice_error'] = s.getvalue()
      return 'Fail(true)', "select 'fail'", []

    with  tempfile.TemporaryDirectory('LogicLM') as temp_dir:
      temp_file = '%s/report.l' % temp_dir
      with open(temp_file, 'w') as w:
        w.write(logic_program)
      header, rows = run_in_terminal.Run(temp_file, 'Report', output_format='header_rows')
    data = [header] + rows
    print('Data:', data)
    print(sqlite3_logica.ArtisticTable(header, rows))
    return logic_program, sql, data


def MakeSimpleLogicLMServer(config):
  heart = LogicLMServerHeart(config)
  class SimpleLogicLMServer(server.SimpleHTTPRequestHandler):
    def __init__(self, request, client_address, server):
      self.heart = heart
      super().__init__(request, client_address, server)

    def do_POST(self) -> None:
      url = parse.urlparse(self.path)
      ctype, pdict = cgi.parse_header(self.headers.get('content-type'))
      if url.path == '/understand_command':
        user_request = self.rfile.read(int(self.headers['Content-Length'])).decode('utf-8')
        print('User request:', user_request)
        json_request = self.heart.NaturalLanguageToRequestJson(user_request)
        print('LLM translation:', json.dumps(json_request, indent=' '))
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(bytes(json.dumps(json_request), 'utf8'))
      if url.path == '/execute_config':
        json_request = json.loads(
          self.rfile.read(int(self.headers['Content-Length'])).decode('utf-8'))
        print('JSON request:', json_request)
        logic_program, sql, data = self.heart.RunJson(json_request)
        response = json_request | {
          'data': data,
          'sql': sql,
          'logical_program': logic_program,
        }
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(bytes(json.dumps(response), 'utf8'))

    def do_GET(self) -> None:
      url = parse.urlparse(self.path)
      supported_paths = ['/index.html', '/logiclm.png']
      if url.path not in supported_paths:
        path = '/index.html'
      else:
        path = url.path
      if path == '/index.html':
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(bytes(self.heart.Html(), 'utf8'))
        return
      if path == '/logiclm.png':
        self.send_response(200)
        self.send_header('Content-type', 'image/png')
        self.end_headers()
        self.wfile.write(self.heart.LogoPng())
        return

      self.send_response(200)
      self.send_header("Content-type", "text/html")
      self.end_headers()
      self.wfile.write(bytes('Congratulations! You achieved impossible!', 'utf8'))
      self.heart.request_counter += 1

  return SimpleLogicLMServer


class ThreadedTCPServer(socketserver.ThreadingMixIn, 
                        socketserver.TCPServer):
    allow_reuse_address = True  # To avoid waiting after killing server.
    pass


def StartServer(config):
  simple_server = MakeSimpleLogicLMServer(config)
  port = config.get('port', 1791)
  server_instance = ThreadedTCPServer(('localhost', port), simple_server)
  print('Starting LogicLM server for "%s" intelligence configuration.' % config['name'])
  try:
    server_instance.serve_forever()
  except KeyboardInterrupt:
    print('Server terminated with Ctrl-C')
  finally:
    server_instance.server_close()


if __name__ == '__main__':
  config = json.loads(open('examples/baby_names/baby_names.json').read())
  StartServer(config)
