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


import glob
import json
import logiclm
import io
import sys
from logica.common import color

def ShowFirstDifference(a, b):
  lines_a = a.split('\n')
  lines_b = b.split('\n')
  lines_a += ['END OF INPUT']
  lines_b += ['END OF INPUT']
  for i, (line_a, line_b) in enumerate(zip(lines_a, lines_b)):
    if line_a != line_b:
      print(color.Warn('Common start:'))
      print('\n'.join(lines_a[:i]))
      print(color.Warn('Actual line') + ' %d: %s' % (i, line_a))
      print(color.Warn('Golden line') + ' %d: %s' % (i, line_b))
      break
  else:
    print('A and B are the same.')


def RunTest(test_file, golden_run):
  print('%-70s %s' % (test_file, color.Format('{warning}RUNNING{end}')))
  with open(test_file) as f:
    content = f.read()
  test_config, golden = content.split('\n-----\n')
  test_config = json.loads(test_config)
  argv = ['',
          test_config['config'],
          test_config['command']] + (
            [json.dumps(test_config['request'])]
            if 'request' in test_config else [])
  true_stdout = sys.stdout
  mock_stdout = io.StringIO()
  sys.stdout = mock_stdout
  logiclm.main(argv)
  sys.stdout = true_stdout
  result = mock_stdout.getvalue()

  print('\033[F', end='')
  if result == golden:
    print('%-70s %s' % (test_file, color.Format('{ok}PASS   {end}')))
  else:
    print('%-70s %s' % (test_file, color.Format('{error}FAIL   {end}')), end='\r')
    if golden_run:
      with open(test_file, 'w') as w:
        w.write(json.dumps(test_config, indent=2))
        w.write('\n-----\n')
        w.write(result)
      print('%-70s %s' % (test_file, color.Format('{warning}UPDATED{end}')))
    else:
      print('')
      ShowFirstDifference(result, golden)


test_files = glob.glob('test_data/integration_tests/*.txt')
for test_file in test_files:
  golden_run = 'golden_run' in sys.argv
  RunTest(test_file, golden_run)