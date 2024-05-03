<!-- 
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
-->

# LogicLM: natural language data analytics

![LogicLM demo](/logiclm_demo.gif)

## What is LogicLM?

LogicLM is a lightweight open source natural language data analytics interface.

Vision of LogicLM: Use of predicate calculus as an intermediate representation between
natural language and data retrieval allows for reliable and efficient application of
artificial intelligence to data analysis.

LogicLM uses [Logica](https://github.com/evgskv/logica), which is an open source logic programming language.

Defining measures, dimensions and filters as predicates makes writing configuration easy and
results in a powerful OLAP query generation.

Large language models are used for translating user request formulated in natural language to
a structured config. User stays in control, since the config is displayed to the user and
can be directly edited.

## Supported back-ends

LogicLM uses Logica to generate and execute data query. Database back-ends supported by Logica are

* SQLite: lightweight in-process database that among other things comes with Python. No installation or configuration is necessary to use this back-end. Perfect for analyzing datasets up to 1GB.

* PostgreSQL: one of the most popular open-source database servers. Perfect for analyzing datasets up to 10GB.

* BigQuery: Google's distributed data warehouse capable of processing practically unlimited volumes of data.
It's a [paid product](https://cloud.google.com/bigquery/pricing), but it comes with a free tier.

To understand user's request LogicLM needs an LLM API key. Supported LLM services are: Google GenAI, OpenAI and MistralAI.

## LogicLM Configuration

You configure an instance of LogicLM describing your data cube measures, dimensions and filters. 
Configuration consists of two files a Logica program and a JSON-format file.

* Logic of measures, dimensions and filtes is defined in a Logica program via rules specifying the
corresponding predicates. 

* Use JSON part-of-config file to specify which predicates correspond to measures, which to dimensions and which to filters. In this file you also specify hints to the LLM, like meaning of the 
measures, dimensions and some examples of answering questions.


## Configuration Examples

LogicLM repo comes with two [examples](/examples) of configurations:

* [reach](/examples/reach/): synthetic dataset for measuring reach (i.e. number of people) that were exposed to a collection of online-advertising campaigns.

* [baby_names](/examples/baby_names/): dataset about names given to babies in United States, broken by gender and state. Configuration uses BigQuery as the back-end.

## Installation

To run LogicLM clone repo and install requirements.

```
git clone https://github.com/google/logiclm
cd logiclm
python3 -m pip install -r requirements.txt
```

You will also need to install an LLM API that you would like to use, e.g. to install Google Generative AI run
```
python3 -m pip install google-generativeai
```

If you want to use BigQuery then you will need [Python SDK](https://cloud.google.com/python/docs/reference/bigquery/latest).

## Starting a UI server

Example `reach` is good for a quick start, as it uses SQLite and runs without any external database dependencies.

To enable natural language query translation you would need an LLM API key for the system that you would like to use,
i.e one of `LOGICLM_GOOGLE_GENAI_API_KEY`, `LOGICLM_OPENAI_API_KEY` or `LOGICLM_MISTRALAI_API_KEY`.

To start a LogicLM instance powered  by `reach` config enter the root repo folder and run

```
export LOGICLM_GOOGLE_GENAI_API_KEY=your_key_should_be_here
python3 logiclm.py examples/reach/reach.json start_server
```
Then proceed to http://localhost:1791/.

## Programmatic usage

You can call `logiclm.py` script from command line. For example to build SQL for a natural language question use `understand_and_sql` command. If you have Google Cloud configured you can pipe the SQL to `bq` tool to query the result.

```
$ python3 logiclm.py examples/baby_names/baby_names.json understand_and_sql "What are top popular names on westcoast?"  | bq query --nouse_legacy_sql
+---------+------------------+
| Name<>  | NumberOfBabies<> |
+---------+------------------+
| Michael |           545822 |
| David   |           475426 |
| Robert  |           457956 |
+---------+------------------+
```

See `main` function in [logiclm.py](/logiclm.py) for examples of calling LogicLM library functions.



_Unless otherwise noted, the LogicLM source files are distributed under the Apache 2.0 license found in the LICENSE file._

_LogicLM is not an officially supported Google product._
