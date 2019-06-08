# KafkaRuleEngine Service

Reads Avro, JSON or CSV formatted data from a Kafka topic, runs business rules (logic) on the data and
outputs the resulting data in JSON format to one or multiple Kafka target topics.

Hardcoded logic causes quality issues when duplicated and spread over multiple programs or services.
And this then is also a problem for agility. On the other hand a centralized management of logic helps
with a quicker maintenance cycle and supports the re-use of existing logic. Another plus is, that separating
code and logic will allow to hand over the maintenance from the developer to a business user or superuser,
as it is not related to the code. So a proper division of responsibilities between IT and business is possible.

There is a web interface available to compose the rule logic in an easy to use way. Complex
logic for checking data and also actions may be defined to update the data. Check:
https://github.com/uwegeercken/rule_maintenance_war for the war archive that can be used e.g. with Tomcat.

Documentation for the ruleengine, the Business Rules maintenance Web tool and for all available checks and actions
as well as samples are available at: https://github.com/uwegeercken/rule_maintenance_documentation.

The KafkaRuleEngine program requires a kafka_ruleengine.properties file, which defines the settings for the program.
Read the documentation in the properties file for the configuration possibilities.

In the properties file there are three output topics that can be defined:
* output topic
* output topic for failed messages
* output topic for detailed logging

If no topic for failed messages is defined, then all messages that come in, go to the output topic. If a topic
for failed messages is defined then passed messages go to the output topic and failed messages go to the topic
for failed messages.

If a topic for detailed result logging is defined then all detailed results of executing the ruleengine will go to the logging
topic. For each input message and rule one output message to the logging topic is generated. So if you have
10 input messages and 5 rules, then 50 messages are generated to the logging topic.

There is a properties file for the kafka consumer and one for the kafka producer configuration. These are passed to
the consumer and producer and you may add additional configuration settings as you wish to these files.

There are two modes possible with the ruleengine:

1) Update only mode: Define the output topic, don't define the failed topic. All input messages will be processed.
The rule logic will define which records are updated (using actions) and which not. And then all messages are
sent to the output topic. You can also use the ruleengine to mark a certain message/row as passed or failed by
using an action when you e.g. want to further process the data at a later point in the process.

2) Check data mode: Define the output topic and define the failed topic. All input messages will be processed.
A single message will need to pass all rulegroups (or the defined number configured). If it does, it is sent to the
output topic only, if not is is sent to the failed topic only. So this mode checks if all logic (based on the rulegroups)
that are defined is correct for each record. This means the messages are divided into "failed" and "passed" messages. SO
this is a way to filter the data. See also the note below.

In any case you may define or not define a topic for logging where the detailed results of the execution of the
ruleengine are sent. I gives detailed information on why a certain message failed or not and which rule was used to check
the data. This is helpful for identifying problems in the data and for debugging the rules logic.

Note: One or multiple rules are composed in a rulegroup. This way you can have rules that pass and rules that fail
but the rulegroup as such passes. E.g. if you have two rules: one checks if the age is smaller than 50 and the
other checks if the age is greater or equal to 50. The rules are connected using an "or" operator. If we have now a
person at the age of 40, the first rule will pass and the other one will fail. But the rulegroup in which both rules
are collected, will pass (because the rules are connected using an "or" condition).

You may also define subgroups within rulegroups, which allow grouping of rules and then the subgroups are connected using
an "and" or "or" condition. This way you have the required flexibility to compose complex logic.

The KafkaRuleEngine service logs the number of messages consumed from Kafka - if defined in the properties file. A field
of the message can be defined and the service will take the VALUE of the field and count the messages with that same
value. Based on the counter or after a defined idle time, where no records have been received, the value and the counter
is written to the log file. The service uses log4j for logging.
This can be helpful if you have a field with e.g. a batch-id. Where all messages of the same batch have the same value
in the field. When the value of the field changes, we know we have a new batch run. In this case the counter for the field is
reset to zero.
The log entries (with the value of the defined field and the counter) then can be parsed and sent to a log analyzer like
Logstash (and then to Elasticsearch) to capture and visualize the processed messages for the given batch-id.

Please send your feedback and help to enhance the tool.

    Copyright (C) 2017-2019  Uwe Geercken


 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.


uwe geercken
email: uwe.geercken@web.de
twitter: uweeegeee

last update: 2019-06-08

