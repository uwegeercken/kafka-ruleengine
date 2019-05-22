#!/bin/bash
# script to run the KafkaRuleEngine java program
#
# the program will read the properties from the given properties files.
# Then messages from the kafka source topic are read, processed using
# the ruleengine project file and the result will be output to a kafka
# target topic. Additonally a topic may be specified to output the
# detailed results of the ruleengine execution.
#
# mandatory: pass the path and name of the ruleengine project zip file as the first parameter
# optional:  pass the path and name of the ruleengine properties file as the second parameter. default is: same folder as script.
# optional:  pass the path and name of the kafka consumer properties file as the third parameter. default is: same folder as script.
# optional:  pass the path and name of the kafka producer properties file as the fourth parameter. default is: same folder as script.
#
# last update: uwe.geercken@web.de - 2019-05-04

# determine the script folder
script_folder=$(dirname "$(readlink -f "$BASH_SOURCE")")

# run the kafka ruleengine program
java -cp "${script_folder}":"${script_folder}/lib/*" com.datamelt.kafka.ruleengine.KafkaRuleEngine config/kafka-ruleengine.properties config/kafka-consumer.properties config/kafka-producer.properties rules/rules.zip

