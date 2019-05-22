#!/bin/bash

RE_HOME=/opt/ruleengine
RE_CONFIG="${RE_HOME}/config"

##
## Inject and over write configuration and properties for the consumer, producer and the rule engine.
##

PREFIXES="KAFKA_CONSUMER,KAFKA_PRODUCER,KAFKA_RULEENGINE"

for PREFIX in `echo ${PREFIXES} | sed "s/,/ /g"` ; do
    FILENAME=`echo "${PREFIX}" | sed -r "s/^${PREFIX}(.*)=.*/\1/g" | tr '[:upper:]' '[:lower:]' | tr _ - | sed "s/$/.properties/g"`

    for VAR in `env` ; do
        
        env_var=`echo "$VAR" | sed -r "s/(.*)=.*/\1/g"`

        if [[ $env_var =~ ^${PREFIX} ]]; then

            echo "variable: $VAR"
        
            case $env_var in
                "RULEENGINE_FAILED_MINIMUM_NUMBER_OF_GROUPS")
                    prop_name="ruleengine.failed.minimum_number_of_groups"
                ;;
                *)
                    prop_name=`echo "$VAR" | sed -r "s/^${PREFIX}_(.*)=.*/\1/g" | tr '[:upper:]' '[:lower:]' | tr _ .`
            esac;

            if egrep -q "(^|^#)$prop_name=" ${RE_CONFIG}/${FILENAME}; then
                # note that no config names or values may contain an '@' char
                sed -r -i "s@(^|^#)($prop_name)=(.*)@\2=${!env_var}@g" ${RE_CONFIG}/${FILENAME}
            else
                #echo "Adding property $prop_name=${!env_var}"
                echo "$prop_name=${!env_var}" >> ${RE_CONFIG}/config/${FILENAME}
            fi

            if [[ "$SENSITIVE_PROPERTIES" = *"$env_var"* ]]; then
                echo "--- Setting property in file: ${RE_CONFIG}/${FILENAME} from $env_var: $prop_name=[hidden]"
            else
                echo "--- Setting property in file: ${RE_CONFIG}/${FILENAME} from $env_var: $prop_name=${!env_var}"
            fi
        fi
    done
done

# We zip the config and remove the original file before copying the zip file to the project folder.
mkdir tmp
cd tmp
zip -r -j rules.zip /opt/ruleengine/rules/*
mv rules.zip /opt/ruleengine/rules

# Call the rule engine startup script
cd ${RE_HOME}
exec ./run_kafka_ruleengine.sh
