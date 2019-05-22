#!/bin/bash -x

SENSITIVE_PROPERTIES="CONNECT_SASL_JAAS_CONFIG,CONNECT_CONSUMER_SASL_JAAS_CONFIG,CONNECT_PRODUCER_SASL_JAAS_CONFIG,PASSWORD"
RE_HOME=/opt/rule-engine/
TMP_DIR=$(mktemp -d)

##
## Generate certificates
##

if [ -z ${RULE_ENGINE_CERT+x} ]; then
    echo "RULE_ENGINE_CERT, the certificate is not set"
fi;

if [ -z ${RULE_ENGINE_KEY+x} ]; then
    echo "RULE_ENGINE_KEY the key to the certificate is not set"
fi;

if [ -z ${RULE_ENGINE_PSWD+x} ]; then
    echo "RULE_ENGINE_PSWD the password to decode the key is not set"
fi;

if [ -z ${RULE_ENGINE_CHAIN+x} ]; then
    echo "RULE_ENGINE_CHAIN the certificate chain is not set"
fi;

if [ -z ${RULE_ENGINE_USER+x} ]; then
    echo "RULE_ENGINE_USER the certificate user is not set"
fi;


echo -e ${RULE_ENGINE_CERT} > ${TMP_DIR}/cert.pem
echo -e ${RULE_ENGINE_KEY} > ${TMP_DIR}/key.pem
openssl rsa -in ${TMP_DIR}/key.pem -out ${TMP_DIR}/cert.key -passin pass:${RULE_ENGINE_PSWD}
echo -e ${RULE_ENGINE_CHAIN} > ${TMP_DIR}/chain.pem

openssl pkcs12 -export -name ${RULE_ENGINE_USER} \
               -in ${TMP_DIR}/cert.pem -inkey ${TMP_DIR}/cert.key \
               -chain -CAfile ${TMP_DIR}/chain.pem \
               -out ${TMP_DIR}/keystore.pfx -name ${RULE_ENGINE_USER} -passout pass:${RULE_ENGINE_PSWD}

keytool -importkeystore \
        -deststorepass ${RULE_ENGINE_PSWD} -destkeypass ${RULE_ENGINE_PSWD} -deststoretype pkcs12 -destkeystore ${RE_HOME}/config/keystore.jks \
        -srckeystore ${TMP_DIR}/keystore.pfx -srcstoretype PKCS12 -srcstorepass ${RULE_ENGINE_PSWD} \
        -alias ${RULE_ENGINE_USER}

# Create the truststore
cat ${TMP_DIR}/chain.pem | awk -v tmp_dir=${TMP_DIR} 'split_after == 1 {n++;split_after=0} /-----END CERTIFICATE-----/ {split_after=1} {print > tmp_dir"/extract_cert" n ".pem"}'

for CERT in `ls -1 ${TMP_DIR} | grep extract` ; do
    keytool -keystore ${RE_HOME}/config/truststore.jks -alias ${CERT} -import -noprompt -deststorepass ${RULE_ENGINE_PSWD} -file ${TMP_DIR}/${CERT}
done
# keytool -keystore ${RE_HOME}/config/truststore.jks -alias CARoot -import -noprompt -deststorepass ${RULE_ENGINE_PSWD} -file ${TMP_DIR}/chain.pem


##
## Inject and over write configuration and properties for the consumer, producer and the rule engine.
##

PREFIXES="KAFKA_CONSUMER,KAFKA_PRODUCER,KAFKA_RULEENGINE"

for PREFIX in `echo ${PREFIXES} | sed "s/,/ /g"` ; do
    FILENAME=`echo "${PREFIX}" | sed -r "s/^${PREFIX}(.*)=.*/\1/g" | tr '[:upper:]' '[:lower:]' | tr _ - | sed "s/$/.properties/g"`

    for VAR in `env` ; do
        env_var=`echo "$VAR" | sed -r "s/(.*)=.*/\1/g"`

        if [[ $env_var =~ ^${PREFIX} ]]; then
            case $env_var in
                "RULEENGINE_FAILED_MINIMUM_NUMBER_OF_GROUPS")
                    prop_name="ruleengine.failed.minimum_number_of_groups"
                ;;
                *)
                    prop_name=`echo "$VAR" | sed -r "s/^${PREFIX}_(.*)=.*/\1/g" | tr '[:upper:]' '[:lower:]' | tr _ .`
            esac;

            if egrep -q "(^|^#)$prop_name=" ${RE_HOME}/config/${FILENAME}; then
                # note that no config names or values may contain an '@' char
                sed -r -i "s@(^|^#)($prop_name)=(.*)@\2=${!env_var}@g" ${RE_HOME}/config/${FILENAME}
            else
                #echo "Adding property $prop_name=${!env_var}"
                echo "$prop_name=${!env_var}" >> ${RE_HOME}/config/${FILENAME}
            fi

            if [[ "$SENSITIVE_PROPERTIES" = *"$env_var"* ]]; then
                echo "--- Setting property from $env_var: $prop_name=[hidden]"
            else
                echo "--- Setting property from $env_var: $prop_name=${!env_var}"
            fi
        fi
    done
done

# We zip the config and remove the original file before copying the zip file to the project folder.
rm -rf jobs
mkdir jobs
zip -r -j jobs.zip project/*
mv jobs.zip jobs

# Call the rule engine startup script
cd ${RE_HOME}
exec bin/run_kafka_ruleengine.sh

