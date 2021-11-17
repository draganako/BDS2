export SPARK_APPLICATION_ARGS="${APP_ARGS_CSV_FILE_PATH}"

sh /template.sh
cat /template.sh
echo ${APP_ARGS_CSV_FILE_PATH}
echo ${SPARK_APPLICATION_JAR_NAME}
echo ${SPARK_APPLICATION_JAR_LOCATION}

ls /usr/src/app
cat /submit.sh
echo "DONE"
