@ECHO OFF
set MICRONAUT_CONFIG_FILES=%userprofile%\.kafkactl\config.yml
%JAVA_HOME%\bin\java -jar .\cli\build\libs\kafkactl-0.1.jar %*
