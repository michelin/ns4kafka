native-image -cp H:\projects\ns4kafka\cli\build\libs\kafkactl-0.1.jar \
  -H:+ReportUnsupportedElementsAtRuntime -H:Name=kafkactl \
  -H:IncludeResourceBundles=org.ocpsoft.prettytime.i18n.Resources \
  --no-server com.michelin.ns4kafka.cli.KafkactlCommand