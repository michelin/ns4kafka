FROM eclipse-temurin:21-jre-alpine

# Use application.jar as the name to comply with legacy Micronaut Dockerfile
COPY build/libs/ns4kafka-*.jar /home/app/application.jar

RUN apk update \
    && echo -e "#!/bin/sh\n\njava -jar /home/app/application.jar \"\$@\"" > /usr/bin/ns4kafka \
    && chmod +x /usr/bin/ns4kafka \
    && apk upgrade \
    && rm -rf /var/cache/apk/*

ENTRYPOINT ["ns4kafka"]