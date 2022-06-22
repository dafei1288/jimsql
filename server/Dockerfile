#FROM eclipse-temurin:17-jre-alpine
FROM openjdk:17-oracle
MAINTAINER dafei1288
ENV rootdir="/jimsql"
EXPOSE 8821
EXPOSE 8825

WORKDIR ${rootdir}
#VOLUME ${rootdir}/data
ARG JAR_FILE=target/*jar-with-dependencies.jar

COPY ${JAR_FILE} ${rootdir}/jimsql.jar
COPY target/classes/datadir/ ${rootdir}/data/

ENTRYPOINT ["sh","-c","java -jar ${JAVA_OPTS}  jimsql.jar ${JAVA_ARGS}"]


