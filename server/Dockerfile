FROM eclipse-temurin:17-jre-alpine
MAINTAINER dafei1288
ENV rootdir="/app"
EXPOSE 8821

WORKDIR ${rootdir}
VOLUME ${rootdir}/data

ARG JAR_FILE=target/*jar-with-dependencies.jar

COPY ${JAR_FILE} ${rootdir}/app.jar



ENTRYPOINT ["sh","-c","java -jar ${JAVA_OPTS}  app.jar ${JAVA_ARGS}"]


