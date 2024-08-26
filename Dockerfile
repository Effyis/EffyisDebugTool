FROM maven:3.8.5-openjdk-17 AS BUILDER
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY DebugTool/pom.xml /usr/src/app/
COPY DebugTool/src /usr/src/app/src/

COPY settings.xml /root/.m2/
COPY settings-security.xml /root/.m2/

RUN mvn package

FROM openjdk:17-jdk-slim

WORKDIR /app
COPY --from=builder /usr/src/app/target/*.jar /app/debugtool.jar

EXPOSE 8080
EXPOSE 1099

CMD java -Dfile.encoding=UTF-8 -jar debugtool.jar