# ===============================
# Build stage (Java 21)
# ===============================
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app

# copy pom first for better layer caching
COPY pom.xml .
RUN mvn -q -e -B dependency:go-offline

# copy source
COPY src ./src

# build fat-jar
RUN mvn -q -DskipTests package


# ===============================
# Runtime stage (Java 21 JRE)
# ===============================
FROM eclipse-temurin:21-jre
WORKDIR /app

COPY --from=build /app/target/aggregator.jar /app/aggregator.jar

# optional: set default JVM options for data processing
ENV JAVA_OPTS="-Xms512m -Xmx3g -XX:+UseG1GC"

ENTRYPOINT ["sh","-c","java $JAVA_OPTS -jar /app/aggregator.jar"]
