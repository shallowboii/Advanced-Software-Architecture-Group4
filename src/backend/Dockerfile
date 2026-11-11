FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY src/backend/pom.xml ./pom.xml
RUN mvn -B dependency:go-offline
COPY src/backend/src ./src
RUN mvn -B verify
FROM eclipse-temurin:21-jre-jammy
RUN useradd --system --uid 10001 --create-home factory
WORKDIR /app
COPY --from=build /build/target/beerfactory-backend-1.0.0.jar /app/backend.jar
COPY src/config /app/config
USER factory
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/backend.jar"]
