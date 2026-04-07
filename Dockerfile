FROM maven:3.9.6-amazoncorretto-17-al2023 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -q
COPY src ./src
RUN mvn package -DskipTests -q

FROM amazoncorretto:17-al2023
WORKDIR /app
COPY --from=build /app/target/twitter-service-1.0-SNAPSHOT.jar app.jar

ENV DB_URL=jdbc:mysql://mysql-service:3306/twitter?useUnicode=true&characterEncoding=UTF-8&connectionCollation=utf8mb4_unicode_ci
ENV DB_USER=root
ENV DB_PASSWORD=password
ENV AUTH_SERVICE_URL=http://auth-service:9000
ENV ANDREW_ID=sgisubiz

EXPOSE 8080
ENTRYPOINT ["java", "-Xmx512m", "-Xms256m", "-jar", "app.jar"]