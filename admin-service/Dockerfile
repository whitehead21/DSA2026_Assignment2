FROM eclipse-temurin:21-jre
WORKDIR /app
COPY target/bin/*.jar app.jar
EXPOSE 8087
CMD ["java", "-jar", "app.jar"]
