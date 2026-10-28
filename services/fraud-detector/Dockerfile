# PayStream Reliability Lab - fraud-detector image
# JAR is pre-built locally; context is repo root.
# Uses eclipse-temurin:21-jre (Debian) not alpine — RocksDB needs libstdc++.
FROM eclipse-temurin:21-jre
RUN groupadd -r app && useradd -r -g app app
USER app
WORKDIR /app
COPY services/fraud-detector/target/fraud-detector-0.1.0.jar app.jar
ENV JAVA_OPTS="-Xmx512m"
EXPOSE 8082
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
