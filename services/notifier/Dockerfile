# PayStream Reliability Lab - notifier image
# JAR is pre-built locally; context is repo root.
FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S app && adduser -S app -G app
USER app
WORKDIR /app
COPY services/notifier/target/notifier-0.1.0.jar app.jar
ENV JAVA_OPTS="-Xmx512m"
EXPOSE 8083
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
