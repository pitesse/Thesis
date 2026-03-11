# multi-stage build for the flink stream processor.
# stage 1 compiles the fat jar inside a maven container (no host java/maven needed).
# stage 2 copies the jar into the flink runtime image at a fixed path.

# build
FROM maven:3.9-eclipse-temurin-17 AS builder
WORKDIR /build

# copy pom first and download deps for layer caching.
# source code changes only re-run `mvn package`, not dependency download.
COPY pom.xml .
RUN mvn dependency:go-offline -q

COPY src/ src/
RUN mvn clean package -q -DskipTests

# runtime
FROM flink:1.20-java17
COPY --from=builder /build/target/f1-telemetry-processor-1.0-SNAPSHOT.jar \
    /opt/flink/usrlib/f1-stream-processor.jar
