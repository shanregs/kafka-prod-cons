# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project state

This is a Spring Boot + Apache Kafka learning project, currently at the Spring Initializr skeleton stage: only the application entry point (`KafkaProdConsApplication`) and a context-load smoke test exist. No producer, consumer, topic config, or REST endpoints have been implemented yet.

- Group/Artifact: `com.shan.kafka:kafka-prod-cons`
- Spring Boot: `4.1.1` (parent POM)
- Java: `21`
- Dependencies: `spring-boot-starter-kafka`, `spring-boot-starter-webmvc` (+ their `-test` counterparts)

## Commands

Use the Maven wrapper (no local Maven install required). On Windows use `mvnw.cmd`; the examples below use the Windows form since this repo is developed on Windows.

```powershell
# Build (compile + test)
.\mvnw.cmd clean install

# Run the app
.\mvnw.cmd spring-boot:run

# Run all tests
.\mvnw.cmd test

# Run a single test class
.\mvnw.cmd test -Dtest=KafkaProdConsApplicationTests

# Run a single test method
.\mvnw.cmd test -Dtest=KafkaProdConsApplicationTests#contextLoads
```

## Architecture notes

- Base package: `com.shan.kafka.kafkaprodcons`. Follow this package for any new producer/consumer/config/controller classes.
- `src/main/resources/application.yaml` only sets `spring.application.name` — Kafka broker connection, topics, and serializer/deserializer config still need to be added here (or in a `@Configuration` class) as the project grows.
- Since `spring-boot-starter-kafka` and `spring-boot-starter-webmvc` are both present, the intended shape is a web-triggered producer (REST endpoint publishes to Kafka) paired with a `@KafkaListener`-based consumer — typical for this kind of learning exercise.
- The POM has empty `<license>`/`<developers>`/`<scm>` overrides — this is intentional (per `HELP.md`) to block unwanted inheritance from the `spring-boot-starter-parent` POM; don't remove them without also removing the parent inheritance concern.
