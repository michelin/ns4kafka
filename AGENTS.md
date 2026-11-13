# Agent Instructions

## Repository Structure

Single-module Gradle Micronaut project with the base package `com.michelin.ns4kafka`.

## Commands

- Build with `./gradlew build`.
- Run the tests with `./gradlew test`.
- Run `./gradlew spotlessApply` before committing to apply Palantir Java Format.

## Coding Standards

- Target Java 25.
- Never use `var`. Always declare variables with their explicit type.
- Use Lombok for models and loggers (`@Data`, `@Builder`, `@Slf4j`, etc.), but use explicit constructor injection in beans.
- Prefer guard clauses (early returns) over `if ... else ...`.
- Code follows Palantir Java Format.
- Add minimal Javadoc to every method in production code (`src/main`), including constructors and `@Override` methods, with descriptions for parameters and return values. Start each description with an uppercase letter.

## Testing Standards

- Name test classes `<ClassName>Test` and integration test classes `<Feature>IntegrationTest`.
- Name unit tests with the "should..." convention (e.g. `shouldValidateEncryptAndDecryptAes256Gcm`).
- Use JUnit Jupiter assertions (`org.junit.jupiter.api.Assertions`).
- Use Mockito with `@ExtendWith(MockitoExtension.class)`.
- Use Testcontainers for Kafka integration tests.
