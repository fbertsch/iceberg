# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is Netflix's internal fork of Apache Iceberg, a high-performance table format for huge analytic tables. The project enables engines like Spark, Trino, Flink, Presto, and Hive to work with the same tables safely and concurrently.

**Key Resources:**
- Upstream Apache Iceberg: https://iceberg.apache.org
- Netflix-specific documentation: go/icebergdev
- Current branch: `netflix/1.4.x`
- Main branch for PRs: `netflix/1.4.x`

## Build System

The project uses Gradle with Java 8, 11, or 17. Netflix uses Nebula plugins for build and publishing.

### Common Build Commands

```bash
# Full build and test
./gradlew build

# Build without tests
./gradlew build -x test -x integrationTest

# Run tests only
./gradlew test

# Run all tests across modules
./gradlew allTests

# Clean build
./gradlew clean build

# Build specific module
./gradlew :iceberg-core:build

# Run integration tests
./gradlew integrationTest
```

### Running Single Tests

```bash
# Run a single test class
./gradlew :iceberg-core:test --tests TestClassName

# Run a single test method
./gradlew :iceberg-core:test --tests TestClassName.testMethodName

# Run tests matching a pattern
./gradlew :iceberg-core:test --tests '*Pattern*'
```

### Code Style

```bash
# Fix code style for default versions
./gradlew spotlessApply

# Fix code style for all versions of Spark/Hive/Flink
./gradlew spotlessApply -DallVersions

# Check code style
./gradlew spotlessCheck

# Run static analysis
./gradlew allStaticAnalysis
```

### API Compatibility Checks

```bash
# Check for API breaking changes
./gradlew revapi

# Accept a specific API break
./gradlew :iceberg-api:revapiAcceptBreak --justification "reason" \
  --code "java.method.addedToInterface" \
  --new "method signature"

# Accept all breaks in a project
./gradlew :iceberg-api:revapiAcceptAllBreaks --justification "reason"
```

## Multi-Version Support

The project supports multiple versions of Spark, Flink, and Hive. Versions are configured in `gradle.properties`:

- **Spark versions:** 3.2, 3.3, 3.4, 3.5 (default: 3.3, 3.5)
- **Scala versions:** 2.12, 2.13 (default: 2.12)
- **Flink versions:** 1.16 (default)
- **Hive versions:** 2, 3 (default: 2)

### Building for Specific Versions

```bash
# Build for specific Spark version
./gradlew build -DsparkVersions=3.3

# Build for multiple Spark versions
./gradlew build -DsparkVersions=3.3,3.5

# Build for all versions
./gradlew build -DallVersions

# Build with specific Scala version
./gradlew build -DscalaVersion=2.13
```

## Module Architecture

### Core Modules (Apache Iceberg)

- **`iceberg-api`**: Public Iceberg API - has strictest API compatibility requirements
- **`iceberg-core`**: Implementations of Iceberg API, Avro support - primary dependency for engines
- **`iceberg-common`**: Utility classes used across modules
- **`iceberg-data`**: Direct table access from JVM applications
- **`iceberg-parquet`**: Parquet file format support
- **`iceberg-orc`**: ORC file format support
- **`iceberg-arrow`**: Arrow memory format for Parquet reading
- **`iceberg-hive-metastore`**: Hive metastore Thrift client implementation

### Engine Integration Modules

- **`iceberg-spark`**: Spark Datasource V2 implementation with version-specific submodules
- **`iceberg-flink`**: Apache Flink integration
- **`iceberg-mr`**: Hive InputFormat and MapReduce integration
- **`iceberg-pig`**: Apache Pig LoadFunc implementation

### Netflix-Specific Modules

- **`bdp-iceberg`**: Netflix BDP (Big Data Platform) Iceberg client (shaded jar)
- **`bdp-iceberg-spark`**: Netflix-specific Spark integrations for versions 3.3 and 3.5
- **`metacat`**: Integration with Netflix's Metacat metadata service
- **`iceberg-metacat-spark-3.3`** and **`iceberg-metacat-spark-3.5`**: Spark-specific Metacat integrations
- **`bdp-view`**: Netflix view implementation
- **`bdp-iceberg-client`**: Netflix client with authentication (S3 auth signing, metatron)

### Cloud Provider Modules

- **`iceberg-aws`**: AWS S3, Glue, DynamoDB integration
- **`iceberg-gcp`**: Google Cloud Storage integration
- **`iceberg-azure`**: Azure Data Lake Storage integration
- **`iceberg-aliyun`**: Alibaba Cloud OSS integration

## Key Architectural Concepts

### Table Metadata Structure

Iceberg uses a tree structure for table metadata:
- **Snapshot**: Immutable state of table at a point in time
- **Manifest List**: List of manifest files for a snapshot
- **Manifest File**: List of data files with partition metadata
- **Data Files**: Actual Parquet/ORC/Avro files containing table data

### Catalog Implementations

Tables are tracked through catalog implementations:
- **HiveCatalog**: Uses Hive metastore (module: `iceberg-hive-metastore`)
- **MetacatCatalog**: Netflix's Metacat service (module: `metacat`)
- **RESTCatalog**: REST-based catalog service
- **Custom catalogs**: Can be implemented via `org.apache.iceberg.catalog.Catalog`

### Table Operations

Core operations are in `iceberg-core`:
- **BaseMetadataTable**: Base class for metadata tables
- **BaseMetastoreCatalog**: Base catalog implementation
- **BaseTableOperations**: Operations for reading/writing metadata
- **Snapshot management**: Creating, managing, and expiring snapshots
- **Schema evolution**: Adding/dropping/renaming columns
- **Partition evolution**: Changing partition specifications without rewriting data

## Testing Guidelines

### Test Framework

- **Prefer JUnit5** for new tests (`org.junit.jupiter.api`)
- Legacy tests use JUnit4 (`org.junit`)
- **Use AssertJ** for assertions over plain JUnit assertions
- **Use Awaitility** instead of `Thread.sleep()` for async tests

### AssertJ Examples

```java
// Good: provides rich context on failure
assertThat(x).isInstanceOf(Xyz.class);
assertThat(catalog.listNamespaces()).containsAll(expected);
assertThat(metadataFileLocations).isNotNull().hasSize(4);

// Testing exceptions
assertThatThrownBy(() -> catalog.createNamespace(deniedNamespace))
    .isInstanceOf(AccessDeniedException.class)
    .hasMessage("User 'testUser' has no permission to create namespace");
```

### Awaitility for Async Tests

```java
deleteTablesAsync();
Awaitility.await("Tables were not deleted")
    .atMost(5, TimeUnit.SECONDS)
    .untilAsserted(() -> assertThat(tables()).isEmpty());
```

## Code Style Guidelines

### Java Style

- **Continuation indents**: 4 spaces (2 indents)
- **Line breaks**: Break at same semantic level
- **Method naming**:
  - Keep names short but clear
  - Avoid `get` prefix unless it's a Java bean
  - Use verbs that read naturally in English: `transform.preservesOrder()`
- **Boolean arguments**: Avoid in public methods - create separate methods instead
- **Config naming**:
  - Use `-` to link words in one concept: `access-key-id`
  - Use `.` for hierarchy: `s3.access-key-id`

### Good Line Breaking

```java
// GOOD: break at same semantic level
doSomething(
    new ArgumentClass(1, 2),
    3);

// GOOD: method calls at same level
SomeObject myNewObject = SomeObject
    .builder(schema, partitionSpec, sortOrder)
    .withProperty("x", "1")
    .build();
```

## API Evolution and Deprecation

### Semantic Versioning Requirements

- **`iceberg-api`**: Major version required for breaking changes (enforced by Revapi)
- **Core modules** (`core`, `common`, `data`, `orc`, `parquet`): Minor version deprecation cycle
- **Other modules**: Deprecation at committer discretion

### Deprecation Process

All deprecated code must include:
1. `@Deprecated` annotation
2. `@deprecated` javadoc with version and alternative
3. Replacement of existing usages

```java
/**
 * Set the sequence number for this manifest entry.
 *
 * @param sequenceNumber a sequence number
 * @deprecated since 1.0.0, will be removed in 1.1.0; use dataSequenceNumber() instead.
 */
@Deprecated
void sequenceNumber(long sequenceNumber);
```

### Adding Non-Breaking API Changes

To add methods to interfaces without breaking the API, use default implementations:

```java
default ManageSnapshots createBranch(String name) {
  throw new UnsupportedOperationException(
      this.getClass().getName() + " doesn't implement createBranch(String)");
}
```

## Netflix-Specific Build Configuration

### Publishing

- Uses Netflix Nebula plugins for publishing to Artifactory
- Only Spark-related artifacts are published (others are skipped)
- Publishing skipped for Scala 2.13 builds of non-Spark modules

### Dependencies

Netflix-specific dependencies (in `versions.props`):
- S3 authentication signing (s3authsigncde)
- Metatron IPC for authentication
- Metacat client for metadata service
- Netflix Spectator for metrics
- BDP libraries (S3FS, Gandalf, etc.)

### Shadow JARs

Netflix modules use shadow JARs with relocation to avoid conflicts:
- Relocation prefix: `org.apache.iceberg.bdp.shaded`
- Key relocations: Guava, Jackson, Parquet, Avro, Arrow, Metacat dependencies

## Development Workflow

### Before Making Changes

1. Check `CONTRIBUTING.md` for detailed contribution guidelines
2. Use `./gradlew spotlessApply` to format code
3. Run `./gradlew revapi` to check API compatibility (for `iceberg-api` changes)
4. Run tests: `./gradlew test` or `./gradlew :module:test`

### Project-Specific Patterns

- Iceberg uses immutable data structures extensively
- Prefer builders for complex object construction
- Table operations use a commit-retry pattern for optimistic concurrency
- Metadata operations should be atomic and fail-safe
- Always close resources properly (AutoCloseable)

## Test Logs

Test logs are written to `build/testlogs/<module-name>.log` for debugging test failures.

## Common Issues

### Java Version

- Project requires Java 8, 11, or 17
- Netflix uses Java 8 (configured in `.newt.yml`)
- Java 17 requires additional JVM args (automatically configured in `build.gradle`)

### Guava Conflicts

- Project uses bundled, relocated Guava (`iceberg-bundled-guava`)
- Direct Guava dependencies are excluded in most modules
- If you see Guava conflicts, check that the module excludes `com.google.guava:guava`

### Spark Version Conflicts

- Spark modules are version-specific (e.g., `spark-3.3_2.12`)
- Ensure you're depending on the correct Spark version module
- Use `./gradlew dependencies` to check transitive dependencies
