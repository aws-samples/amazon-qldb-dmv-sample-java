# Amazon QLDB Java DMV Sample for Streams


The StreamJournal sample provides guidance on how to use Amazon QLDB Streams.

## Requirements

### Basic Configuration

See [Accessing Amazon QLDB](https://docs.aws.amazon.com/qldb/latest/developerguide/accessing.html) for information on connecting to AWS.

### Java 8 and Gradle

The StreamJournal sample is written in Java 8 using the Gradle build tool. Java 8 must be installed to build the examples, however 
the Gradle wrapper is bundled in the project and does not need to be installed. Please see the link below for more 
detail to install Java 8 and information on Gradle:

* [Java 8 Installation](https://docs.oracle.com/javase/8/docs/technotes/guides/install/install_overview.html)
* [Gradle](https://gradle.org/)
* [Gradle Wrapper](https://docs.gradle.org/3.3/userguide/gradle_wrapper.html)

## Running the Sample code

The StreamJournal class contains a code example that demonstrates the following operations:

* Create a ledger named vehicle-registration - `createLedger()`

> **_Note:_** Before running this program, make sure that you don't already have an active ledger named vehicle-registration.

* Create tables named Person, Vehicle, DriversLicense and VehicleRegistration - `createTables()`

* Load sample data into the tables - `insertDocuments()`

* Create an [AWS Kinesis](https://aws.amazon.com/kinesis/) data stream, an IAM role that enables QLDB to assume write permissions for the Kinesis data stream, and a QLDB journal stream - `createQldbStream()`

* Use the [Kinesis Client Library](https://docs.aws.amazon.com/streams/latest/dev/shared-throughput-kcl-consumers.html#shared-throughput-kcl-consumers-overview) to start a stream reader that processes the Kinesis data stream and logs each QLDB data record - `startStreamReader()`

* Use the stream data to validate the hash chain of the vehicle-registration ledger - `validateStreamRecordsHashChain()`

* Clean up all resources by stopping the stream reader, cancelling the QLDB stream, deleting the ledger, and deleting the Kinesis data stream  - `cleanupQldbResources(), cleanupKinesisResources()`

To run the StreamJournal program, enter the following command from your project root directory.

Windows:

```
gradlew run -Dtutorial=streams.StreamJournal
```

Unix:

```
./gradlew run -Dtutorial=streams.StreamJournal
```
