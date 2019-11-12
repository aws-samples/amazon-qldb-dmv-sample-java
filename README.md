# Amazon QLDB Java DMV Sample App

The samples in this project demonstrate several uses of Amazon QLDB.

## Requirements

### Basic Configuration

See [Accessing Amazon QLDB](https://docs.aws.amazon.com/qldb/latest/developerguide/accessing.html) for information on connecting to AWS.

### Java 8 and Gradle

The examples are written in Java 8 using the Gradle build tool. Java 8 must be installed to build the examples, however 
the Gradle wrapper is bundled in the project and does not need to be installed. Please see the link below for more 
detail to install Java 8 and information on Gradle:

* [Java 8 Installation](https://docs.oracle.com/javase/8/docs/technotes/guides/install/install_overview.html)
* [Gradle](https://gradle.org/)
* [Gradle Wrapper](https://docs.gradle.org/3.3/userguide/gradle_wrapper.html)

## Running the Sample code

The sample code creates a ledger with tables and indexes, and inserts some documents into those tables,
among other things. Each of the examples in this project can be run in the following way:

Windows:

```
gradlew run -Dtutorial=CreateLedger
```

Unix:

```
./gradlew run -Dtutorial=CreateLedger
```

The above example will build the CreateLedger class with the necessary dependencies and create a ledger named:
`vehicle-registration`. You may run other examples after creating a ledger.

## Documentation

To generate JavaDoc documentation for this repository, run the following:

```
./gradlew javadoc
```

Generated documentation can be found in `./build/docs/javadoc/`.

## License

This library is licensed under the Apache 2.0 license.
