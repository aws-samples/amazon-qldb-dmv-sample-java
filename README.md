# AmazonQLDB Samples

The samples in this project demonstrate several uses of Amazon QLDB.

## Requirements

### Basic Configuration

You need to set up your AWS security credentials before the sample code is able
to connect to AWS. You can do this by creating a file named "config" at `~/.aws/` 
(`C:\Users\USER_NAME\.aws\` for Windows users) and saving the following lines in the file:

    [default]
    aws_access_key_id = <your access key id>
    aws_secret_access_key = <your secret key>
    region = us-east-1 <or other region>

Alternatively, us the [AWS CLI](https://aws.amazon.com/cli/) and run `aws configure` to 
step through a setup wizard for the config file.

See the [Security Credentials](http://aws.amazon.com/security-credentials) page
for more information on getting your keys.

### Java 8 and Gradle

The examples are written in Java 8 using the Gradle build tool. Java 8 must be installed to build the examples, however 
the Gradle wrapper is bundled in the project and does not need to be installed. Please see the link below for more 
detail to install Java 8 and information on Gradle:

* [Java 8 Installation](https://docs.oracle.com/javase/8/docs/technotes/guides/install/install_overview.html)
* [Gradle]()
* [Gradle Wrapper](https://docs.gradle.org/3.3/userguide/gradle_wrapper.html)

# Running the Sample code

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
