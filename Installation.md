# Installation guide

## Install from RPMs
TODO: RPMs

## Build Dremio Accelerated Yourself

### Prerequisites

* JDK 8 (OpenJDK or Oracle)
* (Optional) Maven 3.3.9 or later (using Homebrew: `brew install maven`)
* [Apache Arrow 2.0.*](https://arrow.apache.org/install/)
* Make

Run the following commands to verify that you have the correct versions of Maven and JDK installed:

    java -version
    mvn --version

### Clone the Repository

    git clone https://github.com/teratide/dremio-accelerated

### Build the Java Code

    cd dremio-accelerated
    mvn clean install -DskipTests (or ./mvnw clean install -DskipTests if maven is not installed on the machine)

The "-DskipTests" option skips most of the tests. Running all tests takes a long time.

### Build Native Code
A MakeFile is provided to compile the native code to the `java.library.path`. Depending on your environment, the location of might need to be altered on line 2 and 3.

    cd native
    make all

### Run/Install

#### Run

    distribution/server/target/dremio-community-{DREMIO_VERSION}/dremio-community-{DREMIO_VERSION}/bin/dremio start

OR to start a server with a default user (dremio/dremio123)

    mvn compile exec:exec -pl dac/daemon

Once run, the UI is accessible at:

    http://localhost:9047


