# Dremio Accelerated

Integrates Fletcher into Dremio.

## Quickstart: How to build and run Dremio Accelerated

### (a) Prerequisites

* JDK 8 (OpenJDK or Oracle)
* (Optional) Maven 3.3.9 or later (using Homebrew: `brew install maven`)

Run the following commands to verify that you have the correct versions of Maven and JDK installed:

    java -version
    mvn --version

### (b) Clone the Repository
For now all work is done in the `c_filter` branch, this will be merged to master at a later stage.

    git clone -b c_filter https://github.com/teratide/dremio-accelerated

### (c) Build the Java Code

    cd dremio
    mvn clean install -DskipTests (or ./mvnw clean install -DskipTests if maven is not installed on the machine)

The "-DskipTests" option skips most of the tests. Running all tests takes a long time.

### (d) Build Native Code
Compile the native code to your `java.library.path`. Example on linux:

    g++ -fPIC -I"$JAVA_HOME/include" -I"$JAVA_HOME/include/linux" -shared -o /usr/lib64/libNativeFilter.so NativeFilter.cpp

### (e) Run/Install

#### Run

    distribution/server/target/dremio-community-{DREMIO_VERSION}/dremio-community-{DREMIO_VERSION}/bin/dremio start

OR to start a server with a default user (dremio/dremio123)

    mvn compile exec:exec -pl dac/daemon

Once run, the UI is accessible at:

    http://localhost:9047

#### Production Install

##### (1) Unpack the tarball to install.

    mkdir /opt/dremio
    tar xvzf distribution/server/target/*.tar.gz --strip=1 -C /opt/dremio

##### (2) Start Dremio Embedded Mode

    cd /opt/dremio
    bin/dremio

