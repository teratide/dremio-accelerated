FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y llvm-8 \
        clang-8 \
        autoconf \
        bash \
        cmake \
        g++ \
        gcc \
        make \
        curl \
        zip \
        git \
        openjdk-8-jdk \
        wget \
        libz-dev \
        maven \
        && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/

# Download & install protobuf
RUN wget -q https://github.com/protocolbuffers/protobuf/releases/download/v3.7.1/protobuf-cpp-3.7.1.tar.gz && \
        mkdir /protobuf &&\
        tar -xzf protobuf-cpp-3.7.1.tar.gz --strip-components 1 -C /protobuf &&\
        rm -f protobuf-cpp-3.7.1.tar.gz &&\
        cd /protobuf &&\
        ./configure &&\
        make &&\
        make install &&\
        ldconfig &&\
        cd / &&\
        rm -rf /protobuf

# Download & install apache arrow and gandiva jar file
RUN wget -q http://apache.40b.nl/arrow/arrow-1.0.0/apache-arrow-1.0.0.tar.gz &&\
        mkdir /arrow &&\
        tar -xzf apache-arrow-1.0.0.tar.gz --strip-components 1 -C /arrow &&\
        rm -f apache-arrow-1.0.0.tar.gz &&\
        mkdir /arrow/cpp/release &&\
        cd /arrow/cpp/release &&\
        cmake -DARROW_PARQUET=ON -DARROW_GANDIVA=ON -DARROW_GANDIVA_JAVA=ON -DARROW_DATASET=ON -DARROW_WITH_SNAPPY=ON .. &&\
        make &&\
        make install &&\
        cd /arrow/java &&\
        mvn clean install -pl gandiva -P arrow-jni -am -DskipTests -Darrow.cpp.build.dir=../../cpp/release/release &&\
        mkdir /gandiva-jar &&\
        cp /arrow/java/gandiva/target/arrow-gandiva-1.0.0.jar /gandiva-jar/arrow-gandiva-1.0.0.jar &&\
        cd /gandiva-jar &&\
        rm -rf /arrow

# Download Fletcher
RUN wget -q https://github.com/abs-tudelft/fletcher/archive/develop.zip &&\
        unzip -q develop.zip  &&\
        mv /fletcher-develop /fletcher &&\
        rm -f develop.zip && \
# Install Fletcher Runtime \
        mkdir /fletcher/runtime/cpp/build &&\
        cd /fletcher/runtime/cpp/build &&\
        cmake .. &&\
        make &&\
        make install &&\
# Install Fletcher Echo Platform
        mkdir /fletcher/platforms/echo/runtime/build &&\
        cd /fletcher/platforms/echo/runtime/build &&\
        cmake .. &&\
        make &&\
        make install

ENV GANDIVA_JAR_DIR /gandiva-jar
ENV LOCAL_LIBRARY_PATH /usr/local/lib

VOLUME /app
WORKDIR /app

CMD ["/bin/bash"]
