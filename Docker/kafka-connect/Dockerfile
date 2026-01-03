FROM confluentinc/cp-kafka-connect:7.5.0

USER root

# Copy plugins from build context
COPY jdbc.zip /tmp/jdbc.zip
COPY hdfs.zip /tmp/hdfs.zip

# Install plugins
RUN echo 'Installing plugins...' && \
    mkdir -p /plugins/kafka-connect-jdbc/lib && \
    mkdir -p /plugins/kafka-connect-hdfs/lib && \

    # Unzip and copy JDBC
    mkdir -p /tmp/jdbc_extracted && \
    cd /tmp/jdbc_extracted && \
    jar xf /tmp/jdbc.zip && \
    cp */lib/*.jar /plugins/kafka-connect-jdbc/lib/ && \
    cd / && \

    # Unzip and copy HDFS
    mkdir -p /tmp/hdfs_extracted && \
    cd /tmp/hdfs_extracted && \
    jar xf /tmp/hdfs.zip && \
    cp */lib/*.jar /plugins/kafka-connect-hdfs/lib/ && \
    cd / && \

    # Cleanup
    rm -rf /tmp/jdbc* /tmp/hdfs* && \
    echo 'Plugins installed successfully!'

# Download Hive 3.1.3 dependencies for HDFS Sink Hive Integration
# These are required for the connector to talk to the Hive Metastore
RUN curl -L -o /plugins/kafka-connect-hdfs/lib/hive-metastore-3.1.3.jar https://repo1.maven.org/maven2/org/apache/hive/hive-metastore/3.1.3/hive-metastore-3.1.3.jar && \
    curl -L -o /plugins/kafka-connect-hdfs/lib/hive-common-3.1.3.jar https://repo1.maven.org/maven2/org/apache/hive/hive-common/3.1.3/hive-common-3.1.3.jar && \
    curl -L -o /plugins/kafka-connect-hdfs/lib/hive-serde-3.1.3.jar https://repo1.maven.org/maven2/org/apache/hive/hive-serde/3.1.3/hive-serde-3.1.3.jar && \
    curl -L -o /plugins/kafka-connect-hdfs/lib/hive-service-3.1.3.jar https://repo1.maven.org/maven2/org/apache/hive/hive-service/3.1.3/hive-service-3.1.3.jar && \
    curl -L -o /plugins/kafka-connect-hdfs/lib/hive-shims-common-3.1.3.jar https://repo1.maven.org/maven2/org/apache/hive/hive-shims-common/3.1.3/hive-shims-common-3.1.3.jar && \
    curl -L -o /plugins/kafka-connect-hdfs/lib/libfb303-0.9.3.jar https://repo1.maven.org/maven2/org/apache/thrift/libfb303/0.9.3/libfb303-0.9.3.jar && \
    # hive-exec is large but often needed for SerDe/Format classes
    curl -L -o /plugins/kafka-connect-hdfs/lib/hive-exec-3.1.3.jar https://repo1.maven.org/maven2/org/apache/hive/hive-exec/3.1.3/hive-exec-3.1.3.jar

USER appuser