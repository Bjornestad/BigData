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

USER appuser