FROM python:3.11-bullseye

# Install Java and dependencies
RUN apt-get update -qq && \
    apt-get install -y -qq openjdk-17-jre-headless procps && \
    rm -rf /var/lib/apt/lists/*

# Install Python packages
# Added confluent-kafka for better compatibility
RUN pip3 install -q --no-cache-dir kafka-python pyspark pandas numpy confluent-kafka

# Set working directory
WORKDIR /app

# Copy SparkML code and models
COPY SparkML /app/SparkML

# Log files before deletion
RUN echo "Files before cleanup:" && find /app/SparkML -name "*.crc"

# Remove .crc files to avoid checksum errors
RUN find /app/SparkML -name "*.crc" -type f -delete

# Log files after deletion to verify
RUN echo "Files after cleanup (should be empty):" && find /app/SparkML -name "*.crc"

# Set environment variables
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV HADOOP_USER_NAME="root"

# Run the prediction service
CMD ["python3", "/app/SparkML/aggregate_and_predict_service.py"]