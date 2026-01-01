FROM eclipse-temurin:17-jre

# Install Python and dependencies
RUN apt-get update -qq && \
    apt-get install -y -qq python3 python3-pip procps && \
    rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip3 install -q --no-cache-dir --break-system-packages kafka-python pyspark pandas numpy

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
ENV JAVA_HOME="/opt/java/openjdk"
ENV HADOOP_USER_NAME="root"

# Run the prediction service
CMD ["python3", "/app/SparkML/predict_from_hive.py"]