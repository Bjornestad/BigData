import pytest
import os
import requests
import psycopg2
from kafka import KafkaAdminClient
from kafka.errors import NoBrokersAvailable

# Configuration
# Local: Defaults assume localhost port-forwarding.
# Helm/K8s: Set env vars to service DNS names (e.g. "kafka:9092") and run tests inside the cluster.
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "power_grid")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "changeme123")
HIVE_METASTORE_URI = os.getenv("HIVE_METASTORE_URI", "thrift://localhost:9083")

@pytest.mark.integration
def test_kafka_connection_and_topics():
    """Verify Kafka is reachable and required topics exist"""
    required_topics = {
        "energy_actual", 
        "energy_predictions", 
        "weather_raw_avro"
    }
    
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
        topics = admin_client.list_topics()
        
        print(f"Found topics: {topics}")
        
        # Check if critical topics exist
        missing = required_topics - set(topics)
        
        # Note: Some topics might be created lazily, so we warn instead of fail if strictness isn't required
        if missing:
            pytest.fail(f"Missing required Kafka topics: {missing}")
            
    except NoBrokersAvailable:
        pytest.fail(f"Could not connect to Kafka at {KAFKA_BOOTSTRAP}")
    except Exception as e:
        pytest.fail(f"Kafka check failed: {e}")

@pytest.mark.integration
def test_schema_registry_reachable():
    """Verify Schema Registry is up and running"""
    try:
        response = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects")
        assert response.status_code == 200
        subjects = response.json()
        print(f"Registered subjects: {subjects}")
        
        # Check for expected schemas
        # Note: Subject names usually follow topic-value convention
        expected_subjects = [
            "energy_predictions-value",
            "energy_actual-value",
            "weather_raw_avro-value"
        ]
        for subject in expected_subjects:
            if subject not in subjects:
                print(f"Warning: Schema subject '{subject}' not found.")
                
    except requests.exceptions.ConnectionError:
        pytest.fail(f"Could not connect to Schema Registry at {SCHEMA_REGISTRY_URL}")

@pytest.mark.integration
def test_timescaledb_connection():
    """Verify TimescaleDB connection and table existence"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cur = conn.cursor()
        
        # Check if measurements table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'measurements'
            );
        """)
        exists = cur.fetchone()[0]
        assert exists, "Table 'measurements' does not exist in TimescaleDB"
        
        # Check if it's a hypertable
        cur.execute("SELECT * FROM timescaledb_information.hypertables WHERE hypertable_name = 'measurements';")
        is_hypertable = cur.fetchone()
        assert is_hypertable, "'measurements' is not a hypertable"
        
        cur.close()
        conn.close()
        
    except psycopg2.OperationalError as e:
        pytest.fail(f"Could not connect to TimescaleDB: {e}")

@pytest.mark.integration
def test_hdfs_reachable():
    """Verify HDFS is reachable via PySpark"""
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("TestHDFSIntegration") \
            .getOrCreate()
            
        # Access Hadoop FileSystem via Spark Context's JVM
        sc = spark.sparkContext
        conf = sc._jsc.hadoopConfiguration()
        FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
        Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
        
        fs = FileSystem.get(conf)
        
        # List root directory
        status = fs.listStatus(Path("/"))
        files = [str(f.getPath().getName()) for f in status]
        print(f"HDFS Root: {files}")
        
        # Check for warehouse directory
        exists = fs.exists(Path("/user/hive/warehouse"))
        assert exists, "/user/hive/warehouse directory missing"
        
        spark.stop()
        
    except Exception as e:
        pytest.fail(f"HDFS check failed: {e}")

@pytest.mark.integration
def test_hive_metastore_connection():
    """Verify Hive Metastore connectivity via Spark"""
    # This requires PySpark and valid Hadoop config
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("TestHiveIntegration") \
            .config("spark.hadoop.hive.metastore.uris", HIVE_METASTORE_URI) \
            .enableHiveSupport() \
            .getOrCreate()
            
        # List tables
        tables = spark.sql("SHOW TABLES").collect()
        table_names = [row.tableName for row in tables]
        print(f"Hive Tables: {table_names}")
        
        expected_tables = ["weather_raw_avro", "energy_actual", "weather_area_hourly_historical"]
        missing = [t for t in expected_tables if t not in table_names]
        
        if missing:
            print(f"Warning: Missing Hive tables: {missing}")
            
        spark.stop()
        
    except Exception as e:
        pytest.fail(f"Hive Metastore check failed: {e}")
