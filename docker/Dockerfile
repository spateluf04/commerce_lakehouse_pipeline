# One image for Airflow, the Spark jobs and the tests, so all three run in exactly the same environment.
ARG AIRFLOW_VERSION=3.3.2
FROM apache/airflow:${AIRFLOW_VERSION}-python3.11
ARG AIRFLOW_VERSION

# Spark runs on the JVM. Copying a Temurin 17 JRE (rather than apt-installing one)
# keeps this independent of which Debian release the Airflow base image uses.
COPY --from=eclipse-temurin:17-jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk \
    PATH="/opt/java/openjdk/bin:${PATH}"

# Bake the Spark jars into the image instead of letting Spark download them on every
# cold start. Versions must match src/lakehouse_common/spark.py and requirements.txt.
ARG DELTA_VERSION=3.3.3
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG MAVEN=https://repo1.maven.org/maven2
USER root
RUN mkdir -p /opt/spark-jars && cd /opt/spark-jars \
 && curl -fsSLO ${MAVEN}/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar \
 && curl -fsSLO ${MAVEN}/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar \
 && curl -fsSLO ${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
 && curl -fsSLO ${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
 && chown -R airflow: /opt/spark-jars
USER airflow
ENV SPARK_EXTRA_JARS=/opt/spark-jars/delta-spark_2.12-${DELTA_VERSION}.jar,/opt/spark-jars/delta-storage-${DELTA_VERSION}.jar,/opt/spark-jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar,/opt/spark-jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# Re-pinning apache-airflow stops pip from up- or downgrading Airflow while it resolves our packages.
# Dev requirements (pytest) are included because this is a local image that also runs the tests.
COPY requirements.txt requirements-dev.txt /tmp/
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements-dev.txt

# The repo is bind-mounted at /opt/project at runtime (see README).
ENV PYTHONPATH=/opt/project/src \
    PYTHONDONTWRITEBYTECODE=1
WORKDIR /opt/project
