FROM quay.io/jupyter/pyspark-notebook

# Install the application dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Ajout des packages Spark necessaires pour la connexion ADLS
USER root
RUN echo "spark.jars.packages org.apache.hadoop:hadoop-azure:3.5.0,org.postgresql:postgresql:42.7.0" >> $SPARK_HOME/conf/spark-defaults.conf
USER jovyan