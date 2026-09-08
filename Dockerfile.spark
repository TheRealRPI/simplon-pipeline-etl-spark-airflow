FROM quay.io/jupyter/pyspark-notebook

# Install the application dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Ajout des packages necessaires pour la connexion ADLS, de facon a ce qu4ils se lancent au spark-submit
USER root
RUN echo "spark.jars.packages org.apache.hadoop:hadoop-azure:3.3.4,org.postgresql:postgresql:42.7.0" >> $SPARK_HOME/conf/spark-defaults.conf
USER jovyan