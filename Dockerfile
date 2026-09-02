FROM quay.io/jupyter/pyspark-notebook

# Install the application dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt