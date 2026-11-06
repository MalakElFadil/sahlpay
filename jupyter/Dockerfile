FROM apache/spark:3.5.3-python3
USER root
RUN apt-get update && apt-get install -y python3-pip && rm -rf /var/lib/apt/lists/*
RUN pip3 install --no-cache-dir jupyterlab pyspark==3.5.3 delta-spark==3.2.0 \
    graphframes pandas matplotlib networkx
WORKDIR /home/work