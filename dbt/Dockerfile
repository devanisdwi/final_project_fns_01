# https://github.com/GoogleCloudPlatform/professional-services/blob/main/examples/dbt-on-cloud-composer/optimized/dbt-project/Dockerfile

FROM python:3.8.5

# Install required system packages and cleanup to reduce image size
RUN apt-get update -y && \
  apt-get install --no-install-recommends -y -q \
  git libpq-dev python-dev && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Install data build tool
# RUN pip install -U pip
RUN python -m pip install --upgrade pip setuptools wheel --no-cache-dir
RUN pip install dbt-bigquery==0.20.1

# Set working directory
ENV DBT_DIR /dbt/
WORKDIR $DBT_DIR

# Copy files to the image
COPY . $DBT_DIR

# Run dbt
ENTRYPOINT ["dbt"]