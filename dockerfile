# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /usr/app/

# Install dbt for BigQuery
RUN pip install --no-cache-dir dbt-bigquery

# Copy the current directory contents into the container at /usr/src/app
COPY . .
COPY ./conf/.dbt/profiles.yml /root/.dbt/profiles.yml
RUN dbt deps

# Run dbt when the container launches
CMD ["dbt", "run"]
