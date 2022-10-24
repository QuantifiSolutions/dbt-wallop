FROM python:3.10.5

RUN mkdir -p /dbt-wallop/
COPY . /dbt-wallop/

WORKDIR /dbt-wallop/

# Downloading gcloud package
RUN curl https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz > /tmp/google-cloud-sdk.tar.gz

# Installing the package
RUN mkdir -p /usr/local/gcloud \
  && tar -C /usr/local/gcloud -xvf /tmp/google-cloud-sdk.tar.gz \
  && /usr/local/gcloud/google-cloud-sdk/install.sh

# Adding the package path to local
ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin

RUN pip install -r requirements.txt

ENTRYPOINT [ "python3", "invoke.py" ]