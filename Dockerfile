FROM ubuntu:24.04

WORKDIR /build

COPY . .

RUN apt-get update
RUN apt-get install -y python3 python3-pip python3-venv

RUN python3 -m venv /virtual_environment

RUN . /virtual_environment/bin/activate && pip install --no-cache-dir --requirement requirements.txt
