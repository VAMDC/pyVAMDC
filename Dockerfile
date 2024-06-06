FROM debian:11.9-slim
RUN apt update && apt upgrade && apt install -y iproute2 openssh-client python3 pip vim curl wget zip

RUN useradd -ms /bin/bash  pyvamdc
RUN mkdir /pyvamdc
WORKDIR /pyvamdc
COPY ./ .

RUN python3 setup.py install
