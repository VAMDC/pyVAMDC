#!/bin/bash
docker build . -t pyvamdc
docker run -it pyvamdc /bin/bash