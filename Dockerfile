FROM python:3.12
WORKDIR /

COPY requirements.txt /tmp
RUN apt update && apt install -y default-jdk build-essential
RUN python3 -m pip install -r /tmp/requirements.txt
RUN curl -Lo /opt/dicomedit6.jar https://bitbucket.org/xnatdcm/dicom-edit6/downloads/dicom-edit6-6.5.0-jar-with-dependencies.jar

COPY router.py /router.py
ENTRYPOINT [ "python3","/router.py" ]