FROM python:3.9-alpine
COPY requirements.txt /otip/
WORKDIR /otip/
RUN pip3 install -r requirements.txt
COPY pars_file.py .
ENTRYPOINT ["python3", "pars_file.py"]