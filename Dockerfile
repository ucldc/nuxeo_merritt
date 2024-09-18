FROM public.ecr.aws/docker/library/python:3.9.19

WORKDIR /nuxeo_merritt

COPY --chmod=744 nuxeo_merritt.py .
COPY requirements.txt .

RUN pip3 install -r requirements.txt

ENTRYPOINT ["python", "nuxeo_merritt.py"]