FROM gcr.io/mtech-emlp-prod/emlp-python3.7

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "./dataflow_broadband_analysis.py"]
