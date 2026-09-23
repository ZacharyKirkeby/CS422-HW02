FROM --platform=linux/amd64 ubuntu:24.04
WORKDIR /usr/local/app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
RUN useradd app
USER app

CMD ["python3", "main.py"]