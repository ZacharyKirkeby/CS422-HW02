FROM --platform=linux/amd64 ubuntu:24.04
WORKDIR /usr/local/app

# Ubuntu 24.04 blocks pip installs into the system Python so we use venv instead
RUN apt-get update \
	&& apt-get install -y --no-install-recommends python3 python3-venv \
	&& rm -rf /var/lib/apt/lists/*
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" \
	PYTHONUNBUFFERED=1 \
	PYTHONDONTWRITEBYTECODE=1 \
	MPLCONFIGDIR=/tmp/matplotlib

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src

RUN useradd app && mkdir -p results && chown app:app results
USER app

# main.py writes to results/ by default and we make this a volume
VOLUME ["/usr/local/app/results"]

ENTRYPOINT ["python3", "src/main.py"]
