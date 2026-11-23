# Airflow Doctor worker image.
#
#   docker build -t airflow-doctor .
#   docker run --rm --env-file .env airflow-doctor check
#   docker run -d --env-file .env -v doctor-data:/data airflow-doctor        # worker
#
# Configuration is environment variables (AIRFLOW_DOCTOR_*); see README.md.

FROM python:3.12-slim AS build
WORKDIR /src
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir build==1.2.2.post1 && python -m build --wheel --outdir /wheels

FROM python:3.12-slim
# git: read the history of the DAG repo when AIRFLOW_DOCTOR_GIT_REPO_PATH is mounted.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /wheels /wheels
RUN pip install --no-cache-dir /wheels/*.whl && rm -rf /wheels \
    && useradd --create-home --uid 50000 doctor \
    && mkdir -p /data && chown doctor /data
USER doctor
ENV AIRFLOW_DOCTOR_HOME=/data \
    PYTHONUNBUFFERED=1
VOLUME ["/data"]
ENTRYPOINT ["airflow-doctor"]
CMD ["-v", "worker"]
