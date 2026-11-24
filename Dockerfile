# Dagnose worker image.
#
#   docker build -t dagnose .
#   docker run --rm --env-file .env dagnose check
#   docker run -d --env-file .env -v dagnose-data:/data dagnose        # worker
#
# Configuration is environment variables (DAGNOSE_*); see README.md.

FROM python:3.12-slim AS build
WORKDIR /src
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir build==1.2.2.post1 && python -m build --wheel --outdir /wheels

FROM python:3.12-slim
# git: read the history of the DAG repo when DAGNOSE_GIT_REPO_PATH is mounted.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /wheels /wheels
# All cloud extras, so one image works on AWS (MWAA, Bedrock), Google Cloud (Composer,
# Vertex AI) and Azure (Entra ID, Foundry).
RUN whl=$(ls /wheels/*.whl) && pip install --no-cache-dir "${whl}[aws,gcp,azure]" && rm -rf /wheels \
    && useradd --create-home --uid 50000 dagnose \
    && mkdir -p /data && chown dagnose /data
USER dagnose
ENV DAGNOSE_HOME=/data \
    PYTHONUNBUFFERED=1
VOLUME ["/data"]
ENTRYPOINT ["dagnose"]
CMD ["-v", "worker"]
