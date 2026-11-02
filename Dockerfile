ARG BASE_IMAGE=python:3.13-slim
FROM ${BASE_IMAGE}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    HOME=/tmp

WORKDIR /opt/app
COPY requirements.txt .
# Optional: on networks with TLS inspection, pass the inspecting CA with
#   docker build --secret id=pip_ca,src=/path/to/ca.crt .
RUN --mount=type=secret,id=pip_ca,required=false \
    if [ -f /run/secrets/pip_ca ]; then export PIP_CERT=/run/secrets/pip_ca; fi; \
    pip install -r requirements.txt
COPY app/ app/

# compose.yaml overrides this with PUID:PGID so bind-mounted files are owned
# by the host user.
USER 65534:65534
ENTRYPOINT ["python", "-m"]
CMD ["app.sync", "daemon"]
