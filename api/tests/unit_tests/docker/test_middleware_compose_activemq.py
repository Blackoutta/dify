from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[4]
COMPOSE_PATH = REPO_ROOT / "docker" / "docker-compose.middleware.yaml"
ENV_EXAMPLE_PATH = REPO_ROOT / "docker" / "middleware.env.example"


def test_middleware_compose_includes_activemq_stomp_service():
    compose = yaml.safe_load(COMPOSE_PATH.read_text())

    activemq = compose["services"]["activemq"]

    assert activemq["image"] == "apache/activemq-classic:${ACTIVEMQ_IMAGE_TAG:-latest}"
    assert activemq["profiles"] == ["", "activemq"]
    assert "${EXPOSE_ACTIVEMQ_STOMP_PORT:-61613}:61613" in activemq["ports"]
    assert "${EXPOSE_ACTIVEMQ_WEB_PORT:-8161}:8161" in activemq["ports"]
    assert "${ACTIVEMQ_HOST_VOLUME:-./volumes/activemq}:/opt/apache-activemq/data" in activemq["volumes"]
    assert activemq["healthcheck"]["test"] == ["CMD-SHELL", "bash -c '</dev/tcp/localhost/61613'"]


def test_middleware_env_example_documents_activemq_defaults():
    env_example = ENV_EXAMPLE_PATH.read_text()

    assert "ACTIVEMQ_IMAGE_TAG=latest" in env_example
    assert "ACTIVEMQ_HOST_VOLUME=./volumes/activemq" in env_example
    assert "EXPOSE_ACTIVEMQ_STOMP_PORT=61613" in env_example
    assert "EXPOSE_ACTIVEMQ_WEB_PORT=8161" in env_example
    assert "EXPOSE_ACTIVEMQ_OPENWIRE_PORT=61616" in env_example
    assert "COMPOSE_PROFILES=weaviate,phoenix,activemq" in env_example
