from pathlib import Path

import yaml

ROOT = Path(__file__).parents[3]


def test_middleware_compose_defines_activemq_stomp_service() -> None:
    compose = yaml.safe_load((ROOT.parent / "docker" / "docker-compose.middleware.yaml").read_text())

    service = compose["services"]["activemq"]

    assert service["image"] == "apache/activemq-classic:${ACTIVEMQ_IMAGE_TAG:-latest}"
    assert service["profiles"] == ["", "activemq"]
    assert service["env_file"] == ["./middleware.env"]
    assert service["volumes"] == ["${ACTIVEMQ_HOST_VOLUME:-./volumes/activemq}:/opt/apache-activemq/data"]
    assert "${EXPOSE_ACTIVEMQ_STOMP_PORT:-61613}:61613" in service["ports"]
    assert "${EXPOSE_ACTIVEMQ_OPENWIRE_PORT:-61616}:61616" in service["ports"]
    assert "${EXPOSE_ACTIVEMQ_WEB_PORT:-8161}:8161" in service["ports"]
    assert service["healthcheck"]["test"] == ["CMD-SHELL", "bash -c '</dev/tcp/localhost/61613'"]


def test_middleware_env_example_includes_activemq_defaults() -> None:
    env = (ROOT.parent / "docker" / "middleware.env.example").read_text()

    assert "ACTIVEMQ_IMAGE_TAG=latest" in env
    assert "ACTIVEMQ_HOST_VOLUME=./volumes/activemq" in env
    assert "activemq" in env
    assert "EXPOSE_ACTIVEMQ_STOMP_PORT=61613" in env
    assert "EXPOSE_ACTIVEMQ_OPENWIRE_PORT=61616" in env
    assert "EXPOSE_ACTIVEMQ_WEB_PORT=8161" in env
