# 客户环境异步 Workflow 日志部署指南

## 目标

客户当前有 3 台 VM 部署 Dify，并在前面自建负载均衡。此次上线目标是把生产 workflow 的 node execution log 和 app log 从 API 同步写 DB 改为：

```text
Dify API/worker -> ActiveMQ -> dify-log-consumer -> 外部 PostgreSQL
```

这样可以降低 API 请求路径上的 DB 写入压力。

## 部署拓扑

推荐每台 VM 都部署：

- 新版 `dify-api` 镜像（替换原 api / worker / beat 使用的 api 镜像）
- 1 个本机 ActiveMQ
- 3 个 `dify-log-consumer` 副本

推荐把 `activemq` 和 `dify_log_consumer` **直接加到客户现有的 Dify `docker-compose.yaml` 里**，和 `api` / `worker` 使用同一个 Compose project 的默认网络。这样 API 和 consumer 都能通过服务名访问 ActiveMQ：

```text
api / worker -> activemq:61613
consumer -> activemq:61613
```

每台 VM 的 API 只发到本机 compose 网络里的 `activemq:61613`。每台 VM 上的 consumer 只消费本机 ActiveMQ，再写入同一个外部 PostgreSQL。

> 不建议暂时把 ActiveMQ 做多实例集群；先按每台 VM 本地一个 broker 的方式上线，最简单也最少改动。

## 镜像

待交付两个 amd64 镜像：

```text
dify-api:async-workflow-logs-amd64
dify-log-consumer:async-workflow-logs-amd64
```

客户需要把原 compose 里的 api 镜像替换为：

```yaml
api:
  image: dify-api:async-workflow-logs-amd64

worker:
  image: dify-api:async-workflow-logs-amd64

worker_beat:
  image: dify-api:async-workflow-logs-amd64
```

如果还有其他使用 `langgenius/dify-api:1.13.3` 的服务，也应一起替换。

## 数据库迁移

新版 Dify 镜像启动时会根据 `MIGRATION_ENABLED=true` 自动执行：

```bash
flask upgrade-db
```

Dify 已有 DB migration lock，同一套 Redis 下多台 VM 同时重启时，只有一个实例会真正执行 migration，其他实例会跳过。

客户需要确认 `.env` 中：

```env
MIGRATION_ENABLED=true
```

如果客户当前关闭了自动 migration，则上线前需要手动执行一次：

```bash
docker compose run --rm api flask upgrade-db
```

## Dify `.env` 新增变量

客户只替换镜像、不更新 `.env.example`，所以需要手动在现有 `.env` 增加以下变量。

```env
# 开启 workflow 异步日志。node execution log 和 workflow_app_logs 共用这个开关。
WORKFLOW_LOG_ASYNC_ENABLED=true
WORKFLOW_LOG_QUEUE_PROVIDER=activemq

# API/worker 连接本机 compose 里的 ActiveMQ。
WORKFLOW_LOG_ACTIVEMQ_HOST=activemq
WORKFLOW_LOG_ACTIVEMQ_PORT=61613
WORKFLOW_LOG_ACTIVEMQ_USERNAME=
WORKFLOW_LOG_ACTIVEMQ_PASSWORD=

# 两条独立队列。
WORKFLOW_NODE_EXECUTION_ACTIVEMQ_DESTINATION=/queue/dify.workflow.node-executions
WORKFLOW_APP_LOG_ACTIVEMQ_DESTINATION=/queue/dify.workflow.app-logs

# 当前压测较稳的 producer 配置。
WORKFLOW_LOG_ACTIVEMQ_POOL_SIZE=4
WORKFLOW_LOG_PUBLISH_TIMEOUT=0.2
WORKFLOW_LOG_PUBLISH_MAX_RETRIES=1
WORKFLOW_LOG_PUBLISH_SLOW_LOG_THRESHOLD=1
```

同时需要把这些变量加入 `docker-compose.yaml` 的 `x-shared-api-worker-env`，否则只写 `.env` 不会自动进入容器：

```yaml
x-shared-api-worker-env: &shared-api-worker-env
  # ...保留原有配置...
  WORKFLOW_LOG_ASYNC_ENABLED: ${WORKFLOW_LOG_ASYNC_ENABLED:-true}
  WORKFLOW_LOG_QUEUE_PROVIDER: ${WORKFLOW_LOG_QUEUE_PROVIDER:-activemq}
  WORKFLOW_LOG_ACTIVEMQ_HOST: ${WORKFLOW_LOG_ACTIVEMQ_HOST:-activemq}
  WORKFLOW_LOG_ACTIVEMQ_PORT: ${WORKFLOW_LOG_ACTIVEMQ_PORT:-61613}
  WORKFLOW_LOG_ACTIVEMQ_USERNAME: ${WORKFLOW_LOG_ACTIVEMQ_USERNAME:-}
  WORKFLOW_LOG_ACTIVEMQ_PASSWORD: ${WORKFLOW_LOG_ACTIVEMQ_PASSWORD:-}
  WORKFLOW_NODE_EXECUTION_ACTIVEMQ_DESTINATION: ${WORKFLOW_NODE_EXECUTION_ACTIVEMQ_DESTINATION:-/queue/dify.workflow.node-executions}
  WORKFLOW_APP_LOG_ACTIVEMQ_DESTINATION: ${WORKFLOW_APP_LOG_ACTIVEMQ_DESTINATION:-/queue/dify.workflow.app-logs}
  WORKFLOW_LOG_ACTIVEMQ_POOL_SIZE: ${WORKFLOW_LOG_ACTIVEMQ_POOL_SIZE:-4}
  WORKFLOW_LOG_PUBLISH_TIMEOUT: ${WORKFLOW_LOG_PUBLISH_TIMEOUT:-0.2}
  WORKFLOW_LOG_PUBLISH_MAX_RETRIES: ${WORKFLOW_LOG_PUBLISH_MAX_RETRIES:-1}
  WORKFLOW_LOG_PUBLISH_SLOW_LOG_THRESHOLD: ${WORKFLOW_LOG_PUBLISH_SLOW_LOG_THRESHOLD:-1}
```

## ActiveMQ compose 服务

在每台 VM 的**现有 Dify `docker-compose.yaml`** 中加入下面的 `activemq` 服务，和 `api`、`worker` 放在同一个 `services:` 下。不要单独用另一个 compose project 启动，除非额外配置共享 Docker network。

```yaml
services:
  activemq:
    image: apache/activemq-classic:latest
    restart: always
    volumes:
      - ./volumes/activemq:/opt/apache-activemq/data
    command: >
      /bin/sh -c '
      if ! grep -q "transport.defaultHeartBeat" /opt/apache-activemq/conf/activemq.xml; then
        sed -i "s#stomp://0.0.0.0:61613?maximumConnections=1000\&amp;wireFormat.maxFrameSize=104857600#stomp://0.0.0.0:61613?maximumConnections=1000\&amp;wireFormat.maxFrameSize=104857600\&amp;transport.defaultHeartBeat=${ACTIVEMQ_STOMP_DEFAULT_HEARTBEAT:-30000,30000}\&amp;transport.hbGracePeriodMultiplier=${ACTIVEMQ_STOMP_HB_GRACE_PERIOD_MULTIPLIER:-2.0}#" /opt/apache-activemq/conf/activemq.xml;
      fi;
      exec /opt/apache-activemq/bin/activemq console
      '
    ports:
      - "${EXPOSE_ACTIVEMQ_STOMP_PORT:-61613}:61613"
      - "${EXPOSE_ACTIVEMQ_WEB_PORT:-8161}:8161"
    healthcheck:
      test: ["CMD-SHELL", "bash -c '</dev/tcp/localhost/61613'"]
      interval: 5s
      timeout: 3s
      retries: 30
```

ActiveMQ 管理界面默认地址和账密：

```text
http://<VM_IP>:8161/admin
username: admin
password: admin
```

可以在管理界面里查看 `dify.workflow.node-executions` 和 `dify.workflow.app-logs` 两条队列的 Enqueue、Dequeue、Pending/QueueSize、Consumer 数量。

如果不希望暴露 ActiveMQ 管理页面，可以删除 `8161:8161` 端口映射。

## dify-log-consumer compose 服务

在同一个 Dify `docker-compose.yaml` 中继续加入 `dify_log_consumer` 服务。它需要和 `activemq` 在同一个 compose 网络里，这样 `ACTIVEMQ_HOST=activemq` 才能解析。

```yaml
services:
  dify_log_consumer:
    image: dify-log-consumer:async-workflow-logs-amd64
    restart: always
    depends_on:
      activemq:
        condition: service_healthy
    deploy:
      replicas: 3
    environment:
      CONSUMER_QUEUE_PROVIDER: activemq

      CONSUMER_NODE_EXECUTION_QUEUE_DESTINATION: /queue/dify.workflow.node-executions
      CONSUMER_NODE_EXECUTION_DLQ_DESTINATION: /queue/dify.workflow.node-executions.dlq
      CONSUMER_NODE_EXECUTION_BATCH_SIZE: 250
      CONSUMER_NODE_EXECUTION_FLUSH_INTERVAL: 1s
      CONSUMER_NODE_EXECUTION_MAX_IN_FLIGHT_BATCHES: 5

      CONSUMER_APP_LOG_QUEUE_DESTINATION: /queue/dify.workflow.app-logs
      CONSUMER_APP_LOG_DLQ_DESTINATION: /queue/dify.workflow.app-logs.dlq
      CONSUMER_APP_LOG_BATCH_SIZE: 500
      CONSUMER_APP_LOG_FLUSH_INTERVAL: 1s
      CONSUMER_APP_LOG_MAX_IN_FLIGHT_BATCHES: 2

      ACTIVEMQ_HOST: activemq
      ACTIVEMQ_PORT: 61613
      ACTIVEMQ_USERNAME: ""
      ACTIVEMQ_PASSWORD: ""
      ACTIVEMQ_TLS_ENABLED: "false"
      ACTIVEMQ_CONNECT_TIMEOUT: 5s
      ACTIVEMQ_HEARTBEAT_SEND: 30s
      ACTIVEMQ_HEARTBEAT_RECEIVE: 0s
      ACTIVEMQ_PREFETCH_SIZE: 500
      ACTIVEMQ_RECONNECT_INITIAL_INTERVAL: 1s
      ACTIVEMQ_RECONNECT_MAX_INTERVAL: 30s

      DB_DIALECT: postgres
      DATABASE_URL: postgres://${DB_USERNAME}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_DATABASE}?sslmode=${DB_SSLMODE:-disable}
      DB_MAX_OPEN_CONNS: 20
      DB_MAX_IDLE_CONNS: 10
      DB_CONN_MAX_LIFETIME: 30m
      DB_SLOW_QUERY_THRESHOLD: 200ms

      CONSUMER_MAX_REDELIVERIES: 10
      CONSUMER_DLQ_INCLUDE_ORIGINAL_BODY: "true"
      CONSUMER_SHUTDOWN_TIMEOUT: 30s
      CONSUMER_MAX_MESSAGE_BYTES: 0
      CONSUMER_MAX_DLQ_BODY_BYTES: 0

      WORKFLOW_VARIABLE_TRUNCATION_MAX_SIZE: 1024000
      WORKFLOW_VARIABLE_TRUNCATION_STRING_LENGTH: 100000
      WORKFLOW_VARIABLE_TRUNCATION_ARRAY_LENGTH: 1000

      METRICS_ENABLED: "true"
      METRICS_ADDR: :9090
      METRICS_PATH: /metrics
      LOG_LEVEL: info
```

如果客户的 Compose 不支持 `deploy.replicas`（普通 `docker compose up` 通常会忽略该字段），可以用下面命令启动 3 个副本：

```bash
docker compose up -d --scale dify_log_consumer=3
```

## 外部 PostgreSQL 配置

客户使用外部 PostgreSQL，需要确认 Dify 原 `.env` 已正确配置：

```env
DB_USERNAME=<postgres_user>
DB_PASSWORD=<postgres_password>
DB_HOST=<external_postgres_host>
DB_PORT=5432
DB_DATABASE=dify
DB_SSLMODE=disable   # 如果客户 PG 要求 SSL，改成 require / verify-full 等
```

consumer 使用 `DATABASE_URL` 连接同一个库。如果不想在 compose 里拼接，也可以在 `.env` 直接写完整 URL：

```env
DIFY_LOG_CONSUMER_DATABASE_URL=postgres://<user>:<password>@<host>:5432/<database>?sslmode=disable
```

然后 compose 里改成：

```yaml
DATABASE_URL: ${DIFY_LOG_CONSUMER_DATABASE_URL}
```

注意：外部 PostgreSQL 防火墙/安全组需要允许 3 台 VM 访问 5432。

## 如果必须分开 compose 部署

不推荐分开部署；如果客户必须把 ActiveMQ / consumer 放到另一个 compose 文件或另一个目录，需要显式共享同一个 Docker network。

一种做法是创建外部网络：

```bash
docker network create dify-shared || true
```

然后两个 compose 文件都加入：

```yaml
networks:
  default:
    external: true
    name: dify-shared
```

并确保服务名仍为 `activemq`。如果 ActiveMQ 不在同一 Docker network，就不能使用 `activemq` 这个服务名，需要把以下变量改成可访问的 IP/域名：

```env
WORKFLOW_LOG_ACTIVEMQ_HOST=<activemq_host_or_ip>
ACTIVEMQ_HOST=<activemq_host_or_ip>
```

## 启动顺序

推荐把 ActiveMQ、consumer、Dify 服务都放进**同一个现有 Dify `docker-compose.yaml`**。客户不需要维护多个 compose 文件。

每台 VM 在 Dify compose 目录下执行：

```bash
# 1. 加载离线镜像包
docker load -i dify-api_async-workflow-logs-amd64.tar
docker load -i dify-log-consumer_async-workflow-logs-amd64.tar

# 2. 用同一个 docker-compose.yaml 一次性启动/更新相关服务
#    --scale 用于把 dify_log_consumer 启成 3 个副本
docker compose up -d --scale dify_log_consumer=3 activemq dify_log_consumer api worker worker_beat
```

这条命令里的所有服务都来自同一个 compose project，因此 `api` / `worker` / `dify_log_consumer` 都能通过同一个 Docker 网络访问 `activemq:61613`。

如果三台 VM 共用同一 Redis，自动 migration 可以三台一起重启；DB migration lock 会避免重复执行。为了更稳，也可以先在第一台 VM 上执行上述命令并确认 migration 成功，再处理另外两台。

## 验证命令

### 1. 查看 Dify producer 是否预热成功

API/worker 启动后，查看 api 日志：

```bash
docker compose logs api | grep -i activemq
```

期望能看到类似日志，表示 ActiveMQ producer 已建立连接或完成预热：

```text
Warmed up workflow node execution ActiveMQ publisher
```

如果没有预热日志，但后续 workflow 调用后没有 publish error，也可以继续观察队列 Enqueue 是否增长。

### 2. 查看 consumer 是否正常

```bash
docker compose ps dify_log_consumer
docker compose logs dify_log_consumer | grep "consumer worker group started"
```

期望能看到每个 consumer 实例都启动两个 worker group：

```text
consumer worker group started name=node_execution destination=/queue/dify.workflow.node-executions batch_size=250 max_in_flight=5
consumer worker group started name=app_log destination=/queue/dify.workflow.app-logs batch_size=500 max_in_flight=2
```

### 3. 查看 ActiveMQ 队列积压

```bash
curl -fsS -u admin:admin -H 'Origin: http://localhost:8161' 'http://localhost:8161/api/jolokia/read/org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=dify.workflow.node-executions/QueueSize,EnqueueCount,DequeueCount,ConsumerCount'

curl -fsS -u admin:admin -H 'Origin: http://localhost:8161' 'http://localhost:8161/api/jolokia/read/org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=dify.workflow.app-logs/QueueSize,EnqueueCount,DequeueCount,ConsumerCount'
```

期望：

```text
QueueSize = 0 或能快速回落到 0
ConsumerCount = 3
```

### 4. 查看 DLQ

```bash
curl -fsS -u admin:admin -H 'Origin: http://localhost:8161' 'http://localhost:8161/api/jolokia/read/org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=dify.workflow.node-executions.dlq/QueueSize,EnqueueCount,DequeueCount,ConsumerCount'

curl -fsS -u admin:admin -H 'Origin: http://localhost:8161' 'http://localhost:8161/api/jolokia/read/org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=dify.workflow.app-logs.dlq/QueueSize,EnqueueCount,DequeueCount,ConsumerCount'
```

期望：DLQ 不存在或 `QueueSize=0`。

### 5. 查看 consumer metrics

```bash
curl -fsS http://localhost:9090/metrics
```

如果 3 个 consumer 副本都映射同一个宿主机端口会冲突，生产环境可以不暴露 `9090`，改由内部监控采集。

## 回滚方案

如果上线后出现异常，最小回滚步骤：

1. `.env` 改回：

```env
WORKFLOW_LOG_ASYNC_ENABLED=false
```

2. 重启 api / worker：

```bash
docker compose up -d api worker worker_beat
```

3. 保留 ActiveMQ 和 consumer 运行一段时间，等已有队列消费完：

```text
QueueSize=0
```

4. 再停止 consumer / ActiveMQ：

```bash
docker compose stop dify_log_consumer activemq
```

关闭异步后，Dify 会回到同步 DB 写 workflow logs 的旧路径。

## 已知注意事项

- `workflow_runs` 初始写入仍是同步 DB 写；本 feature 不异步化它。
- `workflow_node_executions` 和 `workflow_app_logs` 通过 ActiveMQ 异步写入。
- consumer 写入是幂等的；ACK 失败会导致 ActiveMQ 重投，但不会造成重复数据错误。
- 如果外部 PostgreSQL 已经接近连接上限，需要同步评估 API 连接池、consumer 连接池和 PG `max_connections`。
- 当前建议 producer pool 为 `4`，不要直接升太高；过高会增加 ActiveMQ 和 DB 下游压力。
