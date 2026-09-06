# 5장. Apache Kafka로 이벤트 흐름 이해하기

앞 장에서는 Percona Server for MySQL, MongoDB, PostgreSQL을 변경 데이터의 출발점으로 준비했습니다. 이제 데이터베이스에서 발생한 변경 이벤트가 다음 처리 계층으로 이동할 수 있도록 Apache Kafka를 구성합니다.

Kafka는 데이터를 한 번 읽고 버리는 단순한 메시지 큐가 아닙니다. 토픽을 파티션으로 나누어 이벤트를 저장하고, 소비자 그룹별로 읽은 위치를 관리합니다. 이 구조 덕분에 여러 처리 애플리케이션이 같은 이벤트를 독립적으로 읽거나, 하나의 처리 작업을 여러 소비자로 병렬화할 수 있습니다.

이 책에서는 Kafka 4.3.1과 KRaft 모드를 기준으로 단일 브로커 실습 환경을 구성합니다. 이 환경은 Windows 11과 Apple Silicon에서 실행할 수 있는 학습용 구성입니다. 운영 환경의 고가용성 클러스터와 동일한 내결함성을 제공하지 않는다는 점을 처음부터 분명히 해야 합니다.

## 이 장의 목표

- KRaft 기반의 단일 브로커 Kafka를 Docker Compose로 실행합니다.
- 브로커, 토픽, 파티션, 오프셋, 소비자 그룹의 관계를 이해합니다.
- 파티션 수와 소비자 병렬 처리의 관계를 실습합니다.
- 메시지 키를 사용해 같은 엔터티의 이벤트 순서를 유지합니다.
- 오프셋을 조회하고 특정 위치부터 이벤트를 재처리합니다.
- 보존 정책과 로그 컴팩션을 CDC 토픽 설계에 적용합니다.
- Kafka CLI로 토픽, 메시지, 소비자 그룹을 확인합니다.

> **버전과 이미지 확인**
>
> 제공된 조사 자료의 실습 기준은 Apache Kafka 4.3.1입니다. 공식 Docker 이미지의 정확한 태그와 ARM64 매니페스트는 집필 시점에 다시 확인해야 합니다. 또한 Kafka 이미지마다 환경 변수 이름과 초기화 방식이 다를 수 있으므로, 이 장의 Compose 예제는 apache/kafka 이미지 기준의 실습 예제로 사용합니다. [검토 필요: apache/kafka:4.3.1 공식 이미지의 최종 태그, ARM64 지원, 환경 변수·healthcheck 실행 방식]

## 1. Kafka를 이벤트 버스로 사용하는 이유

소스 데이터베이스와 분석 저장소를 직접 연결하면 시스템 수가 늘어날수록 연결 관계가 복잡해집니다. MySQL, MongoDB, PostgreSQL에서 발생한 이벤트를 각각 여러 분석·처리 시스템으로 보내야 한다면, 모든 소스와 목적지를 일대일로 연결하는 구조는 변경과 재처리가 어렵습니다.

Kafka를 사이에 두면 소스 시스템은 토픽에 이벤트를 기록하고, 처리 시스템은 필요한 소비자 그룹으로 토픽을 읽습니다. 이벤트가 일정 기간 저장되기 때문에 처리 애플리케이션이 중단되었을 때 마지막 오프셋부터 다시 읽거나, 새로운 애플리케이션이 과거 이벤트부터 읽을 수 있습니다.

~~~mermaid
flowchart LR
    S["소스 DB"] --> P["Kafka Producer 또는 CDC 커넥터"]
    P --> T["Topic"]
    T --> C1["처리 소비자 그룹"]
    T --> C2["분석 소비자 그룹"]
    T --> C3["재처리 소비자 그룹"]
~~~

Kafka의 핵심은 이벤트를 토픽에 저장하는 것과 소비자가 읽은 위치를 별도로 관리하는 것입니다. 같은 토픽이라도 소비자 그룹이 다르면 각 그룹은 자신의 오프셋에 따라 독립적으로 읽습니다. (출처: [Apache Kafka 공식 문서](https://kafka.apache.org/documentation/), 제공된 5장 조사 자료)

## 2. KRaft 기반 Kafka 구성

### 2.1 KRaft란 무엇인가

KRaft는 Kafka의 메타데이터를 관리하기 위한 내장 합의 프로토콜입니다. 제공된 조사 자료에서는 Kafka 4.x 계열을 ZooKeeper 없이 KRaft 모드로 구성하고, 컨트롤러가 Raft 쿼럼을 통해 클러스터 메타데이터를 관리하는 구조로 설명합니다.

KRaft에서는 데이터 메시지를 처리하는 브로커와 클러스터 메타데이터의 합의를 담당하는 컨트롤러 역할을 구분할 수 있습니다. 학습 환경에서는 하나의 프로세스가 broker와 controller 역할을 동시에 수행하는 Combined Node로 구성합니다.

| 역할 | 책임 |
|---|---|
| Controller | 토픽, 파티션, 브로커 상태 등 클러스터 메타데이터의 합의를 관리합니다. |
| Broker | 메시지를 저장하고 프로듀서와 컨슈머의 요청을 처리합니다. |
| Combined Node | 실습 환경처럼 Controller와 Broker를 한 프로세스에서 함께 실행합니다. |

컨트롤러 쿼럼은 메타데이터 로그를 복제합니다. 운영 환경에서는 여러 컨트롤러가 과반수 쿼럼을 구성하지만, 이 책의 단일 노드 실습에서는 하나의 노드만 실행합니다. 따라서 메타데이터 합의와 데이터 저장 모두 하나의 장애 지점에 의존합니다. (출처: [Apache Kafka KRaft 문서](https://kafka.apache.org/38/operations/kraft/))

### 2.2 단일 브로커 Compose 구성

다음은 KRaft Combined Node를 실행하는 기본 Compose 예제입니다.

~~~yaml
services:
  kafka:
    image: apache/kafka:4.3.1
    container_name: kafka
    restart: unless-stopped
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      CLUSTER_ID: "4L6g3nShT-eMCtK--X86sw"
    ports:
      - "9092:9092"
    volumes:
      - kafka-data:/var/lib/kafka/data
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 60s

volumes:
  kafka-data:
~~~

주요 설정은 다음과 같습니다.

- KAFKA_NODE_ID: 클러스터 안에서 노드를 식별하는 값입니다.
- KAFKA_PROCESS_ROLES: 이 노드가 브로커와 컨트롤러 역할을 함께 수행하도록 지정합니다.
- KAFKA_CONTROLLER_QUORUM_VOTERS: 컨트롤러 쿼럼의 노드 ID와 주소를 지정합니다.
- KAFKA_LISTENERS: 브로커 클라이언트와 컨트롤러 통신용 리스너를 정의합니다.
- KAFKA_ADVERTISED_LISTENERS: 다른 컨테이너가 브로커에 접속할 때 사용할 주소를 광고합니다.
- KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 소비자 그룹 오프셋 토픽의 복제 계수입니다. 단일 브로커이므로 1로 설정합니다.
- KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 트랜잭션 상태 로그의 복제 계수입니다. 단일 브로커 실습에서는 1이 필요합니다.
- CLUSTER_ID: KRaft 저장소를 식별하는 클러스터 ID입니다. 데이터 volume을 유지하는 동안 같은 값을 사용해야 합니다.

컨테이너 내부에서 Kafka CLI를 실행할 때는 localhost:9092를 사용할 수 있습니다. 다른 Compose 서비스에서 Kafka에 연결할 때는 서비스 이름인 kafka:9092를 사용합니다. 호스트 운영체제에서 직접 Kafka 클라이언트를 실행할 경우에는 advertised listener 설정에 맞는 접속 주소를 사용해야 합니다. [검토 필요: 호스트 직접 접속을 위한 외부 listener 추가 구성]

### 2.3 KRaft 초기화와 상태 확인

KRaft 클러스터 ID는 처음 구성할 때 생성할 수 있습니다. 조사 자료에서는 다음과 같은 명령을 제시합니다.

~~~bash
docker run --rm apache/kafka:4.3.1 kafka-storage.sh random-uuid
~~~

생성한 ID를 Compose의 CLUSTER_ID에 지정하고, 이후 컨테이너를 실행합니다.

~~~bash
docker compose up -d kafka
docker compose ps kafka
docker compose logs --tail=100 kafka
~~~

컨테이너가 실행되면 브로커 API 응답을 확인합니다.

~~~bash
docker exec kafka kafka-broker-api-versions \
  --bootstrap-server localhost:9092
~~~

이미지에 따라 CLI 파일명이 kafka-broker-api-versions 또는 kafka-broker-api-versions.sh로 제공될 수 있습니다. 제공된 조사 자료에도 두 표기가 함께 있으므로, 이미지 내부의 실제 실행 파일 이름을 확인해야 합니다. [검토 필요: Apache Kafka 4.3.1 이미지 내 CLI 실행 파일명]

## 3. 단일 브로커 실습 환경의 한계

이 장의 Kafka는 학습을 위한 단일 브로커 구성입니다. 단일 브로커는 토픽과 파티션 개념을 익히고 Kafka CLI를 실행하기에 충분하지만, 장애 허용을 검증할 수 있는 클러스터는 아닙니다.

| 항목 | 이 책의 실습 환경 | 운영 환경에서 검토할 내용 |
|---|---|---|
| 브로커 수 | 1 | 여러 브로커로 분산합니다. |
| 컨트롤러 쿼럼 | 1개 Combined Node | 과반수 쿼럼을 구성합니다. |
| 파티션 복제 계수 | 1 | 데이터 보호를 위해 복제 계수를 높입니다. |
| 장애 발생 시 | 브로커 장애로 전체 Kafka 사용 불가 | 리더 선출과 복제본 승격을 검증합니다. |
| 오프셋 토픽 | 복제 계수 1 | 내부 토픽도 적절한 복제 계수를 사용합니다. |

단일 브로커에서 replication-factor=1로 만든 토픽은 복제본이 없습니다. 브로커의 저장 volume이 손상되면 메시지와 소비자 그룹 오프셋을 함께 잃을 수 있습니다. 그러므로 이 장의 구성을 고가용성이나 내결함성을 제공하는 운영 아키텍처로 설명해서는 안 됩니다.

> **책에서 사용할 표현**
>
> 이 책의 Kafka는 단일 브로커 KRaft 기반의 로컬 실습 환경입니다. Kafka의 토픽·파티션·오프셋·소비자 그룹을 학습하기 위한 구성으로, 브로커 장애에 대한 고가용성이나 데이터 복구를 제공하지 않습니다. 운영 환경에서는 여러 브로커와 컨트롤러 쿼럼, 적절한 복제 계수, 백업·복구 정책을 별도로 설계해야 합니다.

## 4. 브로커, 토픽, 파티션, 오프셋

### 4.1 브로커

브로커는 Kafka 서버의 한 실행 인스턴스입니다. 브로커는 토픽의 파티션을 저장하고, 프로듀서가 보낸 레코드를 기록하며, 컨슈머의 읽기 요청에 응답합니다.

단일 브로커 구성에서는 모든 파티션의 리더가 하나의 서버에 있습니다. 여러 브로커를 사용하는 클러스터에서는 파티션을 브로커에 분산하고, 복제본을 구성해 장애에 대응합니다. 이 장에서는 분산 배치보다 토픽과 파티션의 논리적 동작을 먼저 익힙니다.

### 4.2 토픽

토픽은 이벤트를 분류하는 논리적 이름입니다. 예를 들어 mysql-orders-cdc 토픽에는 MySQL 주문 테이블의 변경 이벤트를 기록할 수 있습니다.

토픽은 하나의 큐가 아니라 하나 이상의 파티션으로 구성됩니다. 프로듀서는 토픽의 특정 파티션에 레코드를 추가하고, 컨슈머는 파티션의 로그를 순서대로 읽습니다.

이 책의 소스 데이터베이스와 연결하면 다음과 같이 토픽을 설계할 수 있습니다.

| 이벤트 출처 | 토픽 예시 |
|---|---|
| Percona MySQL 주문 | mysql-orders-cdc |
| Percona MySQL 고객 | mysql-customers-cdc |
| MongoDB 주문 | mongo-orders-cdc |
| MongoDB 고객 | mongo-customers-cdc |
| PostgreSQL 주문 | postgres-orders-cdc |
| PostgreSQL 고객 | postgres-customers-cdc |

소스별로 하나의 CDC 토픽을 두는 mysql-cdc, postgres-cdc, mongodb-cdc 방식도 가능합니다. 이 방식은 토픽 수가 적지만, 소비자가 테이블 또는 컬렉션을 이벤트 내용으로 다시 분류해야 합니다. 반대로 엔터티별 토픽은 토픽 단위의 보존·처리·권한 정책을 적용하기 쉽지만 토픽 수가 늘어납니다. 이 책에서는 개념 실습을 단순하게 시작할 때 소스별 토픽을 사용하고, 엔터티별 처리 설계가 필요한 경우 엔터티별 토픽으로 확장합니다. [검토 필요: 6장에서 사용할 Flink CDC 커넥터의 실제 토픽 출력 규칙]

### 4.3 파티션

파티션은 토픽 안의 독립적인 append-only 로그입니다. 각 레코드는 자신이 기록된 파티션 안에서 오프셋을 가지며, 파티션 단위로 순서가 유지됩니다.

파티션은 저장 단위이면서 병렬 처리의 단위입니다. 같은 소비자 그룹 안에서는 하나의 파티션이 동시에 한 소비자에게 할당됩니다. 따라서 소비자 수가 파티션 수보다 많으면 일부 소비자는 할당받을 파티션이 없어 대기합니다.

~~~mermaid
flowchart TB
    T["orders 토픽"] --> P0["파티션 0"]
    T --> P1["파티션 1"]
    T --> P2["파티션 2"]
    P0 --> C0["소비자 0"]
    P1 --> C1["소비자 1"]
    P2 --> C2["소비자 2"]
~~~

이 관계를 간단히 표현하면 다음과 같습니다.

소비자 그룹 내 유효한 병렬성 ≤ 토픽의 파티션 수

파티션 수를 늘리면 병렬 처리의 여지가 커지지만, 파티션 수가 무제한으로 늘어날수록 처리량이 선형으로 증가하는 것은 아닙니다. 파티션 메타데이터, 파일, 오프셋 관리, 리밸런싱 비용이 함께 늘어날 수 있습니다. 따라서 파티션 수는 처리량, 순서 요구, 소비자 수, 운영 비용을 함께 고려해 정해야 합니다. (출처: [Apache Kafka Distribution 문서](https://kafka.apache.org/082/implementation/distribution/), 제공된 5장 조사 자료)

### 4.4 오프셋

오프셋은 파티션 안에서 레코드의 위치를 나타내는 순번입니다. 제공된 자료에서는 파티션의 첫 레코드를 0으로 시작하는 순차 위치로 설명합니다.

오프셋에는 두 가지를 구분해야 합니다.

1. **레코드 오프셋**: 파티션에 기록된 각 레코드의 위치입니다.
2. **커밋된 소비자 오프셋**: 특정 소비자 그룹이 다음에 읽기 위해 저장해 둔 위치입니다.

컨슈머가 메시지를 읽었다는 사실과 오프셋이 커밋되었다는 사실은 항상 같은 시점에 발생하는 것은 아닙니다. 애플리케이션이 메시지를 처리하기 전에 오프셋을 커밋하면 장애 후 메시지가 다시 처리되지 않을 수 있습니다. 반대로 처리 후 커밋하는 방식에서는 장애 시 같은 이벤트가 다시 전달될 수 있으므로 처리 결과의 멱등성을 함께 고려해야 합니다. [추가 자료 조사 필요: 이 책에서 사용할 Flink CDC의 체크포인트·오프셋 커밋 semantics]

소비자 그룹의 오프셋은 Kafka 내부의 __consumer_offsets 토픽에서 관리됩니다. 오프셋 보존 기간이 지나거나 그룹이 장기간 비활성 상태이면 커밋된 오프셋이 만료될 수 있으므로, 장기 중단 후 재시작 정책을 별도로 정해야 합니다. 제공된 자료는 기본 오프셋 보존 기간을 7일로 제시합니다. (출처: [Apache Kafka Broker Configs](https://kafka.apache.org/41/configuration/broker-configs/))

### 4.5 소비자 그룹

소비자 그룹은 하나의 처리 작업을 함께 수행하는 소비자 인스턴스의 집합입니다. 같은 그룹의 소비자들은 토픽 파티션을 나누어 읽습니다. 한 파티션은 같은 그룹 안에서 한 소비자에게만 할당됩니다.

반면 서로 다른 소비자 그룹은 독립적인 오프셋을 갖습니다. 예를 들어 flink-cdc-group은 이벤트를 정제하고, audit-group은 감사 로그를 저장하며, backfill-group은 과거 이벤트를 재처리할 수 있습니다. 세 그룹은 같은 토픽을 읽지만 각자의 위치를 관리합니다.

그룹에 소비자가 추가되거나 제거되면 Kafka는 파티션을 다시 배분합니다. 이 과정을 리밸런싱이라고 합니다. 리밸런싱은 처리 중인 애플리케이션의 상태와 오프셋 처리 방식에 영향을 줄 수 있으므로, 운영 환경에서는 리밸런싱으로 인한 처리 지연을 관찰해야 합니다.

## 5. 파티션 수와 병렬 처리 실습

### 5.1 파티션 수 결정의 기준

실습에서는 토픽을 3개 파티션으로 시작합니다. 세 소스 데이터베이스를 구분하는 학습 시나리오와 소비자 병렬 처리 실습에 충분한 크기입니다. 정제 계층에서 더 많은 소비자를 실행하는 예제에서는 6개 파티션 토픽을 추가합니다.

| 구분 | 이 책의 실습 선택 | 의미 |
|---|---:|---|
| 기본 CDC 토픽 | 3개 파티션 | 소스 이벤트 분산과 기본 병렬성 실습 |
| 정제 토픽 예시 | 6개 파티션 | 여러 정제 작업의 병렬성 실습 |
| 복제 계수 | 1 | 단일 브로커에서만 가능한 학습용 설정 |

이 숫자는 모든 시스템에 적용되는 공식 권장값이 아닙니다. 파티션 수는 예상 이벤트 처리량, 메시지 크기, 키 분포, 소비자 수, 순서 보장 범위, 브로커 자원에 따라 결정해야 합니다. 파티션을 추가하면 처리 병렬성은 높아질 수 있지만, 기존의 키 분포와 리밸런싱에도 영향을 줄 수 있습니다.

### 5.2 토픽 생성과 파티션 확인

컨테이너 안에서 Kafka CLI를 실행해 3개 파티션 토픽을 만듭니다.

~~~bash
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 1

docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic orders
~~~

파티션 수를 6개로 늘리는 명령은 다음과 같습니다.

~~~bash
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --alter \
  --topic orders \
  --partitions 6
~~~

토픽의 파티션 수를 늘리는 작업은 신중하게 수행해야 합니다. 제공된 조사 자료는 파티션 수 변경 시 리밸런싱이 발생할 수 있고, 키 기반 이벤트의 분포와 순서 설계에 영향을 줄 수 있다고 설명합니다. 이 책에서는 처음부터 실습에 필요한 파티션 수를 정해 두고, 증가 명령은 동작을 관찰하는 용도로만 실행합니다.

## 6. 메시지 키와 순서 보장

### 6.1 키가 파티션 선택에 미치는 영향

프로듀서가 메시지 키를 지정하면 Kafka는 키를 기준으로 파티션을 선택합니다. 같은 키를 가진 레코드는 같은 파티션으로 라우팅되도록 설계할 수 있습니다. 따라서 한 주문의 상태 변경 순서를 유지해야 한다면 order_id를 키로 사용하는 방식이 적합합니다.

예를 들어 다음 세 이벤트가 있다고 가정합니다.

~~~text
키: order_1, 값: PENDING
키: order_1, 값: CONFIRMED
키: order_1, 값: SHIPPED
~~~

세 이벤트가 같은 파티션에 기록되면 해당 파티션 안에서 기록 순서를 유지할 수 있습니다. 하지만 다른 주문의 이벤트가 다른 파티션에 기록될 수 있으므로, 토픽 전체의 전역 순서가 보장되는 것은 아닙니다.

### 6.2 Kafka가 보장하는 순서의 범위

Kafka의 순서 보장 범위는 파티션입니다. 토픽에 여러 파티션이 있으면 파티션 사이의 상대적인 도착 순서를 전역적으로 비교할 수 없습니다.

| 설계 | 기대할 수 있는 순서 |
|---|---|
| 같은 키, 같은 파티션 | 해당 키의 파티션 내 기록 순서 |
| 서로 다른 키, 같은 파티션 | 해당 파티션의 기록 순서 |
| 서로 다른 파티션 | 토픽 전체의 전역 순서는 보장되지 않음 |
| 키를 지정하지 않은 이벤트 | 특정 엔터티 단위 순서를 보장하기 어려움 |

소스 데이터베이스별 키 선택은 다음과 같이 시작할 수 있습니다.

| 소스 이벤트 | 기본 키 후보 |
|---|---|
| MySQL 주문 | order_id |
| PostgreSQL 주문 | order_id |
| MongoDB 주문 | order_id 또는 이벤트의 문서 식별자 |
| 고객 변경 | customer_id |
| 제품 변경 | product_id |

MongoDB의 실제 변경 이벤트에는 _id와 애플리케이션 업무 키가 함께 존재할 수 있습니다. 이후 CDC 커넥터가 생성하는 이벤트 envelope을 확인하여 어떤 필드를 Kafka 키로 사용할지 확정해야 합니다. [검토 필요: MongoDB CDC 이벤트에서 Kafka key로 사용할 최종 식별자]

### 6.3 키를 포함한 메시지 생산

Kafka Console Producer에서 parse.key=true와 구분자를 지정하면 입력 행을 키와 값으로 나누어 보낼 수 있습니다.

~~~bash
docker exec -it kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --property "parse.key=true" \
  --property "key.separator=:"
~~~

프로듀서가 입력을 기다리면 다음과 같이 입력합니다.

~~~text
order_1:{"order_id":1,"status":"PENDING"}
order_1:{"order_id":1,"status":"CONFIRMED"}
order_2:{"order_id":2,"status":"PENDING"}
~~~

동일한 키를 가진 order_1 이벤트가 같은 파티션으로 라우팅되는지 확인하려면 키와 파티션을 함께 출력하도록 소비자를 실행합니다.

~~~bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning \
  --property "print.key=true" \
  --property "print.partition=true" \
  --property "print.offset=true" \
  --max-messages 3
~~~

출력에서 order_1 이벤트의 파티션이 같은지 확인합니다. 이 실습은 파티션별 순서의 의미를 보여 주기 위한 것이며, 여러 파티션 사이의 전체 이벤트 순서를 검증하는 실습은 아닙니다.

## 7. 오프셋과 이벤트 재처리

### 7.1 자동 시작 위치

컨슈머가 저장된 오프셋을 가지고 있지 않을 때 어디서 시작할지는 auto.offset.reset 설정으로 결정할 수 있습니다.

| 설정 | 오프셋이 없을 때의 시작 위치 | 사용 사례 |
|---|---|---|
| earliest | 보존 중인 가장 오래된 이벤트 | 백필·재처리 |
| latest | 현재 가장 최신 위치 이후 | 새 이벤트만 처리 |
| none | 오프셋이 없으면 오류 | 위치를 명시적으로 관리해야 하는 작업 |

earliest는 토픽에 남아 있는 모든 과거 이벤트를 무조건 보존한다는 뜻이 아닙니다. retention 정책에 의해 이미 삭제된 이벤트는 다시 읽을 수 없습니다. 따라서 오프셋 리셋과 보존 기간은 함께 설계해야 합니다.

### 7.2 소비자 그룹 상태 확인

소비자 그룹의 현재 위치와 지연량(LAG)을 확인합니다.

~~~bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list

docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group cdc_group
~~~

소비자 그룹이 아직 메시지를 읽지 않았다면 그룹 목록에 나타나지 않을 수 있습니다. 먼저 해당 그룹으로 소비자를 실행하거나 커넥터를 연결한 뒤 상태를 확인합니다.

### 7.3 오프셋 리셋

오프셋을 리셋하면 지정한 소비자 그룹이 다시 읽을 위치를 바꿉니다. 조사 자료는 다음과 같은 세 가지 리셋 위치를 제시합니다.

~~~bash
# 보존 중인 가장 오래된 위치부터 재처리
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group cdc_group \
  --topic orders \
  --reset-offsets \
  --to-earliest \
  --execute

# 현재 최신 위치 이후의 새 이벤트부터 처리
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group cdc_group \
  --topic orders \
  --reset-offsets \
  --to-latest \
  --execute

# 파티션별 특정 위치로 이동
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group cdc_group \
  --topic orders \
  --reset-offsets \
  --to-offset 0 \
  --execute
~~~

오프셋 리셋은 대상 소비자 그룹을 중지한 뒤 수행해야 합니다. 실행 중인 소비자와 동시에 리셋하면 리밸런싱이나 새로운 커밋 때문에 기대한 위치와 다른 결과가 발생할 수 있습니다. 재처리 결과가 중복될 수 있으므로, downstream 저장 로직은 동일 이벤트를 여러 번 받아도 안전한지 확인해야 합니다.

## 8. 보존 기간과 로그 컴팩션

### 8.1 보존 정책

Kafka 토픽의 cleanup.policy=delete는 일정 시간이 지나거나 크기 제한에 도달한 오래된 로그 세그먼트를 삭제하는 방식입니다. 제공된 조사 자료는 Kafka 4.x의 기본 retention.ms를 604800000밀리초, 즉 7일로 제시합니다.

주요 설정은 다음과 같습니다.

| 설정 | 역할 | 이 책의 실습 예시 |
|---|---|---|
| retention.ms | 시간 기준 보존 기간 | 604800000ms, 7일 |
| retention.bytes | 크기 기준 보존 한도 | 별도 제한을 두지 않거나 실습 한도로 설정 |
| segment.ms | 로그 세그먼트 롤링 시간 | 필요 시 단축 |
| segment.bytes | 세그먼트 크기 | 필요 시 축소 |
| cleanup.policy | 삭제·컴팩션 정책 | CDC는 delete부터 시작 |

로컬 실습에서는 7일 보존을 사용해 오프셋 리셋과 재처리를 연습할 수 있습니다. 운영 보존 기간은 재처리 요구, 저장 비용, 소비자 장애 시 최대 복구 시간, 법적 보존 요건을 기준으로 정해야 합니다.

### 8.2 로그 컴팩션

로그 컴팩션은 같은 키를 가진 레코드 중 오래된 값을 정리하고 최신 상태를 남기는 정책입니다. 컴팩션은 메시지를 즉시 하나로 합치는 동작이 아니라 백그라운드 정리 과정이며, 컴팩션이 수행되기 전까지 여러 버전의 레코드가 남아 있을 수 있습니다.

컴팩션은 현재 상태를 복원하는 토픽에 유용합니다. 예를 들어 customer_id별 최신 고객 상태만 필요하다면 compact 정책을 검토할 수 있습니다. 반면 CDC 원본 이벤트를 시간 순서대로 재생해야 한다면 원본 토픽은 우선 delete 정책으로 유지하는 편이 이해하기 쉽습니다.

삭제 이벤트를 컴팩션 토픽에서 표현할 때는 tombstone, 즉 키는 있고 값은 null인 레코드를 사용할 수 있습니다. tombstone의 보존 기간과 소비자 처리 시점도 함께 설계해야 합니다. 제공된 자료는 delete.retention.ms=86400000, 24시간을 예시로 제시합니다.

### 8.3 토픽 정책 예시

다음은 CDC 원본 토픽, 정제 상태 토픽, 혼합 정책 토픽의 예입니다.

~~~bash
# CDC 원본 이벤트 토픽: 7일 보존, 컴팩션 없음
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic mysql-cdc \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete

# 상태 중심 정제 토픽: 키별 최신 값 보존
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic orders-cleaned \
  --partitions 6 \
  --replication-factor 1 \
  --config cleanup.policy=compact \
  --config delete.retention.ms=86400000

# 시간 보존과 최신 상태 보존을 함께 사용하는 토픽
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic orders-mart \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete,compact
~~~

delete,compact 혼합 정책은 이벤트를 시간 기준으로 삭제하면서 키 기반 정리도 수행하는 선택지입니다. 실제 정제 토픽에 어떤 정책이 적합한지는 이벤트 재생이 필요한지, 최신 상태만 필요한지, 삭제 이벤트를 얼마나 오래 처리해야 하는지에 따라 결정해야 합니다.

## 9. Kafka CLI 실습

### 9.1 토픽 관리

토픽 목록과 상세 정보를 조회합니다.

~~~bash
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --list

docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic orders
~~~

토픽을 삭제하는 명령은 로컬 실습에서만 주의해서 사용합니다.

~~~bash
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic orders
~~~

토픽 삭제는 이후 재처리 실습에 필요한 이벤트를 제거할 수 있습니다. 이름과 대상을 확인한 뒤 실행합니다.

### 9.2 메시지 생산과 소비

앞 절의 키 포함 프로듀서와 함께 다음 소비자 명령을 사용합니다.

~~~bash
# 보존 중인 메시지를 처음부터 읽고 최대 10개에서 종료
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning \
  --property "print.key=true" \
  --property "print.partition=true" \
  --property "print.offset=true" \
  --max-messages 10

# 특정 파티션의 메시지만 확인
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --partition 0 \
  --from-beginning
~~~

from-beginning은 새로운 소비자 그룹이 토픽의 보존 중인 가장 오래된 메시지부터 읽도록 하는 옵션입니다. 이미 커밋된 그룹 오프셋이 있는 경우에는 소비자 그룹의 커밋 위치가 우선될 수 있으므로, 단순히 옵션 하나만으로 기존 그룹의 오프셋이 항상 무시된다고 생각해서는 안 됩니다.

### 9.3 소비자 그룹의 LAG 확인

~~~bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group cdc_group
~~~

조회 결과에서 파티션별 현재 오프셋, 로그 끝 오프셋, LAG를 비교합니다. LAG가 계속 증가하면 소비자가 이벤트를 처리하는 속도가 생산 속도를 따라가지 못하거나, 소비자 오류·리밸런싱·외부 저장소 지연이 발생했을 가능성이 있습니다. 원인을 확인하려면 Kafka 상태뿐 아니라 소비자 애플리케이션 로그도 함께 확인해야 합니다.

### 9.4 토픽 설정 확인과 변경

~~~bash
docker exec kafka kafka-configs \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name orders \
  --describe

docker exec kafka kafka-configs \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name orders \
  --alter \
  --add-config retention.ms=259200000
~~~

위 변경은 orders 토픽의 보존 기간을 3일로 바꾸는 예입니다. 토픽 설정을 변경하면 재처리 가능한 이벤트 범위가 달라질 수 있으므로, 실습 후 원래 값을 복원하거나 토픽을 재생성합니다.

## 10. 소스 데이터베이스별 토픽 설계

### 10.1 토픽 이름 규칙

토픽 이름은 이벤트의 출처와 엔터티를 빠르게 식별할 수 있어야 합니다. 엔터티별 토픽을 사용할 때는 다음 패턴을 사용합니다.

~~~text
<source>-<entity>-cdc
~~~

| 소스 | 엔터티 | 예시 토픽 |
|---|---|---|
| MySQL | orders | mysql-orders-cdc |
| MySQL | customers | mysql-customers-cdc |
| MySQL | products | mysql-products-cdc |
| MongoDB | orders | mongo-orders-cdc |
| MongoDB | customers | mongo-customers-cdc |
| MongoDB | products | mongo-products-cdc |
| PostgreSQL | orders | postgres-orders-cdc |
| PostgreSQL | customers | postgres-customers-cdc |
| PostgreSQL | products | postgres-products-cdc |

소스별 단일 토픽 방식을 사용할 때는 mysql-cdc, mongo-cdc, postgres-cdc로 이름을 줄일 수 있습니다. 이 경우 이벤트 envelope에 데이터베이스·스키마·테이블 또는 컬렉션 식별자가 반드시 포함되는지 확인해야 합니다. [검토 필요: 6장 CDC 커넥터가 제공하는 source metadata와 토픽 라우팅 규칙]

### 10.2 파티션과 키의 조합

주문 이벤트를 예로 들면 토픽은 3개 파티션으로 만들고, Kafka 키에는 주문 ID를 넣을 수 있습니다.

~~~bash
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic mysql-orders-cdc \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete
~~~

이 구성의 의미는 다음과 같습니다.

- 3개의 파티션을 이용해 하나의 소비자 그룹이 최대 3개 작업 단위로 병렬화할 수 있습니다.
- 같은 order_id를 키로 사용하는 이벤트는 같은 파티션으로 라우팅하도록 설계합니다.
- 토픽의 모든 주문이 하나의 전역 순서로 처리된다는 의미는 아닙니다.
- 단일 브로커에서 복제 계수는 1이므로 브로커 장애에 대한 데이터 복구를 제공하지 않습니다.

### 10.3 토픽 설계의 선택 기준

토픽을 세분화할수록 엔터티별 보존 기간과 처리 정책을 독립적으로 적용하기 쉽습니다. 반면 토픽 수가 많아지면 ACL, 모니터링, 파티션 관리의 대상도 늘어납니다.

| 선택지 | 장점 | 주의점 |
|---|---|---|
| 소스별 단일 토픽 | 구성이 단순하고 토픽 수가 적습니다. | 소비자가 이벤트 출처를 다시 분류해야 합니다. |
| 엔터티별 토픽 | 주문·고객·제품을 독립적으로 처리하기 쉽습니다. | 토픽과 운영 정책의 수가 늘어납니다. |
| 파티션 3개 | 로컬 병렬성 실습에 적합합니다. | 처리량이 커지면 부족할 수 있습니다. |
| 파티션 6개 | 더 많은 소비자 병렬성을 실습할 수 있습니다. | 관리·리밸런싱 비용이 증가할 수 있습니다. |

이 책의 로컬 구성에서는 먼저 소스별 3개 토픽으로 Kafka 기본 개념을 익힌 뒤, 6장에서 CDC 커넥터의 실제 이벤트 구조를 확인하고 엔터티별 토픽으로 확장할 수 있습니다.

## 11. 전체 실습 절차

### 1단계: Kafka 실행

~~~bash
docker compose up -d kafka
docker compose ps kafka
~~~

healthcheck가 정상으로 전환되고 로그에 치명적인 초기화 오류가 없는지 확인합니다.

### 2단계: 기본 토픽 생성

~~~bash
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 1
~~~

### 3단계: 키가 있는 메시지 생산

키를 지정한 Console Producer를 실행한 뒤 order_1과 order_2 이벤트를 입력합니다.

### 4단계: 메시지 소비

키, 파티션, 오프셋을 출력하도록 Console Consumer를 실행합니다. 같은 키의 이벤트가 동일 파티션에 기록되는지 확인합니다.

### 5단계: 소비자 그룹 상태 확인

소비자 그룹을 지정한 소비자를 실행한 뒤 kafka-consumer-groups --describe로 파티션별 오프셋과 LAG를 확인합니다.

### 6단계: 오프셋 리셋

소비자를 중지하고 --to-earliest로 오프셋을 이동한 다음, 동일 이벤트가 다시 전달되는지 확인합니다. 재처리 결과가 중복될 수 있음을 함께 기록합니다.

### 7단계: 보존 정책 변경

CDC 원본 토픽에는 delete, 상태 중심 토픽에는 compact 정책을 적용하고 토픽 설정을 조회합니다.

## 12. 앞 장의 소스 DB와 Kafka의 연결 관계

~~~mermaid
flowchart LR
    M["Percona MySQL\nbinlog"] --> K["Kafka\nCDC topic"]
    G["MongoDB\nChange Streams"] --> K
    P["PostgreSQL\nlogical WAL"] --> K
    K --> F["Flink CDC 또는 처리 소비자"]
~~~

Kafka는 데이터베이스 내부의 binlog, oplog, WAL을 직접 동일한 형식으로 통합하는 구성 요소가 아닙니다. CDC 커넥터가 각 데이터베이스의 변경 기록을 읽고 Kafka 레코드로 변환합니다. 따라서 다음 장에서는 Kafka의 토픽·키·파티션 설계와 CDC 커넥터의 이벤트 envelope을 함께 확인해야 합니다.

이 연결에서 확인할 항목은 다음과 같습니다.

1. 어떤 소스 이벤트가 어떤 Kafka 토픽으로 기록되는가?
2. Kafka 레코드의 key에 어떤 데이터베이스 식별자가 들어가는가?
3. INSERT·UPDATE·DELETE가 어떤 값 구조로 표현되는가?
4. 스냅숏 이벤트와 실시간 변경 이벤트를 어떻게 구분하는가?
5. 커넥터가 중단되었을 때 마지막 위치를 어떻게 복구하는가?

이 장에서는 다섯 번째 항목의 기반이 되는 Kafka 오프셋과 소비자 그룹을 학습했습니다. 커넥터별 정확한 체크포인트와 전달 보장은 선택한 CDC 구현을 기준으로 별도 검증해야 합니다. [추가 자료 조사 필요: 6장 CDC 커넥터의 checkpoint·delivery·schema change semantics]

## 장 요약

Kafka는 토픽과 파티션에 이벤트를 저장하고, 소비자 그룹별로 읽은 위치를 관리하는 이벤트 스트리밍 플랫폼입니다. 이 책에서는 ZooKeeper 없이 KRaft 모드로 Kafka 4.3.1 단일 브로커를 구성합니다.

브로커는 메시지를 저장·전달하는 서버이고, 토픽은 이벤트의 논리적 분류이며, 파티션은 토픽 안의 병렬 로그입니다. 오프셋은 파티션 안의 레코드 위치이고, 소비자 그룹은 여러 소비자가 파티션을 나누어 읽는 처리 단위입니다.

파티션 단위로 순서가 보장되므로 엔터티의 순서가 중요할 때 메시지 키를 지정해야 합니다. 같은 키의 이벤트를 같은 파티션으로 라우팅하면 주문이나 고객 단위의 순서를 유지하는 데 도움이 되지만, 여러 파티션 사이의 전역 순서까지 보장되는 것은 아닙니다.

오프셋 리셋은 장애 복구와 백필에 사용할 수 있지만, 대상 소비자 그룹을 중지하고 데이터 중복 처리 가능성을 고려해야 합니다. CDC 원본 이벤트는 시간 순서 재생을 위해 delete 보존 정책으로 시작하고, 최신 상태가 중요한 정제 토픽은 compact 또는 혼합 정책을 검토합니다.

단일 브로커는 실습에는 적합하지만 고가용성과 데이터 복제를 제공하지 않습니다. 다음 장에서는 앞 장에서 준비한 세 소스 데이터베이스의 변경 기록을 CDC 커넥터를 통해 Kafka 이벤트로 연결합니다.

## 확인 문제

1. KRaft 모드에서 Controller와 Broker의 역할은 어떻게 다릅니까?
2. 단일 브로커 실습 환경에서 replication-factor=1이 갖는 한계는 무엇입니까?
3. 토픽과 파티션의 관계를 설명하고, 파티션이 병렬 처리에 미치는 영향을 설명하십시오.
4. 소비자 그룹이 서로 다를 때 같은 토픽을 어떻게 읽을 수 있습니까?
5. Kafka의 순서 보장이 토픽 단위가 아니라 파티션 단위인 이유는 무엇입니까?
6. 주문 상태 변경 이벤트에 order_id를 Kafka 키로 사용하는 이유는 무엇입니까?
7. 소비자 그룹의 earliest와 latest 시작 위치는 각각 어떤 상황에 사용합니까?
8. 오프셋을 리셋하기 전에 소비자를 중지해야 하는 이유는 무엇입니까?
9. CDC 원본 토픽과 최신 상태 토픽에 서로 다른 cleanup policy를 적용하는 이유는 무엇입니까?
10. docker exec로 실행하는 Kafka CLI에서 localhost:9092를 사용할 수 있는 이유와 다른 컨테이너에서 kafka:9092를 사용하는 이유를 설명하십시오.
11. 소스별 단일 토픽과 엔터티별 토픽의 장단점을 비교하십시오.
12. 단일 브로커 Kafka를 운영 환경에 그대로 적용하면 안 되는 이유는 무엇입니까?

## 이 장에서 자료가 부족했던 부분

- apache/kafka:4.3.1의 최종 공식 이미지 태그와 ARM64 멀티 아키텍처 매니페스트 확인이 필요합니다.
- Kafka 4.3.1 이미지의 환경 변수 이름, KRaft 초기화 방식, CLI 파일명 확인이 필요합니다.
- 호스트 운영체제에서 localhost:9092로 직접 접속하기 위한 외부 listener 구성 자료가 부족합니다.
- Flink CDC가 생성하는 실제 토픽 이름과 소스 메타데이터·Kafka key 매핑이 확정되지 않았습니다.
- Flink CDC의 체크포인트, 오프셋 커밋, 전달 보장, 재처리 semantics 자료가 필요합니다.
- MongoDB Change Streams 이벤트에서 최종 Kafka key로 사용할 식별자 확인이 필요합니다.
- 파티션 수 3개와 6개는 실습 선택값이며, 처리량·메시지 크기·키 분포에 따른 성능 근거는 추가 검증이 필요합니다.
- 단일 브로커의 로컬 자원 사용량과 Windows 11·Apple Silicon 간 Kafka 성능 비교 자료가 없습니다.

## 참고 자료

1. [Apache Kafka 공식 문서](https://kafka.apache.org/documentation/)
2. [Apache Kafka KRaft 문서](https://kafka.apache.org/38/operations/kraft/)
3. [Apache Kafka Topic Configs](https://kafka.apache.org/43/configuration/topic-configs/)
4. [Apache Kafka Broker Configs](https://kafka.apache.org/41/configuration/broker-configs/)
5. [Apache Kafka Distribution 문서](https://kafka.apache.org/082/implementation/distribution/)
6. [Confluent Log Compaction 설명](https://docs.confluent.io/kafka/design/log_compaction.html)
7. [Confluent Consumer Group 관리 문서](https://docs.confluent.io/kafka/operations-tools/manage-consumer-groups.html)
8. [Kafka 메시지 키와 파티션 순서 자료](https://nicheelab.com/en/articles/kafka/message-keys/)
9. [Kafka 소비자 병렬성 자료](https://docs.ol-hub.com/docs/parallelism-in-kafka-consumer-projects)

<!-- 편집 메모: 이 원고는 사용자가 제공한 5장 조사 자료를 기반으로 재구성했다. KRaft·파티션·키·오프셋·보존 정책의 핵심 개념은 본문에 반영했으며, 이미지 태그·CLI 동작·CDC 커넥터별 semantics처럼 조사 자료가 충돌하거나 확정하지 못한 부분은 검토 필요 또는 추가 자료 조사 필요로 표시했다. -->
