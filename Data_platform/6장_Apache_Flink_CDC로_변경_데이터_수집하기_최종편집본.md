# 6장. Apache Flink CDC로 변경 데이터 수집하기

앞 장에서는 Kafka의 토픽, 파티션, 오프셋, 소비자 그룹을 학습했습니다. 이 장에서는 앞에서 준비한 Percona Server for MySQL, PostgreSQL, MongoDB의 변경을 읽어 Kafka 이벤트로 전달하는 흐름을 다룹니다.

변경 데이터 캡처(Change Data Capture, CDC)는 데이터베이스의 현재 상태를 주기적으로 복사하는 기술과 다릅니다. CDC는 데이터가 변경된 사실과 변경 내용을 별도의 이벤트로 추출합니다. INSERT, UPDATE, DELETE를 이벤트로 전달할 수 있기 때문에 분석 저장소, 검색 시스템, 캐시, 다른 애플리케이션을 소스 데이터베이스와 비동기적으로 연결할 수 있습니다.

이 책에서는 Apache Flink CDC를 로그 기반 CDC 커넥터로 사용합니다. MySQL은 binlog, PostgreSQL은 logical replication과 WAL, MongoDB는 Change Streams를 읽습니다. Flink CDC는 초기 스냅샷을 수행한 뒤 변경 로그를 계속 읽는 방식으로 과거 데이터와 실시간 변경을 하나의 파이프라인에서 처리합니다.

## 이 장의 목표

- 로그 기반 CDC와 쿼리 기반 CDC의 차이를 이해합니다.
- MySQL, PostgreSQL, MongoDB의 CDC 프로토콜을 비교합니다.
- 초기 스냅샷과 실시간 스트리밍 전환 과정을 이해합니다.
- Flink CDC 커넥터의 소스 설정과 Kafka 전달 구조를 이해합니다.
- 체크포인트와 소스별 위치 정보로 장애 후 재개 과정을 설명합니다.
- 중복 이벤트가 발생하는 조건과 멱등성 설계 방법을 익힙니다.
- DDL·스키마 변경과 삭제 이벤트를 다룹니다.
- Flink CDC와 Flink 버전 조합 및 세 데이터베이스 지원 범위를 점검합니다.

> **버전과 실행 범위**
>
> 제공된 조사 자료는 Apache Flink CDC 3.6.0과 Apache Flink 1.20.x 또는 2.2.x 조합을 기준으로 제시합니다. JDK는 11 이상으로 정리되어 있으며, 자료의 책 기준은 Flink 1.20 계열과 JDK 17입니다. 이 책의 전체 실습 환경은 Docker Compose 기반이지만, 수집 자료의 대표 배포 예시는 Flink Kubernetes Operator의 FlinkDeployment 형식입니다. 따라서 이 장의 Flink SQL과 커넥터 옵션은 개념·설정 예제로 사용하고, 로컬 Compose에서 실행할 Flink JobManager·TaskManager·커넥터 JAR 구성은 별도로 확정해야 합니다. [검토 필요: Docker Compose 기반 Flink 2.2.1 또는 1.20.x와 Flink CDC 3.6.0의 최종 실행 구성]

## 1. CDC의 두 가지 접근 방식

### 1.1 로그 기반 CDC

로그 기반 CDC는 데이터베이스가 트랜잭션 처리 과정에서 기록하는 변경 로그를 읽습니다. MySQL의 binlog, PostgreSQL의 WAL과 logical decoding, MongoDB의 oplog를 기반으로 한 Change Streams가 이에 해당합니다.

변경이 테이블이나 컬렉션에 반영된 뒤 별도 조회를 수행하는 것이 아니라, 데이터베이스가 이미 기록한 변경 정보를 CDC 커넥터가 해석합니다. 따라서 현재 상태만 조회하는 방식과 달리, INSERT·UPDATE·DELETE를 이벤트로 재구성할 수 있습니다.

| 특성 | 설명 |
|---|---|
| 입력 | 트랜잭션 로그 또는 변경 스트림 |
| 삭제 감지 | 로그에 기록된 삭제 이벤트를 읽을 수 있음 |
| 변경 순서 | 로그 또는 스트림의 위치를 기준으로 처리 |
| 소스 설정 | binlog, logical replication, Replica Set 등 필요 |
| 구현 난이도 | 로그 형식과 스키마 변화 해석이 필요 |
| 일반적인 용도 | 실시간 동기화, 분석 적재, 이벤트 파이프라인 |

로그 기반 CDC의 지연 시간과 처리량은 데이터베이스, 커넥터, 네트워크, Flink 설정, 싱크 성능에 따라 달라집니다. 제공된 자료에는 sub-second 지연과 높은 처리량이라는 정성적 설명이 있지만, 특정 환경에서 이를 보장하는 벤치마크 결과는 포함되어 있지 않습니다. [추가 자료 조사 필요: 이 책의 Windows 11·Apple Silicon 실습 환경에서의 CDC 지연 시간과 처리량]

### 1.2 쿼리 기반 CDC

쿼리 기반 CDC는 updated_at, created_at 같은 컬럼을 이용해 일정 주기로 변경된 행을 조회합니다. 일반적인 형태는 마지막 처리 시각 이후의 행을 찾는 조건입니다.

~~~sql
SELECT *
FROM orders
WHERE updated_at > :last_processed_time
ORDER BY updated_at;
~~~

이 방식은 데이터베이스 로그를 해석하지 않아도 표준 SQL로 구현할 수 있다는 장점이 있습니다. 반면 삭제된 행은 조회 결과에 나타나지 않으므로 soft delete 컬럼이나 별도의 삭제 이력 테이블이 필요합니다. 짧은 시간 안에 같은 행이 여러 번 변경되면 중간 상태를 놓치고 최종 상태만 읽을 수 있습니다.

| 특성 | 로그 기반 CDC | 쿼리 기반 CDC |
|---|---|---|
| 변경 감지 | 트랜잭션 로그·스트림 | 주기적인 SELECT |
| 삭제 감지 | 네이티브 이벤트로 처리 가능 | 별도 삭제 표시 필요 |
| 지연 | 로그 소비와 시스템 지연에 의존 | 폴링 주기에 의존 |
| 소스 부하 | 로그 소비 부하와 설정에 의존 | 반복 조회 부하 발생 |
| 중간 변경 | 로그에 남은 이벤트를 처리 | 폴링 사이의 중간 상태를 놓칠 수 있음 |
| 설정 난이도 | 로그·권한·버전 구성이 필요 | 비교적 단순하지만 누락·중복 설계 필요 |
| Flink CDC 커넥터 | 이 장의 기본 방식 | 이 장의 기본 방식이 아님 |

쿼리 기반 CDC의 실제 지연을 1~5분 또는 수분에서 수시간으로 단정하려면 폴링 주기와 시스템 조건을 함께 제시해야 합니다. 이 장에서는 특정 숫자를 일반 법칙으로 사용하지 않습니다.

### 1.3 Flink CDC의 선택

제공된 자료에 따르면 Flink CDC 커넥터는 로그 기반 CDC를 중심으로 동작합니다. MySQL, PostgreSQL, MongoDB의 변경 프로토콜에 맞는 커넥터가 데이터베이스 로그를 읽고 Flink의 변경 이벤트로 변환합니다.

Flink CDC는 Kafka를 반드시 중간에 두어야만 하는 도구는 아닙니다. 커넥터에서 읽은 이벤트를 Flink 내부 연산으로 처리한 뒤 Iceberg나 다른 싱크로 직접 보낼 수도 있습니다. 그러나 이 책의 전체 아키텍처에서는 Kafka를 이벤트 버스로 사용하므로, 이 장의 대표 흐름은 다음과 같습니다.

~~~mermaid
flowchart LR
    D["소스 데이터베이스"] --> F["Flink CDC Source"]
    F --> K["Kafka Topic"]
    K --> S["후속 처리·저장"]
~~~

Kafka를 중간에 두는 구조와 Flink가 목적지로 직접 쓰는 구조의 선택 기준은 싱크, 재처리, 이벤트 재사용, 운영 팀의 책임 범위에 따라 달라집니다. 이 장은 두 구조의 성능을 비교하지 않고, 소스 변경을 읽어 Kafka로 전달하는 기본 경로에 집중합니다.

## 2. 세 데이터베이스의 CDC 프로토콜

### 2.1 MySQL binlog와 GTID

MySQL CDC 커넥터는 행 기반 바이너리 로그를 읽습니다. 앞 장에서 구성한 Percona Server for MySQL에는 다음 설정이 필요합니다.

- binlog_format=ROW
- binlog_row_image=FULL
- binlog_row_metadata=FULL 권장
- gtid_mode=ON 권장
- enforce_gtid_consistency=ON 권장
- CDC 사용자의 SELECT, RELOAD, REPLICATION SLAVE, REPLICATION CLIENT 권한

GTID는 변경 위치를 식별하는 데 사용할 수 있습니다. 커넥터는 초기 스냅샷과 스트리밍 전환 후 마지막으로 처리한 binlog 위치를 저장하고, 장애 후 해당 위치를 기준으로 다시 읽습니다.

~~~text
MySQL
  └─ binlog ROW event
       └─ GTID와 행 이미지
            └─ Flink MySQL CDC Source
                 └─ Kafka Sink
~~~

MySQL 소스의 설정 발췌는 다음과 같습니다.

~~~text
connector = mysql-cdc
hostname = mysql
port = 3306
username = cdc_user
password = cdc_password
database-name = sourcedb
table-name = customers
scan.startup.mode = initial
server-id = 5400-5408
~~~

server-id 범위는 CDC 커넥터가 복제 클라이언트로 동작할 때 사용할 식별자 범위입니다. 여러 커넥터나 커넥터 병렬 인스턴스를 운영할 때 중복되지 않도록 관리해야 합니다. 정확한 범위 규칙과 병렬 스냅샷 조건은 선택한 Flink CDC 커넥터 버전의 공식 문서를 확인해야 합니다. [검토 필요: Flink CDC 3.6.0 MySQL Connector의 server-id 범위와 병렬 스냅샷 제약]

### 2.2 PostgreSQL logical replication

PostgreSQL CDC는 WAL에서 logical decoding을 수행하고, publication과 replication slot을 사용합니다. 제공된 자료의 대표 플러그인은 PostgreSQL 기본 logical replication 출력 플러그인인 pgoutput입니다.

필요한 서버 설정은 다음과 같습니다.

~~~text
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
max_slot_wal_keep_size = 100MB
wal_compression = lz4
~~~

PostgreSQL CDC 커넥터가 사용하는 핵심 위치 정보는 replication slot과 LSN(Log Sequence Number)입니다. slot은 커넥터가 어디까지 WAL을 소비했는지 추적하는 역할을 합니다. 커넥터가 장기간 중단되거나 slot이 정리되지 않으면 WAL이 계속 보존되어 디스크 사용량이 증가할 수 있습니다.

PostgreSQL 소스의 설정 발췌는 다음과 같습니다.

~~~text
connector = postgres-cdc
hostname = postgres
port = 5432
username = cdc_user
password = cdc_password
database-name = sourcedb
schema-name = public
table-name = customers
scan.startup.mode = initial
slot.name = flink_cdc_slot
publication.name = flink_cdc_publication
~~~

앞 장에서 만든 publication을 커넥터가 사용할지, 커넥터가 publication을 자동으로 만들지 여부는 설정 방식에 따라 달라질 수 있습니다. 수동으로 만든 publication과 커넥터가 기대하는 publication의 범위를 일치시켜야 합니다.

UPDATE와 DELETE에서 이전 행의 전체 값이 필요하다면 Replica Identity 설정을 검토해야 합니다. 기본 키가 있는 테이블은 기본 Replica Identity로 시작할 수 있지만, 기본 키가 없거나 이전 행 전체를 요구하는 싱크라면 REPLICA IDENTITY FULL이 필요할 수 있습니다.

~~~sql
ALTER TABLE orders REPLICA IDENTITY FULL;
~~~

모든 테이블에 FULL을 적용하면 WAL의 양이 증가할 수 있습니다. 따라서 이 설정은 CDC 정확성 요구와 WAL·디스크 비용을 함께 고려해 결정해야 합니다. [추가 자료 조사 필요: 실제 Flink CDC와 downstream sink가 요구하는 PostgreSQL Replica Identity 수준]

### 2.3 MongoDB Change Streams와 resume token

MongoDB CDC 커넥터는 Change Streams를 구독합니다. Change Streams는 standalone 서버가 아니라 Replica Set 또는 Sharded Cluster에서 사용해야 하므로, 앞 장에서 구성한 단일 노드 Replica Set이 필요합니다.

MongoDB 변경 스트림의 위치는 resume token으로 표현됩니다. 커넥터는 스트림의 위치를 토큰으로 저장하고, 장애 후 토큰을 이용해 변경 스트림을 재개합니다.

~~~text
MongoDB
  └─ Replica Set oplog
       └─ Change Streams cursor
            └─ resume token
                 └─ Flink MongoDB CDC Source
~~~

MongoDB 소스의 설정 발췌는 다음과 같습니다.

~~~text
connector = mongodb-cdc
hosts = mongodb:27017
username = cdc_user
password = cdc_password
database = sourcedb
collection = customers
scan.startup.mode = initial
heartbeat.interval.ms = 5000
~~~

heartbeat 설정은 Change Streams 연결과 oplog rollover 위험을 함께 고려하기 위한 항목으로 자료에 제시되어 있습니다. resume token이 가리키는 시점보다 oplog가 먼저 정리되면 이전 위치에서 스트림을 재개하지 못할 수 있습니다. 이 경우 초기 스냅샷 또는 별도 재동기화가 필요할 수 있습니다.

MongoDB의 CDC 사용자 권한, fullDocument 옵션, before 이미지 지원 범위는 버전과 커넥터 구현에 따라 달라질 수 있습니다. [검토 필요: Flink CDC 3.6.0 MongoDB Connector의 최소 권한, resume token 복구, before 이미지 옵션]

### 2.4 세 프로토콜 비교

| 구분 | MySQL | PostgreSQL | MongoDB |
|---|---|---|---|
| 변경 원천 | binlog | WAL logical decoding | Change Streams |
| 위치 식별자 | GTID와 binlog offset | replication slot과 LSN | resume token |
| CDC 전제 조건 | ROW binlog, FULL row image | logical WAL, slot, sender, publication | Replica Set 또는 Sharded Cluster |
| 대표 커넥터 | mysql-cdc | postgres-cdc | mongodb-cdc |
| 삭제 표현 | DELETE 변경 이벤트 | DELETE logical change | delete Change Stream 이벤트 |
| 스키마 변화 | binlog DDL과 커넥터 처리 | logical replication metadata와 커넥터 처리 | 문서 유연성, DDL 처리 제한 |
| 장애 복구 주의점 | binlog 보존과 GTID 만료 | slot 누수와 WAL 보존 | oplog rollover와 resume token 만료 |

세 커넥터의 위치 정보는 서로 다른 형식이지만, Flink 관점에서는 소스 상태로 관리됩니다. 따라서 장애 복구를 설계할 때는 Kafka 오프셋만 보는 것이 아니라, Flink checkpoint와 소스 커넥터의 위치 보존을 함께 확인해야 합니다.

## 3. Flink CDC 파이프라인 구성

### 3.1 파이프라인의 구성 요소

Flink CDC 파이프라인은 크게 다음 요소로 구성됩니다.

1. **CDC Source**: 데이터베이스 로그 또는 변경 스트림을 읽습니다.
2. **Flink Runtime**: 이벤트를 처리하고 상태·체크포인트를 관리합니다.
3. **변환 연산**: 필드 정규화, 필터링, 소스 식별자 추가 등을 수행합니다.
4. **Kafka Sink**: 변경 이벤트를 Kafka 토픽에 기록합니다.
5. **Checkpoint 저장소**: 장애 후 소스 위치와 처리 상태를 복원합니다.

~~~mermaid
flowchart TB
    A["MySQL binlog"] --> M["MySQL CDC Source"]
    B["PostgreSQL WAL"] --> P["PostgreSQL CDC Source"]
    C["MongoDB Change Streams"] --> G["MongoDB CDC Source"]
    M --> F["Flink Job"]
    P --> F
    G --> F
    F --> K["Kafka Sink"]
~~~

하나의 Flink Job에서 세 소스를 모두 읽을 수도 있고, 데이터베이스별 Job으로 분리할 수도 있습니다. 하나의 Job으로 묶으면 공통 변환과 운영 대상이 단순해질 수 있지만, 한 소스의 장애가 다른 소스의 배포·복구 단위와 결합됩니다. 데이터베이스별 Job은 장애 격리와 독립 배포에 유리하지만 Job과 checkpoint 관리 대상이 늘어납니다. 이 책의 실제 선택은 전체 Compose 구성과 운영 장에서 확정해야 합니다. [검토 필요: 세 소스 통합 Job과 데이터베이스별 Job의 최종 운영 기준]

### 3.2 MySQL에서 Kafka로 보내는 설정 예

다음은 제공된 조사 자료의 MySQL CDC와 Kafka Sink 설정을 학습용으로 단순화한 예입니다. 이 예제는 Flink SQL 테이블 정의와 주요 connector 옵션을 보여 주기 위한 것입니다. 실제 실행에는 Flink 런타임, connector JAR, Kafka connector JAR, Job 제출 방식이 추가로 필요합니다.

~~~sql
CREATE TABLE mysql_source (
    id BIGINT,
    name STRING,
    status STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'cdc_user',
    'password' = 'cdc_password',
    'database-name' = 'sourcedb',
    'table-name' = 'customers',
    'scan.startup.mode' = 'initial',
    'server-id' = '5400-5408'
);

CREATE TABLE kafka_sink (
    id BIGINT,
    name STRING,
    status STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) WITH (
    'connector' = 'kafka',
    'topic' = 'mysql-cdc',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json',
    'key.fields' = 'id'
);

INSERT INTO kafka_sink
SELECT id, name, status, created_at, updated_at
FROM mysql_source;
~~~

이 SQL은 변경 이벤트를 단순한 현재 행 구조로 보여 주는 예입니다. 실제 CDC 이벤트의 INSERT·UPDATE·DELETE envelope, before·after 구조, metadata 컬럼 표현은 Flink CDC와 Kafka format 설정에 따라 달라질 수 있습니다. 단순히 SELECT 결과가 Kafka에 기록된다고 해서 DELETE의 의미와 UPDATE 이전 값이 자동으로 보존된다고 단정해서는 안 됩니다. [검토 필요: 최종 Kafka format과 CDC changelog envelope 설계]

### 3.3 PostgreSQL과 MongoDB 설정 발췌

PostgreSQL은 publication과 slot을 지정합니다.

~~~text
connector = postgres-cdc
hostname = postgres
port = 5432
username = cdc_user
password = cdc_password
database-name = sourcedb
schema-name = public
table-name = customers
scan.startup.mode = initial
slot.name = flink_cdc_slot
publication.name = flink_cdc_publication
~~~

MongoDB는 Change Streams 연결 정보와 컬렉션 범위를 지정합니다.

~~~text
connector = mongodb-cdc
hosts = mongodb:27017
username = cdc_user
password = cdc_password
database = sourcedb
collection = customers
scan.startup.mode = initial
heartbeat.interval.ms = 5000
~~~

세 소스의 스키마를 Kafka Sink의 하나의 스키마로 합치려면 필드명과 자료형을 정규화해야 합니다. 예를 들어 MySQL과 PostgreSQL의 id를 하나의 id 필드로 매핑하고, MongoDB의 업무 식별자와 내부 _id 중 어느 것을 사용할지 결정해야 합니다. 데이터베이스 이름 또는 source_db 필드를 추가하면 서로 다른 소스의 같은 엔터티를 구분할 수 있습니다.

~~~sql
CREATE TABLE unified_kafka_sink (
    id BIGINT,
    name STRING,
    email STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    source_db STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'customers-cdc',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json',
    'key.fields' = 'id,source_db'
);
~~~

이 통합 예제에서 source_db를 Kafka key에 포함할지는 순서 요구사항에 따라 결정합니다. 동일한 업무 ID가 여러 소스에 존재할 수 있다면 id만으로 키를 구성할 때 충돌하거나 서로 다른 소스의 이벤트가 같은 파티션으로 섞일 수 있습니다. 반대로 같은 업무 엔터티의 전역 순서를 만들기 위해 source_db를 제거하는 것은 실제 데이터 소유권과 충돌할 수 있습니다. [검토 필요: 통합 토픽의 Kafka key와 source_db 식별자 규칙]

## 4. 초기 스냅샷과 스트리밍 전환

### 4.1 초기 스냅샷이 필요한 이유

CDC 파이프라인을 새로 시작하면 변경 로그에는 파이프라인 시작 이후의 변경만 있을 수 있습니다. 이미 데이터베이스에 존재하던 고객·제품·주문을 downstream에 먼저 채우려면 초기 스냅샷이 필요합니다.

초기 스냅샷 단계는 일반적으로 다음 흐름을 가집니다.

1. 소스 테이블 또는 컬렉션의 기존 데이터를 읽습니다.
2. 스냅샷 중 발생한 변경을 로그 또는 스트림에서 추적합니다.
3. 스냅샷 경계와 소스 위치를 저장합니다.
4. 스냅샷이 끝난 뒤 변경 로그 스트리밍으로 전환합니다.
5. 경계 이후의 이벤트를 순서에 맞춰 downstream으로 전달합니다.

~~~mermaid
sequenceDiagram
    participant D as 소스 DB
    participant F as Flink CDC
    participant K as Kafka
    F->>D: 스냅샷 시작
    F->>D: 기존 데이터 읽기
    D-->>F: 스냅샷 행·문서
    D-->>F: 변경 로그 기록
    F->>F: 소스 위치와 체크포인트 저장
    F->>D: 변경 로그 스트리밍 전환
    F->>K: 스냅샷·변경 이벤트 전달
~~~

초기 스냅샷 중 데이터가 변경되는 경우에도 변경 로그에 이벤트가 남아 있어야 합니다. 그러나 스냅샷과 스트리밍 이벤트의 경계, 중복 제거, 순서 조정은 커넥터의 구현과 버전에 따라 달라질 수 있습니다. 제공된 자료는 스냅샷 이후 변경 사항이 누락되지 않는 구조를 설명하지만, 모든 커넥터에서 중복이 절대 발생하지 않는다고 보장하는 자료는 아닙니다.

### 4.2 데이터베이스별 스냅샷

| 데이터베이스 | 스냅샷·전환의 자료상 표현 |
|---|---|
| MySQL | 테이블 스냅샷과 binlog 위치 저장 후 binlog 스트리밍 |
| PostgreSQL | EXPORT_SNAPSHOT과 WAL·logical replication |
| MongoDB | 기존 문서 복사와 Change Streams watch |
| 공통 결과 | 초기 데이터와 이후 변경 이벤트를 같은 파이프라인으로 전달 |

MySQL의 스냅샷은 자료에서 lock-free chunked 방식과 READ LOCAL 잠금 방식이 함께 언급됩니다. 이는 connector 버전과 스냅샷 옵션에 따라 실제 동작이 달라질 수 있음을 의미합니다. 이 책의 본문에서는 이를 모든 MySQL 환경에 동일하게 적용되는 무잠금 보장으로 표현하지 않습니다. [검토 필요: Flink CDC 3.6.0 MySQL snapshot의 실제 잠금·청크·일관성 동작]

PostgreSQL은 스냅샷과 WAL 위치를 연결해야 합니다. MongoDB는 전체 문서 읽기와 Change Streams의 resume token 경계를 조정해야 합니다. 세 경우 모두 초기 스냅샷이 완료되었다는 사실만으로 이벤트가 downstream에 정확히 반영되었다고 판단하지 말고, Kafka 출력과 대상 저장소의 row·document 수를 함께 확인해야 합니다.

### 4.3 스냅샷 실행 모드

이 책의 예제는 scan.startup.mode=initial을 사용합니다. 이 모드는 기존 데이터를 먼저 읽고, 이후의 변경을 계속 처리하는 실습 흐름입니다.

초기화가 완료된 뒤 새로 시작하는 경우에는 다음을 구분해야 합니다.

- 이미 스냅샷과 스트리밍 위치가 checkpoint에 저장되어 있는가?
- 소스 binlog·WAL·oplog가 재개 위치를 아직 보존하고 있는가?
- downstream에 기존 데이터가 있어 중복 적재를 허용할 수 있는가?
- 다시 initial을 수행할 때 sink가 upsert·merge를 지원하는가?

이 질문에 답하지 않고 initial을 반복 실행하면 동일한 초기 데이터가 downstream에 중복될 수 있습니다.

## 5. Checkpoint, offset과 장애 후 재개

### 5.1 소스별 위치 정보

Flink CDC는 데이터베이스마다 다른 위치 정보를 Flink 상태로 저장합니다.

| 소스 | 위치 정보 | 재개 시 확인할 조건 |
|---|---|---|
| MySQL | GTID와 binlog offset | 해당 binlog와 GTID가 아직 보존되어 있는가 |
| PostgreSQL | replication slot과 LSN | slot이 존재하고 WAL이 필요한 위치까지 보존되어 있는가 |
| MongoDB | resume token | oplog가 token 이전 구간을 덮어쓰지 않았는가 |
| Kafka | topic·partition·consumer offset | Kafka 토픽의 이벤트가 retention으로 삭제되지 않았는가 |

Flink checkpoint는 소스 위치만 저장하는 것이 아니라 연산 상태와 sink 관련 상태를 함께 저장합니다. 체크포인트가 성공적으로 완료된 시점을 기준으로 장애 후 복구가 이루어집니다.

### 5.2 Checkpoint 설정 예

제공된 자료의 checkpoint 설정은 다음과 같습니다.

~~~yaml
pipeline:
  name: mysql-to-iceberg
  parallelism: 4
  checkpoint:
    interval: 30000
    timeout: 300000
    max-concurrent: 1
    min-pause: 10000
~~~

각 항목의 의미는 다음과 같습니다.

- interval: 체크포인트 시도 간격입니다. 예시 값은 30초입니다.
- timeout: 하나의 체크포인트가 완료되어야 하는 최대 시간입니다. 예시 값은 5분입니다.
- max-concurrent: 동시에 진행할 수 있는 체크포인트 수입니다. 예시 값은 1입니다.
- min-pause: 체크포인트 사이에 둘 최소 간격입니다. 예시 값은 10초입니다.

이 설정은 개념을 설명하는 예시입니다. 실제 Flink 배포 방식에 따라 checkpoint 설정 키와 저장소 설정이 달라질 수 있습니다. 특히 로컬 Compose 환경에서는 checkpoint 저장 경로를 컨테이너 재생성 후에도 유지할지 결정해야 합니다. [추가 자료 조사 필요: Docker Compose 환경의 Flink checkpoint 저장소·복구 설정]

### 5.3 장애 후 재개 흐름

장애가 발생하면 Flink는 마지막으로 성공한 checkpoint를 기준으로 상태를 복원합니다.

~~~mermaid
flowchart TB
    A["장애 발생"] --> B["마지막 성공 Checkpoint 선택"]
    B --> C["Flink 상태 복원"]
    C --> D["소스 위치 확인"]
    D --> E["binlog·WAL·resume token 재개"]
    E --> F["Checkpoint 이후 이벤트 재처리"]
~~~

복원 후 마지막 checkpoint 이후에 처리된 이벤트가 다시 전달될 수 있습니다. 따라서 장애 후 재개가 곧 중복 없는 처리와 같은 뜻은 아닙니다. 소스 위치가 만료되었거나 데이터베이스 로그가 이미 삭제된 경우에는 해당 위치에서 재개할 수 없고, 초기 스냅샷 또는 별도 백필이 필요할 수 있습니다.

### 5.4 소스 로그 보존과 checkpoint의 관계

Checkpoint 파일만 오래 보존해도 소스 로그가 삭제되면 재개할 수 없습니다. 반대로 소스 로그가 남아 있어도 Flink checkpoint가 손상되었거나 sink 상태를 복원하지 못하면 동일한 처리 결과를 보장하기 어렵습니다.

따라서 복구 가능성은 다음 조건의 교집합으로 생각해야 합니다.

- Flink checkpoint가 복원 가능해야 합니다.
- MySQL binlog가 GTID와 필요한 위치를 보존해야 합니다.
- PostgreSQL replication slot과 WAL이 필요한 LSN을 보존해야 합니다.
- MongoDB oplog가 resume token 이전의 구간을 보존해야 합니다.
- Kafka 토픽이 downstream 재처리에 필요한 이벤트를 보존해야 합니다.
- sink가 중복 이벤트를 안전하게 처리해야 합니다.

## 6. 중복 이벤트와 멱등성

### 6.1 중복 이벤트가 발생하는 이유

중복 이벤트는 다음과 같은 상황에서 발생할 수 있습니다.

1. checkpoint가 완료되기 전에 Flink Job이 장애가 납니다.
2. 마지막 성공 checkpoint 이후에 읽은 이벤트를 복구 과정에서 다시 읽습니다.
3. 소비자가 offset을 커밋하기 전에 재시작됩니다.
4. 네트워크 재시도 중 같은 이벤트가 다시 전달됩니다.
5. 초기 스냅샷을 다시 수행하면서 기존 데이터를 재전송합니다.

이벤트 스트리밍에서 at-least-once 성격의 재처리는 흔한 복구 경로입니다. exactly-once라는 표현도 Source, Flink 연산, Kafka Sink, 최종 저장소가 모두 같은 경계에서 트랜잭션 또는 상태 복구를 지원하는지 확인해야 합니다. Flink Job의 checkpoint가 성공했다는 사실만으로 최종 ClickHouse나 다른 외부 저장소에 중복이 절대 없다고 단정해서는 안 됩니다.

### 6.2 멱등성이란 무엇인가

멱등성은 같은 이벤트를 여러 번 적용해도 최종 결과가 한 번 적용한 결과와 같도록 만드는 성질입니다. 이벤트 처리에서는 다음 요소를 함께 설계해야 합니다.

- 업무 엔터티의 안정적인 기본 키
- 이벤트의 operation 타입
- 변경 시각 또는 버전
- 이벤트의 소스 식별자
- 중복 이벤트를 판별할 event_id 또는 위치 정보
- DELETE를 적용하는 규칙
- sink의 upsert·merge·delete 지원

### 6.3 저장소별 적용 패턴

#### 기본 키 기반 MERGE

Iceberg와 같은 저장소에서 기본 키를 기준으로 기존 행을 갱신하는 예입니다.

~~~sql
MERGE INTO iceberg_table AS target
USING cdc_stream AS source
ON target.id = source.id
WHEN MATCHED AND source.operation = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    name = source.name,
    status = source.status,
    updated_at = source.updated_at
WHEN NOT MATCHED AND source.operation <> 'DELETE' THEN
    INSERT (id, name, status, updated_at)
    VALUES (source.id, source.name, source.status, source.updated_at);
~~~

이 예제는 개념을 보여 주기 위한 것입니다. 실제 MERGE 문법, source stream의 changelog 형태, primary key 정의는 선택한 저장소와 Flink connector에 맞춰 검증해야 합니다. [검토 필요: 최종 Iceberg·ClickHouse sink의 CDC upsert와 DELETE 구현]

#### 업데이트 시각 비교

같은 행에 대해 이벤트가 여러 번 도착할 수 있을 때 source.updated_at과 target.updated_at을 비교해 더 최신인 변경만 적용하는 방식을 검토할 수 있습니다.

~~~sql
WHERE source.updated_at > target.updated_at
~~~

업데이트 시각의 정밀도와 시계 동기화가 충분하지 않으면 이 규칙만으로 순서를 완전히 보장할 수 없습니다. 가능하면 소스의 로그 위치, 트랜잭션 순번, 커넥터가 제공하는 이벤트 메타데이터를 함께 사용합니다.

#### 작업 유형 처리

| operation | 처리 방식 |
|---|---|
| INSERT | 신규 키를 삽입합니다. 이미 존재하면 upsert 규칙을 적용합니다. |
| UPDATE | 같은 키의 현재 상태를 갱신합니다. |
| DELETE | 대상 행·문서를 삭제하거나 삭제 표시를 기록합니다. |
| READ 또는 SNAPSHOT | 초기 스냅샷 행으로 처리합니다. |
| DDL | 스키마 변경 정책에 따라 적용하거나 별도로 기록합니다. |

### 6.4 ClickHouse Sink 주의점

제공된 자료는 ClickHouse Sink의 멱등성을 ReplacingMergeTree 같은 엔진과 함께 조건부로 설명합니다. ReplacingMergeTree가 최신 행을 선택하는 시점과 FINAL 사용 여부, ORDER BY 키, 버전 컬럼, 삭제 표시 설계가 결과에 영향을 줄 수 있습니다.

따라서 Kafka 이벤트를 ClickHouse에 적재할 때 다음을 문서화해야 합니다.

- Kafka key와 ClickHouse ORDER BY 키의 대응
- UPDATE 이벤트의 새 행 표현
- DELETE 이벤트의 삭제 표시 또는 별도 삭제 처리
- 중복 이벤트가 머지되기 전 조회될 가능성
- 버전 컬럼의 의미
- 재처리 시 동일 파티션과 동일 키의 처리 순서

이 장에서는 ClickHouse의 최종 테이블 엔진과 쿼리 전략을 확정하지 않습니다. [추가 자료 조사 필요: 이 책의 ClickHouse 적재 방식과 Flink CDC 재처리·삭제 이벤트의 정확성 검증]

## 7. DDL과 스키마 변경

### 7.1 스키마 변화의 유형

CDC 파이프라인은 데이터 값뿐 아니라 구조의 변화도 처리해야 합니다. 대표적인 변화는 다음과 같습니다.

- 새 컬럼 또는 필드 추가
- 컬럼 또는 필드 삭제
- 컬럼 이름 변경
- 자료형 변경
- 기본 키 변경
- 테이블·컬렉션 생성과 삭제
- 인덱스 또는 메타데이터 변경

관계형 데이터베이스의 DDL과 MongoDB 문서의 새 필드 추가는 같은 사건이 아닙니다. 관계형 DDL은 명시적인 스키마 변경 명령으로 발생하지만, MongoDB의 문서 필드는 기존 컬렉션의 일부 문서에만 새로 나타날 수 있습니다.

### 7.2 제공된 자료의 스키마 진화 정책

수집 자료는 Flink CDC의 스키마 변경 동작을 다음 세 가지 정책으로 정리합니다.

| 정책 | 동작 |
|---|---|
| lenient | 호환 가능한 변경을 자동 적용하고 위험한 변경은 제한적으로 처리합니다. |
| strict | 허용되지 않은 DDL 또는 스키마 변화가 발생하면 Job 실패를 선택합니다. |
| evolve | 더 넓은 범위의 변화를 downstream 스키마에 전파하는 방식으로 제시됩니다. 데이터 손실에 주의해야 합니다. |

정책 이름과 지원 범위는 Flink CDC 버전, Table API, sink connector에 따라 달라질 수 있습니다. 조사 자료의 표현을 모든 Flink 버전에 공통으로 적용하지 않습니다. [검토 필요: Flink CDC 3.6.0의 실제 schema.change.behavior 옵션과 지원 값]

### 7.3 안전한 스키마 변경

실습에서는 기존 필드를 깨뜨리지 않는 변경부터 시작합니다.

~~~sql
ALTER TABLE orders
    ADD COLUMN discount_amount DECIMAL(10, 2) DEFAULT 0;
~~~

MongoDB에서는 문서에 새 필드를 추가합니다.

~~~javascript
db.orders.updateMany(
  {},
  { $set: { discount_amount: Decimal128("0.00") } }
);
~~~

새 필드 추가는 기존 소비자가 무시할 수 있는 방향으로 설계하기 쉽습니다. 반대로 기존 필드 삭제, 이름 변경, 호환되지 않는 자료형 변경은 downstream의 파싱·저장·쿼리를 깨뜨릴 수 있습니다.

스키마 변경을 적용할 때는 다음 순서를 권장합니다.

1. 새 필드를 nullable 또는 기본값과 함께 추가합니다.
2. 생산자와 소비자가 새 필드를 이해하는지 확인합니다.
3. 일정 기간 동안 구·신 소비자 호환성을 유지합니다.
4. 기존 필드 제거가 필요한 경우 별도 마이그레이션을 수행합니다.
5. 스키마 버전과 변경 시각을 기록합니다.

### 7.4 세 데이터베이스의 차이

| DDL·스키마 변화 | MySQL | PostgreSQL | MongoDB |
|---|---|---|---|
| 컬럼·필드 추가 | DDL 이벤트와 데이터 이벤트 처리 | DDL·relation 정보와 데이터 처리 | 문서별 필드 추가 |
| 컬럼·필드 삭제 | 데이터 손실과 sink 호환성 검토 | 데이터 손실과 sink 호환성 검토 | 문서 구조가 서로 다를 수 있음 |
| 자료형 변경 | 호환성 확인 필요 | 호환성 확인 필요 | 문서별 자료형 혼재 가능 |
| DDL 전파 | 커넥터·sink 정책에 의존 | 커넥터·sink 정책에 의존 | 관계형 DDL과 동일하게 지원되지 않음 |
| 기본 키 변경 | CDC 키와 sink 키 영향 | Replica Identity와 sink 키 영향 | 업무 키와 MongoDB _id 관계 영향 |

MongoDB의 컬렉션·인덱스 작업을 Change Streams로 어느 범위까지 감지할 수 있는지는 구독 수준과 커넥터에 따라 달라질 수 있습니다. 자료에는 MongoDB DDL 지원이 제한적이라는 설명이 있으므로, MongoDB 스키마 변경을 관계형 DDL 전파와 동일하게 다루지 않습니다.

## 8. DELETE 이벤트와 tombstone

### 8.1 CDC 변경 이벤트의 기본 형태

CDC 이벤트는 일반적으로 다음 의미를 가집니다.

| 연산 | before | after | 의미 |
|---|---|---|---|
| INSERT | 없음 | 새 행·문서 | 새 데이터가 생성되었습니다. |
| UPDATE | 이전 값 또는 일부 이전 값 | 변경 후 값 | 기존 데이터가 변경되었습니다. |
| DELETE | 삭제 전 값 또는 식별자 | 없음 | 데이터가 삭제되었습니다. |
| SNAPSHOT READ | 없음 또는 snapshot 표시 | 현재 행·문서 | 초기 스냅샷 데이터입니다. |

실제 필드명과 envelope는 커넥터와 format에 따라 달라질 수 있습니다. 표의 before·after는 개념 모델이며, 모든 소스가 항상 전체 before·after 문서를 제공한다는 뜻은 아닙니다.

### 8.2 DELETE 처리

DELETE 이벤트를 downstream에서 처리하는 방법은 두 가지가 있습니다.

1. 실제 행·문서를 삭제합니다.
2. 삭제 표시 컬럼을 기록하는 soft delete를 사용합니다.

실제 삭제는 최종 상태를 단순하게 만들지만, 감사와 재처리가 어려워질 수 있습니다. soft delete는 이벤트 이력을 보존하기 쉽지만, 모든 조회에서 삭제 표시 조건을 적용해야 합니다.

~~~sql
MERGE INTO target_table AS target
USING delete_events AS source
ON target.id = source.id
WHEN MATCHED THEN DELETE;
~~~

이 SQL은 삭제 이벤트를 실제 삭제로 적용하는 개념 예제입니다. 실제 sink에서 MERGE를 지원하는지와 트랜잭션 범위를 확인해야 합니다.

### 8.3 Kafka tombstone

Kafka tombstone은 특정 키에 null 값을 가진 레코드입니다. compact 정책의 토픽에서 키의 최신 상태를 제거하는 신호로 사용할 수 있습니다.

~~~text
key: 123
value: null
~~~

tombstone은 일반 CDC DELETE envelope과 같은 개념이 아닙니다. CDC 커넥터가 DELETE 이벤트를 발생시킨 뒤 Kafka Sink가 tombstone으로 변환할 수도 있고, DELETE 정보를 일반 JSON 값으로 보낼 수도 있습니다.

| 구분 | CDC DELETE 이벤트 | Kafka tombstone |
|---|---|---|
| 표현 | operation=DELETE와 key·before 정보 등 | key와 null value |
| 목적 | 변경 사실과 삭제 대상 전달 | compact 토픽에서 키 제거 신호 |
| 정보량 | 커넥터에 따라 이전 값 포함 가능 | 값이 없으므로 정보가 적음 |
| 사용 위치 | CDC 원본·처리 파이프라인 | Kafka compact 상태 토픽 |

컴팩션 토픽에서 tombstone이 제거되기 전에 소비자가 삭제를 처리해야 합니다. 제공된 자료는 delete.retention.ms를 24시간으로 제시하지만, 실제 운영값은 소비자 장애 시간과 복구 정책을 기준으로 산정해야 합니다.

### 8.4 DELETE 실습

앞 장에서 만든 MySQL, PostgreSQL, MongoDB 데이터에 삭제를 발생시킵니다.

~~~sql
DELETE FROM orders
WHERE order_id = 1;
~~~

~~~javascript
db.orders.deleteOne({ order_id: NumberLong(1) });
~~~

그 다음 Flink CDC 출력과 Kafka 토픽을 확인합니다. 어떤 경우에는 DELETE 이벤트가 행 식별자만 포함하고, 어떤 경우에는 before 값이 포함될 수 있습니다. 확인 결과를 소스별로 기록해야 합니다. [추가 자료 조사 필요: 세 커넥터의 실제 DELETE envelope과 Kafka tombstone 변환 결과]

## 9. Flink CDC 버전 호환성

### 9.1 조사 자료의 버전 조합

제공된 자료는 Flink CDC 3.6.0을 다음 Flink 계열과 조합할 수 있다고 정리합니다.

| Flink CDC | Flink | JDK | 자료상 비고 |
|---|---|---|---|
| 3.6.0 | 1.20.x | 11 이상 | 책의 안정적인 선택지로 제시 |
| 3.6.0 | 2.2.x | 11 이상 | 공식 지원 조합으로 제시 |
| 3.5.0 | 1.19.x | 11 이상 | 이전 조합 |
| 3.4.0 | 1.18.x | 11 이상 | 이전 조합 |

수집 자료의 다른 부분에는 Flink 2.1.x와 2.2.x에 대한 설명이 함께 나타나므로, 버전 표는 최종 릴리스 노트와 공식 호환성 문서를 기준으로 다시 확정해야 합니다. [검토 필요: Flink CDC 3.6.0의 공식 호환성 매트릭스와 Flink 2.x 릴리스 상태]

### 9.2 이 책의 권장 기준

제공된 자료를 기준으로 한 기본 선택은 다음과 같습니다.

- Apache Flink CDC: 3.6.0
- Apache Flink: 1.20.x 또는 2.2.x
- JDK: 17
- MySQL Connector: mysql-cdc
- PostgreSQL Connector: postgres-cdc
- MongoDB Connector: mongodb-cdc

초보자를 위한 로컬 Docker Compose 실습에서는 Flink 1.20.x를 먼저 선택하고, Flink 2.2.x는 호환성 검증 후 대안으로 제시하는 편이 책의 재현성을 높일 수 있습니다. 그러나 이는 제공된 자료의 안정성 설명을 바탕으로 한 편집 판단이며, 실제 이미지·JAR 조합의 실행 검증 자료는 추가되어야 합니다. [검토 필요: 최종 Docker 이미지와 connector JAR의 Flink 1.20.x·JDK 17 조합]

### 9.3 커넥터 JAR 관리

CDC 커넥터는 Flink 런타임에 적절한 connector JAR이 있어야 합니다. 자료에는 Maven 저장소에서 MySQL CDC connector JAR을 내려받는 예가 있습니다.

~~~bash
wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-mysql-cdc/3.6.0/flink-connector-mysql-cdc-3.6.0.jar
~~~

실제 실행에는 MySQL만이 아니라 PostgreSQL·MongoDB connector, Kafka connector, 필요한 format JAR 및 Flink 버전과 호환되는 의존성이 필요합니다. JAR을 단순히 한 디렉터리에 복사하는 것만으로 의존성 충돌이 해결된다고 볼 수 없습니다. [추가 자료 조사 필요: Flink CDC 3.6.0의 세 소스·Kafka Sink 전체 JAR 목록과 Docker 이미지 배치 절차]

## 10. 세 DB 지원 범위와 제한

### 10.1 기능 비교

| 기능 | MySQL | PostgreSQL | MongoDB |
|---|---|---|---|
| 초기 스냅샷 | 지원 | 지원 | 지원 |
| 스트리밍 CDC | binlog | WAL logical replication | Change Streams |
| INSERT | 지원 | 지원 | 지원 |
| UPDATE | 지원 | 지원 | 지원 |
| DELETE | 지원 | 지원 | 지원 |
| 스키마 진화 | DDL·connector 정책에 의존 | relation·connector 정책에 의존 | 제한적·문서 유연성 |
| GTID | 사용 가능·권장 | 해당 없음 | 해당 없음 |
| Replica Set | 해당 없음 | 해당 없음 | 필수 조건 |
| Replica Identity | 해당 없음 | 필요 시 FULL 검토 | 해당 없음 |
| 재개 위치 | GTID·binlog 위치 | slot·LSN | resume token |

자료에서는 MySQL, PostgreSQL, MongoDB의 지원 버전과 connector 이름을 다음처럼 정리합니다.

| 데이터베이스 | 자료상 지원 버전 | Connector |
|---|---|---|
| MySQL | 5.6, 5.7, 8.0 이상 | mysql-cdc |
| PostgreSQL | 9.6 이상 | postgres-cdc |
| MongoDB | 3.6 이상, 4.0 이상 권장 | mongodb-cdc |

이 표의 버전은 Flink CDC 3.6.0과 개별 데이터베이스 버전의 조합에 대한 최종 보증표가 아닙니다. 이 책에서 사용하는 Percona MySQL 8.4, PostgreSQL 17, MongoDB 8.0 이미지와 connector JAR의 실제 지원 여부를 교차 확인해야 합니다. [검토 필요: Percona 8.4·PostgreSQL 17·MongoDB 8.0과 Flink CDC 3.6.0의 공식 호환성]

### 10.2 MySQL 제한

- binlog_format=ROW가 필요합니다.
- binlog_row_image=FULL을 사용해야 합니다.
- CDC 사용자 권한이 필요합니다.
- 기본 키가 없는 테이블은 증분 스냅샷과 병렬 처리에 제한이 생길 수 있습니다.
- GTID 또는 binlog 위치가 보존되지 않으면 장애 후 재개할 수 없습니다.
- binlog 보존 기간을 초과하면 초기 스냅샷 또는 재동기화가 필요할 수 있습니다.

### 10.3 PostgreSQL 제한

- wal_level=logical이 필요합니다.
- replication slot과 WAL sender를 위한 설정이 필요합니다.
- slot 누수는 WAL과 디스크 사용량을 증가시킬 수 있습니다.
- UPDATE·DELETE에서 전체 이전 행이 필요하면 Replica Identity FULL을 검토해야 합니다.
- WAL 보존 범위를 벗어난 LSN은 재개할 수 없습니다.
- pgoutput, publication, connector의 slot 관리 방식을 일치시켜야 합니다.

### 10.4 MongoDB 제한

- standalone 서버에서는 Change Streams를 사용할 수 없습니다.
- 단일 노드 Replica Set은 로컬 학습에 적합하지만 고가용성을 제공하지 않습니다.
- oplog rollover가 resume token보다 빠르면 재개할 수 없습니다.
- 문서마다 필드와 자료형이 다를 수 있어 downstream 스키마 정규화가 필요합니다.
- 관계형 데이터베이스의 DDL과 같은 방식의 스키마 진화를 기대해서는 안 됩니다.
- before 이미지, fullDocument, heartbeat 옵션은 MongoDB와 connector 버전을 기준으로 확인해야 합니다.

## 11. 로컬 Docker Compose 실습 절차

### 11.1 사전 조건 확인

앞 장의 세 데이터베이스와 5장의 Kafka가 실행되고 있어야 합니다.

~~~bash
docker compose ps
docker compose logs --tail=100 mysql
docker compose logs --tail=100 postgres
docker compose logs --tail=100 mongodb
docker compose logs --tail=100 kafka
~~~

다음 상태를 확인합니다.

| 대상 | 확인 내용 |
|---|---|
| MySQL | binlog 활성화, ROW, FULL row image, CDC 사용자 |
| PostgreSQL | logical WAL, publication, slot·sender 설정 |
| MongoDB | Replica Set primary 상태, Change Streams 실행 조건 |
| Kafka | 브로커 healthy, 대상 토픽과 파티션 |
| Flink | 런타임, connector JAR, checkpoint 저장 위치 |

### 11.2 Kafka 토픽 준비

Flink CDC가 기록할 토픽을 먼저 생성합니다.

~~~bash
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic mysql-cdc \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete

docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic postgres-cdc \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete

docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic mongodb-cdc \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete
~~~

토픽 이름은 Flink CDC의 실제 sink 설정과 일치해야 합니다. 커넥터가 소스별 또는 테이블별 토픽을 생성하도록 구성되는 경우 이름 규칙을 별도로 적용합니다.

### 11.3 Flink Job 제출 전 확인

다음 항목을 확인한 뒤 Job을 제출합니다.

1. Flink 버전과 Flink CDC 버전이 선택한 조합과 일치하는가?
2. MySQL·PostgreSQL·MongoDB connector JAR이 모두 있는가?
3. Kafka connector와 JSON format JAR이 있는가?
4. Flink Job에서 mysql, postgres, mongodb, kafka 서비스 이름을 해석할 수 있는가?
5. CDC 사용자의 비밀번호와 권한이 유효한가?
6. checkpoint 저장 위치가 컨테이너 재생성 후에도 필요한 기간 유지되는가?
7. 대상 Kafka 토픽의 key와 partition 수가 설계와 일치하는가?

현재 수집 자료는 Flink Kubernetes Operator의 FlinkDeployment 예시를 중심으로 제공하므로, 이 책의 Docker Compose 실습에서 사용할 정확한 Job 제출 명령은 보강이 필요합니다. [추가 자료 조사 필요: Docker Compose에서 Flink SQL Client 또는 standalone JobManager로 CDC Job을 제출하는 단계별 명령]

### 11.4 변경 발생과 관찰

Flink Job이 실행된 뒤 소스 데이터베이스에 변경을 발생시킵니다.

~~~sql
INSERT INTO customers (name, email)
VALUES ('Alice', 'alice@example.com');

UPDATE customers
SET email = 'alice-new@example.com'
WHERE name = 'Alice';

DELETE FROM customers
WHERE name = 'Alice';
~~~

MongoDB에서도 같은 업무 변화를 발생시킵니다.

~~~javascript
db.customers.insertOne({
  customer_id: NumberLong(1),
  name: "Alice",
  email: "alice@example.com",
  created_at: new Date(),
  updated_at: new Date()
});

db.customers.updateOne(
  { customer_id: NumberLong(1) },
  { $set: { email: "alice-new@example.com", updated_at: new Date() } }
);

db.customers.deleteOne({
  customer_id: NumberLong(1)
});
~~~

Kafka에서 이벤트를 확인합니다.

~~~bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic mysql-cdc \
  --from-beginning \
  --property "print.key=true" \
  --property "print.partition=true" \
  --property "print.offset=true" \
  --max-messages 20
~~~

이때 반드시 기록할 항목은 다음과 같습니다.

- 이벤트가 어느 토픽에 기록되었는가?
- Kafka key가 어떤 식별자를 포함하는가?
- INSERT·UPDATE·DELETE가 어떤 구조로 표현되는가?
- 스냅샷 이벤트와 실시간 이벤트를 구분할 수 있는가?
- 같은 키의 이벤트가 같은 파티션에 기록되었는가?
- Flink checkpoint가 성공했는가?
- Kafka consumer group의 LAG가 증가하거나 해소되는가?

## 12. 장애·재처리 실습

### 12.1 Flink Job 중지 후 재개

먼저 정상적으로 checkpoint가 완료되었는지 확인합니다. 그 다음 Job을 중지하고 다시 실행합니다. 재개 후 마지막 checkpoint 이후의 이벤트가 다시 전달될 수 있는지 확인합니다.

이 실습의 목적은 중복 가능성을 관찰하는 것입니다. 결과를 비교할 때는 Kafka offset만 보지 말고 sink의 실제 행 수와 키별 최종 상태를 함께 비교합니다.

### 12.2 소비자 그룹 오프셋 재처리

Kafka 소비자 그룹을 별도로 사용해 과거 이벤트를 다시 읽을 수 있습니다.

~~~bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group cdc_group

docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group cdc_group \
  --topic mysql-cdc \
  --reset-offsets \
  --to-earliest \
  --execute
~~~

이 명령을 실행하기 전에 해당 소비자 그룹을 중지해야 합니다. Flink checkpoint와 Kafka consumer group offset을 동시에 사용하는 구성에서는 어느 계층이 source 위치의 기준인지 혼동하지 않아야 합니다. Flink CDC Source의 내부 위치와 Kafka Sink 또는 별도 Kafka Consumer의 offset은 서로 다른 상태입니다.

### 12.3 로그 보존 만료 시나리오

다음 상황을 구분해 기록합니다.

| 상황 | 예상되는 결과 |
|---|---|
| Kafka 이벤트는 남아 있지만 Flink checkpoint가 없음 | Kafka에서 별도 재처리 가능, Flink 소스 재개는 별도 판단 |
| Flink checkpoint는 있지만 MySQL binlog가 만료됨 | 저장된 위치에서 MySQL 소스를 재개하기 어려움 |
| PostgreSQL slot은 남아 있지만 WAL이 필요한 범위에 없음 | slot 상태와 WAL 보존 범위를 확인하고 재동기화 검토 |
| MongoDB resume token은 있지만 oplog가 rollover됨 | 이전 token 재개 실패 가능, 초기 스냅샷 검토 |
| sink에 일부 데이터가 반영된 상태에서 Job 재시작 | 중복·부분 반영 여부와 멱등성 확인 |

## 장 요약

CDC는 데이터베이스의 변경을 downstream으로 전달하는 방법입니다. 쿼리 기반 CDC가 일정 주기로 현재 행을 조회하는 방식이라면, 로그 기반 CDC는 binlog, WAL, oplog와 Change Streams 같은 변경 기록을 읽습니다.

Flink CDC는 세 데이터베이스의 프로토콜에 맞는 커넥터를 제공합니다. MySQL은 ROW binlog와 GTID, PostgreSQL은 logical replication·publication·replication slot·LSN, MongoDB는 Replica Set·Change Streams·resume token을 사용합니다.

초기 스냅샷은 기존 데이터를 downstream에 채우고, 스트리밍 전환은 스냅샷 이후 변경을 계속 전달합니다. 스냅샷과 스트리밍의 경계가 정확해야 하지만, 실제 중복·순서·잠금 동작은 커넥터 버전과 설정을 기준으로 검증해야 합니다.

Flink checkpoint는 소스 위치와 연산 상태를 저장해 장애 후 복구를 돕습니다. 그러나 checkpoint 복구는 중복 없는 최종 결과를 자동으로 보장하지 않습니다. sink는 기본 키, 버전, operation, DELETE 규칙을 이용해 멱등적으로 처리해야 합니다.

스키마 진화에서는 새 필드 추가처럼 호환성이 높은 변경부터 적용하고, 컬럼 삭제·이름 변경·자료형 변경은 downstream과 함께 계획해야 합니다. DELETE 이벤트와 Kafka tombstone은 목적이 다르므로, CDC DELETE를 tombstone으로 변환하는 시점을 명확히 해야 합니다.

마지막으로 Flink CDC 3.6.0과 Flink 버전·JDK·connector JAR의 조합을 고정하고, Docker Compose 기반 로컬 실행 절차를 별도로 검증해야 합니다. 다음 장에서는 이 이벤트의 구조와 데이터 품질·스키마 계약을 다루게 됩니다.

## 확인 문제

1. 로그 기반 CDC와 쿼리 기반 CDC의 가장 큰 구조적 차이는 무엇입니까?
2. 쿼리 기반 CDC가 DELETE를 놓칠 수 있는 이유는 무엇입니까?
3. MySQL CDC에서 binlog_format=ROW와 binlog_row_image=FULL을 사용하는 이유는 무엇입니까?
4. PostgreSQL CDC에서 replication slot이 필요한 이유는 무엇입니까?
5. MongoDB standalone 서버 대신 Replica Set을 사용해야 하는 이유는 무엇입니까?
6. MySQL의 GTID, PostgreSQL의 LSN, MongoDB의 resume token은 각각 어떤 역할을 합니까?
7. 초기 스냅샷 중 발생한 변경을 누락하지 않기 위해 어떤 경계를 관리해야 합니까?
8. Flink checkpoint가 있어도 중복 이벤트가 발생할 수 있는 이유는 무엇입니까?
9. 멱등적인 sink를 설계할 때 기본 키와 operation 타입을 어떻게 사용합니까?
10. Kafka tombstone과 CDC DELETE envelope의 차이는 무엇입니까?
11. PostgreSQL의 REPLICA IDENTITY FULL을 모든 테이블에 적용할 때 고려할 비용은 무엇입니까?
12. MongoDB oplog rollover가 resume token 복구에 미치는 영향은 무엇입니까?
13. 스키마 변경에서 lenient와 strict 정책의 차이는 무엇입니까?
14. Flink CDC connector JAR과 Flink runtime 버전을 함께 관리해야 하는 이유는 무엇입니까?
15. Flink CDC Source의 위치와 Kafka Consumer Group의 offset을 구분해야 하는 이유는 무엇입니까?

## 이 장에서 자료가 부족했던 부분

- Docker Compose에서 실행할 Flink JobManager·TaskManager·Flink CDC 3.6.0의 최종 구성 예제가 부족합니다.
- Flink 1.20.x와 2.2.x 중 초보자용 기준 버전 및 공식 Docker 이미지가 확정되지 않았습니다.
- Flink CDC 3.6.0의 MySQL·PostgreSQL·MongoDB·Kafka Sink 전체 JAR 의존성 목록이 필요합니다.
- Flink CDC 3.6.0의 각 커넥터별 실제 CDC 이벤트 envelope과 before·after 필드 구조가 필요합니다.
- MySQL 초기 스냅샷의 잠금, 청크, 일관성 동작은 connector 버전과 옵션별 공식 확인이 필요합니다.
- PostgreSQL Replica Identity 설정과 Flink CDC·최종 sink의 정확한 요구 수준이 확정되지 않았습니다.
- MongoDB CDC 사용자의 최소 권한, fullDocument, before 이미지, heartbeat 옵션의 공식 근거가 더 필요합니다.
- 세 소스의 Kafka key와 토픽 라우팅 규칙이 확정되지 않았습니다.
- Flink checkpoint 저장소와 Docker Compose 컨테이너 재생성 후 복구 절차가 부족합니다.
- CDC의 실제 지연 시간·처리량과 Windows 11·Apple Silicon 간 성능 비교 자료가 없습니다.
- ClickHouse 또는 Iceberg sink에서 중복·UPDATE·DELETE·tombstone을 최종적으로 처리하는 구현이 확정되지 않았습니다.
- Flink CDC의 schema.change.behavior 실제 옵션명과 버전별 지원 범위를 공식 문서로 확인해야 합니다.

## 참고 자료

1. [Apache Flink 공식 사이트](https://flink.apache.org/)
2. [Apache Flink CDC 3.6.0 릴리스 자료](https://flink.apache.org/2026/03/30/apache-flink-cdc-3.6.0-release-announcement.html)
3. [Flink CDC MySQL Connector 참고 자료](https://help.aliyun.com/en/flink/realtime-flink/developer-reference/best-practices-for-mysql-connector)
4. [Flink CDC PostgreSQL Connector 참고 자료](https://help.aliyun.com/en/flink/realtime-flink/developer-reference/postgresql-cdc-connector/)
5. [Flink CDC MongoDB Connector 참고 자료](https://help.aliyun.com/en/flink/realtime-flink/developer-reference/feishu-cdc)
6. [Flink CDC Connector와 스키마 처리 자료](https://pipecode.ai/blogs/flink-cdc-connectors-schema-first-streaming-ingest)
7. [Apache Flink 2.3.0 릴리스 자료](https://flink.apache.org/2026/06/25/apache-flink-2.3.0-release-announcement/)
8. [CDC 기본 개념 자료](https://www.conduktor.io/glossary/what-is-change-data-capture-cdc-fundamentals)
9. [CDC 스트림 처리 자료](https://risingwave.com/blog/cdc-stream-processing-complete-guide/)
10. [CDC tombstone 처리 자료](https://streamkap.com/resources-and-guides/cdc-soft-deletes-tombstones)
11. [Flink Schema Evolution 참고 자료](https://docs.confluent.io/cloud/current/flink/concepts/schema-statement-evolution.html)
12. [Flink 버전 상호 운용성 자료](https://docs.confluent.io/cp-flink/current/installation/versions-interoperability.html)

<!-- 편집 메모: 이 원고는 사용자가 제공한 6장 조사 자료를 기반으로 작성했다. 로그 기반 CDC, 세 DB 프로토콜, snapshot·streaming, checkpoint·복구, 멱등성, schema evolution, tombstone의 개념은 본문에 반영했다. Docker Compose 실행 구성, connector envelope, 버전·JAR 호환성, 최종 sink 정확성처럼 자료가 부족하거나 상충하는 항목은 검토 필요 또는 추가 자료 조사 필요로 표시했다. -->
