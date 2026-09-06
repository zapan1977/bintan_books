# 11장. Apache Flink CDC와 Iceberg 데이터 파이프라인 통합

## 이 장의 목표

이 장에서는 앞 장에서 구성한 Apache Polaris Catalog와 Apache Ozone 저장소를 Apache Flink CDC 파이프라인에 연결한다. 소스 데이터베이스의 변경 이벤트를 Kafka에서 읽고, Iceberg 테이블에 저장하는 전체 흐름을 단계별로 이해한다.

특히 Flink checkpoint와 Iceberg Snapshot commit이 어떤 관계를 갖는지 설명한다. Upsert와 Merge-on-Read를 이용한 CDC 처리, 작은 파일 문제와 Compaction, 스키마·파티션 변경, 장애 복구와 exactly-once semantics를 실습 설계 관점에서 다룬다.

- 난이도: 중급
- 선수 지식: Docker Compose, Kafka 토픽, Flink CDC, Iceberg Snapshot, Polaris REST Catalog, Ozone S3 Gateway
- 기준 흐름: 소스 DB → Flink CDC 또는 Kafka → Flink → Polaris Catalog → Ozone의 Iceberg Warehouse
- 기준 후보 버전: Flink 2.2.1, Flink CDC 3.6.0, Iceberg 1.11.0, Polaris 1.7.0, Ozone 2.2.0
- 다음 장과의 연결: Spark를 이용한 백필·대량 변환·유지보수 작업으로 확장한다

> 이 장의 버전과 엔진 조합은 자료 조사 기준 후보다. Flink 2.2.1·Flink CDC 3.6.0·Iceberg 1.11.0·Polaris 1.7.0·Ozone 2.2.0의 실제 호환성은 출간 전 Docker Compose 통합 테스트로 확정해야 한다.

## 1. 전체 데이터 흐름

5장에서는 Kafka를 이벤트 전달 계층으로 구성했고, 6장에서는 Flink CDC가 소스 데이터베이스의 변경을 수집하는 방법을 배웠다. 8장에서는 Ozone을 객체 저장소로 준비했고, 9장에서는 Iceberg가 여러 Parquet 파일을 하나의 테이블로 관리하는 원리를 살펴보았다. 10장에서는 Polaris를 Iceberg REST Catalog로 연결했다.

이 장에서는 이 구성 요소를 하나의 처리 흐름으로 묶는다.

~~~mermaid
flowchart LR
    D["Percona MySQL · PostgreSQL · MongoDB"] --> C["Flink CDC"]
    C --> K["Apache Kafka"]
    K --> F["Apache Flink"]
    F --> P["Apache Polaris"]
    F --> O["Apache Ozone"]
    P --> I["Iceberg Table"]
    O --> I
~~~

실제 역할은 다음과 같이 나뉜다.

| 구성 요소 | 이 장에서의 책임 |
|---|---|
| Flink CDC | 데이터베이스 변경을 이벤트로 수집 |
| Kafka | 변경 이벤트를 토픽에 전달하고 보관 |
| Apache Flink | 이벤트 변환과 Iceberg Sink 실행 |
| Apache Polaris | Iceberg Catalog, Table 위치, 권한과 커밋 경로 관리 |
| Apache Ozone | Parquet, Manifest, Metadata 파일 저장 |
| Apache Iceberg | Snapshot과 테이블 상태 관리 |

Polaris가 데이터를 저장하고 Flink가 Catalog를 대신하는 구조가 아니다. Flink는 처리 엔진이고, Polaris는 Catalog이며, Ozone은 저장소다. 이 경계를 지켜야 장애 발생 시 확인할 위치를 정확히 정할 수 있다.

## 2. Flink CDC와 Iceberg Sink 연결

### 2.1 두 가지 입력 구조

Flink가 Iceberg에 데이터를 쓰는 입력은 크게 두 가지로 구성할 수 있다.

1. Flink CDC Source가 데이터베이스에서 변경을 직접 읽는 방식
2. Kafka Source가 CDC 이벤트를 읽고 Iceberg Sink로 전달하는 방식

이 책의 전체 아키텍처에서는 Kafka를 이벤트 버스이자 재처리 지점으로 사용하므로, 11장 기본 흐름은 Kafka Source에서 Iceberg Sink로 연결한다. Flink CDC가 직접 Iceberg Sink를 구성하는 경로는 별도 비교 실습으로 둔다.

| 구조 | 장점 | 확인할 항목 |
|---|---|---|
| DB → Flink CDC → Iceberg | 경로가 짧고 구성 단순 | CDC Source와 Sink의 checkpoint 연계 |
| DB → Flink CDC → Kafka → Flink → Iceberg | 재처리·다중 소비자·이벤트 보관 | Kafka offset, 스키마, 중복 처리 |
| DB → Flink CDC → Kafka → Dynamic Iceberg Sink | 여러 토픽·테이블 확장 후보 | 버전과 자동 Table 관리 |

현재 책의 기본 경로는 두 번째 구조다.

### 2.2 CDC 이벤트의 논리

CDC 이벤트는 단순한 현재 행 데이터가 아니라 변경 유형을 포함한다. 일반적으로 다음 이벤트 의미를 구분해야 한다.

| 이벤트 유형 | 의미 | Iceberg 처리 방향 |
|---|---|---|
| INSERT | 새 행 생성 | 데이터 파일에 추가 |
| UPDATE_BEFORE | 변경 전 값 | Upsert 처리 정책에 따라 사용 |
| UPDATE_AFTER | 변경 후 값 | 기존 키를 대체 |
| DELETE | 행 삭제 | Delete file 또는 재작성 정책 |

실제 이벤트 필드와 직렬화 형식은 Flink CDC 커넥터와 Kafka 메시지 포맷에 따라 달라질 수 있다. Iceberg Sink에서 사용하는 primary key와 Kafka 이벤트의 키가 일치하지 않으면, 동일 행을 찾아 갱신하거나 삭제하기 어렵다.

[추가 자료 조사 필요: Percona MySQL·PostgreSQL·MongoDB별 CDC 이벤트 스키마와 이 책의 공통 Kafka 이벤트 계약]

### 2.3 Iceberg Table과 Primary Key

CDC 데이터를 Iceberg에 Upsert하려면 테이블의 논리적 식별자와 이벤트의 primary key를 일치시켜야 한다.

예를 들어 customers_cdc 테이블의 id를 기본 키로 사용한다고 가정한다.

~~~sql
CREATE TABLE lakehouse.raw.customers_cdc (
  id BIGINT,
  name STRING,
  email STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
  'format-version' = '2'
);
~~~

이 DDL은 Spark SQL 형태의 예시다. Flink SQL에서의 PRIMARY KEY 선언과 Iceberg Sink의 equality field 설정은 실제 커넥터 버전에 맞춰 검증해야 한다.

## 3. Polaris와 Ozone을 사용하는 Flink 설정

### 3.1 Catalog 설정

Flink가 Iceberg REST Catalog를 사용하려면 다음 항목을 준비해야 한다.

- Iceberg Flink runtime JAR
- REST Catalog 구현체와 URI
- Polaris 인증 정보
- Catalog 이름과 warehouse
- Ozone S3A 또는 S3 FileIO 설정
- path-style access
- checkpoint 저장 위치

개념적인 Flink SQL Catalog 설정 예시는 다음과 같다.

~~~sql
CREATE CATALOG lakehouse WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://polaris:8181/api/catalog',
  'warehouse' = 'lakehouse_catalog'
);
~~~

10장에서 설명한 것처럼 Polaris endpoint의 정확한 경로는 배포 버전과 설정에 따라 달라질 수 있다. 위 SQL은 구조를 설명하는 예시이며, 이 책의 최종 Compose 파일에서는 실제 Polaris 1.7.0 응답을 확인한 뒤 확정한다.

[추가 자료 조사 필요: Flink 2.2.1과 Iceberg 1.11.0의 REST Catalog 설정 키, Polaris OAuth2 credential 속성]

### 3.2 Ozone S3 설정

Flink 컨테이너에서 Ozone S3 Gateway에 접근할 때는 호스트의 localhost가 아니라 Docker Compose 네트워크의 서비스명을 사용한다.

~~~properties
fs.s3a.endpoint=http://ozone-s3g:9878
fs.s3a.path.style.access=true
fs.s3a.connection.ssl.enabled=false
fs.s3a.access.key=ozone
fs.s3a.secret.key=ozone-secret
~~~

호스트의 AWS CLI는 다음 endpoint를 사용한다.

~~~text
http://localhost:9878
~~~

두 endpoint를 혼동하면 Flink는 정상적으로 실행되는 것처럼 보이지만, 다른 컨테이너 또는 존재하지 않는 포트에 연결할 수 있다.

### 3.3 Catalog·Namespace 확인

Catalog 연결이 준비되면 Flink SQL에서 Namespace를 확인한다.

~~~sql
USE CATALOG lakehouse;

SHOW DATABASES;

CREATE DATABASE IF NOT EXISTS raw;

USE raw;

SHOW TABLES;
~~~

실제 Flink SQL에서 CREATE DATABASE, SHOW DATABASES 등의 지원 범위는 Flink SQL Gateway와 Catalog 구현에 따라 확인한다.

## 4. Checkpoint와 Iceberg Snapshot Commit

### 4.1 Checkpoint의 역할

Flink checkpoint는 스트리밍 작업의 상태와 소스 위치를 일관된 시점에 저장하는 메커니즘이다. Kafka Source를 사용하는 경우 checkpoint에는 처리한 Kafka offset과 연산자 상태가 연결된다.

Iceberg Sink는 처리 중인 데이터를 즉시 현재 테이블 Snapshot으로 공개하는 대신, checkpoint와 연계해 커밋할 수 있다. 조사 자료에서는 다음 흐름을 제시한다.

1. Flink가 Kafka 이벤트를 읽는다.
2. Sink가 데이터 파일과 관련 메타데이터를 작성한다.
3. checkpoint가 성공할 때까지 해당 결과를 대기 상태로 둔다.
4. 모든 연산자의 checkpoint 완료를 확인한다.
5. Sink가 pending transaction을 커밋한다.
6. Iceberg가 새 Snapshot을 생성한다.
7. Catalog가 새 테이블 상태를 현재 상태로 공개한다.

~~~mermaid
sequenceDiagram
    participant K as Kafka
    participant F as Flink
    participant O as Ozone
    participant P as Polaris
    K->>F: CDC 이벤트와 offset
    F->>O: 데이터 파일 작성
    F->>O: Manifest·Metadata 작성
    F->>F: Checkpoint 완료
    F->>P: 테이블 커밋 요청
    P->>O: 현재 Metadata 위치 갱신
    P-->>F: Snapshot commit 결과
~~~

### 4.2 Snapshot 생성 빈도

checkpoint 간격은 Iceberg Snapshot이 생성되는 빈도와 작은 파일 수에 영향을 줄 수 있다. 체크포인트를 너무 자주 수행하면 낮은 지연시간을 얻을 수 있지만, 작은 데이터 파일과 많은 메타데이터가 생성될 수 있다.

반대로 checkpoint 간격을 너무 길게 설정하면 하나의 커밋에 포함되는 데이터가 커지고 장애 발생 시 재처리 범위가 늘어날 수 있다.

이 관계는 다음과 같이 정리한다.

| checkpoint 간격 | 장점 | 부담 |
|---|---|---|
| 짧음 | 낮은 적재 지연, 작은 재처리 범위 | Snapshot·파일·Manifest 증가 |
| 김 | 커밋 횟수와 메타데이터 감소 | 적재 지연, 재처리 범위 증가 |
| 과도하게 짧음 | 실시간성 우선 | 작은 파일과 Catalog 커밋 경합 |
| 과도하게 김 | 파일 크기 확보 | 실패 시 복구 지연과 데이터 대기 |

조사 자료에는 CDC 테이블의 checkpoint 간격으로 60초 이상을 권장하는 예시가 있지만, 이는 모든 데이터량과 지연 요구사항에 적용되는 고정값이 아니다. 이 책에서는 60초를 실험 시작점으로 사용하고, 실제 파일 수·커밋 시간·처리 지연을 측정한다.

[검토 필요: 60초 checkpoint를 기본값으로 고정할지, 독자가 환경에 따라 실험하도록 범위로 제시할지]

### 4.3 Exactly-once를 조건부로 이해하기

Flink와 Iceberg의 조합에서 exactly-once 결과를 목표로 할 수 있는 핵심은 checkpoint의 처리 상태와 Iceberg Snapshot commit을 연결하는 것이다. 그러나 다음 조건을 모두 확인해야 한다.

- Flink checkpointing mode
- Kafka Source offset과 checkpoint 상태
- Sink의 pending transaction 처리
- Iceberg Catalog의 원자적 commit
- 재시작 시 checkpoint 복구
- 동일 이벤트의 재수신과 primary key 처리
- 커밋되지 않은 파일의 orphan cleanup
- Ozone과 Polaris의 가용성

따라서 “모든 설정에서 exactly-once가 보장된다”라고 쓰면 안 된다. 이 장에서는 정확한 표현을 다음처럼 사용한다.

> Flink checkpoint와 Iceberg Snapshot commit이 올바르게 연계되고, 소스 offset·Sink commit·Catalog·복구 조건이 충족된 환경에서 exactly-once 결과를 목표로 한다.

## 5. Checkpoint 설정과 장애 복구

### 5.1 설정 예시

Flink 설정의 개념 예시는 다음과 같다.

~~~yaml
execution:
  checkpointing:
    mode: EXACTLY_ONCE
    interval: 60s
    min-pause: 30s
    timeout: 10m
    max-concurrent-checkpoints: 1
  restart-strategy:
    type: failure-rate
~~~

이 YAML은 설명을 위한 예시다. 실제 Flink 설정 키는 사용하는 배포 방식과 Flink 버전에 맞춰 확인해야 한다.

checkpoint 저장 위치도 설정해야 한다.

~~~yaml
state:
  checkpoints:
    dir: s3://iceberg-bucket/flink-checkpoints/
  savepoints:
    dir: s3://iceberg-bucket/flink-savepoints/
~~~

Flink의 checkpoint와 savepoint는 Iceberg Table Metadata와 다른 상태 저장 영역이다. Iceberg warehouse에 Flink 상태 파일을 무분별하게 섞지 않고, 별도 prefix를 사용하는 것이 관리에 유리하다.

### 5.2 정상적인 재시작

정상적인 재시작 흐름은 다음과 같다.

1. 마지막으로 성공한 checkpoint를 확인한다.
2. Kafka Source가 checkpoint에 저장된 offset에서 재개되는지 확인한다.
3. Sink가 이전 pending transaction을 중복 커밋하지 않는지 확인한다.
4. 새 Snapshot 생성 시점을 확인한다.
5. 같은 primary key의 중복 행이 생기지 않았는지 검증한다.

~~~text
Flink Job
  ├─ 마지막 성공 Checkpoint
  ├─ Kafka offset 복구
  ├─ Sink transaction 상태 복구
  └─ Iceberg Snapshot commit 재개
~~~

### 5.3 실패한 checkpoint와 orphan file

데이터 파일과 Manifest가 Ozone에 먼저 작성된 뒤 checkpoint 또는 Catalog commit이 실패하면, 해당 파일이 현재 Snapshot에서 참조되지 않을 수 있다. 이 파일은 orphan file 후보가 된다.

복구 순서는 다음과 같다.

1. Flink Job의 실패 시점과 마지막 성공 checkpoint 확인
2. Kafka offset과 재처리 구간 확인
3. Iceberg Table history와 현재 Snapshot 확인
4. Ozone에 남은 파일 중 유효 Snapshot이 참조하는 파일 확인
5. 진행 중인 재시도가 끝난 뒤 orphan file 정리 계획 수립

재시작 직후 바로 Orphan File Cleanup을 실행하면 안 된다. 아직 재시도 중인 작업의 파일을 유효하지 않은 파일로 판단할 수 있기 때문이다.

## 6. Upsert와 Merge-on-Read

### 6.1 CDC에서 Upsert가 필요한 이유

CDC는 INSERT만 전달하지 않는다. UPDATE와 DELETE를 처리하려면 이벤트의 primary key를 이용해 기존 논리 행을 찾아야 한다.

Iceberg Sink의 Upsert 모드는 primary key를 기준으로 변경을 처리하고, equality delete와 같은 삭제 파일을 이용해 기존 행을 읽기에서 제외하는 방식으로 구성될 수 있다.

조사 자료의 Java API 예시는 다음과 같다.

~~~java
FlinkSink.forRowData(cdcStream)
    .tableLoader(TableLoader.fromCatalog(catalogLoader, "raw.customers_cdc"))
    .upsert(true)
    .equalityFieldColumns(Arrays.asList("id"))
    .build();
~~~

위 코드는 구조를 보여 주는 예시다. 실제 API의 패키지, 메서드, 테이블 로더 생성 방법은 Flink와 Iceberg 라이브러리 버전에 따라 달라질 수 있다.

[추가 자료 조사 필요: Flink 2.2.1과 Iceberg 1.11.0 조합에서 Upsert API와 equalityFieldColumns의 실제 컴파일·실행 결과]

### 6.2 Merge-on-Read와 Copy-on-Write

CDC 데이터는 UPDATE와 DELETE가 자주 발생하므로, 변경 때마다 기존 데이터 파일을 모두 다시 쓰는 방식은 쓰기 비용이 커질 수 있다.

| 전략 | 동작 | CDC 적용 관점 |
|---|---|---|
| Merge-on-Read | Delete file을 기록하고 읽을 때 데이터 파일과 병합 | 빈번한 변경에 적합한 후보 |
| Copy-on-Write | 변경 시 새 데이터 파일을 작성하고 기존 파일을 대체 | 읽기 단순화, 쓰기 증폭 가능 |

Merge-on-Read를 사용하면 데이터 파일을 즉시 다시 쓰지 않아도 되지만, 읽을 때 delete file을 함께 처리해야 한다. delete file이 지나치게 쌓이면 읽기 비용이 증가하므로, 주기적인 Rewrite 또는 Compaction이 필요하다.

Copy-on-Write는 읽기 시점의 병합 부담을 줄일 수 있지만, 변경량이 많을수록 기존 데이터 파일의 재작성 비용이 커질 수 있다.

따라서 “CDC는 언제나 Merge-on-Read를 사용한다”라고 고정하기보다, 변경 빈도·읽기 빈도·파일 크기·유지보수 주기를 기준으로 선택한다.

### 6.3 Delete file 확인

Iceberg 테이블의 files 메타테이블을 이용해 데이터 파일과 삭제 파일을 확인할 수 있다. 실제 메타테이블 이름과 컬럼은 엔진에 따라 달라질 수 있다.

~~~sql
-- 예시 형식
SELECT file_path, file_size_in_bytes, record_count
FROM lakehouse.raw.customers_cdc.files;
~~~

[추가 자료 조사 필요: 선택한 Flink·Spark 엔진에서 delete file의 content 값과 files 메타테이블 컬럼]

## 7. 스키마 진화와 파티션 진화

### 7.1 스키마 변경

9장에서 설명한 것처럼 Iceberg는 필드 ID 기반으로 스키마를 관리한다. CDC 소스 데이터베이스에 컬럼이 추가되면, Flink CDC 이벤트와 Iceberg Table Schema가 함께 변경되어야 한다.

소스 DB에서 다음 변경이 발생했다고 가정한다.

~~~sql
ALTER TABLE customers
ADD COLUMN phone VARCHAR(30);
~~~

이 변경을 바로 자동 반영한다고 가정하지 않는다. 다음 항목을 확인한다.

- CDC Source가 ADD COLUMN 이벤트를 전달하는가
- Kafka 이벤트 계약이 새 필드를 허용하는가
- Flink 직렬화와 역직렬화가 새 필드를 처리하는가
- Iceberg Sink가 스키마 변경을 지원하는가
- Polaris Catalog에 새 Table Metadata가 커밋되는가
- ClickHouse와 downstream 소비자가 새 스키마를 처리하는가

조사 자료는 Flink CDC 3.6.0의 Iceberg Sink에서 스키마 진화 지원을 제시하지만, 모든 소스 데이터베이스·데이터 타입·엔진 조합에 대한 자동 반영을 보장하는 근거로 확대해서는 안 된다.

### 7.2 파티션 변경

Iceberg는 파티션 사양의 진화를 지원한다. 예를 들어 일별 파티션을 시간별 파티션으로 바꾸면 기존 파일은 이전 사양을 사용하고 새 파일은 새 사양을 사용할 수 있다.

~~~sql
-- Spark SQL 형태의 예시
CREATE TABLE lakehouse.raw.orders (
  order_id BIGINT,
  order_date TIMESTAMP,
  total_amount DECIMAL(10, 2)
)
USING iceberg
PARTITIONED BY (days(order_date));

-- 파티션 변환 변경 예시
ALTER TABLE lakehouse.raw.orders
REPLACE PARTITION FIELD days(order_date)
WITH hours(order_date);
~~~

Flink CDC가 런타임에 파티션 변경을 자동 감지하고 새 사양을 적용하는지는 별도 검증 대상이다.

[검토 필요: Flink CDC 3.6.0에서 Iceberg Sink의 런타임 파티션 진화 자동 적용 여부]

## 8. 작은 파일과 Compaction

### 8.1 작은 파일 발생 원인

Flink 스트리밍 적재에서 다음 조건은 작은 파일을 늘릴 수 있다.

- checkpoint 간격이 짧음
- 이벤트량이 적음
- 파일 롤링 조건이 작음
- 테이블 또는 파티션 수가 많음
- UPDATE·DELETE에 따른 delete file이 증가함
- 스키마·파티션 변경이 자주 발생함

작은 파일이 많아지면 쿼리 계획 단계에서 읽어야 할 Manifest 수가 늘고, 객체 저장소에 요청하는 파일 수도 증가할 수 있다.

### 8.2 Binpack과 Sort

Compaction은 여러 작은 파일을 더 큰 파일로 재작성하는 유지보수 작업이다.

| 전략 | 동작 | 적합한 시점 |
|---|---|---|
| Binpack | 정렬 없이 작은 파일을 목표 크기로 병합 | 자주 실행하는 일반 Compaction |
| Sort | 정렬하면서 파일을 재작성 | 백필 후 또는 드문 최적화 작업 |

조사 자료에는 RewriteDataFiles와 rewrite_data_files 절차를 이용한 예시가 있다. 실제 호출 형식은 Spark·Flink와 Iceberg 버전에 따라 확인해야 한다.

~~~sql
-- 개념적인 Binpack 예시
CALL lakehouse.system.rewrite_data_files(
  table => 'raw.customers_cdc',
  strategy => 'binpack'
);
~~~

[추가 자료 조사 필요: 최종 Spark 버전에서 rewrite_data_files의 실제 procedure 이름과 options 문법]

### 8.3 Compaction과 CDC

Merge-on-Read CDC 테이블은 delete file이 누적될 수 있다. Compaction은 데이터 파일과 delete file을 함께 재작성해 읽기 시 병합 부담을 줄이는 방향으로 사용한다.

유지보수 작업의 조건은 다음처럼 관리한다.

| 기준 | 의미 |
|---|---|
| target-file-size | 새로 생성할 데이터 파일의 목표 크기 |
| min-input-files | 최소 입력 파일 수 |
| delete-file-threshold | 삭제 파일 수가 기준을 넘었는지 |
| delete-ratio-threshold | 삭제 비율이 기준을 넘었는지 |
| partial-progress | 일부 커밋 단위로 진행할지 |
| max-commits | 부분 진행의 최대 커밋 수 |

조사 자료에는 128MB 또는 256MB를 예시 목표 파일 크기로 제시하지만, 이 값을 모든 로컬·운영 환경의 정답으로 사용하지 않는다. Ozone의 저장 특성, 쿼리 엔진, 파티션 크기, 메모리와 네트워크를 함께 측정해야 한다.

## 9. Dynamic Iceberg Sink

### 9.1 개념

Dynamic Iceberg Sink는 하나의 Sink가 여러 Kafka 토픽을 처리하고, 토픽 또는 이벤트 정보에 따라 여러 Iceberg 테이블에 동적으로 쓰는 패턴이다.

조사 자료에서는 다음 특성이 제시된다.

- 단일 Sink로 여러 토픽 처리
- 런타임 Table 생성 후보
- 스키마 진화 후보
- 파티션 진화 후보
- Kafka 기반 다중 테이블 스트리밍

이 패턴은 일반적인 Flink CDC Pipeline과 동일한 개념은 아니다. Flink CDC는 소스 데이터베이스 변경 수집과 이벤트 생성에 초점을 두고, Dynamic Iceberg Sink는 여러 스트림을 여러 Iceberg Table로 분배하는 Sink 패턴에 가깝다.

### 9.2 이 책에서의 적용 범위

책의 기본 실습에서는 먼저 하나의 Kafka 토픽과 하나의 Iceberg Table을 연결한다. 이후 다음 조건을 확인한 뒤 Dynamic Sink를 확장 주제로 다룬다.

- Flink 버전 지원
- Iceberg 라이브러리 버전
- Catalog 권한
- 런타임 Table 생성 권한
- 이벤트의 대상 Table 식별자
- 스키마와 파티션 변경 처리
- 체크포인트와 다중 Table commit

조사 자료에는 Iceberg 1.10.0 이상과 Flink 1.20·2.0·2.1 계열 지원 예시가 있으나, 이 책의 Flink 2.2.1 후보 버전과 Flink CDC 3.6.0 조합으로 바로 확정하지 않는다.

[추가 자료 조사 필요: Flink 2.2.1·Iceberg 1.11.0에서 Dynamic Iceberg Sink의 공식 지원과 실제 실행 예]

## 10. 단계별 로컬 실습

### 10.1 실습 전제

다음 서비스가 실행 중이라고 가정한다.

| 서비스 | 확인 내용 |
|---|---|
| PostgreSQL 또는 다른 소스 DB | CDC 대상 테이블 존재 |
| Kafka | CDC 이벤트 토픽 존재 |
| Flink JobManager | 작업 제출 가능 |
| Flink TaskManager | Task 실행 가능 |
| Polaris | REST Catalog 응답 |
| Polaris metastore PostgreSQL | Catalog 상태 영속화 |
| Ozone S3 Gateway | warehouse 접근 가능 |

Ozone, Polaris, Flink의 실제 Compose 서비스명은 프로젝트 파일에 따라 달라질 수 있다.

### 10.2 1단계: 서비스 상태 확인

~~~bash
docker compose ps

docker compose logs --tail=100 kafka
docker compose logs --tail=100 flink-jobmanager
docker compose logs --tail=100 polaris
docker compose logs --tail=100 ozone-s3g
~~~

서비스 이름이 다르면 docker compose config로 실제 이름과 프로파일을 확인한다.

~~~bash
docker compose config
~~~

### 10.3 2단계: Kafka 이벤트 확인

먼저 CDC 이벤트가 Kafka에 들어오는지 확인한다.

~~~bash
# 토픽 목록 확인
docker exec kafka kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --list

# 토픽 메시지 확인
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic dbserver.public.customers \
  --from-beginning \
  --max-messages 5
~~~

실제 Kafka 이미지와 CLI 경로는 사용하는 배포판에 따라 다를 수 있다.

### 10.4 3단계: Polaris Catalog endpoint 확인

~~~bash
# 실습 token을 사용하는 개념 예시
curl -s \
  -H "Authorization: Bearer TOKEN" \
  "http://localhost:8181/api/catalog/v1/config?warehouse=lakehouse_catalog"
~~~

응답이 없거나 401이 반환되면 Flink 설정을 확인하기 전에 Polaris 인증과 endpoint를 먼저 해결한다.

### 10.5 4단계: Flink Catalog와 Table 확인

~~~sql
CREATE CATALOG lakehouse WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://polaris:8181/api/catalog',
  'warehouse' = 'lakehouse_catalog'
);

USE CATALOG lakehouse;

SHOW DATABASES;

USE raw;

SHOW TABLES;
~~~

이 SQL은 설정 형태의 예시이며, 실제 실행 가능 여부는 Flink Iceberg connector와 Polaris REST Catalog 설정에 따라 검증한다.

### 10.6 5단계: CDC 이벤트 적재

~~~sql
-- Kafka Source와 Iceberg Sink를 연결하는 개념 예시
INSERT INTO lakehouse.raw.customers_cdc
SELECT
  id,
  name,
  email,
  created_at,
  updated_at
FROM kafka_customers_cdc;
~~~

이 명령이 실제로 실행되려면 kafka_customers_cdc 테이블이 Flink SQL에서 먼저 정의되어 있어야 하고, 이벤트의 컬럼과 Iceberg Table의 스키마가 일치해야 한다.

[추가 자료 조사 필요: 최종 Compose에서 사용할 Kafka CDC 포맷, Flink SQL Source DDL, Iceberg Sink DDL, checkpoint 설정]

### 10.7 6단계: Checkpoint와 Snapshot 확인

Flink UI 또는 REST API에서 checkpoint 성공 여부를 확인한다. 그 다음 Iceberg Table history에서 Snapshot 변화를 확인한다.

~~~sql
-- 엔진별 Snapshot history 확인 예시
SELECT *
FROM lakehouse.raw.customers_cdc.snapshots;
~~~

실제 snapshots 메타테이블의 호출 방식과 컬럼은 Iceberg·Flink 통합 버전에 따라 달라질 수 있다.

Ozone에서도 warehouse 객체를 확인한다.

~~~bash
aws s3 ls s3://iceberg-bucket/warehouse/ \
  --recursive \
  --endpoint-url http://localhost:9878
~~~

다음 결과를 기록한다.

| 확인 항목 | 결과 |
|---|---|
| Kafka 이벤트 유입 | 메시지 수와 이벤트 키 |
| Flink Job | RUNNING 여부 |
| Checkpoint | 마지막 성공 시각 |
| Polaris Catalog | Table load와 commit 응답 |
| Iceberg Snapshot | Snapshot 증가 여부 |
| Ozone Metadata | JSON 파일 생성 |
| Ozone Manifest | Avro 파일 생성 |
| Ozone Data | Parquet 파일 생성 |
| Table 조회 | 적재 행과 CDC 상태 일치 |

### 10.8 7단계: 스키마 변경 실험

소스 PostgreSQL에 컬럼을 추가한다.

~~~sql
ALTER TABLE customers
ADD COLUMN phone VARCHAR(30);
~~~

그 다음 순서로 확인한다.

1. CDC Source가 스키마 변경 이벤트를 받는지 확인
2. Kafka 이벤트의 새 필드 확인
3. Flink Job 로그와 checkpoint 확인
4. Iceberg Table Schema 확인
5. 새 Snapshot 생성 여부 확인
6. 기존 소비자와 ClickHouse 조회 영향 확인

자동 진화가 실패하더라도 원인을 한 계층으로 단정하지 않는다. Source schema, Kafka serialization, Flink schema mapping, Iceberg Sink, Polaris 권한을 순서대로 확인한다.

## 11. Flink CDC 3.6 Oracle Source 고급 실습

앞의 실습에서는 로컬 환경에서 비교적 준비가 쉬운 MySQL·PostgreSQL Source를 사용했다. 이 절에서는 같은 파이프라인에 Oracle Source를 연결한다. Oracle CDC는 단순히 JDBC 접속 정보를 입력하는 작업이 아니다. 초기 스냅샷은 테이블을 읽어 만들고, 이후 변경분은 Oracle redo log를 LogMiner로 읽는다. 따라서 Oracle 데이터베이스의 운영 모드, 보조 로깅, CDC 사용자 권한, PDB 서비스 이름, Flink CDC 커넥터 의존성을 함께 맞춰야 한다.

이 절의 설정은 다음 조합을 기준으로 한 자료 조사본 예시다.

| 구성 요소 | 이 절의 기준 | 비고 |
|---|---|---|
| Flink CDC | 3.6.0 | Oracle Source 설정과 YAML 예시는 실제 배포판 스키마 확인 필요 |
| Flink | 2.2.1 | Flink CDC 3.6.0과 함께 검증할 런타임 |
| Oracle | 19c 이상, 실습은 XE 21c 이미지 예시 | CDB/PDB 서비스 이름을 환경에 맞게 변경 |
| 변경 로그 | LogMiner | Debezium Oracle Connector 기반 |
| 목적지 | Iceberg + Polaris + Ozone | 이 장 앞부분에서 구성한 서비스 이름을 사용 |

여기서 Oracle XE 컨테이너는 Oracle 공식 배포 이미지가 아니라 로컬 실습에 널리 사용되는 서드파티 이미지 예시다. 라이선스, 배포 정책, CPU 아키텍처, 이미지의 실제 태그를 확인한 뒤 사용해야 한다. 특히 Apple Silicon에서는 이미지의 ARM64 지원 여부를 먼저 확인한다. Windows 11에서는 Docker Desktop의 Linux 컨테이너 모드에서 실행한다.

### 11.1 Oracle Source가 동작하는 방식

Oracle Source의 처리 흐름은 두 단계로 나뉜다.

1. 초기 스냅샷 단계에서 JDBC로 기존 테이블을 읽는다.
2. 스냅샷 기준점 이후에 생성된 redo log 변경을 LogMiner로 읽어 INSERT·UPDATE·DELETE 이벤트로 전달한다.

Flink CDC의 Oracle 커넥터는 Debezium Oracle Connector를 기반으로 한다. 따라서 Source 설정을 작성할 때 Flink CDC의 옵션뿐 아니라 Debezium의 LogMiner 관련 옵션과 Oracle의 로그 보존 조건을 함께 고려한다 (출처: [Flink CDC 3.6.0 Release Announcement](https://flink.apache.org/2026/03/30/apache-flink-cdc-3.6.0-release-announcement/), [Debezium Oracle Connector](https://debezium.io/docs/connectors/oracle/)).

~~~mermaid
flowchart TD
    A["Oracle 테이블"] --> B["초기 스냅샷: JDBC"]
    A --> C["redo log"]
    C --> D["LogMiner"]
    B --> E["Flink CDC Oracle Source"]
    D --> E
    E --> F["Kafka 또는 Flink 처리"]
    F --> G["Iceberg Sink"]
~~~

초기 스냅샷과 변경 스트리밍 사이의 기준점이 보장되어야 같은 변경을 빠뜨리지 않는다. 또한 Flink Job이 중단된 동안 필요한 redo 또는 archive log가 삭제되면 재시작 시 SCN 간격 오류가 발생할 수 있다. 보관 기간은 특정 숫자를 기계적으로 적용하기보다 예상 중단 시간, 재처리 시간, CDC 지연을 기준으로 결정한다.

### 11.2 실습 전 Oracle 사전 조건

#### ARCHIVELOG 모드 확인

LogMiner가 읽을 수 있는 redo/archive log 환경인지 먼저 확인한다.

~~~sql
SELECT log_mode
FROM v$database;
~~~

결과가 'ARCHIVELOG'가 아니면 테스트용 Oracle에서 다음과 같이 전환한다. 이 명령은 데이터베이스 운영 상태를 변경하므로 운영 데이터베이스에서 그대로 실행하지 않는다.

~~~sql
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
~~~

#### 보조 로깅 확인 및 활성화

보조 로깅은 redo log에 변경 행을 재구성하는 데 필요한 추가 정보를 기록하도록 한다. 현재 설정은 다음 쿼리로 확인한다.

~~~sql
SELECT supplemental_log_data_min,
       supplemental_log_data_pk,
       supplemental_log_data_all
FROM v$database;
~~~

실습에서는 데이터베이스 수준의 최소 보조 로깅과 기본 키 보조 로깅을 활성화한다.

~~~sql
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;
~~~

테이블 수준에서는 변경 이벤트에 필요한 이전 값의 범위를 고려한다. 실습을 단순하게 만들고 UPDATE 전후 값을 충분히 확인하려면 모든 컬럼을 기록하는 방법을 사용할 수 있다.

~~~sql
ALTER TABLE DEMO.CUSTOMERS
  ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
~~~

'ALL COLUMNS'는 실습에서 이해하기 쉬운 보수적인 선택이지만 redo 양을 늘릴 수 있다. 운영 환경에서는 테이블의 기본 키, UPDATE·DELETE 재구성 요건, 커넥터 버전을 기준으로 최소 범위를 검토한다. “모든 Oracle 테이블에 항상 ALL COLUMNS가 필수”라고 일반화해서는 안 된다.

#### CDC 사용자와 권한

다음은 실습용 CDC 사용자 예시다. 실제 최소 권한은 Oracle 버전, CDB/PDB 구성, Flink CDC·Debezium 버전, 대상 테이블 제한 방식에 따라 달라질 수 있으므로 배포 전에 Oracle 공식 문서와 커넥터 권한 문서를 대조한다.

~~~sql
CREATE USER cdc_user IDENTIFIED BY cdc_password;

GRANT CREATE SESSION TO cdc_user;
GRANT LOGMINING TO cdc_user;
GRANT SELECT ANY TRANSACTION TO cdc_user;
GRANT SELECT ANY DICTIONARY TO cdc_user;
GRANT SELECT ANY TABLE TO cdc_user;

GRANT EXECUTE ON DBMS_LOGMNR TO cdc_user;
GRANT EXECUTE ON DBMS_LOGMNR_D TO cdc_user;

GRANT SELECT ON V_$LOG TO cdc_user;
GRANT SELECT ON V_$LOGFILE TO cdc_user;
GRANT SELECT ON V_$ARCHIVED_LOG TO cdc_user;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO cdc_user;
~~~

구형 Oracle 버전에서는 'LOGMINING' 시스템 권한 대신 카탈로그 관련 역할을 요구하는 경우가 있다. 다음 역할은 대체 경로의 예시일 뿐, 위 권한과 무조건 함께 부여하는 기본값으로 취급하지 않는다.

~~~sql
GRANT EXECUTE_CATALOG_ROLE TO cdc_user;
GRANT SELECT_CATALOG_ROLE TO cdc_user;
~~~

권한을 넓게 부여한 실습 스크립트를 운영에 재사용하지 않는다. 운영에서는 대상 스키마와 테이블에 필요한 권한만 남기는 방식으로 축소하고, 'V_$' 동의어와 실제 객체 권한 여부를 별도로 확인한다. 정확한 최소 권한 목록은 [추가 자료 조사 필요: Oracle 19c·21c PDB 환경에서 Flink CDC 3.6.0이 요구하는 공식 최소 권한 매트릭스].

#### CDB와 PDB 서비스 확인

Oracle 12c 이후의 멀티테넌트 환경에서는 CDB와 PDB를 구분한다. Flink 설정의 'database-name'은 실제 접속 대상 서비스 또는 컨테이너 구성과 일치해야 한다. 예를 들어 'XEPDB1' 또는 'ORCLCDB'는 모든 환경에서 고정되는 값이 아니다. 다음 명령으로 컨테이너와 서비스 정보를 확인한다.

~~~sql
SELECT name, open_mode
FROM v$pdbs;

SELECT name
FROM v$services;
~~~

PDB를 대상으로 CDC할 때는 CDC 사용자, 보조 로깅, 대상 스키마가 같은 컨테이너 범위에 존재해야 한다. CDB 공통 사용자와 PDB 로컬 사용자의 차이, PDB별 LogMiner 접근 범위는 Oracle 구성과 커넥터 버전에 따라 달라질 수 있으므로 [추가 자료 조사 필요: 사용하는 Oracle 이미지의 CDB/PDB 사용자 생성 및 LogMiner 권한 절차]로 남긴다.

### 11.3 Docker Compose로 Oracle XE 준비하기

다음 Compose 조각은 앞 장의 로컬 Compose 파일에 추가할 수 있는 실습용 예시다. 기존 파일에 이미 networks, volumes, healthcheck 정책이 있다면 서비스 이름과 네트워크를 통합한다.

~~~yaml
services:
  oracle:
    image: gvenzl/oracle-xe:21.3.0
    container_name: oracle
    environment:
      ORACLE_PASSWORD: <ORACLE_PASSWORD>
      APP_USER: DEMO
      APP_USER_PASSWORD: <DEMO_PASSWORD>
    ports:
      - "1521:1521"
    volumes:
      - oracle_data:/opt/oracle/oradata
      - ./oracle/init-scripts:/docker-entrypoint-initdb.d
    healthcheck:
      test: ["CMD-SHELL", "healthcheck.sh"]
      interval: 30s
      timeout: 10s
      retries: 5

volumes:
  oracle_data:
~~~

'<ORACLE_PASSWORD>'와 '<DEMO_PASSWORD>'는 설명을 위한 자리표시자다. 실제 .env 또는 Compose 변수 치환 정책을 사용할 때는 비밀번호를 Git 저장소에 커밋하지 않는다. 이미지의 초기화 스크립트 실행 시점과 권한 컨텍스트는 이미지 태그에 따라 달라질 수 있으므로, 컨테이너 로그에서 초기화 완료 여부를 확인한다.

Apple Silicon에서 먼저 확인할 항목은 다음과 같다.

| 점검 항목 | 확인 방법 |
|---|---|
| 이미지 아키텍처 | 이미지 레지스트리의 linux/arm64 지원 여부 확인 |
| Docker Desktop 자원 | 메모리·CPU를 Oracle과 Flink가 함께 사용할 수 있도록 할당 |
| 네트워크 이름 | oracle이라는 서비스명이 Flink 컨테이너에서 해석되는지 확인 |
| 초기화 시간 | 첫 기동 시 데이터베이스 생성이 완료될 때까지 healthcheck 대기 |
| 성능 기대치 | 로컬 XE는 운영용 처리량 시험의 기준으로 사용하지 않음 |

이미지가 ARM64에서 실행되지 않거나 초기화가 불안정하면 Oracle을 로컬 기본 경로로 강제하지 않는다. 이 책의 MySQL·PostgreSQL 실습을 먼저 완료한 뒤, 별도 Oracle 실습 환경 또는 호환 가능한 이미지로 이 절을 수행한다. Oracle XE 이미지의 실제 아키텍처와 배포 조건은 [검토 필요: 집필 시점의 이미지 태그·라이선스·Apple Silicon 지원 상태]다.

### 11.4 Oracle 초기화 스크립트 작성

프로젝트 디렉터리는 다음과 같이 둔다.

~~~text
project/
├─ compose.yaml
└─ oracle/
   └─ init-scripts/
      └─ 01_cdc_setup.sql
~~~

다음 스크립트는 예제 테이블과 CDC 권한을 한 번에 준비하는 형태다. 초기화 스크립트는 데이터베이스가 완전히 열린 뒤 실행되어야 하며, 사용 중인 이미지가 /docker-entrypoint-initdb.d를 어떤 사용자로 실행하는지 확인한다.

~~~sql
-- 접속 대상은 이미지의 초기화 방식에 맞게 조정한다.
-- 예: XEPDB1 서비스에 접속한 뒤 실행

CREATE USER cdc_user IDENTIFIED BY cdc_password;
GRANT CREATE SESSION TO cdc_user;
GRANT LOGMINING TO cdc_user;
GRANT SELECT ANY TRANSACTION TO cdc_user;
GRANT SELECT ANY DICTIONARY TO cdc_user;
GRANT SELECT ANY TABLE TO cdc_user;
GRANT EXECUTE ON DBMS_LOGMNR TO cdc_user;
GRANT EXECUTE ON DBMS_LOGMNR_D TO cdc_user;
GRANT SELECT ON V_$LOG TO cdc_user;
GRANT SELECT ON V_$LOGFILE TO cdc_user;
GRANT SELECT ON V_$ARCHIVED_LOG TO cdc_user;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO cdc_user;

ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;

CREATE TABLE DEMO.CUSTOMERS (
  id NUMBER(10) NOT NULL,
  name VARCHAR2(100),
  email VARCHAR2(255),
  created_at TIMESTAMP(3),
  updated_at TIMESTAMP(3),
  CONSTRAINT customers_pk PRIMARY KEY (id)
);

ALTER TABLE DEMO.CUSTOMERS
  ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

INSERT INTO DEMO.CUSTOMERS
  (id, name, email, created_at, updated_at)
VALUES
  (1, 'Alice', 'alice@example.com', SYSTIMESTAMP, SYSTIMESTAMP);

INSERT INTO DEMO.CUSTOMERS
  (id, name, email, created_at, updated_at)
VALUES
  (2, 'Bob', 'bob@example.com', SYSTIMESTAMP, SYSTIMESTAMP);

INSERT INTO DEMO.CUSTOMERS
  (id, name, email, created_at, updated_at)
VALUES
  (3, 'Carol', 'carol@example.com', SYSTIMESTAMP, SYSTIMESTAMP);

COMMIT;
~~~

위 스크립트는 DEMO 스키마가 이미 생성되어 있다는 전제를 둔다. Compose 이미지가 APP_USER=DEMO를 PDB에 생성하지 않거나 스크립트를 SYSDBA 컨텍스트에서 실행하지 않는다면 실패할 수 있다. 실제 컨테이너의 접속 서비스와 초기화 순서를 확인한 뒤 다음처럼 직접 접속해 단계별로 실행하는 것이 안전하다.

~~~bash
docker compose up -d oracle
docker compose ps oracle
docker compose logs oracle
~~~

초기화가 끝나면 SQL*Plus 또는 이미지에 포함된 SQL 클라이언트로 접속해 상태를 확인한다.

~~~bash
sqlplus system/<ORACLE_PASSWORD>@//localhost:1521/XEPDB1
~~~

~~~sql
SELECT log_mode FROM v$database;
SELECT supplemental_log_data_min,
       supplemental_log_data_pk,
       supplemental_log_data_all
FROM v$database;

SELECT owner, table_name
FROM all_tables
WHERE owner = 'DEMO'
  AND table_name = 'CUSTOMERS';
~~~

### 11.5 Flink CDC 3.6 YAML Pipeline 설정

다음은 자료 조사본에 포함된 YAML DSL 형태의 예시다. Oracle Source에서 DEMO.CUSTOMERS를 읽고 Iceberg의 raw.customers_cdc로 전달하는 흐름을 표현한다.

~~~yaml
source:
  type: oracle
  tables: DEMO.CUSTOMERS
  hostname: oracle
  port: 1521
  username: cdc_user
  password: <CDC_PASSWORD>
  database-name: XEPDB1
  schema-name: DEMO
  scan.startup.mode: initial
  scan.incremental.snapshot.enabled: true
  schema-change.enabled: true
  debezium.log.mining.strategy: online_catalog

pipeline:
  name: oracle-to-iceberg
  parallelism: 2
  local-time-zone: Asia/Seoul
  schema.change.behavior: lenient

sink:
  type: iceberg
  catalog:
    type: rest
    uri: http://polaris:8181/api/catalog
    warehouse: ozone_catalog
    credential: <POLARIS_CLIENT_ID>:<POLARIS_CLIENT_SECRET>
    scope: PRINCIPAL_ROLE:ALL
  table:
    - database: raw
      table: customers_cdc
  primary-key: id
  upsert: true
~~~

실행 전 확인할 항목은 세 가지다.

1. oracle과 polaris가 Flink 컨테이너에서 해석되는 서비스명인지 확인한다.
2. XEPDB1, DEMO, CUSTOMERS가 실제 Oracle 접속 대상과 일치하는지 확인한다.
3. Flink CDC 3.6.0 배포판이 위 YAML 키와 Iceberg Sink 필드를 실제로 지원하는지 확인한다.

Flink CDC의 YAML DSL은 배포판과 버전에 따라 필드명이 달라질 수 있다. 따라서 다음 실행 명령은 명령 이름과 설정 파일 형식을 확인한 뒤 사용한다.

~~~bash
./bin/flink-cdc run oracle-to-iceberg.yaml
~~~

[검토 필요: Flink CDC 3.6.0의 Oracle Source YAML DSL에서 type: oracle, tables, sink.table, primary-key, upsert가 최종 배포판에서 동일하게 지원되는지 확인]. 지원되지 않는 경우에는 아래 Flink SQL 방식으로 Source를 먼저 검증하고, Iceberg Sink 연결은 이 장의 기존 실습 설정에 맞춰 조정한다.

### 11.6 Flink SQL로 Oracle Source만 먼저 검증하기

전체 Pipeline을 한 번에 실행하면 Oracle 접속 오류와 Iceberg Catalog 오류를 구분하기 어렵다. 먼저 Flink SQL Client에서 Oracle Source 테이블을 정의하고 Source 연결만 검증한다.

~~~sql
CREATE TABLE oracle_customers (
  id INT NOT NULL,
  name STRING,
  email STRING,
  created_at TIMESTAMP(3),
  updated_at TIMESTAMP(3),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'oracle-cdc',
  'hostname' = 'oracle',
  'port' = '1521',
  'username' = 'cdc_user',
  'password' = '<CDC_PASSWORD>',
  'database-name' = 'XEPDB1',
  'schema-name' = 'DEMO',
  'table-name' = 'CUSTOMERS',
  'scan.startup.mode' = 'initial',
  'scan.incremental.snapshot.enabled' = 'true',
  'debezium.log.mining.strategy' = 'online_catalog'
);
~~~

테이블 정의가 성공한 뒤 다음 쿼리로 초기 스냅샷을 확인한다.

~~~sql
SELECT *
FROM oracle_customers;
~~~

Oracle Source 커넥터 JAR와 Debezium·Oracle JDBC 의존성은 Flink 배포판 구성에 따라 준비 방법이 달라진다. JAR를 Flink lib 디렉터리에 넣어야 하는지, SQL Client에서 별도 -j 옵션을 사용하는지, 어떤 artifact 조합이 필요한지는 반드시 사용 중인 Flink CDC 3.6.0 배포 문서를 기준으로 결정한다.

[추가 자료 조사 필요: Flink CDC 3.6.0 Oracle connector의 정확한 artifact 이름, Flink 2.2.1 호환 의존성, Oracle JDBC 드라이버 배포 위치와 라이선스 안내].

Source만 검증할 때는 다음 순서가 유용하다.

| 단계 | 확인 결과 |
|---|---|
| 1 | Flink 컨테이너에서 oracle:1521 이름과 포트가 해석됨 |
| 2 | cdc_user로 PDB 서비스에 로그인 가능 |
| 3 | ARCHIVELOG와 보조 로깅 상태가 조건에 맞음 |
| 4 | 초기 스냅샷 행이 SQL Client에 표시됨 |
| 5 | Oracle 변경 후 새 INSERT·UPDATE·DELETE 이벤트가 표시됨 |

### 11.7 Oracle 변경 이벤트와 Iceberg 결과 확인

Source 연결이 확인되면 Oracle에서 의도적으로 한 건을 수정하고 한 건을 추가한다. 변경마다 별도의 COMMIT을 실행해 이벤트 경계를 명확하게 만든다.

~~~bash
sqlplus cdc_user/<CDC_PASSWORD>@//oracle:1521/XEPDB1
~~~

~~~sql
UPDATE DEMO.CUSTOMERS
SET email = 'alice.new@example.com',
    updated_at = SYSTIMESTAMP
WHERE id = 1;

COMMIT;

INSERT INTO DEMO.CUSTOMERS
  (id, name, email, created_at, updated_at)
VALUES
  (4, 'David', 'david@example.com', SYSTIMESTAMP, SYSTIMESTAMP);

COMMIT;
~~~

검증은 Oracle, Flink, Iceberg의 세 지점에서 수행한다.

1. Oracle에서 변경된 행을 다시 조회한다.
2. Flink Job 로그 또는 SQL Client에서 UPDATE와 INSERT 이벤트를 확인한다.
3. Iceberg 테이블의 현재 데이터와 Snapshot 이력을 조회한다.

Iceberg 조회 방법은 사용하는 SQL 엔진과 Catalog 연결 방식에 따라 다르다. 앞 장에서 구성한 Flink 또는 Spark SQL 세션으로 raw.customers_cdc를 조회한다.

~~~sql
SELECT id, name, email, updated_at
FROM raw.customers_cdc
ORDER BY id;
~~~

Snapshot 이력 확인 예시는 다음과 같다.

~~~sql
SELECT *
FROM raw.customers_cdc.history;
~~~

테이블 메타테이블 이름과 history 조회 문법은 사용하는 SQL 엔진의 Iceberg 확장 기능에 따라 다를 수 있다. 데이터가 보이지 않으면 먼저 Iceberg의 현재 Snapshot과 Polaris Catalog 응답을 확인하고, 그 다음 Ozone 객체와 Flink Sink commit 로그를 확인한다.

### 11.8 Oracle Source 주요 옵션

| 옵션 | 실습 값 | 의미와 주의점 |
|---|---|---|
| scan.startup.mode | initial | 기존 행을 먼저 읽은 뒤 변경 스트리밍을 시작 |
| scan.incremental.snapshot.enabled | true | 초기 스냅샷을 분할해 읽는 경로. 버전별 지원 여부 확인 |
| debezium.log.mining.strategy | online_catalog | Oracle LogMiner 전략. Oracle·Debezium 버전과 환경에 맞게 확인 |
| schema-change.enabled | true | 스키마 변경 이벤트 처리 여부. Sink의 진화 정책과 함께 검증 |
| schema.change.behavior | lenient | 스키마 변경 처리 정책 예시. 데이터 손실을 숨기지 않는지 확인 |
| debezium.log.mining.batch.size.default | 20000 예시 | LogMiner 배치 크기 조정 예시. 처리량과 메모리·redo 부하를 함께 측정 |

Oracle 19c에서는 continuous_mine을 기본 설정으로 추가하지 않는다. 해당 옵션의 지원 여부와 Debezium 버전별 동작을 확인하지 않은 채 복사하면 연결 실패의 원인이 될 수 있다 (출처: [Oracle Database Changes, Deprecations, and Desupports](https://docs.oracle.com/en/database/oracle/oracle-database/26/upgrd/oracle-database-changes-deprecations-desupports.html)).

online_catalog은 자료 조사본에서 권장하는 실습 전략이다. redo_log_catalog나 다른 전략은 Oracle 버전, dictionary 보관 방식, 커넥터 버전에 따라 동작 조건이 달라질 수 있으므로 이 책의 기본 예제에는 넣지 않는다. 전략별 성능과 장애 복구 기준은 [추가 자료 조사 필요: Flink CDC 3.6.0과 Debezium 3.6 조합에서 Oracle LogMiner 전략별 공식 비교].

### 11.9 Oracle Source 장애 점검표

| 증상 | 가능한 원인 | 우선 확인 |
|---|---|---|
| ARCHIVELOG 오류 | 데이터베이스가 NOARCHIVELOG | SELECT log_mode FROM v$database |
| 초기 스냅샷은 되지만 변경이 안 보임 | 보조 로깅 또는 LogMiner 권한 부족 | 데이터베이스·테이블 보조 로깅, CDC 사용자 권한 |
| ORA 권한 오류 | CDB/PDB의 사용자·객체 권한 범위 불일치 | 접속 서비스, 사용자 컨테이너, V_$ 객체 권한 |
| PDB 접속 실패 | database-name이 서비스 이름과 다름 | v$services, Compose 포트와 hostname |
| SCN gap 또는 로그를 찾을 수 없음 | redo/archive log가 재시작 전에 삭제됨 | LogMiner 시작 SCN, archive log 보관 정책 |
| 커넥터 클래스를 찾을 수 없음 | Oracle CDC JAR 또는 JDBC 드라이버 누락 | Flink lib, SQL Client JAR 로딩, 버전 조합 |
| Apple Silicon에서 컨테이너가 기동하지 않음 | 이미지가 ARM64 미지원 | 이미지 manifest와 Docker Desktop 아키텍처 |
| Snapshot이 생성되지 않음 | Oracle Source는 정상이나 Iceberg commit 실패 | Polaris URI·credential, Ozone endpoint, Sink 로그 |
| 중복 또는 최신 값 미반영 | primary key·upsert·checkpoint 설정 불일치 | Source PK, Sink PK, checkpoint 복구 결과 |

장애를 해결할 때 Oracle 오류를 곧바로 Iceberg 오류로 해석하지 않는다. 다음 순서로 경계를 나누면 원인을 좁히기 쉽다.

1. Oracle SQL*Plus에서 사용자 로그인과 대상 테이블 조회
2. ARCHIVELOG·보조 로깅·PDB 상태 확인
3. Flink에서 Oracle Source 초기 스냅샷 확인
4. Oracle COMMIT 이후 Source 변경 이벤트 확인
5. Flink checkpoint와 Job 상태 확인
6. Polaris Catalog commit과 Iceberg Snapshot 확인
7. Ozone의 실제 파일과 접근 권한 확인

이 절의 Oracle 예시는 로컬 학습을 위한 최소 경로다. 운영 적용에는 권한 최소화, LogMiner 부하, archive log 보존, 장애 시 재처리 범위, Oracle 라이선스·배포 조건, 보안 비밀번호 관리에 대한 별도 설계가 필요하다.

## 12. 장애 시나리오별 점검

| 증상 | 우선 확인할 계층 |
|---|---|
| Kafka에 이벤트 없음 | Source DB 로그, CDC 설정, Debezium 또는 Flink CDC 상태 |
| Kafka 이벤트는 있지만 Flink 처리량 0 | Source DDL, Kafka deserializer, Flink operator |
| Flink Job이 실패 | TaskManager 로그, JAR 의존성, schema mapping |
| Checkpoint 실패 | 상태 저장소, 네트워크, timeout, 리소스 |
| Parquet는 있지만 Snapshot 없음 | Sink commit, Polaris API, Catalog 권한 |
| Snapshot은 있지만 조회 실패 | Table Metadata, Manifest, Ozone endpoint |
| AccessDenied | Polaris Principal·Role 또는 Ozone S3 권한 |
| Table commit 충돌 | 동시 writer, 현재 Snapshot, 재시도 설정 |
| 작은 파일 급증 | checkpoint 간격, 파일 롤링, 파티션 수 |
| Delete file 급증 | Upsert 정책, MOR 유지보수, Compaction |
| 재시작 후 중복 데이터 | Kafka offset, checkpoint 복구, primary key |
| 커밋 실패 후 파일 잔존 | orphan file 여부와 재시도 중 작업 여부 |

장애 조사는 다음 순서로 진행한다.

1. Flink JobManager와 TaskManager 상태
2. Kafka Source offset과 이벤트 내용
3. checkpoint 성공·실패 이력
4. Polaris REST Catalog 응답
5. Ozone S3 Gateway 접근
6. Iceberg Snapshot과 Metadata
7. Manifest와 데이터·삭제 파일
8. 재시작과 중복 처리 결과

## 13. 이 장의 핵심 정리

Flink CDC와 Iceberg를 연결하는 작업은 단순히 Sink 하나를 추가하는 일이 아니다. Kafka 이벤트, Flink 상태, checkpoint, Iceberg Snapshot, Polaris Catalog, Ozone 객체 저장소가 함께 동작해야 한다.

Flink는 이벤트를 처리하고, Iceberg Sink는 데이터 파일과 메타데이터를 작성한다. Polaris는 테이블의 현재 메타데이터 위치와 권한·Catalog 상태를 관리하며, Ozone은 실제 Parquet·Manifest·Metadata 파일을 저장한다.

Flink checkpoint가 성공했다고 Iceberg Snapshot이 자동으로 항상 커밋되는 것은 아니다. Sink의 pending transaction, Catalog commit, 저장소 접근, 장애 복구 조건이 함께 충족되어야 한다. exactly-once는 설정 한 줄의 속성이 아니라 전체 경로에 대한 검증 결과로 판단해야 한다.

CDC 테이블에서는 UPDATE와 DELETE 때문에 Upsert와 Delete file이 중요하다. Merge-on-Read는 변경 처리를 효율적으로 수행할 수 있는 후보지만, delete file 누적과 읽기 비용을 줄이기 위한 Compaction이 필요하다.

짧은 checkpoint 간격과 낮은 이벤트량은 작은 파일을 만들 수 있다. 따라서 checkpoint·파일 롤링·파티션·Compaction·Snapshot 보존을 함께 설계해야 한다.

이 책의 기본 역할 분리는 다음과 같다.

- Flink CDC: 변경 이벤트 수집
- Kafka: 이벤트 전달과 재처리 지점
- Flink: 실시간 변환과 Iceberg 적재
- Polaris: REST Catalog와 테이블 커밋 경로
- Ozone: 객체와 Iceberg 파일 저장
- Iceberg: 테이블 상태와 Snapshot
- Spark: 후속 백필과 유지보수
- ClickHouse: 분석 Serving

## 확인 문제

1. Kafka를 거치지 않고 Flink CDC에서 Iceberg로 직접 쓰는 구조와 Kafka를 거치는 구조의 차이는 무엇인가?
2. Ozone, Polaris, Iceberg, Flink의 책임을 각각 설명하라.
3. Flink checkpoint가 Iceberg Snapshot commit과 연결되는 이유는 무엇인가?
4. checkpoint 간격이 지나치게 짧을 때 발생할 수 있는 문제는 무엇인가?
5. exactly-once를 단순히 Sink 옵션 하나로 설명하면 안 되는 이유는 무엇인가?
6. CDC에서 primary key와 equality field가 일치해야 하는 이유는 무엇인가?
7. Merge-on-Read와 Copy-on-Write의 차이를 쓰기와 읽기 관점에서 설명하라.
8. 실패한 checkpoint 이후 Ozone에 파일이 남을 수 있는 이유는 무엇인가?
9. Snapshot Expiration이나 Orphan File Cleanup을 Flink 재시작 직후 실행하면 안 되는 이유는 무엇인가?
10. Dynamic Iceberg Sink와 일반적인 Flink CDC Pipeline은 어떻게 다른가?
11. 소스 데이터베이스에 컬럼을 추가했을 때 자동으로 Iceberg 스키마가 바뀐다고 단정하면 안 되는 이유는 무엇인가?
12. Parquet 파일이 생성되었지만 Iceberg Snapshot이 증가하지 않을 때 어떤 계층을 점검해야 하는가?

## 이 장에서 자료가 부족했거나 검증이 필요한 부분

- Flink 2.2.1·Flink CDC 3.6.0·Iceberg 1.11.0·Polaris 1.7.0·Ozone 2.2.0 통합 실행 결과
- 최종 Kafka CDC 이벤트 포맷과 MySQL·PostgreSQL·MongoDB별 필드 매핑
- Flink Iceberg Sink의 실제 Upsert API와 equality field 설정
- Flink REST Catalog의 정확한 Polaris URI·credential·scope 설정
- Flink checkpoint와 Iceberg Snapshot commit의 실제 exactly-once 재현
- checkpoint interval 60초의 책 기본값 채택 여부
- Flink 설정 YAML의 버전별 실제 키
- Flink checkpoint·savepoint와 Ozone 저장 경로의 최종 구성
- Flink CDC 3.6.0의 자동 스키마 진화 범위
- Flink CDC 3.6.0의 런타임 파티션 진화 자동 적용 여부
- Flink CDC 3.6.0 Oracle Source YAML DSL의 최종 필드명과 Iceberg Sink 연동 문법
- Flink CDC 3.6.0 Oracle connector의 정확한 artifact·JAR 의존성과 Oracle JDBC 드라이버 배포 방법
- Oracle 19c·21c PDB 환경에서 LogMiner에 필요한 공식 최소 권한과 대상 테이블 제한 방식
- Oracle XE Docker 이미지의 최신 태그·라이선스·Apple Silicon ARM64 지원 여부
- Oracle ARCHIVELOG와 archive log 보관 기간을 포함한 SCN gap 재현 및 복구 절차
- Merge-on-Read와 Copy-on-Write의 최종 커넥터 설정
- Iceberg delete file 메타테이블의 실제 컬럼과 content 값
- Spark 또는 Flink에서 실제 Compaction·RewriteDataFiles 명령
- Dynamic Iceberg Sink의 Flink 2.2.1·Iceberg 1.11.0 지원 여부
- Apple Silicon에서 Flink CDC와 Iceberg 전체 Compose 실행 여부
- ClickHouse가 Polaris Catalog를 직접 사용하는 최종 연결 방식

## 참고 자료

- [Apache Flink 공식 사이트](https://flink.apache.org/)
- [Apache Flink Dynamic Iceberg Sink 소개](https://flink.apache.org/2025/11/11/from-stream-to-lakehouse-kafka-ingestion-with-the-flink-dynamic-iceberg-sink/)
- [Apache Flink CDC 3.6.0 Release Announcement](https://flink.apache.org/2026/03/30/apache-flink-cdc-3.6.0-release-announcement/)
- [Apache Iceberg 공식 Table Specification](https://iceberg.apache.org/spec/)
- [Apache Iceberg Releases](https://iceberg.apache.org/releases/)
- [Apache Polaris 공식 문서](https://polaris.apache.org/docs/)
- [Apache Polaris Getting Started](https://polaris.apache.org/releases/latest/getting-started/)
- [Apache Ozone 공식 문서](https://ozone.apache.org/docs/)
- [Apache Ozone S3A 클라이언트 인터페이스](https://ozone.apache.org/docs/next/user-guide/client-interfaces/s3a/)
- [Flink CDC Oracle Source 문서](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.5/docs/connectors/flink-sources/oracle-cdc/)
- [Debezium Oracle Connector 문서](https://debezium.io/docs/connectors/oracle/)
- [Debezium 3.6 Final Release](https://debezium.io/blog/2026/07/01/debezium-3-6-final-release/)
- [Oracle LogMiner 공식 문서](https://docs.oracle.com/en/database/oracle/oracle-database/26/sutil/oracle-logminer-utility.html)
- [Oracle Database 변경·지원 중단 문서](https://docs.oracle.com/en/database/oracle/oracle-database/26/upgrd/oracle-database-changes-deprecations-desupports.html)
