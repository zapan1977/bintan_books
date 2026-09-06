# 13장. ClickHouse Serving과 Iceberg 연동

12장에서는 Spark를 이용해 Iceberg 테이블을 백필하고 유지보수했다. 13장에서는 이 데이터를 분석 서비스에 제공하는 Serving 계층을 구성한다. 여기서 ClickHouse의 역할은 두 가지로 나뉜다. Iceberg 테이블을 직접 읽어 대용량·감사성 조회를 처리하는 경로와, Iceberg 데이터를 ClickHouse의 MergeTree 계열 테이블에 적재해 반복적인 대시보드·API 조회를 빠르게 처리하는 경로다.

이 장의 기본 원칙은 ClickHouse를 Iceberg의 유일한 변경 엔진으로 취급하지 않는 것이다. Flink와 Spark가 Iceberg의 변경·백필·유지보수를 담당하고, ClickHouse는 Iceberg를 조회하거나 Serving용 Mart를 생성한다. 특히 일반 Polaris REST Catalog와 ClickHouse 조합에서는 읽기 경로를 기본으로 삼고, Iceberg의 행 단위 변경은 Flink 또는 Spark에서 처리한다.

## 이 장의 기준과 버전 주의사항

조사 자료에는 ClickHouse 26.6과 26.7이 함께 제시되어 있다. 이 책의 최종 실습 기준은 다음과 같이 정리한다.

| 구성 요소 | 이 장의 기준 | 편집 판단 |
|---|---|---|
| ClickHouse | 26.7 | 조사본의 최신 로컬 기준 |
| Apache Iceberg | 1.11.0 | 9장·11장·12장과 동일 |
| Apache Polaris | 1.7.0 | REST Catalog |
| Apache Ozone | 2.2.x | Iceberg 데이터 파일 저장 |
| 실행 환경 | Windows 11, Apple Silicon Mac | Docker Compose 로컬 실습 |

ClickHouse의 Iceberg 기능은 버전과 카탈로그 유형에 따라 달라진다. 공식 문서도 REST Catalog와 DataLakeCatalog 기능을 실험적 기능으로 설명하는 부분이 있으므로, 이 장의 SQL은 실행 전 사용 중인 이미지의 설정명과 지원 범위를 확인한다 (출처: [ClickHouse REST Catalog 문서](https://clickhouse.com/docs/guides/use-cases/data-warehousing/rest-catalog), [DataLakeCatalog 문서](https://clickhouse.com/docs/reference/engines/database-engines/datalake)).

## 학습 목표

이 장을 마치면 다음 작업을 수행할 수 있습니다.

- ClickHouse를 Polaris REST Catalog에 연결하고 Iceberg 테이블을 직접 조회할 수 있습니다.
- Iceberg Table Function, Iceberg Table Engine, DataLakeCatalog의 차이를 설명할 수 있습니다.
- Iceberg 데이터를 ClickHouse MergeTree 계열 Mart로 적재할 수 있습니다.
- Cold tier와 Hot tier를 구분하고 쿼리 목적에 맞는 경로를 선택할 수 있습니다.
- ClickHouse의 Iceberg 기능 제한과 버전별 검증 항목을 식별할 수 있습니다.

## 1. ClickHouse를 Serving 계층으로 배치하기

### 1.1 전체 데이터 플랫폼에서의 위치

앞 장까지 구성한 데이터 플랫폼에서 각 구성 요소의 책임은 다음과 같다.

| 계층 | 주요 구성 요소 | 책임 |
|---|---|---|
| Source | Percona MySQL, PostgreSQL, MongoDB, Oracle | 업무 원천 데이터 |
| Ingestion | Flink CDC, Kafka | 변경 이벤트 수집과 전달 |
| Lakehouse | Iceberg, Ozone | 원본·정제 데이터와 Snapshot 보존 |
| Catalog | Polaris | Iceberg 테이블의 메타데이터와 권한 경로 |
| Batch | Spark | 백필, 변환, Compaction, 유지보수 |
| Serving | ClickHouse | 분석 조회와 Mart 제공 |
| Orchestration | Airflow | 배치·유지보수 일정 관리 |

ClickHouse는 Iceberg 파일을 직접 읽을 수도 있고, 조회에 적합한 형태로 데이터를 복사해 자체 테이블에 저장할 수도 있다. 이 둘은 동일한 사용 사례가 아니다. 직접 조회는 데이터 복제를 줄이는 대신 원격 파일과 Iceberg 메타데이터를 읽는 비용을 감수한다. Mart는 데이터가 복제되지만 반복 조회와 낮은 지연 시간에 유리하다.

### 1.2 Cold tier와 Hot tier

이 책의 기본 Serving 구조는 두 계층으로 나눈다.

~~~mermaid
flowchart TD
    A["Flink CDC"] --> B["Iceberg raw"]
    C["Spark batch"] --> D["Iceberg clean"]
    B --> E["ClickHouse cold tier"]
    D --> E
    B --> F["ClickHouse hot tier"]
    D --> F
    E --> G["감사·대용량 조회"]
    F --> H["대시보드·API"]
~~~

Cold tier는 Polaris를 통해 raw·clean 테이블을 직접 조회한다. 이 경로는 과거 데이터 탐색, 감사, 재현, ad-hoc 분석에 적합하다. Hot tier는 Iceberg에서 필요한 데이터만 ClickHouse MergeTree 계열 테이블로 적재한다. 이 경로는 반복적인 집계, 대시보드, API 응답에 적합하다.

### 1.3 조회 경로 선택

| 조회 목적 | 기본 경로 | 이유 |
|---|---|---|
| 최신 대시보드 지표 | ClickHouse Mart | 반복 조회와 낮은 응답 시간 |
| API용 고객·주문 요약 | ClickHouse Mart | 조회 패턴에 맞는 정렬·집계 |
| 과거 시점 재현 | Iceberg 직접 조회 | Snapshot과 Time Travel 활용 |
| 대규모 ad-hoc 분석 | Iceberg 또는 Mart | 데이터 양과 반복성에 따라 선택 |
| 운영 데이터 검증 | 원천 DB·Iceberg 비교 | Serving 결과만으로 원본을 대체하지 않음 |

Serving 테이블을 만들었다고 해서 Iceberg를 삭제하거나 원천 데이터의 기준을 ClickHouse로 바꾸는 것은 아니다. ClickHouse Mart는 파생 데이터이며, 재생성 가능한 적재 범위와 기준 Snapshot을 기록해야 한다.

## 2. Iceberg REST Catalog 연결

### 2.1 DataLakeCatalog의 역할

ClickHouse의 DataLakeCatalog 데이터베이스 엔진은 외부 Catalog에 연결해 open table format 데이터를 조회하는 경로다. Catalog에 등록된 Iceberg 테이블을 ClickHouse 데이터베이스 아래에서 탐색할 수 있으므로, 테이블별 저장 경로를 직접 입력하는 작업을 줄일 수 있다. 일반적인 REST Catalog 연결은 Iceberg 테이블을 대상으로 한다 (출처: [ClickHouse DataLakeCatalog 문서](https://clickhouse.com/docs/reference/engines/database-engines/datalake)).

공식 문서의 기본 구조는 다음과 같다.

~~~sql
SET allow_experimental_database_iceberg = 1;

CREATE DATABASE polaris_catalog
ENGINE = DataLakeCatalog(
    'http://polaris:8181/api/catalog/v1'
)
SETTINGS
    catalog_type = 'rest',
    catalog_credential = '<POLARIS_CLIENT_ID>:<POLARIS_CLIENT_SECRET>',
    warehouse = 'ozone_catalog',
    auth_scope = 'PRINCIPAL_ROLE:ALL',
    oauth_server_uri =
        'http://polaris:8181/api/catalog/v1/oauth/tokens',
    storage_endpoint = 'http://ozone-s3g:9878';
~~~

위 URI와 storage endpoint는 앞 장에서 만든 Compose 서비스 이름을 기준으로 한 예시다. Polaris가 실제로 제공하는 API 경로, OAuth token endpoint, warehouse 이름은 설치 방식에 따라 확인해야 한다. ClickHouse 문서에는 실험 설정명이 버전별로 다르게 보일 수 있으므로, 26.7 이미지에서 SHOW SETTINGS와 공식 문서를 함께 확인한다.

[검토 필요: ClickHouse 26.7의 DataLakeCatalog 사용 시 필요한 최종 설정명이 allow_database_iceberg인지 allow_experimental_database_iceberg인지, 그리고 self-hosted Apache Polaris 1.7.0의 OAuth endpoint 경로를 실행 검증해야 합니다.]

### 2.2 연결 후 Catalog 확인

~~~sql
SHOW DATABASES;
SHOW TABLES FROM polaris_catalog;
SHOW TABLES FROM polaris_catalog.raw;
~~~

테이블 목록이 나타나지 않으면 다음 순서로 원인을 좁힌다.

1. ClickHouse 컨테이너에서 Polaris hostname이 해석되는지 확인한다.
2. Polaris URI의 API 버전과 실제 서비스 경로를 확인한다.
3. client ID와 client secret이 유효한지 확인한다.
4. principal role이 warehouse와 namespace를 읽을 권한을 갖는지 확인한다.
5. storage endpoint가 ClickHouse 컨테이너에서 접근되는지 확인한다.
6. Spark 또는 Flink가 실제로 Iceberg 테이블을 생성했는지 확인한다.

### 2.3 Polaris Iceberg 테이블 조회

~~~sql
SELECT *
FROM polaris_catalog.raw.customers_cdc
LIMIT 10;

SELECT count()
FROM polaris_catalog.raw.customers_cdc;
~~~

일부 ClickHouse 버전과 Catalog 경로에서는 namespace와 테이블 이름을 하나의 식별자로 표현해야 할 수 있다. 이때 공식 문서의 예시처럼 다음 형태를 사용한다.

~~~sql
USE polaris_catalog;

SHOW TABLES;

SELECT count()
FROM polaris_catalog.raw.customers_cdc;
~~~

[추가 자료 조사 필요: ClickHouse 26.7 self-hosted DataLakeCatalog에서 namespace.table과 단일 문자열 식별자 중 최종적으로 사용해야 하는 테이블 표기법].

## 3. Iceberg 직접 조회 방법

### 3.1 세 가지 접근 방법

ClickHouse에서 Iceberg를 읽는 방법은 목적에 따라 구분한다.

| 방법 | 용도 | 저장 경로 | 기본 성격 |
|---|---|---|---|
| Iceberg Table Function | 임시·ad-hoc 조회 | 호출 시 직접 지정 | 읽기 중심 |
| Iceberg Table Engine | ClickHouse의 영속 테이블 매핑 | 테이블 생성 시 지정 | 읽기 중심, 버전별 쓰기 제한 |
| DataLakeCatalog | REST Catalog의 테이블 탐색 | Catalog가 제공 | Catalog 기반 조회 |

공식 문서는 기존 Iceberg 테이블을 직접 읽을 때 Table Function을 사용하고, 영속적인 ClickHouse 테이블 매핑이나 쓰기 가능한 별도 Iceberg 테이블 생성에는 Table Engine을 사용한다고 구분한다. 다만 외부에서 스키마가 변경되는 테이블은 ClickHouse의 Table Engine에서 기능 제약이 발생할 수 있다 (출처: [ClickHouse Iceberg Table Engine 문서](https://clickhouse.com/docs/reference/engines/table-engines/integrations/iceberg)).

### 3.2 IcebergS3 Table Function

Ozone가 S3 호환 endpoint를 제공한다는 전제에서 저장 경로를 직접 지정하는 예시다.

~~~sql
SELECT *
FROM icebergS3(
    'http://ozone-s3g:9878/warehouse/iceberg/raw/customers_cdc',
    '<ACCESS_KEY>',
    '<SECRET_KEY>'
)
LIMIT 10;
~~~

이 방식은 Catalog를 거치지 않고 테이블 경로를 직접 지정한다. 따라서 경로와 credential을 알고 있어야 하며, Catalog가 제공하는 최신 메타데이터 경로를 자동으로 탐색하는 사용성은 줄어든다.

공식 문서에는 iceberg가 icebergS3의 별칭으로 동작하는 내용도 제시되어 있다.

~~~sql
SELECT *
FROM iceberg(
    'http://ozone-s3g:9878/warehouse/iceberg/raw/customers_cdc',
    '<ACCESS_KEY>',
    '<SECRET_KEY>'
)
LIMIT 10;
~~~

### 3.3 Iceberg Table Engine

Table Engine은 ClickHouse에 영속적인 테이블 이름을 만들고 Iceberg 저장 경로를 매핑하는 방법이다.

~~~sql
CREATE TABLE iceberg_customers
(
    id Int64,
    name String,
    email String,
    created_at DateTime64(3),
    updated_at DateTime64(3)
)
ENGINE = IcebergS3(
    'http://ozone-s3g:9878/warehouse/iceberg/raw/customers_cdc',
    '<ACCESS_KEY>',
    '<SECRET_KEY>'
);

SELECT *
FROM iceberg_customers
LIMIT 10;
~~~

스키마를 명시하지 않고 이미 존재하는 Iceberg 테이블을 읽는 경우와, ClickHouse가 별도의 Iceberg 테이블을 생성하는 경우의 동작은 다르다. 특히 ClickHouse 타입과 Iceberg·Parquet 타입의 매핑에는 제한이 있다. 예를 들어 ClickHouse의 Enum, LowCardinality, UInt 계열 타입을 Iceberg 테이블에 직접 기록할 때는 호환되는 타입으로 변환해야 할 수 있다 (출처: [ClickHouse open table format 쓰기 문서](https://clickhouse.com/docs/guides/use-cases/data-warehousing/getting-started/writing-data)).

### 3.4 Time Travel 조회

Iceberg Table Engine 또는 Table Function을 이용한 Time Travel은 Snapshot ID나 timestamp 설정으로 수행할 수 있다.

~~~sql
SELECT *
FROM iceberg_customers
ORDER BY id
SETTINGS iceberg_snapshot_id = <SNAPSHOT_ID>;
~~~

timestamp 기반 예시는 다음과 같다.

~~~sql
SELECT *
FROM iceberg_customers
ORDER BY id
SETTINGS iceberg_timestamp_ms = <TIMESTAMP_MILLISECONDS>;
~~~

Snapshot ID와 timestamp를 한 쿼리에 동시에 지정하지 않는다. 실제 설정 키와 시간 단위는 ClickHouse 이미지의 공식 문서를 확인한다. Iceberg Table Engine 문서는 snapshot ID와 timestamp 기반 Time Travel을 지원한다고 설명한다 (출처: [ClickHouse Iceberg Table Engine 문서](https://clickhouse.com/docs/reference/engines/table-engines/integrations/iceberg)).

## 4. Iceberg 쓰기 지원과 책임 경계

### 4.1 조회와 쓰기를 분리해 이해하기

조사 자료에는 ClickHouse의 Iceberg 쓰기 기능이 버전별로 확장되어 왔다고 정리되어 있다. 동시에 카탈로그 유형에 따라 쓰기 지원이 다르다는 제한도 제시되어 있다. 이 두 내용을 하나의 일반 규칙으로 합치면 안 된다.

이 책의 기본 로컬 구조는 다음과 같다.

| 작업 | 기본 담당 엔진 | 이유 |
|---|---|---|
| Iceberg raw·clean 기록 | Flink 또는 Spark | CDC·백필의 기준 엔진 |
| Iceberg Compaction | Spark | 12장에서 유지보수 절차 수행 |
| Iceberg Snapshot expiration | Spark | 보존 정책과 함께 실행 |
| Iceberg 직접 분석 | ClickHouse | DataLakeCatalog로 파일을 직접 조회 |
| Serving Mart 생성 | ClickHouse | MergeTree 계열 테이블에 적재 |
| ClickHouse에서 Iceberg로 직접 INSERT | 조건부 | ClickHouse·Catalog·버전별 지원 여부 확인 |

특히 일반 Polaris REST Catalog에서 ClickHouse가 Iceberg 테이블에 직접 INSERT·UPDATE·DELETE할 수 있다고 가정하지 않는다. 일반 Polaris 경로는 조회를 기본으로 하고, 변경이 필요한 데이터는 Flink 또는 Spark에서 처리한다. ClickHouse 공식 Polaris 가이드도 Polaris 데이터를 ClickHouse 테이블로 로드하는 경로를 예시로 제시한다 (출처: [ClickHouse Polaris Catalog 문서](https://clickhouse.com/docs/guides/use-cases/data-warehousing/polaris-catalog)).

### 4.2 조사본에 제시된 버전별 기능

| 조사본의 기준 | 제시된 기능 | 최종 원고의 처리 |
|---|---|---|
| 25.7 이상 | 기존 Iceberg 테이블에 INSERT | 버전별 조건부 기능 |
| 25.8 이상 | CREATE TABLE·ALTER DELETE 관련 기능 | 실제 Catalog와 테이블 유형 확인 |
| 25.9 이상 | ALTER UPDATE·분산 쓰기 관련 기능 | 기본 경로에서 제외 |
| 26.2 이상 | Iceberg writes의 production-ready 선언 | 카탈로그별 지원과 별도로 검증 |
| 26.7 기준 | Iceberg v3 deletion vectors 미지원 | 미지원 항목으로 기록 |

위 표는 조사본을 편집한 것이며, 모든 REST Catalog에서 동일하게 동작한다는 의미가 아니다. ClickHouse 공식 문서의 Catalog 안내는 Polaris를 조회 경로로 설명하고, Snowflake Horizon은 Snowflake-managed Iceberg 테이블의 읽기·쓰기를 별도로 설명한다. Horizon과 self-hosted Apache Polaris를 동일한 기능 집합으로 취급하지 않는다.

[검토 필요: ClickHouse 26.7, Apache Polaris 1.7.0, Iceberg 1.11.0 조합에서 일반 Polaris REST Catalog에 대한 INSERT·UPDATE·DELETE의 최종 지원 범위를 실행 검증해야 합니다.]

### 4.3 조건부 Iceberg INSERT 예제

다음 예제는 ClickHouse 버전과 Catalog가 직접 쓰기를 지원하는 경우에만 사용한다.

~~~sql
INSERT INTO polaris_catalog.raw.customers_batch
SELECT
    toInt64(number) AS id,
    concat('name_', toString(number)) AS name,
    concat('email_', toString(number), '@example.com') AS email,
    now64(3) AS created_at,
    now64(3) AS updated_at
FROM numbers(100);
~~~

실행 전에 대상 테이블이 ClickHouse가 직접 기록할 수 있는 유형인지, Catalog가 쓰기를 허용하는지, 데이터 타입이 Iceberg와 호환되는지 확인한다. 이 책의 기본 실습에서는 같은 결과를 ClickHouse 내부 Mart에 기록하는 경로를 사용한다.

## 5. ClickHouse Mart 만들기

### 5.1 Mart의 의미

Mart는 원천 데이터나 Iceberg raw 테이블을 그대로 노출하는 대신, 조회 목적에 맞게 집계·정렬·가공해 저장한 Serving용 테이블이다. ClickHouse에서는 MergeTree 계열 엔진을 사용해 Mart를 구성한다. MergeTree 계열은 높은 ingest rate와 큰 데이터 볼륨을 처리하도록 설계된 엔진군이며, INSERT가 생성한 parts를 백그라운드에서 병합한다 (출처: [ClickHouse MergeTree 문서](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/mergetree)).

### 5.2 집계 Mart 생성

다음은 일자·고객별 주문 건수와 매출을 저장하는 예제다.

~~~sql
CREATE DATABASE IF NOT EXISTS mart;

CREATE TABLE mart.daily_orders
(
    order_date Date,
    customer_id UInt64,
    order_count UInt64,
    total_revenue Decimal(18, 2)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_date, customer_id);
~~~

AggregatingMergeTree를 사용할 때는 집계 상태를 저장하는 컬럼 타입과 INSERT 쿼리를 함께 설계해야 한다. 조사본의 예제처럼 SimpleAggregateFunction을 사용하는 방식은 단순 집계에 사용할 수 있지만, 최종 엔진·버전에서 타입과 INSERT 결과를 확인한다.

~~~sql
INSERT INTO mart.daily_orders
SELECT
    toDate(order_date) AS order_date,
    toUInt64(customer_id) AS customer_id,
    count() AS order_count,
    sum(total_amount) AS total_revenue
FROM polaris_catalog.raw.orders
WHERE order_date >= '2026-08-01'
GROUP BY order_date, customer_id;
~~~

위 쿼리는 Iceberg에서 읽은 결과를 ClickHouse Mart에 적재하는 예시다. 재실행하면 같은 기간의 집계가 중복될 수 있으므로, 기간 단위 overwrite·중간 테이블 교체·중복 방지 키 중 하나를 명시한다.

### 5.3 MergeTree Mart와 최신 행

현재 상태를 보여 주는 고객 Mart처럼 동일 키의 여러 버전을 적재해야 할 때는 ReplacingMergeTree를 검토할 수 있다.

~~~sql
CREATE TABLE mart.customers_current
(
    id Int64,
    name String,
    email String,
    updated_at DateTime64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id;

INSERT INTO mart.customers_current
SELECT
    id,
    name,
    email,
    updated_at
FROM polaris_catalog.raw.customers_cdc;
~~~

ReplacingMergeTree는 INSERT 시점에 즉시 중복 행을 제거하는 엔진이 아니다. 백그라운드 merge가 아직 끝나지 않았다면 동일한 id의 여러 행이 보일 수 있다. 조회 시 최신 상태가 반드시 필요하다면 FINAL 사용 여부와 비용을 검토하고, 대량 조회에 FINAL을 무조건 붙이지 않는다.

이 예제는 Iceberg raw 테이블을 ClickHouse Mart로 복사하는 하나의 방법이다. Iceberg의 UPDATE·DELETE 이벤트를 ClickHouse에서 자동으로 동일하게 재현한다고 단정하지 않는다. 최신성 기준, 삭제 표현, 재적재 범위는 Flink·Spark 파이프라인에서 먼저 정한다.

### 5.4 Iceberg에서 Mart로 배치 적재

~~~sql
INSERT INTO mart.daily_orders
SELECT
    toDate(order_date) AS order_date,
    toUInt64(customer_id) AS customer_id,
    count() AS order_count,
    sum(total_amount) AS total_revenue
FROM polaris_catalog.raw.orders
WHERE order_date >= '2026-08-01'
GROUP BY order_date, customer_id;
~~~

적재가 끝난 뒤 다음 항목을 확인한다.

~~~sql
SELECT
    order_date,
    customer_id,
    order_count,
    total_revenue
FROM mart.daily_orders
ORDER BY order_date, customer_id
LIMIT 20;
~~~

Iceberg의 기준 Snapshot과 Mart의 적재 실행 ID를 함께 기록하면, 나중에 어떤 Iceberg 상태에서 Mart가 생성되었는지 추적할 수 있다.

## 6. Refreshable Materialized View

### 6.1 사용 목적

Refreshable Materialized View는 외부 Iceberg 테이블을 주기적으로 읽어 ClickHouse 대상 테이블을 갱신하는 자동화 방식이다. 조사본은 ClickHouse 26.6 기준으로 Iceberg에서 Mart로 이어지는 refreshable materialized view 예시를 제시한다.

버전별 문법과 외부 테이블 변경 감지 방식은 실행 검증이 필요하므로, 처음에는 수동 INSERT SELECT로 결과를 확인한 뒤 자동 갱신을 적용한다.

~~~sql
CREATE MATERIALIZED VIEW mart.daily_orders_mv
REFRESH EVERY 1 HOUR
TO mart.daily_orders
AS
SELECT
    toDate(order_date) AS order_date,
    toUInt64(customer_id) AS customer_id,
    count() AS order_count,
    sum(total_amount) AS total_revenue
FROM polaris_catalog.raw.orders
WHERE order_date >= now() - INTERVAL 7 DAY
GROUP BY order_date, customer_id;
~~~

이 예제는 최근 7일을 매 시간 다시 계산하는 형태다. 대상 테이블이 append 방식이면 매 refresh마다 중복이 생길 수 있으므로, 실제 refresh 동작이 대상 테이블을 교체하는지 추가 적재하는지 확인한다. 기간 집계 Mart에는 재계산 범위와 쓰기 모드를 함께 설계한다.

[추가 자료 조사 필요: ClickHouse 26.7에서 Refreshable Materialized View가 Polaris DataLakeCatalog 테이블을 source로 사용할 때의 정확한 refresh·replace semantics와 중복 방지 방식].

## 7. Iceberg 조회 성능과 기능 제한

### 7.1 파티션 프루닝

Iceberg 테이블을 조회할 때 파티션 조건을 사용하면 ClickHouse가 불필요한 데이터 파일을 건너뛸 수 있다. ClickHouse 공식 문서는 Iceberg 파티션 프루닝을 활성화하는 설정을 제공한다.

~~~sql
SET use_iceberg_partition_pruning = 1;

SELECT
    order_date,
    customer_id,
    total_amount
FROM polaris_catalog.raw.orders
WHERE order_date >= '2026-08-01'
  AND order_date < '2026-09-01';
~~~

파티션 프루닝은 WHERE 조건이 파티션 표현식과 연결되고, Catalog와 파일 메타데이터를 정상적으로 읽을 수 있을 때 의미가 있다. 설정을 켰다는 사실만으로 항상 일정한 성능 향상이 발생한다고 단정하지 않는다. 실행 계획, 실제 파일 읽기 수, 응답 시간을 함께 측정한다.

### 7.2 메타데이터 캐시

Iceberg Table Engine과 Table Function은 Manifest, Manifest List, metadata.json을 대상으로 메타데이터 캐시를 사용할 수 있다. 메타데이터 캐시는 ClickHouse 메모리에 저장되며, 최신 Snapshot 반영 지연과 메모리 사용량 사이의 균형을 고려해야 한다 (출처: [ClickHouse Iceberg Table Engine 문서](https://clickhouse.com/docs/reference/engines/table-engines/integrations/iceberg)).

자료 조사본에는 다음과 같은 캐시 설정 예시가 있다.

~~~sql
SET iceberg_metadata_cache_size = 1000;
SET parquet_metadata_cache_size = 10000;
~~~

설정명과 단위는 ClickHouse 버전에 따라 달라질 수 있다. 캐시를 무조건 크게 설정하지 않고, 테이블 수·Manifest 수·ClickHouse 메모리·Snapshot 갱신 빈도를 함께 관찰한다.

### 7.3 삭제 파일과 Deletion Vector

ClickHouse는 Iceberg v2의 position delete와 equality delete를 읽을 수 있지만, 조사 기준에서는 Iceberg v3 deletion vector를 지원하지 않는 것으로 정리되어 있다. 따라서 Flink 또는 Spark가 생성한 삭제 표현을 ClickHouse가 읽을 수 있는지 Iceberg format version과 함께 확인한다.

| Iceberg 기능 | 이 장의 처리 |
|---|---|
| Position delete | ClickHouse 버전과 테이블 형식 확인 후 조회 |
| Equality delete | ClickHouse 25.8 이상 자료 기준, 실행 검증 필요 |
| Deletion vector | ClickHouse 26.7 기준 미지원 항목 |
| Iceberg v3 전체 기능 | 부분 지원으로 취급 |

ClickHouse에서 조회가 된다는 사실은 ClickHouse가 동일한 삭제 파일을 생성하거나 유지보수할 수 있다는 의미가 아니다. 삭제·갱신의 기준 엔진은 Flink 또는 Spark로 유지한다.

### 7.4 Manifest 유지보수의 경계

Iceberg의 데이터 파일 Compaction은 12장에서 다룬 Spark의 rewrite_data_files를 기본 경로로 사용한다. ClickHouse 공식 문서에는 Iceberg 테이블의 Manifest 파일만 재구성하는 OPTIMIZE TABLE ... MANIFEST 기능이 별도로 설명되어 있다.

~~~sql
OPTIMIZE TABLE polaris_catalog.raw.orders
MANIFEST
SETTINGS allow_experimental_iceberg_compaction = 1;
~~~

이 기능은 데이터 파일을 다시 쓰거나 행을 중복 제거하는 작업이 아니다. Manifest 계층만 재구성하며, 공식 문서 기준으로 Iceberg format-version 2와 실험 설정을 요구한다. 이 책의 기본 유지보수 경로는 Spark로 두고, ClickHouse Manifest 작업은 버전별 검증 후 선택한다 (출처: [ClickHouse Iceberg Table Engine 문서](https://clickhouse.com/docs/reference/engines/table-engines/integrations/iceberg)).

[검토 필요: ClickHouse 26.7의 MANIFEST 유지보수 명령이 Polaris DataLakeCatalog로 노출된 테이블에 적용되는지, format-version 2 조건과 실험 설정을 로컬에서 확인해야 합니다.]

## 8. Serving 쿼리 설계

### 8.1 쿼리 유형별 라우팅

~~~mermaid
flowchart TD
    A["사용자 쿼리"] --> B{"조회 목적"}
    B -->|반복·저지연| C["ClickHouse Mart"]
    B -->|감사·과거 시점| D["Iceberg 직접 조회"]
    C --> E["대시보드·API"]
    D --> F["분석·재현"]
~~~

다음 표는 쿼리를 어느 경로로 보낼지 결정하는 기준이다.

| 쿼리 유형 | 기본 라우팅 | 예시 |
|---|---|---|
| 실시간 대시보드 | Mart | 고객별 일일 주문량 |
| API 응답 | Mart | 고객 요약·최근 상태 |
| 과거 시점 재현 | Iceberg 직접 조회 | Snapshot ID 기준 조회 |
| 대용량 일회성 분석 | Iceberg 직접 조회 또는 별도 배치 | 전체 기간 스캔 |
| 반복적인 다차원 집계 | 집계 Mart | 날짜·고객·상품별 KPI |

라우팅 기준은 데이터의 최신성, 응답 시간, 조회 빈도, 데이터 양으로 정한다. Cold tier의 결과를 Hot tier와 비교할 때는 두 경로가 동일한 Snapshot 또는 동일한 적재 시점을 기준으로 하는지 확인한다.

### 8.2 ClickHouse Mart의 정렬 키

ClickHouse MergeTree 계열 테이블은 ORDER BY로 지정한 키에 따라 각 part 내부의 정렬 순서를 만든다. 정렬 키는 가장 자주 사용하는 필터와 집계 기준을 반영한다. ClickHouse 공식 문서는 MergeTree 계열이 큰 데이터 볼륨과 높은 수집 속도를 위한 엔진군이며, INSERT가 생성한 parts를 백그라운드에서 병합한다고 설명한다 (출처: [ClickHouse MergeTree 문서](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/mergetree)).

예를 들어 일자와 고객별 집계 Mart는 다음과 같이 정의할 수 있다.

~~~sql
CREATE TABLE mart.daily_customer_orders
(
    order_date Date,
    customer_id UInt64,
    order_count UInt64,
    total_revenue Decimal(18, 2)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_date, customer_id);
~~~

PARTITION BY와 ORDER BY를 같은 개념으로 사용하지 않는다. PARTITION BY는 물리적 파티션 그룹을 나누는 표현식이고, ORDER BY는 각 part 안의 정렬·검색 구조에 영향을 준다.

### 8.3 데이터 타입 매핑

Iceberg와 ClickHouse 사이의 타입 매핑은 Serving 설계에 영향을 준다.

| Iceberg 타입 | ClickHouse 읽기 예시 | 주의 |
|---|---|---|
| int | Int32 | 범위와 변환 확인 |
| long | Int64 | 식별자와 큰 정수에 사용 |
| date | Date32 | 날짜 의미 확인 |
| timestamp | DateTime64(6) | 시간대 의미 확인 |
| string | String | 인코딩과 길이 확인 |
| decimal | Decimal(P,S) | 직접 생성·파티션 사용 제한 확인 |
| list | Array | 중첩 타입 변경 제한 확인 |
| map | Map | 원소 타입 변경 제한 확인 |
| struct | Tuple | 외부 스키마 변경 확인 |

ClickHouse 공식 문서는 위 매핑이 읽기 기준임을 명시하고, ClickHouse가 Iceberg 스키마를 생성하거나 진화시킬 때 Bool, Decimal, FixedString, 일부 Int·UInt 타입과 직접 파티션 필드에 제한이 있을 수 있다고 설명한다. 따라서 raw Iceberg 스키마를 그대로 ClickHouse Mart DDL에 복사하지 않고, Serving용 타입 매핑을 별도로 검토한다 (출처: [ClickHouse Iceberg Table Engine 문서](https://clickhouse.com/docs/reference/engines/table-engines/integrations/iceberg)).

## 9. Docker Compose 로컬 실습

### 9.1 ClickHouse 서비스 추가

앞 장의 Compose 네트워크에 다음과 같은 ClickHouse 서비스를 추가한다. 실제 네트워크 이름과 Polaris·Ozone 서비스명은 프로젝트의 Compose 파일에 맞춘다.

~~~yaml
services:
  clickhouse:
    image: clickhouse/clickhouse-server:26.7
    container_name: clickhouse
    ports:
      - "8123:8123"
      - "9000:9000"
    volumes:
      - clickhouse_data:/var/lib/clickhouse
      - clickhouse_logs:/var/log/clickhouse-server
    depends_on:
      - polaris
      - ozone-s3g

volumes:
  clickhouse_data:
  clickhouse_logs:
~~~

ClickHouse 이미지의 실제 26.7 태그, 플랫폼 manifest, Polaris·Ozone 서비스의 healthcheck 조건은 집필 시점에 다시 확인한다. Apple Silicon에서는 이미지가 ARM64를 지원하는지 확인하고, 지원되지 않으면 컨테이너 에뮬레이션에 따른 성능과 메모리 사용량을 실습 기록에 남긴다.

### 9.2 ClickHouse Client 접속

~~~bash
docker compose up -d clickhouse
docker compose ps clickhouse
docker compose logs clickhouse
docker exec -it clickhouse clickhouse-client
~~~

호스트에서 HTTP 포트로 접속할 때는 localhost를 사용하고, ClickHouse 컨테이너에서 Polaris와 Ozone에 접속할 때는 Compose 서비스명을 사용한다. 이 둘을 혼동하면 ClickHouse는 기동했지만 Catalog 연결에 실패할 수 있다.

### 9.3 Polaris DataLakeCatalog 연결

~~~sql
SET allow_experimental_database_iceberg = 1;

CREATE DATABASE polaris_catalog
ENGINE = DataLakeCatalog(
    'http://polaris:8181/api/catalog/v1'
)
SETTINGS
    catalog_type = 'rest',
    catalog_credential = '<POLARIS_CLIENT_ID>:<POLARIS_CLIENT_SECRET>',
    warehouse = 'ozone_catalog',
    auth_scope = 'PRINCIPAL_ROLE:ALL',
    oauth_server_uri =
        'http://polaris:8181/api/catalog/v1/oauth/tokens',
    storage_endpoint = 'http://ozone-s3g:9878';

SHOW TABLES FROM polaris_catalog;
~~~

테이블이 아직 보이지 않으면 11장의 Flink CDC 또는 12장의 Spark 실습을 통해 Iceberg 테이블을 먼저 생성한다. ClickHouse가 Catalog에 연결되었다고 해서 Catalog 안에 테이블이 자동으로 만들어지는 것은 아니다.

### 9.4 Iceberg 직접 조회

~~~sql
SELECT *
FROM polaris_catalog.raw.customers_cdc
LIMIT 10;

SELECT count()
FROM polaris_catalog.raw.customers_cdc;
~~~

조회가 성공하면 파티션 조건과 Time Travel을 차례로 확인한다.

~~~sql
SET use_iceberg_partition_pruning = 1;

SELECT id, name, email
FROM polaris_catalog.raw.customers_cdc
WHERE id <= 10
LIMIT 10;
~~~

### 9.5 ClickHouse Mart 생성

~~~sql
CREATE DATABASE IF NOT EXISTS mart;

CREATE TABLE mart.customer_summary
(
    id Int64,
    name String,
    email String,
    updated_at DateTime64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id;

INSERT INTO mart.customer_summary
SELECT
    id,
    name,
    email,
    updated_at
FROM polaris_catalog.raw.customers_cdc;

SELECT *
FROM mart.customer_summary
ORDER BY id;
~~~

이 예제의 Mart는 현재 상태 조회를 위한 단순한 예시다. ReplacingMergeTree의 background merge가 완료되기 전에는 같은 id에 여러 행이 남을 수 있다. 최신 행을 엄격하게 보장해야 하는 API에서는 FINAL의 비용, 별도의 최신 상태 집계, 재적재 전략을 함께 검토한다.

### 9.6 집계 Mart 생성

~~~sql
CREATE TABLE mart.daily_orders
(
    order_date Date,
    customer_id UInt64,
    order_count UInt64,
    total_revenue Decimal(18, 2)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_date, customer_id);

INSERT INTO mart.daily_orders
SELECT
    toDate(order_date) AS order_date,
    toUInt64(customer_id) AS customer_id,
    count() AS order_count,
    sum(total_amount) AS total_revenue
FROM polaris_catalog.raw.orders
WHERE order_date >= '2026-08-01'
GROUP BY order_date, customer_id;
~~~

적재 후 다음 결과를 확인한다.

~~~sql
SELECT *
FROM mart.daily_orders
ORDER BY order_date, customer_id
LIMIT 20;
~~~

## 10. 단계별 검증 체크리스트

| 단계 | 실행 또는 확인 | 성공 기준 |
|---|---|---|
| 1 | ClickHouse 컨테이너 기동 | health 상태와 로그가 정상 |
| 2 | Polaris hostname 확인 | ClickHouse 컨테이너에서 이름 해석 |
| 3 | DataLakeCatalog 생성 | CREATE DATABASE 성공 |
| 4 | Catalog 탐색 | SHOW TABLES에 Iceberg 테이블 표시 |
| 5 | 직접 조회 | SELECT 결과와 Iceberg 결과 일치 |
| 6 | 파티션 조회 | 조건에 맞는 데이터 반환 |
| 7 | Mart 생성 | MergeTree 테이블 생성 성공 |
| 8 | Iceberg → Mart 적재 | 행 수·집계값 검증 |
| 9 | Snapshot 비교 | 기준 Snapshot과 Mart 실행 시점 기록 |
| 10 | 장애 점검 | 실패 계층을 Catalog·Storage·SQL로 구분 |

실습 결과에는 ClickHouse 버전, Iceberg format version, Polaris URI, 기준 Snapshot ID, 적재 시각, 대상 테이블, 행 수를 함께 기록한다. 그래야 같은 데이터를 다시 적재하거나 장애를 재현할 수 있다.

## 11. 장애 시나리오별 점검

| 증상 | 가능한 원인 | 우선 확인 |
|---|---|---|
| ClickHouse가 기동하지 않음 | 이미지 아키텍처·메모리·볼륨 문제 | Docker 로그, 플랫폼 manifest, 자원 |
| DataLakeCatalog 생성 실패 | URI·OAuth endpoint·설정명 불일치 | Polaris API 경로와 설정명 |
| Catalog는 연결되지만 테이블이 없음 | Iceberg 테이블 미생성 또는 namespace 불일치 | Spark·Flink 로그, Polaris namespace |
| 테이블은 보이지만 파일을 읽지 못함 | Ozone storage endpoint·credential 문제 | ClickHouse 컨테이너에서 Ozone 접근 |
| 직접 경로 조회 실패 | IcebergS3 경로 또는 S3 호환 API 불일치 | metadata 경로, endpoint, access key |
| Iceberg와 Mart 행 수가 다름 | 적재 시점 또는 필터 범위 차이 | 기준 Snapshot, WHERE 조건 |
| Mart에 중복 행이 보임 | ReplacingMergeTree merge 미완료 | ORDER BY 키, version 컬럼, FINAL 비용 |
| 집계 Mart가 중복됨 | append 재실행 | 기간 overwrite 또는 재생성 정책 |
| Time Travel 실패 | Snapshot expiration 또는 잘못된 ID | snapshots 메타데이터와 보존 정책 |
| Iceberg 변경이 ClickHouse에 즉시 보이지 않음 | 메타데이터 캐시 또는 refresh 지연 | 캐시 설정, 최신 Snapshot, 재조회 |
| v3 테이블의 삭제 결과가 다름 | deletion vectors 미지원 | Iceberg format version과 delete 방식 |
| Windows와 Mac 결과가 다름 | 이미지·파일 권한·네트워크 차이 | Docker Desktop, ARM64, 볼륨 권한 |

장애 조사에서 ClickHouse의 쿼리 오류와 Iceberg 파일의 상태를 한 계층으로 묶지 않는다. Catalog 탐색, 파일 접근, Parquet 타입 변환, ClickHouse SQL, Mart 적재를 각각 분리해 확인한다.

## 12. 이 장의 핵심 정리

ClickHouse는 Iceberg와 함께 사용할 때 Serving 계층의 역할을 맡는다. DataLakeCatalog는 Polaris REST Catalog를 통해 테이블을 탐색하고 조회하는 경로를 제공하며, Iceberg Table Function과 Table Engine은 저장 경로를 직접 지정하는 조회 방식이다.

Iceberg 직접 조회는 데이터 복제를 줄이고 raw·clean 데이터의 현재 상태나 과거 Snapshot을 확인하는 데 적합하다. 반면 대시보드와 API처럼 반복적이고 낮은 지연 시간이 필요한 쿼리는 Iceberg 데이터를 MergeTree 계열 Mart로 적재하는 방식이 적합하다.

일반 Polaris REST Catalog를 ClickHouse의 행 단위 변경 엔진으로 가정하지 않는다. Iceberg의 CDC upsert, 삭제, Compaction, Snapshot expiration은 Flink와 Spark가 담당하고, ClickHouse는 조회와 Serving용 파생 테이블을 담당하는 구조를 기본으로 한다.

ClickHouse의 Iceberg 지원은 버전·Catalog·format version·저장소 endpoint에 따라 달라진다. Iceberg v2의 position delete와 equality delete를 읽는 것과 v3 deletion vector를 지원하는 것은 다른 문제다. 기능을 사용할 때는 ClickHouse 버전과 Iceberg 메타데이터를 함께 확인한다.

Serving 결과는 기준 Snapshot과 적재 실행 시점을 기록해야 한다. 그래야 ClickHouse Mart의 결과를 Iceberg 원본과 비교하고, 잘못된 배치 결과를 재생성하거나 과거 상태를 추적할 수 있다.

## 확인 문제

1. ClickHouse를 Iceberg의 유일한 변경 엔진으로 취급하면 안 되는 이유는 무엇인가?
2. Cold tier와 Hot tier는 각각 어떤 조회에 적합한가?
3. Iceberg Table Function, Iceberg Table Engine, DataLakeCatalog의 차이를 설명하라.
4. Docker Compose 안에서 polaris와 localhost가 서로 다른 의미를 갖는 이유는 무엇인가?
5. 일반 Polaris REST Catalog에서 조회와 쓰기의 책임을 어떻게 구분해야 하는가?
6. Iceberg 직접 조회와 ClickHouse Mart의 장단점은 무엇인가?
7. ReplacingMergeTree에서 최신 행이 항상 즉시 한 건만 보인다고 가정하면 안 되는 이유는 무엇인가?
8. Iceberg 데이터 파일 Compaction을 ClickHouse의 Manifest 재작성과 혼동하면 안 되는 이유는 무엇인가?
9. Iceberg v2의 equality delete와 Iceberg v3의 deletion vector는 어떻게 다른가?
10. Snapshot ID와 Mart 적재 시점을 함께 기록해야 하는 이유는 무엇인가?
11. ClickHouse Iceberg 타입 매핑에서 Decimal·Enum·LowCardinality를 별도로 검토해야 하는 이유는 무엇인가?
12. Apple Silicon에서 ClickHouse 이미지를 사용할 때 확인해야 할 항목은 무엇인가?

## 이 장에서 자료가 부족했거나 검증이 필요한 부분

- ClickHouse 26.7·Iceberg 1.11.0·Polaris 1.7.0·Ozone 2.2.x의 전체 Compose 실행 결과
- ClickHouse 26.7에서 allow_database_iceberg와 allow_experimental_database_iceberg의 최종 설정명
- self-hosted Apache Polaris 1.7.0의 실제 OAuth token endpoint와 ClickHouse DataLakeCatalog 연동
- namespace.table과 단일 문자열 식별자를 사용하는 ClickHouse 26.7의 최종 표기법
- 일반 Polaris REST Catalog에서 ClickHouse Iceberg INSERT·UPDATE·DELETE를 사용할 수 있는 정확한 범위
- ClickHouse 26.7에서 Refreshable Materialized View와 Polaris DataLakeCatalog의 중복 방지·교체 semantics
- Spark·Flink가 생성한 Iceberg delete 파일을 ClickHouse 26.7에서 읽는 조합별 검증
- rewrite_data_files와 ClickHouse MANIFEST 유지보수의 동시 실행 조건
- IcebergS3와 Ozone S3 Gateway의 endpoint·credential·경로 호환성
- ClickHouse 26.7 이미지의 Windows 11·Apple Silicon ARM64 지원과 재현성

## 참고 자료

- [ClickHouse REST Catalog 문서](https://clickhouse.com/docs/guides/use-cases/data-warehousing/rest-catalog)
- [ClickHouse Polaris Catalog 문서](https://clickhouse.com/docs/guides/use-cases/data-warehousing/polaris-catalog)
- [ClickHouse DataLakeCatalog 문서](https://clickhouse.com/docs/reference/engines/database-engines/datalake)
- [ClickHouse Iceberg Table Engine 문서](https://clickhouse.com/docs/reference/engines/table-engines/integrations/iceberg)
- [ClickHouse Iceberg Table Function 문서](https://clickhouse.com/docs/reference/functions/table-functions/iceberg)
- [ClickHouse open table format 쓰기 문서](https://clickhouse.com/docs/guides/use-cases/data-warehousing/getting-started/writing-data)
- [ClickHouse MergeTree 문서](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/mergetree)
- [Apache Iceberg 공식 문서](https://iceberg.apache.org/docs/latest/)
- [Apache Polaris 공식 문서](https://polaris.apache.org/docs/)

