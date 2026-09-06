# 12장. Apache Spark 배치 처리와 Iceberg 테이블 유지보수

11장에서는 Flink CDC를 이용해 소스 데이터베이스의 변경을 실시간으로 수집하고 Iceberg에 기록했다. 그러나 실시간 파이프라인만으로 데이터 플랫폼의 운영이 완성되지는 않는다. 과거 데이터의 초기 적재, 누락 구간의 재처리, 작은 파일 병합, 오래된 Snapshot 정리, 스키마와 파티션 변경은 별도의 배치 작업으로 다루는 편이 적합하다.

이 장에서는 Apache Spark를 Iceberg의 배치 처리 엔진으로 사용한다. Spark는 Polaris REST Catalog를 통해 Iceberg 테이블을 찾고, Ozone에 저장된 데이터와 메타데이터를 읽고 쓴다. Flink가 변경을 지속적으로 전달한다면, Spark는 대량 데이터와 유지보수 작업을 정해진 시점에 처리한다.

## 이 장의 기준과 버전 주의사항

조사 자료에는 Spark 기준 버전이 두 가지로 제시되어 있다.

| 자료에 제시된 기준 | 편집 판단 |
|---|---|
| Apache Spark 3.5.4 | Iceberg 1.11.0의 Spark 3.5 런타임 패키지를 사용하는 로컬 실습 기준 |
| Apache Spark 4.1.2 | Spark 4.1과 Iceberg 1.11.0의 새로운 DSv2 및 MERGE 기능을 검토하는 조건부 기준 |

입문자가 Windows 11과 Apple Silicon Mac에서 다시 실행할 수 있도록 이 장의 기본 실습은 Spark 3.5.4와 Iceberg 1.11.0 조합을 기준으로 작성한다. Spark 4.1.2에서 조사된 기능은 별도의 검토 항목으로 구분하며, Spark 3.5.4에서 그대로 동작한다고 가정하지 않는다.

[검토 필요: 집필 시점의 Apache Spark 4.1.2·Iceberg 1.11.0 호환성, Scala 바이너리 버전, Apple Silicon용 이미지와 공식 배포 패키지를 최종 확정해야 합니다.]

이 장에서 사용하는 Polaris URI, warehouse, OAuth2 credential, Ozone endpoint는 앞 장에서 만든 Compose 네트워크를 전제로 한 자리표시자다. 독자의 환경에서 서비스명이 다르면 실제 Compose 서비스명으로 바꾼다.

## 학습 목표

이 장을 마치면 다음 작업을 수행할 수 있습니다.

- Spark를 Polaris REST Catalog에 연결하고 Iceberg 테이블을 조회할 수 있습니다.
- Spark로 초기 데이터 백필과 대량 변환을 수행할 수 있습니다.
- 작은 파일을 Compaction하고 Manifest·Snapshot·Orphan 파일을 순서에 맞게 유지보수할 수 있습니다.
- 스키마 진화, 파티션 진화, Time Travel, 롤백의 의미와 주의점을 설명할 수 있습니다.

## 1. Spark와 Flink의 역할 나누기

### 1.1 Spark가 필요한 이유

Flink는 계속 들어오는 변경 이벤트를 낮은 지연 시간으로 처리하는 데 적합하다. 반면 전체 기간의 데이터를 다시 읽거나 특정 날짜 범위를 재처리하거나 다수의 파일을 한 번에 재작성하는 작업은 배치 엔진으로 분리하는 편이 관리하기 쉽다.

이 장에서는 Spark의 역할을 다음과 같이 정의한다.

| 작업 | Flink | Spark |
|---|---|---|
| 실시간 CDC 수집 | 주 역할 | 담당하지 않음 |
| 지속적인 스트리밍 변환 | 주 역할 | 보조 |
| 과거 데이터 백필 | 제한적 | 주 역할 |
| 대량 데이터 변환 | 제한적 | 주 역할 |
| 작은 파일 Compaction | 제한적 | 주 역할 |
| Manifest 재작성 | 제한적 | 주 역할 |
| Snapshot 만료 | 일반 배치 작업으로 수행 | 주 역할 |
| Orphan 파일 정리 | 일반 배치 작업으로 수행 | 주 역할 |
| Time Travel·감사 조회 | 보조 | 주 역할 |

이 구분은 기능의 절대적인 한계를 의미하지 않는다. 특정 작업을 어느 엔진으로 실행할 수 있는지는 실제 커넥터와 런타임 버전에 따라 달라진다. 여기서는 데이터 플랫폼의 책임을 단순하게 유지하기 위해 실시간 처리는 Flink, 대량·유지보수 처리는 Spark라는 운영 경계를 사용한다.

### 1.2 Spark 실행 구조

Spark 애플리케이션은 Driver가 실행 계획을 만들고 Executor가 태스크를 병렬로 수행한다. 이 장의 실습에서는 Spark가 Iceberg DataSource V2 인터페이스를 통해 Polaris REST Catalog와 통신하고, Catalog가 가리키는 Ozone 저장 위치에서 데이터 파일을 읽거나 새 파일을 기록한다.

~~~mermaid
flowchart TD
    A["Spark Driver"] --> B["Spark Executors"]
    A --> C["Polaris REST Catalog"]
    B --> D["Iceberg Data Files"]
    C --> D
    D --> E["Apache Ozone"]
~~~

Spark Driver와 Executor가 모두 Polaris와 Ozone에 접근해야 하는지, 네트워크 경로가 분리되는지는 배포 형태에 따라 달라진다. Docker Compose 로컬 환경에서는 컨테이너 내부에서 서비스 이름이 해석되는지부터 확인한다.

## 2. Spark를 Polaris REST Catalog에 연결하기

### 2.1 Catalog 설정의 기본 구조

Spark에서 Iceberg REST Catalog를 등록할 때는 다음 형식의 설정을 사용한다.

~~~text
spark.sql.catalog.<catalog-name>=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.<catalog-name>.type=rest
spark.sql.catalog.<catalog-name>.uri=<POLARIS_URI>
~~~

이 장에서는 Catalog 이름을 polaris로 사용한다. Catalog 이름은 논리 이름이므로 다른 이름을 사용할 수 있지만, 이후 SQL의 테이블 경로와 일관되어야 한다.

### 2.2 PySpark 연결 예제

~~~python
from pyspark.sql import SparkSession

CLIENT_ID = "<POLARIS_CLIENT_ID>"
CLIENT_SECRET = "<POLARIS_CLIENT_SECRET>"

spark = (
    SparkSession.builder
    .config(
        "spark.sql.catalog.polaris",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    .config("spark.sql.catalog.polaris.type", "rest")
    .config(
        "spark.sql.catalog.polaris.uri",
        "http://polaris:8181/api/catalog",
    )
    .config(
        "spark.sql.catalog.polaris.credential",
        f"{CLIENT_ID}:{CLIENT_SECRET}",
    )
    .config(
        "spark.sql.catalog.polaris.scope",
        "PRINCIPAL_ROLE:ALL",
    )
    .config(
        "spark.sql.catalog.polaris.warehouse",
        "ozone_catalog",
    )
    .config("spark.sql.defaultCatalog", "polaris")
    .getOrCreate()
)
~~~

credential은 설명을 위한 자리표시자다. 실제 비밀번호와 client secret을 소스 코드에 직접 기록하지 않는다. 환경 변수, Docker Secret 또는 조직의 보안 저장소를 사용한다.

자료 조사본은 Polaris 인증에 credential과 scope를 사용하는 OAuth2 client credentials 방식을 제시한다. 실제 Polaris 배포판의 인증 설정과 principal role 이름은 설치 방법과 버전에 따라 달라질 수 있으므로, 앞 장에서 만든 Polaris 설정과 맞춰야 한다 (출처: [Apache Polaris Spark Integration](https://polaris.apache.org/guides/spark/)).

### 2.3 spark-defaults.conf 설정

반복 실행할 때는 SparkSession마다 설정을 작성하는 대신 spark-defaults.conf에 공통 설정을 둘 수 있다.

~~~text
spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.polaris.type=rest
spark.sql.catalog.polaris.uri=http://polaris:8181/api/catalog
spark.sql.catalog.polaris.credential=<POLARIS_CLIENT_ID>:<POLARIS_CLIENT_SECRET>
spark.sql.catalog.polaris.scope=PRINCIPAL_ROLE:ALL
spark.sql.catalog.polaris.warehouse=ozone_catalog
spark.sql.defaultCatalog=polaris
~~~

이 파일을 여러 환경에서 공유하면 credential이 노출될 수 있다. 개발·검증·운영 환경에서 credential을 분리하고, 버전 관리 대상에는 비밀 값이 들어가지 않은 템플릿만 둔다.

### 2.4 연결 확인

~~~sql
SHOW CATALOGS;
SHOW NAMESPACES IN polaris;
SHOW TABLES IN polaris.raw;
~~~

polaris가 목록에 없으면 Spark Iceberg 런타임 JAR, Catalog 설정 키, Polaris URI를 순서대로 확인한다. namespace가 보이지 않으면 인증된 principal의 권한과 namespace 이름을 확인한다. 테이블은 보이지만 실제 파일을 읽지 못하면 Ozone endpoint와 파일 경로 접근 권한을 확인한다.

### 2.5 Spark Shell 실행 예제

~~~bash
spark-shell \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0 \
  --conf spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.polaris.type=rest \
  --conf spark.sql.catalog.polaris.uri=http://polaris:8181/api/catalog \
  --conf spark.sql.catalog.polaris.credential="<POLARIS_CLIENT_ID>:<POLARIS_CLIENT_SECRET>" \
  --conf spark.sql.catalog.polaris.scope=PRINCIPAL_ROLE:ALL \
  --conf spark.sql.catalog.polaris.warehouse=ozone_catalog \
  --conf spark.sql.defaultCatalog=polaris
~~~

이 명령은 Spark 3.5와 Scala 2.12 런타임을 전제로 한 자료 조사본 예시다. 로컬 이미지의 Spark·Scala 버전이 다르면 runtime artifact 이름을 그대로 복사하지 않는다.

[추가 자료 조사 필요: 최종 Compose 이미지에서 Spark 3.5.4의 Scala 버전, Iceberg runtime artifact, JDBC 드라이버의 설치 위치를 하나의 호환성 표로 확정해야 합니다.]

## 3. Iceberg 테이블 읽기와 쓰기

### 3.1 테이블 생성

~~~sql
CREATE TABLE polaris.raw.orders (
    order_id BIGINT,
    customer_id BIGINT,
    order_date TIMESTAMP,
    status STRING,
    total_amount DECIMAL(18, 2)
) USING iceberg
PARTITIONED BY (days(order_date))
TBLPROPERTIES (
    'format-version' = '2'
);
~~~

파티션은 파일 저장 구조를 결정하므로 실제 필터와 데이터 분포를 먼저 확인한다. 파티션 수가 지나치게 많아지지 않는지도 확인한다.

### 3.2 배치 읽기와 Append

~~~sql
SELECT order_id, customer_id, order_date, total_amount
FROM polaris.raw.orders
WHERE order_date >= TIMESTAMP '2026-08-01 00:00:00';

INSERT INTO polaris.raw.orders
SELECT order_id, customer_id, order_date, status, total_amount
FROM source_db.orders
WHERE order_date >= TIMESTAMP '2026-08-01 00:00:00';
~~~

PySpark에서는 DataFrame을 Iceberg 테이블에 추가할 수 있다.

~~~python
orders_df.writeTo("polaris.raw.orders").append()
~~~

### 3.3 Overwrite

특정 기간을 다시 계산해 덮어쓸 때는 INSERT OVERWRITE를 사용할 수 있다.

~~~sql
INSERT OVERWRITE polaris.raw.orders
SELECT order_id, customer_id, order_date, status, total_amount
FROM source_db.orders
WHERE order_date >= TIMESTAMP '2026-08-01 00:00:00';
~~~

덮어쓰기는 조건과 파티션 overwrite 정책에 따라 영향 범위가 달라진다. 운영 데이터에 실행하기 전에 테스트 테이블에서 어떤 파티션과 Snapshot이 바뀌는지 확인한다.

[검토 필요: Spark 3.5.4와 Iceberg 1.11.0에서 동적 파티션 덮어쓰기의 기본 동작과 INSERT OVERWRITE 영향 범위를 최종 실행 검증해야 합니다.]

### 3.4 Time Travel 읽기

~~~sql
SELECT *
FROM polaris.raw.orders
TIMESTAMP AS OF '2026-08-25 00:00:00';

SELECT *
FROM polaris.raw.orders
VERSION AS OF <SNAPSHOT_ID>;
~~~

실제 Snapshot ID는 메타데이터에서 조회한다.

~~~sql
SELECT snapshot_id, committed_at, operation
FROM polaris.raw.orders.snapshots
ORDER BY committed_at DESC;
~~~

Time Travel은 Snapshot 보존 정책의 영향을 받는다. Snapshot expiration 이후에는 만료된 시점의 데이터를 조회하지 못할 수 있다.

## 4. 과거 데이터 백필과 대량 변환

### 4.1 백필의 목적

백필은 과거 데이터를 현재 Iceberg 테이블에 채우는 작업이다. CDC 파이프라인을 시작하기 전의 과거 데이터를 적재하거나, 초기 적재에 실패한 기간을 다시 처리하거나, 변환 로직 변경 후 과거 데이터를 재계산할 때 사용한다.

백필은 단순한 INSERT가 아니다. 대상 기간, 중복 키, 현재 실시간 CDC와의 경계, 재실행 결과를 함께 정의해야 한다.

### 4.2 PostgreSQL JDBC 읽기

~~~python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

jdbc_url = "jdbc:postgresql://postgres:5432/postgres"
properties = {
    "user": "<POSTGRES_USER>",
    "password": "<POSTGRES_PASSWORD>",
    "driver": "org.postgresql.Driver",
}

customers_df = spark.read.jdbc(
    url=jdbc_url,
    table="(SELECT * FROM customers) AS customers_source",
    properties=properties,
)

orders_df = spark.read.jdbc(
    url=jdbc_url,
    table=(
        "(SELECT * FROM orders "
        "WHERE order_date >= '2026-01-01') AS orders_source"
    ),
    properties=properties,
)

customers_df.writeTo("polaris.raw.customers_batch").append()
orders_df.writeTo("polaris.raw.orders_batch").append()
~~~

대용량 테이블을 단일 JDBC 연결로 읽으면 병목이 될 수 있다. 조사본은 partition pushdown을 이용한 병렬 읽기를 확인 대상으로 제시하지만, 분할 컬럼·파티션 수·데이터베이스 연결 수의 구체적인 기준은 환경에 따라 달라진다.

[추가 자료 조사 필요: Spark 3.5.4 JDBC 병렬 읽기의 최종 코드, 소스 데이터베이스별 연결 수 제한, Percona MySQL·PostgreSQL·Oracle의 백필 성능 검증].

### 4.3 백필 재실행 정책

백필 작업은 읽을 기간과 쓰기 모드를 실행 인자로 분리한다.

| 인자 | 예시 | 목적 |
|---|---|---|
| 시작 시각 | 2026-01-01 | 읽을 기간의 시작 |
| 종료 시각 | 2026-02-01 | 읽을 기간의 끝 |
| 대상 테이블 | polaris.raw.orders_batch | 쓰기 대상 |
| 실행 ID | backfill-20260830-01 | 로그와 결과 추적 |
| 쓰기 모드 | append 또는 overwrite | 재실행 정책 |

기간을 명확하게 제한하면 같은 작업을 다시 실행하기 쉽다. 그러나 append를 단순 재실행하면 중복 행이 발생할 수 있다. primary key가 있는 테이블은 MERGE 또는 중간 테이블을 이용한 교체 전략을 검토한다.

### 4.4 MERGE로 병합하기

~~~sql
MERGE INTO polaris.raw.orders AS target
USING source_db.orders_enriched AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN
    UPDATE SET
        target.customer_id = source.customer_id,
        target.order_date = source.order_date,
        target.status = source.status,
        target.total_amount = source.total_amount
WHEN NOT MATCHED THEN
    INSERT (
        order_id,
        customer_id,
        order_date,
        status,
        total_amount
    )
    VALUES (
        source.order_id,
        source.customer_id,
        source.order_date,
        source.status,
        source.total_amount
    );
~~~

MERGE의 결과는 소스의 키가 유일하다는 전제를 둔다. 소스에 같은 order_id가 여러 번 나타나면 실패하거나 예측하지 못한 결과를 만들 수 있다.

~~~sql
SELECT order_id, COUNT(*) AS row_count
FROM source_db.orders_enriched
GROUP BY order_id
HAVING COUNT(*) > 1;
~~~

### 4.5 백필과 Flink CDC의 경계

자료 조사본은 다음과 같은 역할 분리를 제시한다.

~~~text
1. Spark로 과거 데이터를 백필한다.
2. Flink CDC로 실시간 변경을 수집한다.
3. 백필 데이터와 CDC 데이터를 대상 테이블에서 병합한다.
4. Compaction으로 작은 파일을 정리한다.
~~~

이 순서만으로 데이터 중복과 누락이 자동으로 해결된다고 판단해서는 안 된다. 백필이 읽는 시점과 Flink CDC가 변경을 읽기 시작하는 시점 사이의 변경, 초기 Snapshot 기준점, 동일 키의 최신성 기준을 별도로 정의해야 한다.

실습에서는 소스 쓰기를 잠시 멈추거나 백필 범위와 CDC 시작 기준점을 기록한다.

[추가 자료 조사 필요: Spark 백필과 Flink CDC 초기 Snapshot을 동시에 운영할 때 중복·누락을 방지하는 공식 절차와 LSN·SCN 기준점 관리 방법].

## 5. 작은 파일과 Compaction

### 5.1 작은 파일이 생기는 이유

Flink가 짧은 주기로 데이터를 기록하거나 배치 작업이 많은 작은 입력을 여러 번 커밋하면 Iceberg 테이블에 작은 데이터 파일이 누적될 수 있다. 파일 수가 많아지면 쿼리 계획과 파일 열기 비용이 커지고 Manifest 메타데이터도 증가한다.

작은 파일 문제는 특정 파일 크기 하나로 판단하지 않는다. 파일 개수, 평균 파일 크기, 파티션별 분포, 쿼리 지연 시간, Snapshot과 Manifest 증가 추세를 함께 확인한다.

### 5.2 rewrite_data_files 기본 예제

Iceberg는 Spark SQL 프로시저를 통해 데이터 파일을 다시 작성할 수 있다. 다음은 binpack 전략의 예시다.

~~~sql
CALL polaris.system.rewrite_data_files(
    table => 'raw.orders',
    strategy => 'binpack',
    options => map(
        'target-file-size-bytes', '268435456',
        'min-file-size-bytes', '67108864',
        'min-input-files', '5',
        'partial-progress.enabled', 'true',
        'partial-progress.max-commits', '10'
    )
);
~~~

목표 파일 크기와 최소 입력 파일 수는 실습을 위한 시작값이다. Ozone 저장 성능, Spark Executor 메모리, 파티션 수, 파일 형식, 조회 패턴에 따라 다시 측정한다.

### 5.3 Compaction 전략 비교

| 전략 | 동작 | 비용 | 시작하기 좋은 경우 |
|---|---|---|---|
| binpack | 정렬 없이 작은 파일을 병합 | 상대적으로 낮음 | 일반적인 작은 파일 정리 |
| sort | 지정한 정렬 기준으로 파일을 다시 작성 | 정렬과 셔플 비용 발생 | 특정 컬럼 범위 필터가 많은 경우 |
| z-order | 여러 컬럼을 기준으로 클러스터링하는 예시 | 상대적으로 높음 | 여러 컬럼 필터를 함께 검토할 때 |

binpack은 파일 크기 정리가 주된 목적일 때 먼저 선택한다. sort는 정렬로 얻는 파일 통계와 읽기 이득이 셔플 비용을 상쇄하는지 측정해야 한다. z-order는 실제 런타임과 Iceberg 버전에서 지원되는 구문인지 확인한 뒤 사용한다.

### 5.4 sort Compaction 예제

~~~sql
CALL polaris.system.rewrite_data_files(
    table => 'raw.orders',
    strategy => 'sort',
    sort_order => 'order_date ASC NULLS LAST, customer_id ASC NULLS LAST',
    options => map(
        'target-file-size-bytes', '536870912',
        'max-file-group-size-bytes', '107374182400',
        'partial-progress.enabled', 'true',
        'partial-progress.max-commits', '10'
    )
);
~~~

정렬 컬럼은 조회 조건과 일치해야 한다. 많은 컬럼을 정렬 기준에 추가하면 셔플 비용과 유지보수 시간이 커질 수 있다.

### 5.5 z-order 예제와 주의점

~~~sql
CALL polaris.system.rewrite_data_files(
    table => 'raw.orders',
    strategy => 'sort',
    sort_order => 'zorder(customer_id, order_date)',
    options => map(
        'target-file-size-bytes', '536870912'
    )
);
~~~

위 구문은 조사본에 포함된 z-order 예시다. z-order 표현식과 rewrite_data_files 옵션은 Iceberg·Spark 버전과 procedure 구현에 의존할 수 있으므로, 기본 실습은 binpack으로 진행하고 z-order는 별도 검증 대상으로 둔다.

[검토 필요: Iceberg 1.11.0과 Spark 3.5.4에서 rewrite_data_files의 zorder 표현식, sort_order 인자, 부분 커밋 옵션을 실제로 실행해야 합니다.]

### 5.6 Compaction 전후 확인

~~~sql
SELECT file_path, record_count, file_size_in_bytes
FROM polaris.raw.orders.files;

DESCRIBE HISTORY polaris.raw.orders;
~~~

Compaction이 성공해도 모든 파일이 하나로 합쳐진다고 가정하지 않는다. 파티션별 입력 조건, 파일 그룹 크기, 동시 writer, 삭제 파일 유무에 따라 결과가 달라진다.

## 6. Manifest 재작성

데이터 파일이 많아지면 Manifest 파일도 많아질 수 있다. Manifest 재작성은 데이터 내용을 변환하는 작업이 아니라 테이블 메타데이터를 재구성하는 작업이다.

~~~sql
CALL polaris.system.rewrite_manifests(
    table => 'raw.orders'
);
~~~

캐시 사용 옵션 예시는 다음과 같다.

~~~sql
CALL polaris.system.rewrite_manifests(
    table => 'raw.orders',
    use_caching => true
);
~~~

Compaction은 데이터 파일을 다시 쓰고 Manifest 재작성은 Manifest 구조를 다시 만든다. 두 작업을 한 명령으로 이해하지 않는다.

[검토 필요: Polaris 1.7.0에서 rewrite_manifests의 Catalog 경로와 Spark SQL procedure 지원 여부를 실행 검증해야 합니다.]

## 7. Snapshot expiration

### 7.1 Snapshot의 역할

Iceberg Snapshot은 테이블의 특정 시점 상태를 가리킨다. Snapshot이 보존되어 있으면 Time Travel과 장애 분석에 사용할 수 있지만, 계속 쌓이면 메타데이터와 참조 파일이 증가한다.

Snapshot expiration은 보존 정책에 따라 오래된 Snapshot을 만료시키는 작업이다. 만료된 Snapshot만 참조하던 데이터 파일과 삭제 파일은 더 이상 보존할 이유가 없어질 수 있다. 현재 유효한 Snapshot이 계속 참조하는 파일은 expiration만으로 삭제해서는 안 된다.

### 7.2 만료 예제

~~~sql
CALL polaris.system.expire_snapshots(
    table => 'raw.orders',
    older_than => TIMESTAMP '2026-08-23 00:00:00',
    retain_last => 10
);
~~~

older_than과 retain_last는 서로 다른 보호 조건이다. 시간 기준과 최소 보존 Snapshot 수를 함께 설정할 때 실제 procedure의 적용 순서를 버전 문서로 확인한다.

Snapshot expiration을 실행하기 전에 다음을 확인한다.

- Time Travel과 감사에 필요한 최소 기간
- Flink checkpoint·savepoint 및 재처리에 필요한 Snapshot
- 현재 실행 중인 Spark 또는 Flink writer
- 장애 분석에 필요한 Snapshot ID
- 규제·감사 정책에 따른 보존 기간

조사본에는 스트리밍 테이블에 3~7일, 배치 테이블에 7~30일 등의 범위가 제시되어 있다. 이 값은 보편적인 법칙이 아니라 운영 정책을 설계할 때 검토할 시작점으로만 사용한다.

[추가 자료 조사 필요: 이 책의 로컬 실습에서 사용할 Snapshot 보존 기간과 실제 운영 환경의 감사·복구 보존 정책].

## 8. Orphan 파일 정리

### 8.1 Orphan 파일의 의미

Orphan 파일은 현재 유효한 Iceberg 메타데이터에서 참조되지 않는 파일이다. 실패한 쓰기, 중단된 작업, 메타데이터 커밋 실패로 인해 저장소에 남을 수 있다.

Orphan 파일 정리는 저장 위치의 파일과 Iceberg 메타데이터를 비교한 뒤 참조되지 않는 파일을 삭제하는 작업이다. 잘못된 저장 위치나 너무 짧은 기준 시각을 사용하면 아직 진행 중인 쓰기의 파일을 삭제할 위험이 있다.

### 8.2 정리 예제

~~~sql
CALL polaris.system.remove_orphan_files(
    table => 'raw.orders',
    older_than => TIMESTAMP '2026-08-27 00:00:00'
);
~~~

older_than은 in-flight 작업과 최근 실패 작업을 보호하기 위한 시간 경계다. 조사본에서는 24시간 또는 72시간 이상의 안전 장치가 제시되지만, 실제 값은 작업의 최대 실행 시간과 재시작 시간을 기준으로 정한다.

운영에서는 다음 순서를 지킨다.

1. 현재 writer와 checkpoint 상태를 확인한다.
2. Snapshot expiration이 예상대로 완료되었는지 확인한다.
3. 다른 테이블이 같은 저장 위치를 공유하지 않는지 확인한다.
4. 충분히 긴 older_than을 지정한다.
5. 삭제 대상과 삭제 결과를 로그로 남긴다.

Orphan 파일 정리는 단순한 디렉터리 삭제가 아니다. Ozone의 객체 경로와 Polaris가 관리하는 warehouse 범위를 정확히 구분해야 한다.

## 9. 유지보수 작업의 권장 순서

조사 자료는 다음 순서를 제시한다.

1. rewrite_data_files로 작은 데이터 파일을 병합한다.
2. rewrite_manifests로 Manifest를 재작성한다.
3. expire_snapshots로 오래된 Snapshot을 만료한다.
4. remove_orphan_files로 더 이상 참조되지 않는 파일을 정리한다.

~~~mermaid
flowchart TD
    A["데이터 파일 Compaction"] --> B["Manifest 재작성"]
    B --> C["Snapshot 만료"]
    C --> D["Orphan 파일 정리"]
~~~

이 순서는 모든 환경에서 변경할 수 없는 규칙은 아니다. 동시 writer가 많거나 실패 작업이 남아 있는 환경에서는 각 단계의 결과를 확인한 뒤 다음 단계로 넘어간다. Orphan 파일 정리는 실제 파일을 삭제할 수 있으므로 마지막에 수행하고 보존 경계를 충분히 길게 잡는다.

### 9.1 Airflow와의 경계

이 장에서는 유지보수 명령과 실행 순서를 이해한다. 일정에 따른 자동 실행은 후속 Airflow 장에서 다룬다. Airflow DAG를 설계할 때는 각 유지보수 명령을 독립적인 작업으로 나누고 앞 단계의 성공 여부를 다음 단계의 조건으로 사용한다.

~~~text
maintenance_compaction
        ↓ 성공
maintenance_manifest_rewrite
        ↓ 성공
maintenance_snapshot_expiration
        ↓ 성공
maintenance_orphan_cleanup
~~~

각 작업의 로그에는 대상 테이블, 실행 시각, Snapshot ID, 처리 파일 수, 오류 메시지를 남긴다. 특히 Snapshot expiration과 Orphan cleanup은 실행 전후 보존 정책을 확인할 수 있어야 한다.

## 10. 스키마 진화

### 10.1 컬럼 ID 기반 변경

Iceberg는 컬럼 위치가 아니라 필드 ID를 사용해 스키마를 관리한다. 따라서 컬럼의 추가·삭제·이름 변경을 데이터 파일 전체를 다시 쓰지 않고 메타데이터 변경으로 처리할 수 있다. 단, 실제 엔진의 DDL 지원 범위와 데이터 타입 승격 규칙은 Spark·Iceberg 버전을 함께 확인한다.

### 10.2 주요 DDL 예제

~~~sql
ALTER TABLE polaris.raw.orders
ADD COLUMNS (discount_amount DECIMAL(18, 2));

ALTER TABLE polaris.raw.orders
DROP COLUMN status;

ALTER TABLE polaris.raw.orders
RENAME COLUMN customer_id TO client_id;

ALTER TABLE polaris.raw.orders
ALTER COLUMN order_id TYPE BIGINT;
~~~

컬럼 이름 변경과 컬럼 삭제 후 동일한 이름의 컬럼을 다시 추가하는 것은 의미가 다르다. 이름만 같다고 동일한 컬럼으로 복구된다고 가정하지 않는다.

### 10.3 스키마 변경 후 확인

~~~sql
DESCRIBE TABLE polaris.raw.orders;

SELECT *
FROM polaris.raw.orders
LIMIT 10;
~~~

Source의 새 컬럼, Kafka 이벤트의 필드, Flink의 스키마, Iceberg Sink의 테이블 스키마가 모두 일치해야 한다. Spark에서 DDL이 성공했다는 사실만으로 Flink CDC 파이프라인이 자동으로 새 컬럼을 기록한다고 판단하지 않는다.

## 11. 파티션 진화

### 11.1 파티션 진화의 의미

Iceberg의 파티션 진화는 기존 데이터 파일을 즉시 모두 다시 쓰지 않고 새로운 파티션 사양을 추가하는 방식으로 이해한다. 기존 파일은 이전 파티션 레이아웃을 유지하고 변경 이후의 쓰기는 새 파티션 사양을 따를 수 있다.

따라서 파티션 진화는 기존 데이터를 자동으로 균일한 레이아웃으로 재작성하는 기능이 아니다. 모든 데이터를 새 파티션에 맞추려면 별도의 Rewrite 또는 백필 작업이 필요하다.

### 11.2 파티션 사양 변경 예제

~~~sql
ALTER TABLE polaris.raw.orders
ADD PARTITION FIELD days(order_date);
~~~

조사본에는 다음과 같은 파티션 교체 예시도 포함되어 있다.

~~~sql
ALTER TABLE polaris.raw.orders
REPLACE PARTITION FIELD months(order_date)
WITH days(order_date);
~~~

REPLACE PARTITION FIELD의 정확한 SQL 문법과 Spark 지원 여부는 버전별로 다를 수 있다. 실행 전 테스트 테이블에서 DDL을 수행하고 기존 파일의 partition spec과 새로 기록되는 파일의 spec을 비교한다.

[검토 필요: Spark 3.5.4·Iceberg 1.11.0에서 ADD·DROP·REPLACE PARTITION FIELD의 실제 지원 범위와 SQL 문법].

### 11.3 파티션 변경 후 데이터 재작성

파티션 진화만 실행하면 기존 데이터의 물리적 파일이 즉시 새 파티션으로 이동하지 않는다. 새 파티션 레이아웃으로 전체 데이터를 통일하려면 다음과 같은 별도 계획이 필요하다.

1. 새 파티션 사양을 테스트 테이블에 적용한다.
2. 쿼리 성능과 파티션 수를 비교한다.
3. 백필 또는 INSERT OVERWRITE 범위를 결정한다.
4. Rewrite 결과와 Snapshot을 확인한다.
5. 기존 Snapshot과 Time Travel 보존 정책을 검토한다.

## 12. Time Travel과 롤백

### 12.1 Snapshot 이력 확인

~~~sql
SELECT snapshot_id, committed_at, operation
FROM polaris.raw.orders.snapshots
ORDER BY committed_at DESC;
~~~

특정 Snapshot의 데이터를 조회해 잘못된 배치 결과를 비교할 수 있다.

~~~sql
SELECT order_id, status, total_amount
FROM polaris.raw.orders
VERSION AS OF <SNAPSHOT_ID>;
~~~

Snapshot ID는 설명을 위한 자리표시자다. 실제 비교에서는 snapshots 메타테이블에서 유효한 ID를 선택한다.

### 12.2 Snapshot 간 데이터 비교

~~~sql
SELECT old_data.order_id,
       old_data.total_amount AS old_amount,
       new_data.total_amount AS new_amount
FROM (
    SELECT order_id, total_amount
    FROM polaris.raw.orders
    VERSION AS OF <OLD_SNAPSHOT_ID>
) AS old_data
JOIN (
    SELECT order_id, total_amount
    FROM polaris.raw.orders
    VERSION AS OF <NEW_SNAPSHOT_ID>
) AS new_data
ON old_data.order_id = new_data.order_id
WHERE old_data.total_amount <> new_data.total_amount;
~~~

### 12.3 롤백 예제

조사본에는 다음과 같은 롤백 프로시저 예제가 포함되어 있다.

~~~sql
CALL polaris.system.rollback_to_snapshot(
    'raw.orders',
    <SNAPSHOT_ID>
);
~~~

시점으로 되돌리는 예시는 다음과 같다.

~~~sql
CALL polaris.system.rollback_to_timestamp(
    'raw.orders',
    TIMESTAMP '2026-08-25 00:00:00'
);
~~~

롤백은 과거 데이터를 물리적으로 복사해 새 테이블을 만드는 것과 다르다. Catalog의 현재 참조를 과거 Snapshot으로 변경하는 작업이므로 동시 writer와 downstream 소비자가 어떤 상태를 보게 되는지 확인해야 한다.

[추가 자료 조사 필요: Polaris 1.7.0에서 Iceberg rollback procedure를 호출하는 정확한 Catalog·namespace 경로와 동시 writer가 있는 상태에서의 롤백 절차].

## 13. 단계별 로컬 실습

### 13.1 실습 준비

앞 장에서 Polaris와 Ozone을 실행했다는 전제로 시작한다. Spark를 같은 Docker Compose 네트워크에서 실행한다면 URI에 polaris를 사용하고 호스트에서 실행한다면 호스트가 접근할 수 있는 주소로 바꾼다.

~~~text
POLARIS_URI=http://polaris:8181/api/catalog
POLARIS_CLIENT_ID=<POLARIS_CLIENT_ID>
POLARIS_CLIENT_SECRET=<POLARIS_CLIENT_SECRET>
POLARIS_WAREHOUSE=ozone_catalog
~~~

### 13.2 Catalog 연결 확인

~~~python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .config(
        "spark.sql.catalog.polaris",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    .config("spark.sql.catalog.polaris.type", "rest")
    .config(
        "spark.sql.catalog.polaris.uri",
        "http://polaris:8181/api/catalog",
    )
    .config(
        "spark.sql.catalog.polaris.credential",
        "<POLARIS_CLIENT_ID>:<POLARIS_CLIENT_SECRET>",
    )
    .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
    .config("spark.sql.catalog.polaris.warehouse", "ozone_catalog")
    .config("spark.sql.defaultCatalog", "polaris")
    .getOrCreate()
)

spark.sql("SHOW CATALOGS").show()
spark.sql("SHOW NAMESPACES IN polaris").show()
~~~

### 13.3 실습 테이블 생성과 데이터 추가

~~~sql
CREATE NAMESPACE IF NOT EXISTS polaris.raw;

CREATE TABLE IF NOT EXISTS polaris.raw.customers_batch (
    id BIGINT,
    name STRING,
    email STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) USING iceberg;

INSERT INTO polaris.raw.customers_batch
VALUES
    (1, 'Alice', 'alice@example.com', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    (2, 'Bob', 'bob@example.com', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    (3, 'Charlie', 'charlie@example.com', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

SELECT *
FROM polaris.raw.customers_batch
ORDER BY id;
~~~

### 13.4 작은 파일 만들기와 Compaction

실습에서는 작은 INSERT를 여러 번 실행해 파일과 Snapshot의 변화를 관찰한다.

~~~sql
INSERT INTO polaris.raw.customers_batch
VALUES (4, 'David', 'david@example.com', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

INSERT INTO polaris.raw.customers_batch
VALUES (5, 'Eve', 'eve@example.com', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

SELECT file_path, record_count, file_size_in_bytes
FROM polaris.raw.customers_batch.files;

CALL polaris.system.rewrite_data_files(
    table => 'raw.customers_batch',
    strategy => 'binpack',
    options => map(
        'target-file-size-bytes', '268435456',
        'min-file-size-bytes', '67108864',
        'min-input-files', '2'
    )
);
~~~

Compaction 후 파일 수와 Snapshot 이력을 다시 확인한다.

~~~sql
SELECT file_path, record_count, file_size_in_bytes
FROM polaris.raw.customers_batch.files;

DESCRIBE HISTORY polaris.raw.customers_batch;
~~~

### 13.5 스키마 진화

~~~sql
ALTER TABLE polaris.raw.customers_batch
ADD COLUMNS (phone STRING);

DESCRIBE TABLE polaris.raw.customers_batch;
~~~

기존 행의 phone 값이 어떻게 보이는지 확인한다. 새 컬럼의 기본값과 기존 파일 읽기 결과는 DDL과 엔진 버전에 따라 다를 수 있으므로 결과를 직접 기록한다.

### 13.6 Time Travel 확인

먼저 Snapshot ID를 확인한다.

~~~sql
SELECT snapshot_id, committed_at, operation
FROM polaris.raw.customers_batch.snapshots
ORDER BY committed_at DESC;
~~~

그 다음 특정 Snapshot을 조회한다.

~~~sql
SELECT *
FROM polaris.raw.customers_batch
VERSION AS OF <SNAPSHOT_ID>;
~~~

### 13.7 실습 결과 기록

| 확인 대상 | 기록할 내용 |
|---|---|
| Catalog | Spark에서 polaris Catalog가 보였는가 |
| 인증 | namespace·table 조회 권한이 있었는가 |
| 쓰기 | INSERT 후 새 Snapshot이 생성되었는가 |
| 파일 | Compaction 전후 파일 수와 크기는 어떻게 달라졌는가 |
| 스키마 | ADD COLUMNS 후 기존 행을 어떻게 읽었는가 |
| Time Travel | 과거 Snapshot을 조회할 수 있었는가 |
| 저장소 | Ozone에 생성된 경로와 접근 결과는 무엇인가 |

## 14. 장애 시나리오별 점검

| 증상 | 우선 확인할 항목 |
|---|---|
| Spark가 Polaris에 연결되지 않음 | URI, Docker 네트워크, Catalog JAR, client credential |
| Catalog는 보이지만 namespace가 없음 | principal role, scope, namespace 권한 |
| 테이블은 보이지만 파일을 읽지 못함 | Ozone endpoint, warehouse 경로, 객체 접근 권한 |
| JDBC 읽기가 실패함 | JDBC 드라이버, hostname, 포트, 계정, 네트워크 |
| 백필 결과가 중복됨 | append 재실행, primary key, MERGE 조건 |
| 백필과 CDC 결과가 어긋남 | 초기 Snapshot 기준점, 변경 경계, 실행 순서 |
| Compaction procedure를 찾지 못함 | Iceberg runtime 버전, Catalog 이름, procedure 지원 |
| Compaction 시간이 지나치게 김 | 파일 그룹 크기, sort 전략, Executor 메모리, 셔플 |
| Snapshot이 예상보다 빨리 사라짐 | expire_snapshots의 older_than, retain_last, 보존 정책 |
| Orphan 정리 후 파일이 사라짐 | 저장 위치 범위, older_than, 동시 writer, shared warehouse |
| Time Travel이 실패함 | Snapshot expiration, 잘못된 Snapshot ID, Catalog 경로 |
| 파티션 변경 후 성능이 개선되지 않음 | 기존 파일의 partition spec, 새 데이터 분포, 실제 필터 |

유지보수 작업에서 실패가 발생하면 즉시 다음 삭제 작업으로 넘어가지 않는다. 현재 Snapshot, 작업 로그, Ozone 파일, Polaris Catalog 상태를 확인한 뒤 재시도 여부를 결정한다.

## 15. 이 장의 핵심 정리

Spark는 이 책의 데이터 플랫폼에서 실시간 CDC를 대신하는 엔진이 아니라 대량 배치와 Iceberg 유지보수를 담당하는 엔진이다. Flink가 지속적인 변경을 기록하는 동안 Spark는 백필, 재처리, Compaction, Manifest 재작성, Snapshot expiration, Orphan cleanup을 수행한다.

Spark를 Iceberg에 연결하려면 먼저 Polaris REST Catalog를 등록해야 한다. Catalog 이름, REST URI, warehouse, credential, scope가 서로 맞아야 하며 Spark 실행 위치에 따라 polaris와 localhost의 의미가 달라진다.

백필은 대상 기간과 재실행 정책을 명확히 해야 한다. Spark 백필과 Flink CDC를 단순히 순서대로 실행한다고 데이터 중복과 누락이 자동으로 사라지는 것은 아니다. 초기 Snapshot 경계와 동일 키의 최신성 기준을 별도로 설계해야 한다.

Compaction은 작은 데이터 파일을 줄이는 작업이고 Manifest 재작성은 메타데이터 구조를 정리하는 작업이다. Snapshot expiration은 Time Travel 보존 기간과 연결되며 Orphan cleanup은 실제 파일을 삭제할 수 있으므로 가장 보수적으로 실행해야 한다.

스키마 진화와 파티션 진화는 기존 파일을 즉시 모두 다시 쓰는 기능이 아니다. 기존 파일과 새로 기록되는 파일이 서로 다른 상태로 존재할 수 있으므로 변경 후 조회와 유지보수 결과를 직접 확인해야 한다.

## 확인 문제

1. Flink와 Spark의 역할을 실시간 처리와 배치 처리 관점에서 구분하라.
2. Spark에서 Polaris REST Catalog를 등록할 때 필요한 핵심 설정은 무엇인가?
3. Docker Compose 네트워크 안에서 polaris와 호스트의 localhost가 서로 다른 이유는 무엇인가?
4. 백필을 append로 재실행할 때 중복이 생길 수 있는 이유는 무엇인가?
5. Spark 백필과 Flink CDC 사이의 경계를 정의해야 하는 이유는 무엇인가?
6. binpack과 sort Compaction의 차이를 설명하라.
7. Compaction과 Manifest rewrite는 각각 어떤 파일을 다시 작성하는가?
8. Snapshot expiration과 Orphan cleanup을 같은 작업으로 보면 안 되는 이유는 무엇인가?
9. Orphan 파일 정리에서 older_than이 필요한 이유는 무엇인가?
10. 스키마 진화와 파티션 진화가 기존 데이터 파일에 미치는 영향은 어떻게 다른가?
11. Time Travel 조회가 Snapshot expiration 정책의 영향을 받는 이유는 무엇인가?
12. Spark 4.1.2의 MERGE 스키마 진화 구문을 Spark 3.5.4에서 그대로 사용하면 안 되는 이유는 무엇인가?

## 이 장에서 자료가 부족했거나 검증이 필요한 부분

- Spark 3.5.4·Iceberg 1.11.0·Polaris 1.7.0·Ozone 2.2.x의 최종 Docker Compose 실행 결과
- Spark 4.1.2·Iceberg 1.11.0의 실제 호환성, Scala 바이너리 버전, Apple Silicon 지원
- Spark 3.5.4 JDBC 병렬 읽기의 최종 옵션과 소스 데이터베이스별 연결 수 기준
- Spark 백필과 Flink CDC 초기 Snapshot을 결합할 때 중복·누락을 방지하는 공식 기준점 절차
- MERGE INTO WITH SCHEMA EVOLUTION의 정확한 구문과 Spark 버전별 지원 범위
- rewrite_data_files의 z-order, sort_order, partial-progress 옵션 실행 결과
- rewrite_manifests의 Polaris Catalog 경로와 Spark SQL procedure 지원 여부
- expire_snapshots와 remove_orphan_files의 실제 삭제 범위 및 동시 writer 보호 조건
- Spark 3.5.4·Iceberg 1.11.0에서 파티션 진화 DDL의 ADD·DROP·REPLACE 지원 범위
- Polaris 1.7.0에서 rollback_to_snapshot·rollback_to_timestamp 호출 경로
- Airflow에서 Spark 유지보수 작업을 실행할 최종 Operator와 격리 방식
- Apple Silicon과 Windows 11에서 Spark·Polaris·Ozone 전체 경로의 재현성

## 참고 자료

- [Apache Spark 공식 문서](https://spark.apache.org/docs/latest/)
- [Apache Polaris Spark Integration](https://polaris.apache.org/guides/spark/)
- [Apache Iceberg 공식 문서](https://iceberg.apache.org/docs/latest/)
- [Apache Iceberg 1.11.0 Release Announcement](https://opensource.googleblog.com/2026/05/announcing-apache-iceberg-1110.html)
- [Iceberg REST Catalog와 Apache Polaris](https://iceberglakehouse.com/apache-iceberg-rest-catalog/)
- [Iceberg Spark REST Catalog 설정](https://iceberglakehouse.com/iceberg/iceberg-rest-catalog/)
- [Iceberg Spark rewrite_data_files 절차](https://iceberglakehouse.com/iceberg/iceberg-spark-procedure-rewrite-data-files/)
- [Iceberg Spark rewrite_manifests 절차](https://iceberglakehouse.com/iceberg/iceberg-spark-procedure-rewrite-manifests/)
- [Iceberg Spark expire_snapshots 절차](https://iceberglakehouse.com/iceberg/iceberg-spark-procedure-expire-snapshots/)
- [Iceberg Spark remove_orphan_files 절차](https://iceberglakehouse.com/iceberg/iceberg-spark-procedure-remove-orphan-files/)
- [Iceberg Time Travel](https://iceberglakehouse.com/iceberg/iceberg-time-travel/)

