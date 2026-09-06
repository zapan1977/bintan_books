# 9장. Apache Iceberg와 Parquet로 테이블 만들기

## 이 장의 목표

이 장에서는 8장에서 준비한 Apache Ozone 저장 공간 위에 Apache Iceberg 테이블을 구성하는 원리를 배운다. 먼저 Parquet와 Iceberg가 서로 다른 계층의 기술이라는 점을 구분하고, Iceberg가 여러 데이터 파일을 하나의 논리적 테이블로 관리하는 방식을 이해한다.

이후 Table Metadata, Snapshot, Manifest List, Manifest File의 관계를 살펴본다. 스키마 진화, 파티션 진화, 시간 여행, 롤백, 작은 파일 문제와 유지보수 작업까지 다룬 뒤, Flink·Spark·ClickHouse를 이 책의 데이터 흐름에서 어떻게 나누어 사용할지 정리한다.

- 난이도: 초중급
- 선수 지식: Docker Compose, SQL, Kafka, Flink CDC, Apache Ozone의 S3 Gateway
- 기준 저장소: 8장에서 구성한 Ozone S3 Gateway
- 기준 기술: Apache Iceberg 1.11.0 계열과 Iceberg Table Format Spec v3를 자료 기준으로 사용
- 다음 장과의 연결: Iceberg Catalog와 테이블 등록·접근 정책을 구성한다

> Iceberg 라이브러리 버전과 Table Format Spec 버전은 서로 다른 체계다. 예를 들어 Iceberg 1.11.0은 라이브러리 릴리스 버전이고, v3는 테이블 포맷 사양 버전이다. 두 숫자를 같은 의미로 해석하면 안 된다.

## 1. Ozone 위에 Iceberg를 놓는 이유

8장에서 Apache Ozone을 객체 저장소로 구성했다. Ozone은 객체를 저장하고 S3 Gateway를 통해 파일을 읽고 쓸 수 있게 한다. 그러나 Ozone에 Parquet 파일을 여러 개 올려 두는 것만으로는 “이 파일들이 하나의 테이블을 구성한다”는 정보를 충분히 관리하기 어렵다.

Apache Iceberg는 이 문제를 해결하는 테이블 포맷이다. Iceberg는 실제 행 데이터를 저장하는 Parquet 파일과, 그 파일들의 관계·스키마·파티션·스냅샷을 설명하는 메타데이터를 함께 관리한다.

이 책의 저장 구조는 다음과 같다.

~~~mermaid
flowchart TB
    F["Flink CDC 이벤트"] --> W["Flink 또는 Spark"]
    W --> I["Apache Iceberg 테이블"]
    I --> P["Parquet 데이터 파일"]
    I --> M["Iceberg 메타데이터"]
    P --> O["Apache Ozone"]
    M --> O
    O --> C["ClickHouse 분석 조회"]
~~~

구성 요소의 책임을 구분하면 다음과 같다.

| 구성 요소 | 책임 |
|---|---|
| Apache Ozone | 객체와 파일의 실제 저장 |
| Apache Parquet | 개별 데이터 파일 내부의 컬럼형 저장 |
| Apache Iceberg | 여러 데이터 파일을 하나의 논리적 테이블로 관리 |
| Flink | CDC 이벤트의 실시간 처리와 Iceberg 적재 |
| Spark | 배치 적재, 백필, 일부 유지보수 작업 |
| ClickHouse | 분석용 Serving과 필요 시 Iceberg 직접 조회 |

Iceberg는 Ozone을 대체하지 않는다. Ozone은 저장 계층이고 Iceberg는 그 저장 공간 위에서 테이블 상태를 관리하는 계층이다.

## 2. Iceberg와 Parquet는 무엇이 다른가

### 2.1 파일 포맷과 테이블 포맷

Parquet는 하나의 파일 안에서 데이터를 어떻게 저장할지 정의하는 파일 포맷이다. 컬럼 단위 저장, 데이터 인코딩, 압축, 파일 내부 통계와 같은 물리적 저장 방식을 다룬다.

Iceberg는 여러 데이터 파일을 하나의 논리적 테이블로 묶고 관리하는 테이블 포맷이다. 테이블의 현재 스키마, 파티션 사양, 파일 목록, 스냅샷 이력과 커밋 상태를 메타데이터로 관리한다.

[Apache Iceberg 공식 사양](https://iceberg.apache.org/spec/)과 조사 자료를 기준으로 두 기술을 비교하면 다음과 같다.

| 구분 | Parquet | Apache Iceberg |
|---|---|---|
| 기술 계층 | 파일 포맷 | 테이블 포맷 |
| 관리 범위 | 단일 데이터 파일 | 여러 데이터 파일과 테이블 상태 |
| 주요 정보 | 행 데이터의 물리적 저장 | 스키마·파티션·스냅샷·파일 참조 |
| 스키마 관리 | 파일별 스키마 | 테이블 단위 스키마 관리 |
| 트랜잭션 관점 | 테이블 커밋을 정의하지 않음 | 메타데이터 기반 테이블 커밋 |
| 시간 여행 | 파일 자체의 기능이 아님 | 스냅샷 보존을 이용해 지원 |
| 대표 사용 위치 | data 디렉터리의 데이터 파일 | metadata와 스냅샷 관리 계층 |

Parquet 파일만 저장소에 쌓아 두면 파일 목록, 디렉터리 규칙, 파일 이름에 테이블 상태를 의존하게 된다. 이 방식은 파일이 추가·삭제·재작성되는 과정에서 일관된 테이블 상태를 판단하기 어렵다.

Iceberg는 현재 테이블 상태를 메타데이터로 표현한다. 쿼리 엔진은 모든 파일을 무조건 열어 보는 대신 메타데이터를 먼저 읽고, 조건에 맞지 않는 파일을 계획 단계에서 제외할 수 있다.

### 2.2 Iceberg가 제공하는 테이블 수준 기능

Iceberg는 다음 기능을 테이블 포맷의 범위에서 제공한다.

- 여러 데이터 파일을 하나의 논리적 테이블로 관리
- 테이블 스키마의 진화
- 파티션 사양의 진화
- 커밋된 테이블 상태를 스냅샷으로 관리
- 스냅샷 기반 시간 여행
- 파일 목록과 통계 기반의 파일 프루닝
- 데이터 파일과 삭제 파일의 참조 관리

이 기능들은 Parquet 파일 내부의 기능과 구별해야 한다. 예를 들어 Parquet는 한 파일의 컬럼 데이터를 효율적으로 읽게 해 주지만, 여러 Parquet 파일 가운데 현재 테이블에 속하는 파일이 무엇인지 결정하지는 않는다.

## 3. Iceberg 메타데이터 트리

Iceberg 테이블을 이해하려면 데이터 파일보다 메타데이터의 계층을 먼저 살펴보는 것이 좋다.

~~~mermaid
flowchart TB
    C["Catalog"] --> T["Table Metadata<br/>JSON"]
    T --> S["Current Snapshot"]
    S --> L["Manifest List<br/>Avro"]
    L --> F["Manifest Files<br/>Avro"]
    F --> D["Data Files<br/>Parquet"]
~~~

### 3.1 Table Metadata

Table Metadata 파일은 테이블의 현재 상태를 나타내는 JSON 파일이다. 조사 자료에서는 다음 정보가 포함되는 것으로 정리한다.

- 현재 스키마
- 파티션 사양
- 테이블 속성
- 활성 스냅샷 목록
- 현재 스냅샷 참조
- 메타데이터 파일의 이전 상태와 연결 정보

예시 경로는 다음과 같은 형태다.

~~~text
s3://iceberg-bucket/warehouse/iceberg/db/table/metadata/v1.metadata.json
~~~

실제 파일 이름과 경로는 사용하는 Catalog와 엔진의 설정에 따라 달라질 수 있다. 경로 이름을 사람이 임의로 계산해 테이블 상태를 판단해서는 안 된다. Catalog 또는 Iceberg 라이브러리가 가리키는 현재 메타데이터를 기준으로 읽어야 한다.

### 3.2 Snapshot

Snapshot은 특정 시점에 커밋된 테이블 상태를 나타낸다. 하나의 스냅샷은 테이블의 모든 데이터를 복사해 두는 별도 백업본이 아니다. 대신 그 시점에 테이블을 구성하는 파일 목록과 메타데이터의 관계를 참조한다.

스냅샷에는 일반적으로 다음과 같은 정보가 포함된다.

| 정보 | 의미 |
|---|---|
| Snapshot ID | 스냅샷 식별자 |
| Parent Snapshot ID | 이전 스냅샷과의 연결 |
| Timestamp | 커밋 시점 |
| Operation | 추가·삭제·교체 등 변경 유형 |
| Summary | 변경된 파일·레코드 관련 요약 |
| Manifest List 위치 | 스냅샷의 파일 목록 진입점 |

스냅샷은 커밋된 테이블 상태를 표현하는 불변 기록으로 다룬다. 새로운 변경이 발생하면 기존 스냅샷을 수정하는 대신 새로운 테이블 메타데이터와 스냅샷을 만든다.

### 3.3 Manifest List

Manifest List는 특정 스냅샷이 사용하는 Manifest File들의 목록을 기록하는 Avro 파일이다. 하나의 스냅샷은 자신이 사용하는 Manifest List를 참조하고, Manifest List는 여러 Manifest File을 가리킨다.

Manifest List에는 다음과 같은 정보가 포함될 수 있다.

- Manifest File의 위치
- 각 Manifest의 파티션 사양
- 파티션 요약 통계
- 파일 수와 레코드 수 관련 요약
- 스냅샷과 Manifest의 관계

쿼리 엔진은 Manifest List의 파티션 요약을 사용해 조건에 맞지 않는 Manifest를 먼저 제외할 수 있다. 이를 통해 모든 Manifest를 열지 않고도 읽을 범위를 줄일 수 있다.

### 3.4 Manifest File

Manifest File은 데이터 파일과 삭제 파일의 목록을 기록하는 Avro 메타데이터 파일이다. 개별 파일에 대해 다음 정보를 관리할 수 있다.

- 데이터 파일 또는 삭제 파일의 위치
- 파일 포맷
- 파티션 값
- 레코드 수
- 컬럼별 하한·상한 값
- null 수와 값 수 관련 통계
- 해당 스냅샷에서 파일이 추가·삭제·기존 상태인지에 대한 정보

Manifest File은 쿼리의 파일 프루닝에 사용되는 중요한 정보원이다. 예를 들어 특정 날짜 조건이 들어오면 파티션 값과 파일의 컬럼 통계를 비교해 읽지 않아도 되는 파일을 제외할 수 있다.

### 3.5 전체 관계를 파일 경로로 읽기

다음 구조는 메타데이터와 데이터 파일의 관계를 단순화한 예다.

~~~text
Iceberg Table
├── metadata/
│   ├── v1.metadata.json
│   ├── v2.metadata.json
│   ├── snap-1001-1-manifest-list.avro
│   ├── snap-1002-1-manifest-list.avro
│   ├── manifest-001.avro
│   └── manifest-002.avro
└── data/
    ├── part-00001.parquet
    ├── part-00002.parquet
    └── part-00003.parquet
~~~

중요한 점은 metadata 디렉터리의 파일과 data 디렉터리의 파일을 이름만 보고 직접 조합하지 않는 것이다. 현재 유효한 테이블은 Catalog가 가리키는 Table Metadata에서 현재 Snapshot으로 이동하고, Manifest List와 Manifest File을 따라가며 결정한다.

## 4. 원자적 커밋은 무엇을 보장하는가

### 4.1 커밋의 기본 흐름

Iceberg 테이블에 데이터를 적재할 때 엔진은 대체로 다음 순서를 따른다.

1. 새로운 데이터 파일을 객체 저장소에 쓴다.
2. 새로운 Manifest File 또는 관련 메타데이터를 작성한다.
3. 새 Snapshot과 Table Metadata를 구성한다.
4. Catalog가 가리키는 현재 메타데이터 위치를 새 위치로 갱신한다.
5. 갱신이 성공하면 새 Snapshot이 현재 테이블 상태가 된다.

~~~mermaid
sequenceDiagram
    participant E as 엔진
    participant O as Ozone
    participant C as Catalog
    E->>O: 데이터 파일 작성
    E->>O: Manifest와 Metadata 작성
    E->>C: 현재 Metadata 포인터 갱신
    C-->>E: 커밋 성공 또는 충돌
    E-->>E: 새 Snapshot 확인
~~~

이때 “원자적”이라는 표현은 객체 저장소에 이루어지는 모든 개별 파일 쓰기가 하나의 원자적 작업으로 묶인다는 뜻이 아니다. 정확한 표현은 현재 테이블 메타데이터를 가리키는 참조가 새 상태로 전환되거나, 충돌로 인해 전환되지 않는다는 의미에 가깝다.

따라서 데이터 파일을 작성한 뒤 Catalog 커밋에 실패하면, 어느 유효한 스냅샷에서도 참조하지 않는 파일이 남을 수 있다. 이런 파일을 orphan file이라고 하며, 별도의 유지보수 작업에서 정리 대상이 된다.

### 4.2 동시 커밋과 충돌

두 작업이 같은 테이블을 동시에 변경하면 모두가 같은 현재 상태를 기준으로 커밋하려고 할 수 있다. 먼저 커밋한 작업이 현재 메타데이터 포인터를 갱신하면, 나중 작업은 자신이 사용한 기준 상태가 더 이상 현재 상태가 아님을 감지하고 재시도하거나 실패할 수 있다.

동시성 제어의 구체적인 방식은 Catalog 구현과 엔진에 따라 다르다. 따라서 “Iceberg는 여러 엔진의 동시 쓰기를 언제나 자동 해결한다”라고 표현해서는 안 된다.

[추가 자료 조사 필요: 이 책에서 선택할 Catalog의 동시 커밋 충돌 정책, 재시도 설정, Flink와 Spark의 동시 쓰기 실습]

## 5. 스키마 진화

### 5.1 필드 ID 기반 스키마 관리

Iceberg는 컬럼 이름이나 물리적 순서만으로 컬럼을 식별하지 않고 고유 필드 ID를 사용해 스키마를 추적한다. 이 설계는 컬럼 이름 변경이나 일부 호환 가능한 스키마 변경에서 기존 파일을 모두 다시 작성하지 않고 메타데이터를 갱신할 수 있도록 한다.

테이블 메타데이터 JSON에는 현재 스키마와 스키마 이력이 기록된다. 새로운 스키마 변경도 커밋된 테이블 상태의 일부가 되므로, 스냅샷 이력과 함께 추적할 수 있다.

### 5.2 대표적인 스키마 변경

조사 자료에서는 다음 변경을 호환 가능한 스키마 진화의 예로 제시한다.

| 변경 | 일반적인 의미 | 주의점 |
|---|---|---|
| 컬럼 추가 | 새 필드 추가 | 기존 파일에는 값이 없을 수 있음 |
| 컬럼 삭제 | 현재 스키마에서 필드 제거 | 과거 스냅샷과 소비자 영향 확인 |
| 컬럼 이름 변경 | 필드 이름 변경 | 필드 ID와 엔진 지원 범위 확인 |
| 타입 승격 | 더 넓은 타입으로 변경 | 모든 타입 조합이 허용되는 것은 아님 |
| 기본값 추가 | 새 필드의 기본값 정의 | 엔진과 포맷 사양의 지원 범위 확인 |

예를 들어 Spark SQL에서는 다음과 같은 형태의 명령을 사용할 수 있다.

~~~sql
-- 초기 테이블 생성 예시
CREATE TABLE iceberg_db.customers (
  id BIGINT,
  name STRING,
  email STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING iceberg
TBLPROPERTIES ('format-version' = '2');

-- 컬럼 추가 예시
ALTER TABLE iceberg_db.customers ADD COLUMN phone STRING;

-- 컬럼 이름 변경 예시
ALTER TABLE iceberg_db.customers RENAME COLUMN name TO full_name;

-- 스키마 확인
DESCRIBE iceberg_db.customers;

-- 스냅샷 및 변경 이력 확인
DESCRIBE HISTORY iceberg_db.customers;
~~~

이 SQL은 Spark SQL 환경을 전제로 한 예시다. Flink SQL과 다른 엔진은 DDL 문법이나 지원 범위가 다를 수 있다.

[검토 필요: 조사 자료에는 Iceberg 1.11.0 기준의 스키마 진화 원칙은 정리되어 있으나, 책의 최종 Compose에 포함할 엔진과 Catalog 조합에서 각 DDL을 실제 실행한 결과는 별도 검증이 필요하다.]

### 5.3 스키마 진화 시 확인할 항목

스키마를 변경하기 전에는 다음을 확인한다.

1. 현재 테이블 포맷 버전
2. 쓰기 엔진과 읽기 엔진의 지원 범위
3. 기존 스냅샷을 읽는 소비자의 호환성
4. CDC 이벤트 계약과의 일치 여부
5. 컬럼 삭제 또는 이름 변경이 downstream 쿼리에 미치는 영향
6. 변경 후 생성되는 새 Snapshot과 Metadata 파일

Iceberg가 스키마 진화를 지원한다는 사실이 데이터 계약을 생략해도 된다는 뜻은 아니다. 이 책의 7장에서 정리한 이벤트 계약과 품질 규칙은 스키마 진화 전후에도 유지되어야 한다.

## 6. 파티션 진화와 숨김 파티셔닝

### 6.1 파티션은 파일 배치 전략이다

파티션은 데이터를 조건별로 나누어 저장하고, 쿼리에서 읽을 파일 범위를 줄이는 데 사용한다. Iceberg는 테이블 메타데이터에 파티션 사양을 관리하며, 파티션 필드와 변환 규칙을 함께 기록한다.

Hive 스타일 경로를 애플리케이션이 직접 계산하는 방식과 달리, Iceberg는 소스 컬럼에 적용할 파티션 변환을 테이블 메타데이터에 정의한다. 쿼리 엔진은 필터와 파티션 변환을 비교해 파일을 선택한다.

이 기능은 숨김 파티셔닝으로 설명된다. 사용자는 원본 컬럼을 기준으로 조건을 작성하고, 파티션 경로의 물리적 표현을 직접 알 필요가 줄어든다.

### 6.2 파티션 진화

Iceberg는 한 테이블의 이력 안에서 여러 파티션 사양을 보존할 수 있다. 기존 파일은 이전 파티션 사양을 사용하고, 변경 이후 생성되는 파일은 새 파티션 사양을 사용할 수 있다.

이 방식은 파티션 전략을 변경할 때 기존 데이터 파일을 한 번에 모두 재작성해야 하는 부담을 줄인다. 다만 과거 파일과 새 파일이 서로 다른 파티션 사양을 사용하므로, 쿼리 엔진과 유지보수 작업이 두 사양을 올바르게 처리하는지 확인해야 한다.

### 6.3 파티션 변경 예시

~~~sql
-- 초기 파티션을 사용한 테이블 생성 예시
CREATE TABLE iceberg_db.orders (
  order_id BIGINT,
  customer_id BIGINT,
  order_date TIMESTAMP,
  status STRING,
  total_amount DECIMAL(10, 2)
) USING iceberg
PARTITIONED BY (order_date)
TBLPROPERTIES ('format-version' = '2');

-- 파티션 필드 추가 예시
ALTER TABLE iceberg_db.orders ADD PARTITION FIELD customer_id;

-- 파티션 필드 삭제 예시
ALTER TABLE iceberg_db.orders DROP PARTITION FIELD customer_id;

-- 메타데이터 확인
DESCRIBE EXTENDED iceberg_db.orders;
~~~

위 명령은 Spark SQL 계열 예시다. 파티션 변환을 변경하는 세부 문법은 엔진 버전과 Iceberg 통합 방식에 따라 달라질 수 있다.

파티션을 설계할 때는 단순히 컬럼을 많이 넣는 것보다 다음을 먼저 생각한다.

- 주요 쿼리 필터가 무엇인가
- 시간 범위 조회가 많은가
- 하루 또는 한 시간 단위 파일 수가 지나치게 많아지지 않는가
- 특정 값에 데이터가 편중되는가
- 파티션 변경이 예상되는가
- Flink 스트리밍 적재에서 파일이 너무 작게 생성되지 않는가

## 7. 시간 여행과 롤백

### 7.1 시간 여행

Iceberg는 스냅샷 이력을 이용해 과거에 커밋된 테이블 상태를 조회할 수 있다. 이를 시간 여행(Time Travel)이라고 한다.

시간 여행은 다음 상황에서 유용하다.

- 잘못된 적재 직전의 데이터 확인
- 파이프라인 변경 전후 비교
- 데이터 감사와 재현성 검증
- 특정 시점에 ClickHouse에 공급된 원본 상태 추적
- 삭제 또는 스키마 변경 전의 테이블 상태 확인

Spark SQL 예시는 다음과 같다.

~~~sql
-- 스냅샷 이력 확인
DESCRIBE HISTORY iceberg_db.customers;

-- 시간 기준 조회 예시
SELECT *
FROM iceberg_db.customers
TIMESTAMP AS OF '2026-08-29 15:00:00';

-- 스냅샷 ID 기준 조회 예시
SELECT *
FROM iceberg_db.customers
VERSION AS OF 123456789;
~~~

Timestamp AS OF와 VERSION AS OF의 실제 문법과 지원 범위는 엔진마다 다를 수 있다. 위 예시는 Spark SQL 형태로만 사용한다.

### 7.2 롤백

롤백은 현재 테이블이 가리키는 상태를 과거 스냅샷으로 되돌리는 관리 작업이다. 롤백은 데이터 파일을 과거 상태로 복사하는 작업이 아니라, 현재 테이블 상태의 참조를 이전 스냅샷으로 전환하는 작업으로 이해해야 한다.

롤백 전에는 다음을 확인한다.

1. 되돌릴 Snapshot ID 또는 시각
2. 현재 Snapshot과 되돌릴 Snapshot의 차이
3. 롤백 이후 새로 생성될 Snapshot 여부
4. Flink와 Spark가 동시에 해당 테이블을 쓰고 있는지
5. ClickHouse 등 소비 계층이 새 상태를 다시 읽어야 하는지
6. 보존 정책에 의해 대상 Snapshot이 만료되지 않았는지

구체적인 롤백 SQL 또는 프로시저는 Spark, Flink, Trino 등 엔진마다 다르다.

[추가 자료 조사 필요: 최종 실습 엔진에서 사용할 Iceberg 롤백 명령과 실패 시 복구 절차]

## 8. 스트리밍 적재와 작은 파일 문제

### 8.1 작은 파일이 생기는 이유

Flink CDC와 같이 지속적으로 이벤트를 처리하는 작업은 체크포인트, 파일 롤링, 커밋 간격에 따라 데이터 파일을 생성한다. 이벤트 양이 적거나 체크포인트 간격이 짧으면 많은 작은 Parquet 파일이 만들어질 수 있다.

작은 파일이 누적되면 다음 비용이 증가할 수 있다.

- 쿼리 계획 단계에서 확인해야 할 파일 수
- 객체 저장소 요청 수
- 파일 열기와 연결 비용
- Manifest File 수
- 메타데이터 읽기 비용
- 파일 병합과 유지보수 비용

작은 파일 문제는 “Parquet가 느리다”는 문제와 다르다. 파일 자체의 컬럼형 읽기 성능과, 지나치게 많은 파일을 관리하는 테이블 운영 비용을 구분해야 한다.

### 8.2 원인별 대응

| 원인 | 발생 결과 | 대응 방향 |
|---|---|---|
| 짧은 체크포인트 간격 | 잦은 Snapshot 커밋 | 체크포인트와 파일 롤링 정책 조정 |
| 적은 이벤트량 | 작은 Parquet 파일 | 배치 크기와 파일 크기 조정 |
| 잦은 UPSERT·DELETE | 삭제 파일 증가 | Rewrite 또는 Compaction 검토 |
| 지속적인 파티션 변경 | 여러 파티션 사양과 Manifest 증가 | 파티션 변경 영향 평가 |
| 잦은 스키마 변경 | 메타데이터 이력 증가 | 변경 승인과 적용 주기 관리 |

### 8.3 Compaction과 Rewrite

Compaction 또는 데이터 파일 Rewrite는 여러 작은 데이터 파일을 더 큰 파일로 재작성하는 유지보수 작업이다. 이 작업은 새로운 스냅샷을 만들 수 있으며, 기존 파일은 이전 스냅샷이 보존되는 동안 바로 삭제되지 않을 수 있다.

유지보수 작업을 설계할 때는 다음을 기록한다.

- 대상 테이블과 파티션
- 최소·최대 파일 크기
- 동시에 실행할 Rewrite 작업 수
- 새 Snapshot 생성 주기
- 실패한 작업에서 생성될 수 있는 orphan file
- Snapshot 보존 기간
- ClickHouse 조회 중 파일 교체가 미치는 영향

구체적인 유지보수 명령은 엔진별로 다르다. Spark는 Iceberg 유지보수 프로시저를 사용할 수 있고, Flink는 배치 작업 또는 별도의 maintenance job으로 수행할 수 있다.

[추가 자료 조사 필요: 책의 최종 Spark 또는 Flink 버전에서 실제로 실행할 Compaction 명령과 파일 크기 기준]

## 9. Snapshot Expiration과 Orphan File Cleanup

### 9.1 두 작업을 구분하기

Snapshot Expiration은 보존 정책 밖의 오래된 스냅샷을 정리하는 작업이다. Orphan File Cleanup은 유효한 Snapshot이나 Metadata에서 더 이상 참조하지 않는 파일을 찾아 정리하는 작업이다.

두 작업은 목적이 다르다.

| 작업 | 주 대상 | 목적 | 주의점 |
|---|---|---|---|
| Snapshot Expiration | 오래된 Snapshot과 그 Snapshot 전용 파일 | 시간 여행 보존 범위 관리 | 만료된 Snapshot으로 조회할 수 없음 |
| Orphan File Cleanup | 어떤 유효 Snapshot에서도 참조되지 않는 파일 | 실패한 쓰기 잔여물 정리 | 진행 중인 쓰기 파일을 삭제할 위험 |
| Data File Rewrite | 작은 데이터 파일 | 파일 수와 쿼리 비용 감소 | 새 Snapshot 생성 |
| Manifest Rewrite | 과도한 Manifest File | 계획 단계 메타데이터 비용 감소 | 엔진별 지원 확인 |

Snapshot Expiration을 실행하면 과거 Snapshot에만 필요했던 파일이 정리 대상이 될 수 있다. 그러나 실제 파일 삭제 시점과 범위는 Catalog, 엔진, 객체 저장소, 보존 설정에 따라 달라질 수 있다.

Orphan File Cleanup은 특히 조심해야 한다. 아직 커밋되지 않은 작업이 작성 중인 파일을 유효하지 않은 파일로 잘못 판단하면 데이터 손실이 발생할 수 있다. 따라서 운영 환경에서는 동시 쓰기 작업, cutoff 시각, 보존 기간과 객체 저장소의 목록 가시성을 함께 고려해야 한다.

### 9.2 권장 실행 순서

일반적인 유지보수 흐름은 다음처럼 설명할 수 있다.

1. 현재 실행 중인 쓰기와 유지보수 작업 확인
2. 보존할 Snapshot과 Time Travel 기간 결정
3. Snapshot Expiration 실행
4. 테이블 메타데이터와 파일 참조 확인
5. 안전한 cutoff 이후 Orphan File Cleanup 실행
6. 작업 결과와 삭제 범위 기록
7. 쿼리 계획 시간과 파일 수 변화 확인

실습에서는 작은 데이터셋으로만 수행하고, 삭제 전 대상 목록을 확인한다.

~~~sql
-- Spark SQL 예시 형식
-- 실제 프로시저와 인자는 엔진 버전에 따라 확인해야 한다.
CALL system.expire_snapshots(
  table => 'iceberg_db.customers',
  older_than => TIMESTAMP '2026-08-22 00:00:00'
);
~~~

[검토 필요: Snapshot Expiration과 Orphan File Cleanup의 실제 순서 및 인자는 선택한 Catalog와 Spark·Flink 버전에 맞춰 고정해야 한다.]

## 10. Flink·Spark·ClickHouse의 역할 분리

### 10.1 기능 비교

조사 자료는 다음과 같은 역할 분리를 제안한다. 다만 각 기능의 정확한 지원 범위는 엔진 버전, Iceberg 커넥터, Catalog, 테이블 포맷 버전에 따라 달라진다.

| 기능 | Apache Flink | Apache Spark | ClickHouse |
|---|---|---|---|
| 배치 읽기 | 지원 범위 확인 | 지원 | 지원 범위 확인 |
| 스트리밍 읽기 | 주요 사용 사례 | Structured Streaming 검토 | 일반 스트리밍 처리 엔진 역할 아님 |
| 스트리밍 쓰기 | CDC 실시간 적재 | micro-batch 검토 | 원본 Iceberg 관리 엔진으로 사용하지 않음 |
| 스냅샷 커밋 | 체크포인트와 연계 | 작업 커밋과 연계 | 직접 관리 엔진으로 두지 않음 |
| 배치 백필 | 가능 | 주요 사용 사례 | 분석용 조회 |
| Compaction | 버전별 확인 | 유지보수 프로시저 후보 | 원본 유지보수 주체로 두지 않음 |
| Snapshot Expiration | 버전별 확인 | 유지보수 프로시저 후보 | 직접 수행 범위 확인 필요 |
| Time Travel | SQL·커넥터별 확인 | 지원 범위 확인 | 버전별 기능 확인 |
| 이 책의 주 역할 | Flink CDC 실시간 적재 | 백필·대량 변환·유지보수 | Serving과 직접 조회 |

### 10.2 Flink와 Iceberg

Flink에서 Iceberg Sink를 사용할 때 커밋은 체크포인트 완료와 연계될 수 있다. 이 구조에서는 체크포인트가 성공한 처리 구간의 결과가 Iceberg Snapshot으로 커밋되고, 실패한 구간에서 작성된 파일은 유효 Snapshot에서 참조되지 않을 수 있다.

따라서 “Flink Iceberg Sink는 언제나 exactly-once를 보장한다”라고 단순하게 쓰면 부정확하다. 정확한 평가는 다음 요소를 함께 확인해야 한다.

- Flink 체크포인트 설정
- 소스의 재개 위치
- Sink의 커밋 방식
- Iceberg Catalog의 커밋 동작
- 실패와 재시작 조건
- 중복 이벤트와 이벤트 계약
- 커밋되지 않은 파일 정리 방식

이 책에서는 Flink를 CDC 이벤트를 실시간으로 Iceberg에 적재하는 엔진으로 사용한다. 실제 exactly-once 검증은 다음 장의 Catalog와 Flink 실습에서 별도로 다룬다.

[추가 자료 조사 필요: 최종 Flink 버전과 Iceberg Sink 커넥터 버전, checkpoint·savepoint·commit의 실제 재현 시나리오]

### 10.3 Spark와 Iceberg

Spark는 배치 읽기·쓰기, 과거 데이터 백필, 대량 변환과 유지보수 작업에 적합한 후보로 둔다. 이 책의 구성에서는 실시간 CDC의 주 처리 엔진이라기보다 다음 작업에 사용한다.

- 과거 데이터 백필
- 실패한 기간의 재처리
- 작은 파일 Compaction
- Manifest Rewrite
- Snapshot Expiration
- Orphan File Cleanup
- 테이블 상태와 이력 확인

Spark SQL 명령은 엔진과 Catalog가 올바르게 연결된 경우에만 실행된다. SQL 문법만 맞는다고 테이블이 자동으로 존재하는 것은 아니다.

### 10.4 ClickHouse와 Iceberg

ClickHouse는 Iceberg 테이블을 직접 읽는 분석 Serving 계층으로 사용할 수 있다. 그러나 Iceberg 원본 테이블의 스키마·파티션·스냅샷·유지보수를 ClickHouse가 주도한다고 가정하면 안 된다.

이 책에서는 다음 원칙으로 역할을 나눈다.

- Flink: 실시간 CDC 적재
- Spark: 백필과 유지보수
- ClickHouse: 분석 조회와 Serving
- Ozone: 객체 저장
- Iceberg: 테이블 상태 관리

ClickHouse의 Iceberg 기능은 버전, Catalog, 테이블 포맷 버전, 데이터 타입과 삭제 파일 처리에 따라 달라질 수 있다. 따라서 ClickHouse에서 지원하는 기능을 “Iceberg 전체 기능”으로 일반화하지 않는다.

[추가 자료 조사 필요: ClickHouse 26.6 계열의 Iceberg Table Engine, Catalog 연동, Time Travel, 삭제 파일과 쓰기 지원 여부를 공식 문서로 검증]

## 11. 로컬 환경에서 Iceberg 테이블 준비하기

이 절은 8장에서 준비한 Ozone S3 Gateway가 실행 중이고, 다음 조건을 만족한다고 가정한다.

- 호스트에서 Ozone S3 Gateway에 접근할 수 있다.
- Flink 또는 Spark 컨테이너가 같은 Docker Compose 네트워크에 있다.
- Ozone Bucket이 생성되어 있다.
- 사용할 Iceberg Catalog의 종류가 결정되어 있다.
- S3A 또는 S3 파일 I/O 설정이 엔진에 제공되어 있다.

### 11.1 저장 경로 결정

로컬 실습의 warehouse 경로를 하나로 고정한다.

~~~text
호스트에서 확인하는 예시:
s3://iceberg-bucket/warehouse/

컨테이너 내부에서 사용하는 예시:
s3a://iceberg-bucket/warehouse/
~~~

호스트의 AWS CLI는 8장에서 사용한 endpoint를 적용한다.

~~~bash
# Ozone Bucket 목록 확인
aws s3 ls \
  --endpoint-url http://localhost:9878

# 테스트 경로 확인
aws s3 ls s3://iceberg-bucket/warehouse/ \
  --recursive \
  --endpoint-url http://localhost:9878
~~~

Flink 또는 Spark 컨테이너에서는 localhost가 Ozone 컨테이너가 아닐 수 있다. 컨테이너에서 접근할 때는 Compose 네트워크의 S3 Gateway 서비스명을 사용한다.

~~~properties
fs.s3a.endpoint=http://ozone-s3g:9878
fs.s3a.path.style.access=true
fs.s3a.connection.ssl.enabled=false
fs.s3a.access.key=ozone
fs.s3a.secret.key=ozone-secret
~~~

ozone-s3g는 서비스 이름의 예시다. 이 책의 최종 Compose 파일에서 실제 서비스명이 다르면 해당 이름으로 바꾼다.

### 11.2 Spark SQL로 테이블 생성하기

다음 SQL은 Iceberg 통합이 구성된 Spark SQL 세션을 전제로 하는 예시다.

~~~sql
CREATE DATABASE IF NOT EXISTS iceberg_db;

CREATE TABLE iceberg_db.customers (
  id BIGINT,
  name STRING,
  email STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
USING iceberg
LOCATION 's3a://iceberg-bucket/warehouse/customers'
TBLPROPERTIES ('format-version' = '2');

DESCRIBE EXTENDED iceberg_db.customers;
DESCRIBE HISTORY iceberg_db.customers;
~~~

이 명령이 성공하면 테이블 메타데이터와 관련 파일이 Ozone의 warehouse 경로에 생성될 수 있다. 단, 실제 LOCATION 지정 방식은 Catalog 설정에 따라 달라질 수 있다.

[검토 필요: 다음 장에서 Apache Polaris를 기본 Catalog로 사용할지, Hadoop Catalog 또는 다른 Apache Catalog 구성을 사용할지에 따라 CREATE TABLE 문과 LOCATION 설정을 하나로 고정해야 한다.]

### 11.3 테스트 데이터 작성

~~~sql
INSERT INTO iceberg_db.customers VALUES
  (1, 'Alice', 'alice@example.com', TIMESTAMP '2026-08-29 10:00:00', TIMESTAMP '2026-08-29 10:00:00'),
  (2, 'Bob', 'bob@example.com', TIMESTAMP '2026-08-29 10:01:00', TIMESTAMP '2026-08-29 10:01:00');

SELECT * FROM iceberg_db.customers;

DESCRIBE HISTORY iceberg_db.customers;
~~~

테이블에 데이터를 작성한 뒤에는 Ozone의 객체 목록만 확인하지 말고 Iceberg 엔진을 통해 테이블을 읽는다. 객체가 존재하는 것과 현재 Snapshot에서 해당 객체를 유효한 데이터 파일로 참조하는 것은 다른 문제다.

### 11.4 Ozone에서 파일 구조 확인

AWS CLI로 warehouse 경로를 확인하는 예시는 다음과 같다.

~~~bash
aws s3 ls s3://iceberg-bucket/warehouse/customers/ \
  --recursive \
  --endpoint-url http://localhost:9878
~~~

예상되는 파일 유형은 다음과 같다.

| 파일 유형 | 예시 확장자 | 의미 |
|---|---|---|
| Table Metadata | .json | 테이블 상태와 스키마 |
| Manifest List | .avro | 스냅샷이 참조하는 Manifest 목록 |
| Manifest File | .avro | 데이터·삭제 파일과 통계 |
| Data File | .parquet | 실제 행 데이터 |

실습에서는 파일 이름의 숫자나 UUID를 직접 해석하지 않는다. 엔진의 DESCRIBE HISTORY와 테이블 메타데이터를 기준으로 Snapshot의 변화를 확인한다.

## 12. 실습 장애 점검

| 증상 | 우선 확인할 항목 |
|---|---|
| Bucket을 찾을 수 없음 | endpoint, Bucket 이름, Ozone S3 Gateway |
| Spark에서 경로 연결 실패 | S3A endpoint, 서비스명, path-style 설정 |
| 테이블 생성은 되었지만 조회 실패 | Catalog 설정, warehouse 위치, 권한 |
| Parquet 파일은 있지만 테이블에 보이지 않음 | 현재 Metadata와 Snapshot의 참조 여부 |
| Snapshot이 늘지 않음 | 엔진 커밋, 체크포인트, 작업 성공 여부 |
| 파일이 지나치게 많음 | 체크포인트 간격, 파일 롤링, 작은 파일 정책 |
| Time Travel 실패 | Snapshot 보존 여부, 엔진 SQL 문법 |
| Compaction 명령 실패 | 엔진 버전과 Iceberg 유지보수 기능 |
| AccessDenied | Access Key, Secret Key, ACL, 보안 모드 |
| 테이블 상태가 서로 다름 | Catalog 공유 여부와 동시 커밋 충돌 |

점검 순서는 다음처럼 유지한다.

1. 클라이언트의 endpoint와 URI 확인
2. Ozone S3 Gateway 접근 확인
3. Bucket과 warehouse 경로 확인
4. Catalog가 가리키는 테이블 확인
5. Table Metadata와 Snapshot 이력 확인
6. Manifest와 데이터 파일 참조 확인
7. 엔진 버전과 Iceberg 커넥터 조합 확인

## 13. 이 장의 핵심 정리

Parquet는 개별 데이터 파일의 물리적 저장 형식이고, Iceberg는 여러 파일을 하나의 논리적 테이블로 관리하는 테이블 포맷이다. Ozone은 그 파일과 메타데이터를 저장하는 객체 저장소다.

Iceberg의 메타데이터 구조는 Table Metadata, Snapshot, Manifest List, Manifest File, Data File의 관계로 이해할 수 있다. 쿼리 엔진은 이 구조를 따라 현재 테이블 상태를 확인하고, 파티션과 컬럼 통계를 사용해 읽지 않아도 되는 파일을 제외한다.

Iceberg의 원자적 커밋은 모든 파일 쓰기가 하나의 물리적 작업으로 묶인다는 뜻이 아니다. 새 테이블 메타데이터를 가리키는 현재 참조가 새 상태로 전환되는 과정을 통해, 독자가 보는 커밋된 테이블 상태를 일관되게 전환한다는 의미로 이해해야 한다.

스키마와 파티션은 테이블 메타데이터를 통해 진화할 수 있다. 그러나 엔진과 Catalog에 따라 실제 DDL과 지원 범위가 다르므로, 사양 수준의 기능과 실행 엔진 수준의 기능을 구분해야 한다.

Flink는 실시간 CDC 적재, Spark는 백필과 유지보수, ClickHouse는 분석 Serving을 담당하도록 역할을 나눈다. 스트리밍 적재에서는 작은 파일과 orphan file이 생길 수 있으므로 체크포인트·파일 롤링·Compaction·Snapshot 보존 정책을 함께 설계해야 한다.

## 확인 문제

1. Parquet와 Iceberg의 책임 범위를 각각 설명하라.
2. Table Metadata, Snapshot, Manifest List, Manifest File의 연결 관계를 순서대로 적어 보라.
3. Iceberg Snapshot은 데이터 파일의 복사본인가? 아니라면 무엇을 표현하는가?
4. Manifest List와 Manifest File이 쿼리 프루닝에 어떻게 기여하는가?
5. Iceberg의 원자적 커밋을 “모든 객체 저장소 쓰기의 원자성”이라고 표현하면 안 되는 이유는 무엇인가?
6. 필드 ID 기반 스키마 관리가 컬럼 이름 변경에 미치는 장점은 무엇인가?
7. 파티션 진화에서 기존 파일과 신규 파일이 서로 다른 파티션 사양을 사용할 수 있는 이유는 무엇인가?
8. Flink CDC 스트리밍 적재에서 작은 파일이 증가하는 원인은 무엇인가?
9. Snapshot Expiration과 Orphan File Cleanup의 차이는 무엇인가?
10. 이 책에서 Flink·Spark·ClickHouse의 역할을 어떻게 분리했는가?

## 이 장에서 자료가 부족했거나 검증이 필요한 부분

- Apache Iceberg 1.11.0과 Table Format Spec v3의 최종 출간 기준 및 호환성
- 연구 자료 간 Apache Spark 버전 표기 불일치(4.1.2와 3.5.4)
- 최종 Compose에서 사용할 Iceberg Catalog의 종류와 설정
- Apache Polaris를 기본 Catalog로 채택할지 여부
- Flink 버전과 Iceberg Sink 커넥터의 정확한 조합
- Flink checkpoint와 Iceberg Snapshot commit의 exactly-once 재현 절차
- Spark SQL의 스키마·파티션 진화 명령을 실제 환경에서 실행한 결과
- 실제 롤백 SQL 또는 프로시저와 실패 복구 절차
- Compaction과 Manifest Rewrite의 최종 엔진별 명령
- Snapshot Expiration과 Orphan File Cleanup의 실제 인자와 실행 순서
- ClickHouse 26.6 계열의 Iceberg 읽기·쓰기·Time Travel·삭제 파일 지원 범위
- Ozone S3A endpoint와 Catalog 설정의 최종 Compose 서비스명
- 작은 파일의 목표 크기와 체크포인트·파일 롤링 기준

## 참고 자료

- [Apache Iceberg 공식 Table Specification](https://iceberg.apache.org/spec/)
- [Apache Iceberg Releases](https://iceberg.apache.org/releases/)
- [Apache Iceberg 1.11.0 Release](https://iceberg.apache.org/blog/apache-iceberg-1.11.0-release/)
- [Apache Iceberg Architecture: Metadata Tree, Snapshots, and Catalogs](https://iceberglakehouse.com/apache-iceberg-architecture/)
- [Iceberg Manifest List](https://iceberglakehouse.com/iceberg/iceberg-manifest-list/)
- [Iceberg Manifest File](https://iceberglakehouse.com/iceberg/iceberg-manifest-file/)
- [Apache Iceberg Avro Metadata Format](https://iceberglakehouse.com/iceberg/iceberg-avro-format/)
- [Iceberg vs Parquet: Table Format vs File Format](https://risingwave.com/blog/iceberg-vs-parquet-table-format-file-format/)
- [Apache Iceberg Metadata Explained](https://olake.io/blog/2025/10/03/iceberg-metadata/)
- [ClickHouse와 Apache Iceberg](https://clickhouse.com/resources/engineering/apache-iceberg)
- [Apache Flink Native S3 File System](https://flink.apache.org/2026/06/26/announcing-native-s3-fs/)

