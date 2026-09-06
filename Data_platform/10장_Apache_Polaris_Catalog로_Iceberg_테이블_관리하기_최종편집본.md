# 10장. Apache Polaris Catalog로 Iceberg 테이블 관리하기

## 이 장의 목표

9장에서는 Apache Iceberg가 Parquet 파일과 테이블 메타데이터를 이용해 논리적인 테이블 상태를 관리하는 방법을 살펴보았다. 그러나 Flink와 Spark 같은 여러 엔진이 같은 테이블을 사용하려면, 테이블의 이름과 현재 메타데이터 위치를 공통으로 찾을 수 있어야 한다.

이 장에서는 Apache Polaris를 Iceberg REST Catalog 구현체로 사용해 이 문제를 해결한다. Polaris가 Ozone에 저장된 데이터 파일을 직접 처리하는 엔진이 아니라, Catalog·Namespace·Table·권한·현재 메타데이터 위치를 관리하는 계층이라는 점을 먼저 구분한다.

그 다음 Docker Compose 기반 로컬 환경에서 Polaris와 전용 PostgreSQL 메타스토어를 구성하고, OAuth2 인증·Principal·Role·Catalog를 연결한다. Spark 연결 예시와 Flink·ClickHouse 연결의 검증 범위, Catalog 장애와 복구 시 고려할 사항도 함께 정리한다.

- 난이도: 초중급
- 선수 지식: Docker Compose, SQL, Apache Ozone, Apache Iceberg 메타데이터와 Snapshot
- 기준 후보 버전: Apache Polaris 1.7.0, Iceberg 1.11.0 계열
- 이 장의 기본 역할: Polaris는 Catalog, Ozone은 객체 저장소, Iceberg는 테이블 포맷, Spark와 Flink는 처리 엔진
- 다음 장과의 연결: Flink CDC 결과를 Iceberg Catalog에 등록된 테이블에 적재한다

> 이 장의 일부 설정과 명령은 조사 자료에 제시된 예시를 정리한 것이다. Polaris 1.7.0, Iceberg 1.11.0, Ozone 2.2.0, Spark·Flink의 최종 조합은 출간 전 통합 테스트로 확정해야 한다.

## 1. Catalog가 필요한 이유

9장에서 Iceberg는 Table Metadata, Snapshot, Manifest List, Manifest File, Data File을 연결해 테이블 상태를 표현한다고 설명했다. 하지만 처리 엔진이 테이블을 읽거나 쓰려면 먼저 다음 질문에 답할 수 있어야 한다.

- 이 테이블의 논리적 이름은 무엇인가?
- 테이블의 현재 metadata.json 위치는 어디인가?
- 어떤 저장소와 warehouse를 사용하고 있는가?
- 현재 요청을 보낸 주체가 이 테이블을 읽거나 쓸 권한이 있는가?
- 동시에 변경이 발생했을 때 커밋 충돌을 어떻게 처리하는가?

Catalog는 이 정보를 엔진이 공통 방식으로 조회하고 갱신하게 하는 계층이다. Apache Polaris는 Iceberg REST Catalog API를 제공하는 Apache 프로젝트로 조사되었으며, 2026년 2월 19일 ASF Incubator를 졸업한 것으로 자료에 정리되어 있다.

~~~mermaid
flowchart LR
    F["Flink"] --> R["Iceberg REST Catalog"]
    S["Spark"] --> R
    R --> P["Apache Polaris"]
    P --> O["Ozone S3"]
    O --> D["Parquet · Metadata"]
~~~

Polaris가 테이블의 실제 행 데이터를 직접 저장하는 것은 아니다. 데이터 파일과 Iceberg 메타데이터 파일은 Ozone 같은 객체 저장소에 남고, Polaris는 엔진이 그 위치와 테이블 상태를 찾을 수 있도록 Catalog 정보를 제공한다.

## 2. Ozone·Iceberg·Polaris의 책임 경계

세 기술은 서로 다른 계층의 문제를 해결한다.

| 구성 요소 | 책임 | 담당하지 않는 것 |
|---|---|---|
| Apache Ozone | Parquet·Avro·JSON과 같은 실제 객체 저장 | 테이블 이름과 Snapshot 정책의 주 관리 |
| Apache Iceberg | 테이블 포맷, Snapshot, Manifest, 스키마와 파티션 진화 | REST API 서버 자체 |
| Apache Polaris | Catalog·Namespace·Table 관리, 현재 메타데이터 위치, REST API, 인증·RBAC | 데이터 파일 처리와 Compaction 실행 |
| Apache Spark | 배치 읽기·쓰기, 백필, 유지보수 작업 후보 | Catalog 자체의 저장소 역할 |
| Apache Flink | 스트리밍 처리와 CDC 적재 | Ozone의 데이터 복제 관리 |
| ClickHouse | 분석 Serving 또는 Iceberg 조회 | Iceberg 원본 테이블의 주 관리 |

이 경계를 지키면 장애 원인도 분리해서 판단할 수 있다.

- Ozone 장애: 데이터 파일과 메타데이터 파일에 접근하지 못한다.
- Polaris API 장애: 테이블 이름, 권한, 현재 메타데이터 위치를 조회하거나 커밋하기 어렵다.
- Polaris 메타스토어 장애: Catalog·Role·Namespace·Table 등록 상태를 읽지 못할 수 있다.
- Iceberg 커밋 실패: 새 파일은 남았지만 현재 Snapshot에서 참조되지 않을 수 있다.
- Spark·Flink 장애: 엔진 작업이 중단되지만, 이미 커밋된 테이블 상태는 별도로 남는다.

Polaris는 Compaction, Snapshot Expiration, Orphan File Cleanup을 직접 수행하는 저장·처리 엔진이 아니다. 이런 유지보수 작업은 Spark, Flink, Airflow 또는 별도의 maintenance job이 실행한다.

## 3. Catalog·Namespace·Table 구조

### 3.1 계층 구조

Polaris는 Catalog 아래에 Namespace를 두고, Namespace 안에서 Iceberg Table을 관리한다.

~~~mermaid
flowchart TB
    C["Catalog<br/>lakehouse_catalog"] --> N1["Namespace<br/>raw"]
    C --> N2["Namespace<br/>clean"]
    C --> N3["Namespace<br/>mart"]
    N1 --> T["Table<br/>customers_cdc"]
~~~

이 책에서는 다음 이름 체계를 사용한다.

~~~text
<catalog>.<namespace>.<table>

예:
lakehouse_catalog.raw.customers_cdc
lakehouse_catalog.clean.orders
lakehouse_catalog.mart.daily_sales
~~~

Catalog는 저장소 설정과 접근 경계를 갖는 논리적 컨테이너로 사용한다. Namespace는 raw, clean, mart처럼 데이터 처리 단계나 도메인을 구분하는 단위로 사용한다.

| 개체 | 이 책의 예시 | 의미 |
|---|---|---|
| Catalog | lakehouse_catalog | Iceberg 저장소와 접근 경계 |
| Namespace | raw | 원본 CDC 데이터 영역 |
| Namespace | clean | 정제된 데이터 영역 |
| Namespace | mart | 분석용 데이터 영역 |
| Table | customers_cdc | 실제 Iceberg 테이블 |

### 3.2 관리 작업

Polaris가 관리하는 대표 작업은 다음과 같다.

| 대상 | 대표 작업 | 결과 |
|---|---|---|
| Catalog | 생성, 저장소 설정, 권한 부여 | 저장소와 보안 경계 생성 |
| Namespace | 생성, 목록, 속성 확인, 삭제 | 테이블 논리 그룹 관리 |
| Table | 생성, 조회, 목록, 이름 변경, 삭제 | 현재 메타데이터 위치 관리 |
| Principal | 생성, 조회, 삭제 | 사용자·서비스 정체성 관리 |
| Role | 생성, 권한 부여, Principal 연결 | 인증 주체의 작업 범위 관리 |

Polaris에는 Iceberg Table API 외에 Generic Table API도 확장되고 있다. 그러나 이 책의 중심은 Iceberg 기반 레이크하우스이므로, Generic Table은 확장 가능성으로만 언급하고 실습 범위에서는 Iceberg Table API에 집중한다.

## 4. REST Catalog의 동작

### 4.1 엔진과 Catalog의 호출 관계

Spark나 Flink 같은 엔진은 테이블 이름만으로 Ozone의 파일을 직접 계산하지 않는다. REST Catalog에 테이블 정보를 요청하고, 응답으로 받은 메타데이터 위치와 저장소 설정을 이용해 Iceberg 테이블을 읽고 쓴다.

일반적인 조회 흐름은 다음과 같다.

1. 엔진이 Catalog에 인증한다.
2. 엔진이 Catalog에 Namespace 또는 Table을 요청한다.
3. Polaris가 테이블의 현재 메타데이터 위치와 설정을 반환한다.
4. 엔진이 Ozone에서 Table Metadata를 읽는다.
5. 엔진이 Snapshot과 Manifest를 따라 데이터 파일을 찾는다.
6. 엔진이 Parquet 파일을 읽는다.

쓰기 흐름은 다음과 같다.

1. 엔진이 Ozone에 새로운 데이터 파일을 작성한다.
2. 새로운 Manifest와 Table Metadata를 작성한다.
3. 엔진이 Catalog에 현재 메타데이터 위치 변경을 요청한다.
4. Polaris가 권한과 현재 상태를 확인한다.
5. 커밋이 성공하면 새 Snapshot이 현재 상태가 된다.

~~~mermaid
sequenceDiagram
    participant E as Flink 또는 Spark
    participant P as Polaris
    participant O as Ozone
    E->>P: Table load
    P-->>E: Metadata 위치와 설정
    E->>O: Data·Manifest·Metadata 읽기 또는 쓰기
    E->>P: 새 Metadata 위치 커밋
    P-->>E: 성공 또는 충돌
~~~

### 4.2 설정 endpoint

조사 자료에서는 다음과 같은 설정 endpoint 확인 예시를 제시한다.

~~~bash
# TOKEN은 실습에서 발급받은 access token으로 대체한다.
curl -s \
  -H "Authorization: Bearer $TOKEN" \
  "http://polaris:8181/api/catalog/v1/config?warehouse=ozone_catalog"
~~~

이 endpoint 경로는 배포 버전과 설정에 따라 달라질 수 있다. 위 명령은 REST Catalog 설정이 실제로 응답하는지 확인하는 용도이며, 운영 환경의 고정된 API 계약으로 사용하기 전에 Polaris 1.7.0 공식 문서와 실제 배포 결과를 확인해야 한다.

[추가 자료 조사 필요: Polaris 1.7.0의 공식 REST endpoint 목록과 배포별 base URL 차이]

## 5. 인증과 RBAC

### 5.1 인증 모드

조사 자료는 Polaris의 인증 모드를 다음과 같이 구분한다.

| 모드 | 설명 | 권장 환경 |
|---|---|---|
| Internal OAuth2 | Polaris가 자체 토큰 발급 | 로컬 실습과 개발 환경 |
| External OIDC | 외부 Identity Provider의 토큰 검증 | 운영 환경 |
| Mixed | 내부·외부 인증을 함께 허용 | 단계적 전환 또는 혼합 환경 |

로컬 실습에서는 Internal OAuth2를 사용하면 외부 IdP를 별도로 설치하지 않고도 Principal과 Role의 동작을 확인할 수 있다. 운영 환경에서는 외부 OIDC, 짧은 수명의 토큰, Secret 관리 정책을 함께 검토해야 한다.

### 5.2 Principal과 Role

Polaris의 보안 모델은 사람이나 애플리케이션을 바로 테이블 권한에 연결하는 대신, 정체성과 역할을 분리한다.

- Principal: 사용자 또는 애플리케이션의 인증 주체
- Principal Role: 하나 이상의 Principal을 묶는 역할
- Catalog Role: Catalog, Namespace, Table 등의 보안 대상에 대한 권한 묶음
- Privilege: 실제 허용 작업
- Securable: 권한을 적용할 Catalog, Namespace, Table 등의 대상

~~~mermaid
flowchart TB
    P["Principal<br/>flink-service"] --> PR["Principal Role<br/>streaming-writers"]
    PR --> CR["Catalog Role<br/>raw-writer"]
    CR --> G["Privilege"]
    G --> T["raw Namespace · Table"]
~~~

워크로드별 역할을 분리하면 Flink가 분석용 mart 테이블을 임의로 변경하거나, ClickHouse가 원본 테이블에 쓰는 것을 제한할 수 있다.

| 워크로드 | Principal 예시 | Catalog Role 예시 | 권한 방향 |
|---|---|---|---|
| Flink CDC | flink-cdc | raw-writer | raw Namespace 쓰기 |
| Spark 백필 | spark-batch | clean-writer | clean·mart 읽기·쓰기 |
| ClickHouse | clickhouse-reader | analytics-reader | 분석 테이블 읽기 |
| Airflow | airflow-orchestrator | maintenance-operator | 유지보수 실행에 필요한 권한 |
| Superset | superset-reader | serving-reader | 직접 접근 시 읽기 전용 |

위 이름은 권한 설계 예시다. 실제 privilege 이름과 REST API 요청 형식은 채택한 Polaris 릴리스 문서에서 다시 확인해야 한다.

### 5.3 OAuth2 토큰 발급 예시

조사 자료의 내부 OAuth2 예시는 다음과 같다.

~~~bash
# CLIENT_ID와 CLIENT_SECRET은 실습용 Principal의 자격 증명으로 대체한다.
curl -X POST "http://polaris:8181/api/catalog/v1/oauth/tokens" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "client_id=CLIENT_ID" \
  -d "client_secret=CLIENT_SECRET" \
  -d "scope=PRINCIPAL_ROLE:ALL"
~~~

반환된 access token은 REST Catalog 요청의 Authorization 헤더에 사용한다.

~~~bash
curl -s \
  -H "Authorization: Bearer TOKEN" \
  "http://polaris:8181/api/catalog/v1/config?warehouse=ozone_catalog"
~~~

토큰을 셸 명령, Compose 파일, Git 저장소에 직접 기록하지 않는다. 로컬 실습에서도 .env 파일은 버전 관리 대상에서 제외하고, 실제 운영 환경에서는 Secret 관리 도구를 사용한다.

[추가 자료 조사 필요: Polaris 1.7.0 bootstrap credential과 Principal 생성 절차, 토큰 scope의 공식 명칭, 만료 시간 설정]

## 6. Ozone Storage Backend 연결

### 6.1 저장소 설정

Polaris의 Catalog는 Iceberg 파일을 저장할 기본 위치와 허용된 위치를 가질 수 있다. 이 책에서는 Ozone S3 Gateway를 저장소 backend로 사용한다.

개념적인 저장소 설정은 다음과 같다.

~~~text
Catalog: lakehouse_catalog
Storage type: S3
Default base location: s3://iceberg-bucket/warehouse/
Allowed location: s3://iceberg-bucket/warehouse/
S3 endpoint: http://ozone-s3g:9878
Path-style access: true
SSL: false
~~~

여기서 s3://iceberg-bucket/warehouse/는 논리적인 warehouse 경로다. 실제 데이터 파일과 Table Metadata는 Ozone에 저장되고, Polaris는 Catalog 요청을 통해 엔진이 그 위치에 접근할 수 있도록 한다.

Allowed location은 중요한 보안 경계다. Catalog가 허용하지 않은 경로에 임의로 파일을 쓰지 못하도록, 실습과 운영 모두에서 저장 위치를 좁게 설정해야 한다.

### 6.2 호스트와 컨테이너의 endpoint

8장과 9장에서 설명한 것처럼 호스트에서 실행하는 AWS CLI와 Docker 컨테이너 안의 Flink·Spark가 사용하는 endpoint는 다를 수 있다.

| 실행 위치 | endpoint 예시 |
|---|---|
| Windows 11 또는 Mac 호스트 | http://localhost:9878 |
| Flink 컨테이너 | http://ozone-s3g:9878 |
| Spark 컨테이너 | http://ozone-s3g:9878 |
| Polaris 컨테이너가 Ozone에 접근 | Compose 네트워크에서 해석되는 S3 Gateway 서비스명 |

컨테이너 안의 localhost는 일반적으로 해당 컨테이너 자신을 가리킨다. 그러므로 Flink가 localhost:9878로 Ozone에 접근한다고 작성하면, Ozone이 아니라 Flink 컨테이너 내부의 9878 포트를 조회할 수 있다.

### 6.3 자격 증명 위임

Polaris는 Catalog 요청을 처리하면서 엔진에 저장소 접근 자격 증명을 전달하는 credential vending을 지원할 수 있다. 이 구조의 목적은 Spark·Flink 설정에 장기 Ozone/S3 Access Key를 직접 넣지 않고, 허용된 위치와 제한된 수명의 자격 증명을 전달하는 것이다.

다만 다음 조건을 함께 확인해야 한다.

- Polaris 버전의 credential vending 지원
- Ozone S3 Gateway의 자격 증명 모델
- 저장 위치 제한과 allowed location 검사
- Flink·Spark Iceberg REST client의 지원
- 토큰과 vended credential의 만료 처리
- 로그에 Secret이 노출되지 않는지

로컬 실습에서는 고정된 개발용 Access Key를 .env 또는 Docker Compose Secret으로 제공할 수 있다. credential vending의 Ozone 통합은 운영 확장 항목으로 분리한다.

[검토 필요: Polaris 1.7.0과 Ozone 2.2.0 조합에서 credential vending을 실제로 재현했는지 확인되지 않음]

## 7. PostgreSQL 메타스토어와 영속성

### 7.1 Polaris 메타스토어의 역할

Polaris의 메타스토어는 Polaris가 관리하는 Catalog, Principal, Role, Namespace, Table 등록 정보와 관련 상태를 저장한다. Iceberg의 Parquet 데이터 파일과 Table Metadata 파일은 Ozone 객체 저장소에 저장된다.

두 저장 영역을 분리해서 기억한다.

| 저장 대상 | 위치 |
|---|---|
| Polaris Catalog·Role·Principal·Namespace 상태 | Polaris metastore |
| Iceberg Table Metadata JSON | Ozone |
| Iceberg Manifest List·Manifest File | Ozone |
| Parquet 데이터 파일 | Ozone |

로컬 컨테이너를 재시작해도 상태가 유지되어야 한다면, Polaris 메타스토어와 PostgreSQL의 영속 볼륨을 보존해야 한다.

### 7.2 전용 PostgreSQL 권장

이 책의 소스 데이터베이스 목록에는 PostgreSQL이 포함되어 있다. 그렇더라도 Polaris 메타스토어를 소스 PostgreSQL 데이터베이스와 무조건 같은 데이터베이스에 넣는 방식은 권장하지 않는다.

가장 명확한 로컬 구성은 다음과 같다.

~~~mermaid
flowchart LR
    S["소스 PostgreSQL"] --> C["CDC"]
    P["Polaris"] --> M["Polaris 전용 PostgreSQL"]
    P --> O["Ozone S3"]
~~~

소스 데이터베이스와 Polaris 메타스토어를 분리하면 다음 점검이 쉬워진다.

- CDC 장애와 Catalog 장애를 분리
- 실습 초기화 대상 분리
- 계정과 권한 분리
- 메타스토어 백업과 복구 범위 분리
- 소스 데이터 손상 위험 감소

리소스가 부족해 하나의 PostgreSQL 컨테이너를 공유해야 한다면, 최소한 데이터베이스·스키마·사용자·권한을 분리한다.

[추가 자료 조사 필요: Polaris 1.7.0 공식 이미지의 JDBC 환경 변수, 스키마 초기화 명령, PostgreSQL 최소 버전, migration 절차]

### 7.3 Docker Compose 구성 방향

조사 자료는 다음과 같은 구성을 권장 방향으로 제시한다.

~~~text
Docker Compose
├─ polaris
│   ├─ REST Catalog API
│   ├─ OAuth2와 RBAC
│   └─ JDBC metastore 연결
├─ polaris-postgres
│   └─ Catalog·Principal·Role 상태 영속화
├─ ozone-s3g
│   └─ Iceberg 파일 저장
└─ polaris-setup
    └─ Catalog·Role·Namespace 초기화
~~~

이 구조는 역할을 설명하기 위한 구성도다. 실제 공식 이미지의 환경 변수와 실행 명령이 확정되지 않은 상태에서 그대로 복사해 실행할 수 있는 완성 Compose 파일로 취급하지 않는다.

[추가 자료 조사 필요: Polaris 1.7.0과 Ozone 2.2.0을 함께 기동하는 공식 또는 검증 완료 Compose 파일]

## 8. Polaris 초기화와 setup

### 8.1 초기화 순서

로컬 실습의 초기화 순서는 다음처럼 구성한다.

1. Ozone S3 Gateway를 기동한다.
2. Ozone에 warehouse Bucket을 생성한다.
3. Polaris 전용 PostgreSQL을 기동한다.
4. Polaris API를 기동한다.
5. bootstrap credential을 확인한다.
6. Principal과 Principal Role을 만든다.
7. Catalog Role과 privilege를 구성한다.
8. Ozone을 storage backend로 하는 Catalog를 만든다.
9. raw, clean, mart Namespace를 만든다.
10. Spark 또는 검증된 엔진으로 Iceberg Table을 생성한다.
11. Polaris API와 Ozone 객체 목록을 각각 확인한다.

초기화 순서를 지키는 이유는 각 계층이 의존성을 갖기 때문이다. Ozone이 먼저 준비되지 않으면 Catalog에 저장 위치를 등록할 수 없고, Polaris 메타스토어가 준비되지 않으면 Principal과 Catalog 상태를 저장할 수 없다.

### 8.2 setup 명령과 선언형 구성

조사 자료에는 Polaris Python CLI의 setup 명령이 YAML로 Catalog, Storage, Role, Namespace를 선언해 적용하는 방식으로 제시되어 있다.

다음은 개념적인 선언 예시다.

~~~yaml
catalogs:
  - name: lakehouse_catalog
    storage_type: s3
    default_base_location: s3://iceberg-bucket/warehouse/
    allowed_locations:
      - s3://iceberg-bucket/warehouse/

roles:
  lakehouse_admin:
    privileges:
      catalog:
        - CATALOG_MANAGE_CONTENT

namespaces:
  - raw
  - clean
  - mart
~~~

이 방식의 장점은 초기 설정을 문서화하고 반복 실행할 수 있다는 점이다. 그러나 YAML의 실제 속성명과 privilege 이름은 Polaris 릴리스와 CLI 구현에 따라 달라질 수 있다.

위 예시는 설계 방향을 설명하기 위한 것으로, 다음 명령을 그대로 실행하는 확정 예제로 사용하지 않는다.

~~~bash
# 실제 Polaris CLI 설치와 옵션은 공식 문서 확인 필요
polaris setup --config polaris.yaml
~~~

[추가 자료 조사 필요: Polaris 1.7.0 Python CLI의 설치 방법, setup 명령의 정확한 옵션, 반복 실행 시 멱등성 보장]

## 9. Spark에서 Polaris 연결하기

### 9.1 REST Catalog 설정

Spark는 Iceberg SparkCatalog에 Catalog type을 rest로 지정해 Polaris 같은 REST Catalog에 연결할 수 있다. 조사 자료의 설정 형태는 다음과 같다.

~~~properties
spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lakehouse.type=rest
spark.sql.catalog.lakehouse.uri=http://polaris:8181/api/catalog
spark.sql.catalog.lakehouse.warehouse=lakehouse_catalog
spark.sql.catalog.lakehouse.credential=CLIENT_ID:CLIENT_SECRET
spark.sql.catalog.lakehouse.scope=PRINCIPAL_ROLE:ALL
~~~

이 설정에서 각 항목의 의미는 다음과 같다.

| 속성 | 의미 |
|---|---|
| spark.sql.catalog.lakehouse | Spark에서 사용할 논리 Catalog 이름 |
| SparkCatalog | Iceberg Spark Catalog 구현 |
| type=rest | REST Catalog 사용 |
| uri | Polaris REST Catalog endpoint |
| warehouse | Polaris Catalog 또는 저장소 설정과 연결되는 이름 |
| credential | 실습용 client credential 예시 |
| scope | Principal Role 범위 예시 |

uri에 사용하는 경로와 warehouse 속성의 의미는 Polaris 버전과 배포 설정에 따라 달라질 수 있다. 자료에는 /api/catalog와 /api/v1 형태가 모두 나타나므로, 책의 최종 Compose 환경에서는 하나의 값을 실제 호출로 고정해야 한다.

[검토 필요: Polaris 1.7.0·Iceberg 1.11.0·Spark 버전 조합에서 uri, warehouse, credential, scope 속성의 실제 동작]

### 9.2 Spark SQL 확인

Catalog 연결이 성공하면 다음과 같은 확인을 수행할 수 있다.

~~~sql
-- Catalog의 Namespace 목록 확인
SHOW NAMESPACES IN lakehouse;

-- raw Namespace 생성
CREATE NAMESPACE IF NOT EXISTS lakehouse.raw;

-- Namespace의 Table 목록 확인
SHOW TABLES IN lakehouse.raw;

-- Iceberg Table 생성
CREATE TABLE lakehouse.raw.customers_cdc (
  id BIGINT,
  name STRING,
  email STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');

-- Table 구조 확인
DESCRIBE TABLE EXTENDED lakehouse.raw.customers_cdc;
~~~

이 SQL의 성공 여부를 판단할 때는 Spark 출력만 보지 않는다. Polaris API에서 Namespace와 Table을 조회하고, Ozone에서 metadata와 data 파일이 생성되는지도 확인해야 한다.

### 9.3 Ozone 파일 확인

~~~bash
# 호스트에서 Ozone S3 Gateway 확인
aws s3 ls s3://iceberg-bucket/warehouse/ \
  --recursive \
  --endpoint-url http://localhost:9878
~~~

예상되는 파일 유형은 다음과 같다.

| 파일 유형 | 확장자 예시 | 확인 의미 |
|---|---|---|
| Table Metadata | .json | 테이블 상태와 현재 스키마 |
| Manifest List | .avro | Snapshot이 참조하는 Manifest 목록 |
| Manifest File | .avro | 데이터·삭제 파일과 통계 |
| Data File | .parquet | 실제 행 데이터 |

파일이 생성되었다고 곧바로 Table 커밋이 성공했다고 판단하지 않는다. 현재 Snapshot과 Manifest에서 해당 파일이 참조되는지 엔진의 Table history와 함께 확인해야 한다.

## 10. Flink 연결은 조건부로 검증한다

Flink가 Iceberg REST Catalog 규격을 지원한다면 Polaris를 사용할 수 있는 구조는 성립한다. 그러나 조사 자료에서는 Flink 2.2.1, Iceberg 1.11.0, Polaris 1.7.0을 한 조합으로 명시한 공식 실행 예제가 확보되지 않았다.

따라서 이 책에서 Flink 연결은 다음처럼 표현한다.

- REST Catalog 개념: 확정
- Polaris를 REST Catalog 구현체로 사용하는 구조: 확정
- Flink와 Polaris의 일반적인 연결 가능성: 조건부 확정
- Flink 2.2.1·Iceberg 1.11.0·Polaris 1.7.0의 실제 Write: 통합 검증 필요
- Ozone S3 Gateway까지 포함한 실제 CDC Write: 통합 검증 필요

### 10.1 Flink 설정에서 확인할 항목

Flink와 Polaris를 연결할 때는 다음을 함께 준비해야 한다.

- Iceberg Flink runtime JAR
- REST Catalog URI
- OAuth2 client credential 또는 token
- Ozone S3A 또는 S3 FileIO 설정
- path-style access
- Iceberg table format version
- checkpoint와 Snapshot commit 설정
- Docker 네트워크에서 해석되는 Polaris·Ozone 서비스명

[추가 자료 조사 필요: 최종 Flink 이미지에 포함할 Iceberg runtime JAR, REST Catalog 설정 키, OAuth2와 S3 credential 전달 방식]

### 10.2 CDC 적재의 역할

이 책의 흐름에서 Flink CDC는 소스 데이터베이스의 변경을 수집하고, Flink는 그 이벤트를 Iceberg Raw 테이블에 적재한다.

~~~mermaid
flowchart LR
    D["MySQL · PostgreSQL · MongoDB"] --> C["Flink CDC"]
    C --> K["Kafka"]
    K --> F["Flink Sink"]
    F --> P["Polaris Catalog"]
    F --> O["Ozone Iceberg Warehouse"]
~~~

Flink가 Table을 생성하거나 커밋할 때 Polaris가 필요하고, Parquet 데이터와 Metadata를 실제로 저장할 때 Ozone이 필요하다. 이 두 계층 중 하나라도 준비되지 않으면 CDC 파이프라인은 정상적으로 완료되지 않을 수 있다.

9장에서 설명한 것처럼 Flink 체크포인트와 Iceberg Snapshot 커밋이 올바르게 연결되었을 때 exactly-once 결과를 목표로 할 수 있다. 이를 Flink와 Polaris의 단순 연결만으로 보장한다고 쓰면 안 된다.

## 11. ClickHouse와 Polaris의 연결 범위

ClickHouse는 Iceberg 테이블을 직접 읽거나, 별도의 Serving 테이블로 데이터를 제공할 수 있다. 하지만 ClickHouse를 Iceberg 원본 테이블의 Catalog 관리자나 유지보수 엔진으로 두지 않는 것이 이 책의 역할 분리 원칙이다.

| 목적 | 권장 주체 |
|---|---|
| Iceberg 테이블 생성과 메타데이터 커밋 | Spark 또는 Flink |
| Raw CDC 적재 | Flink |
| 백필과 대량 변환 | Spark |
| Compaction과 Snapshot Expiration | Spark·Flink·Airflow가 실행 |
| Catalog와 권한 | Polaris |
| 객체와 데이터 파일 저장 | Ozone |
| 분석 Serving | ClickHouse |

조사 자료에는 ClickHouse의 Iceberg 기능이 버전별로 확장되고 있지만, ClickHouse 26.6과 Polaris 1.7.0의 직접 조합을 확인하는 공식 설정은 확보되지 않은 것으로 정리되어 있다.

따라서 이 장에서는 다음처럼 제한한다.

- ClickHouse의 Iceberg 직접 조회: 13장에서 별도 검증
- ClickHouse와 Polaris REST Catalog 직접 연결: 조건부
- ClickHouse를 Iceberg 원본 대체 저장소로 사용: 채택하지 않음
- ClickHouse Mart Serving: 권장

[추가 자료 조사 필요: ClickHouse 26.6 계열에서 Polaris REST Catalog를 통한 Iceberg 조회와 인증 설정]

## 12. Catalog 장애와 복구

### 12.1 Polaris API 중단

Polaris API가 중단되어도 Ozone에 이미 저장된 Parquet, Manifest, Table Metadata 파일이 자동으로 삭제되는 것은 아니다. 그러나 엔진이 Catalog를 통해 현재 metadata location을 조회해야 한다면 새 테이블 조회, 생성, 쓰기, 커밋이 실패할 수 있다.

“Catalog가 중단되어도 기존 테이블은 항상 읽을 수 있다”라고 단정하지 않는다. 엔진의 metadata cache, 이전 위치 보유 여부, Catalog 의존성에 따라 결과가 달라질 수 있다.

### 12.2 메타스토어 손실

Polaris 메타스토어가 손상되면 Catalog, Namespace, Table, Principal, Role 등록 정보가 손실될 수 있다. Ozone의 객체 파일이 남아 있더라도, 논리적 테이블 이름과 현재 메타데이터 포인터를 복원하지 못하면 엔진이 정상적으로 테이블을 찾기 어렵다.

객체 파일이 남아 있다는 사실과 Catalog가 정상적으로 복구되었다는 사실은 다르다.

### 12.3 상태별 영향

| 장애 상황 | Ozone 객체 | 기존 테이블 조회 | 새 쓰기·커밋 | 우선 조치 |
|---|---|---|---|---|
| Polaris API 중단 | 보존 | Catalog 의존성에 따라 실패 가능 | 실패 가능 | Polaris 재시작 |
| Polaris 메타스토어 중단 | 보존 | 현재 포인터 조회 실패 가능 | 실패 | PostgreSQL과 볼륨 확인 |
| 메타스토어 볼륨 삭제 | 보존 | 등록 정보 손실 가능 | 실패 | bootstrap과 Catalog 재생성 |
| 커밋 전 엔진 실패 | 일부 파일 잔존 가능 | 이전 Snapshot 유지 | 새 커밋 실패 | orphan file 검토 |
| Ozone 데이터 볼륨 삭제 | 파일 손실 | 실패 | 실패 | 실습 데이터 재생성 |
| Polaris만 재구성 | 파일은 남을 수 있음 | Table 재등록 필요 가능 | 설정에 따라 실패 | register 절차 검증 |

Polaris만 재구성하고 Ozone 파일을 재사용할 수 있는지는 Catalog와 Iceberg 버전, Table register 기능, 경로 정책에 따라 달라진다.

[추가 자료 조사 필요: Polaris 1.7.0 메타스토어 백업·복구 및 기존 Ozone Table 재등록 절차]

## 13. Branch·Tag와 Credential Vending의 범위

### 13.1 Snapshot reference

Iceberg의 branch와 tag는 Snapshot reference 기능이다. Polaris가 REST Catalog로 Snapshot reference와 연동될 수 있더라도, Polaris가 branch와 tag를 독립적인 Git 저장소처럼 관리한다고 단정하면 안 된다.

실제 지원 여부는 다음 조합을 기준으로 확인해야 한다.

- Iceberg Table Format Spec
- Iceberg Java client
- Spark 또는 Flink Catalog 구현
- Polaris REST Catalog API
- 선택한 엔진의 SQL·프로시저

이 장에서는 branch·tag를 기본 실습 범위에 포함하지 않고, 9장의 Snapshot 개념과 후속 심화 검증 항목으로 둔다.

### 13.2 Credential Vending

Credential vending은 Catalog가 엔진에 저장소 접근 자격 증명을 전달하는 기능이다. 장기 Access Key를 엔진 설정에 보관하지 않을 수 있다는 장점이 있지만, 다음 조건을 충족해야 안전하다.

- 허용된 저장 위치가 명확할 것
- 자격 증명의 수명과 범위가 제한될 것
- 엔진이 전달된 자격 증명을 실제로 사용할 것
- 로그와 오류 메시지에서 Secret이 제거될 것
- Ozone S3 Gateway와 권한 모델이 호환될 것

로컬 학습에서는 고정된 개발용 키를 사용해 전체 흐름을 단순화하고, 운영 보안 설계에서 credential vending을 별도로 검증한다.

## 14. Docker Compose 단계별 실습

### 14.1 사전 확인

먼저 8장의 Ozone S3 Gateway와 Polaris 전용 PostgreSQL이 실행될 수 있는지 확인한다.

~~~bash
docker version
docker compose config
docker compose ps
~~~

Polaris 이미지는 조사 자료상 1.7.0을 우선 후보로 둔다.

~~~bash
# 이미지 태그와 플랫폼은 배포 전에 공식 이미지에서 재확인한다.
docker pull apache/polaris:1.7.0
docker image inspect apache/polaris:1.7.0
~~~

[검토 필요: apache/polaris:1.7.0 공식 이미지의 실제 배포 태그와 AMD64·ARM64 manifest]

### 14.2 Ozone warehouse 확인

~~~bash
# 호스트에서 실행하는 AWS CLI 기준
aws s3 ls \
  --endpoint-url http://localhost:9878

# warehouse 경로 확인
aws s3 ls s3://iceberg-bucket/warehouse/ \
  --recursive \
  --endpoint-url http://localhost:9878
~~~

Ozone이 실행 중이어도 warehouse Bucket이 없으면 Polaris Catalog의 저장 위치가 유효하지 않을 수 있다.

### 14.3 Polaris와 메타스토어 기동

실제 Compose 서비스명과 환경 변수는 최종 검증본에 맞춰 사용한다.

~~~bash
# 예시 프로파일
docker compose --profile lakehouse up -d polaris-postgres
docker compose --profile lakehouse up -d polaris

# 상태 확인
docker compose ps
docker compose logs --tail=100 polaris-postgres
docker compose logs --tail=100 polaris
~~~

이 명령은 서비스명이 polaris-postgres와 polaris인 구성을 가정한다. 현재 자료에는 구성 방향은 있으나 모든 환경 변수와 공식 Compose 파일이 확정되어 있지 않다.

[추가 자료 조사 필요: Polaris 1.7.0의 실제 Compose 기동 명령과 필수 환경 변수]

### 14.4 Token과 Catalog endpoint 확인

~~~bash
# 실습용 토큰 발급 예시
curl -X POST "http://localhost:8181/api/catalog/v1/oauth/tokens" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "client_id=CLIENT_ID" \
  -d "client_secret=CLIENT_SECRET" \
  -d "scope=PRINCIPAL_ROLE:ALL"

# 설정 endpoint 확인 예시
curl -s \
  -H "Authorization: Bearer TOKEN" \
  "http://localhost:8181/api/catalog/v1/config?warehouse=lakehouse_catalog"
~~~

컨테이너 내부에서 Polaris에 접근할 때는 localhost 대신 polaris 서비스명을 사용한다.

### 14.5 Namespace 확인

Spark 연결이 준비되었다면 다음과 같이 Namespace를 확인한다.

~~~sql
SHOW NAMESPACES IN lakehouse;

CREATE NAMESPACE IF NOT EXISTS lakehouse.raw;
CREATE NAMESPACE IF NOT EXISTS lakehouse.clean;
CREATE NAMESPACE IF NOT EXISTS lakehouse.mart;

SHOW NAMESPACES IN lakehouse;
~~~

### 14.6 Table 생성과 파일 확인

~~~sql
CREATE TABLE lakehouse.raw.customers_cdc (
  id BIGINT,
  name STRING,
  email STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');

DESCRIBE TABLE EXTENDED lakehouse.raw.customers_cdc;
~~~

그 다음 Ozone에서 파일을 확인한다.

~~~bash
aws s3 ls s3://iceberg-bucket/warehouse/ \
  --recursive \
  --endpoint-url http://localhost:9878
~~~

검증 결과를 다음 표에 기록한다.

| 확인 항목 | 기대 결과 |
|---|---|
| Polaris API | 인증된 요청에 응답 |
| Catalog | lakehouse Catalog 조회 가능 |
| Namespace | raw, clean, mart 조회 가능 |
| Table | customers_cdc 조회 가능 |
| Ozone metadata | .json 파일 생성 |
| Ozone manifest | .avro 파일 생성 |
| Ozone data | 데이터 적재 후 .parquet 파일 생성 |
| Table history | CREATE 또는 WRITE Snapshot 확인 |

## 15. 로컬 실습에서 자주 발생하는 오류

| 증상 | 우선 점검할 항목 |
|---|---|
| Polaris 컨테이너가 종료됨 | 로그, JVM 메모리, 환경 변수, PostgreSQL 연결 |
| Catalog endpoint 연결 실패 | 포트, 서비스명, API base path |
| 401 Unauthorized | token, client ID, client secret, scope |
| 403 Forbidden | Principal Role, Catalog Role, privilege |
| Catalog를 찾지 못함 | Spark catalog 이름과 Polaris Catalog 이름 |
| Namespace 생성 실패 | Catalog 권한과 REST URI |
| Table 생성 후 파일 없음 | Ozone endpoint, warehouse, S3 credential |
| Ozone 연결 실패 | 컨테이너 내부 DNS와 포트 |
| Table load 실패 | metadata location, Catalog 상태, Ozone 파일 |
| 파일은 있지만 Table이 보이지 않음 | 현재 Snapshot과 Manifest 참조 |
| 재시작 후 Catalog가 사라짐 | PostgreSQL 데이터 볼륨과 metastore 영속성 |
| Commit 충돌 | 동시 쓰기, 현재 Metadata, 재시도 설정 |

점검은 다음 순서로 진행한다.

1. Docker Compose 서비스 상태
2. Polaris API endpoint
3. 인증 token
4. Polaris Catalog와 Namespace
5. Ozone S3 Gateway
6. warehouse와 저장소 자격 증명
7. Iceberg Table Metadata와 Snapshot
8. Spark·Flink·Iceberg 버전 조합

## 16. 버전과 출간 기준

조사 자료의 2026년 8월 기준 버전 후보는 다음과 같다.

| 구성 요소 | 기준 후보 | 상태 |
|---|---:|---|
| Apache Polaris | 1.7.0 | 출간 후보, 이미지와 통합 테스트 필요 |
| Apache Iceberg | 1.11.0 | 출간 후보 |
| Iceberg Table Format | v3 | 라이브러리 버전과 별도 관리 |
| Apache Ozone | 2.2.0 | 8장 기준 |
| Apache Flink | 2.2.1 | Polaris·Iceberg 조합 검증 필요 |
| Apache Spark | 4.1.2 또는 3.5.4 | 자료 간 불일치, 최종 선택 필요 |
| ClickHouse | 26.6 계열 | Polaris 직접 연결 검증 필요 |
| PostgreSQL | Polaris metastore와 소스 DB 역할 분리 | JDBC 설정 확인 필요 |

특히 Spark 버전은 조사 자료에 4.1.2와 3.5.4가 함께 제시되어 있다. 이 두 버전은 같은 실습 기준으로 동시에 사용할 수 없으므로, 전체 Compose와 Iceberg 런타임의 호환성을 기준으로 하나를 선택해야 한다.

[검토 필요: 책 전체 버전 매트릭스에서 Spark 기준 버전 최종 확정]

Polaris 1.4.1은 조사 자료에서 보안 이슈가 보고된 이전 실습 후보로 분류되어 있다. 따라서 출간본의 기본 버전으로 사용하지 않고, 최신 보안 수정 릴리스와 공식 이미지 정보를 확인한 뒤 버전을 고정한다.

## 17. 이 장의 핵심 정리

Catalog는 Iceberg 테이블의 논리적 이름과 현재 메타데이터 위치를 엔진이 공통 방식으로 찾게 하는 계층이다. Apache Polaris는 Iceberg REST Catalog API를 제공하며, Catalog·Namespace·Table·Principal·Role과 같은 관리 대상을 다룬다.

Ozone은 실제 Parquet·Avro·JSON 파일을 저장하고, Iceberg는 테이블 상태·Snapshot·Manifest 관계를 정의한다. Polaris는 이 상태를 엔진이 찾고 커밋할 수 있도록 Catalog와 접근 정책을 제공한다. Polaris가 데이터를 직접 저장하거나 Compaction을 직접 실행한다고 이해하면 안 된다.

보안 모델은 Principal, Principal Role, Catalog Role, Privilege의 연결로 구성된다. 로컬 실습에서는 Internal OAuth2와 bootstrap credential로 시작할 수 있지만, 운영 환경에서는 External OIDC, 짧은 수명의 토큰, 최소 권한 역할과 자격 증명 보호가 필요하다.

Polaris 메타스토어의 영속성은 Ozone 파일의 영속성과 별개다. Ozone의 파일이 남아 있어도 Catalog·Namespace·Table·Role 정보가 사라지면 일반 엔진이 테이블을 정상적으로 찾거나 커밋하기 어렵다. 따라서 Polaris 전용 PostgreSQL과 named volume을 보존하고, 복구 절차를 따로 기록해야 한다.

이 책의 기본 역할 분리는 다음과 같다.

- Ozone: 객체 저장
- Iceberg: 테이블 포맷과 Snapshot
- Polaris: Catalog와 권한
- Flink: CDC 실시간 적재
- Spark: 백필과 유지보수
- ClickHouse: 분석 Serving

## 확인 문제

1. Iceberg만 사용하고 Catalog를 별도로 두지 않을 때 여러 엔진에서 발생할 수 있는 관리 문제는 무엇인가?
2. Ozone, Iceberg, Polaris의 책임을 각각 설명하라.
3. Table Metadata 위치를 Catalog가 관리한다는 말은 무엇을 의미하는가?
4. Principal과 Catalog Role을 분리하는 이유는 무엇인가?
5. Polaris가 중단되었을 때 Ozone의 Parquet 파일이 자동으로 삭제되지 않는 이유는 무엇인가?
6. Polaris 메타스토어를 소스 PostgreSQL과 분리하는 운영상의 이유는 무엇인가?
7. 호스트의 localhost와 Flink 컨테이너의 ozone-s3g가 다른 이유는 무엇인가?
8. Credential vending은 어떤 문제를 줄이기 위해 사용하는가?
9. Spark와 Flink의 Polaris 연결을 확정하기 전에 확인해야 할 버전과 설정은 무엇인가?
10. ClickHouse를 Iceberg 원본 테이블의 주 관리자로 사용하지 않는 이유는 무엇인가?

## 이 장에서 자료가 부족했거나 검증이 필요한 부분

- Apache Polaris 1.7.0 공식 Docker 이미지와 실제 배포 태그
- Polaris 1.7.0의 AMD64·ARM64 multi-architecture manifest
- Polaris 1.7.0의 공식 Compose 예제와 필수 환경 변수
- Polaris JDBC metastore의 PostgreSQL 최소 버전과 migration 절차
- bootstrap credential과 Principal 생성의 정확한 공식 명령
- OAuth2 token endpoint, scope, 만료 시간의 정확한 API
- Polaris 1.7.0의 Catalog·Namespace·Table 관리 API 전체 목록
- Catalog Role과 privilege의 공식 명칭 및 상속 규칙
- Ozone S3 Gateway를 storage backend로 연결하는 최종 설정
- allowed location 검증과 credential vending의 Ozone 통합 결과
- Spark REST Catalog의 uri·warehouse·credential·scope 설정 조합
- Flink 2.2.1·Iceberg 1.11.0·Polaris 1.7.0 실제 read/write
- Flink CDC에서 Iceberg Snapshot commit과 checkpoint의 재현 결과
- ClickHouse 26.6 계열의 Polaris REST Catalog 직접 연결
- Iceberg branch·tag와 Polaris의 실제 API·SQL 지원
- Spark 4.1.2와 3.5.4 중 최종 책 기준 버전
- Polaris 메타스토어 손실 후 Ozone Table 재등록·복구 절차

## 참고 자료

- [Apache Polaris 공식 문서](https://polaris.apache.org/docs/)
- [Apache Polaris Getting Started](https://polaris.apache.org/releases/latest/getting-started/)
- [Apache Polaris ASF Top-Level Project 승격 안내](https://polaris.apache.org/blog/2026/02/19/apache-polaris-graduates-to-top-level-project/)
- [Apache Polaris의 Policy-Driven Table Maintenance 설명](https://polaris.apache.org/blog/2026/02/04/floe-and-apache-polaris-policy-driven-table-maintenance-for-apache-iceberg/)
- [Apache Polaris setup 명령 소개](https://polaris.apache.org/blog/2026/03/29/introducing-the-setup-command-in-apache-polaris/)
- [Apache Iceberg Table Specification](https://iceberg.apache.org/spec/)
- [Apache Ozone 공식 문서](https://ozone.apache.org/docs/)
- [Apache Ozone S3 Gateway](https://ozone.apache.org/docs/core-concepts/architecture/s3-gateway/)
- [Apache Ozone S3A 클라이언트 인터페이스](https://ozone.apache.org/docs/next/user-guide/client-interfaces/s3a/)

