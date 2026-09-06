# 15장. Apache Superset으로 데이터 시각화하기

앞 장에서 ClickHouse Mart를 만들었다. 이제 그 결과를 사람이 읽고 비교하고 의사결정에 사용할 수 있는 화면으로 제공해야 한다. Apache Superset은 데이터베이스와 SQL 쿼리 결과를 Dataset으로 등록하고, 차트와 대시보드로 구성하는 오픈 소스 BI 플랫폼이다.

Superset의 역할은 데이터를 저장하거나 변환하는 것이 아니다. Superset은 사용자의 요청을 SQL로 바꾸어 연결된 쿼리 엔진에 전달하고, 결과를 차트로 표현한다. 따라서 시각화 계층을 설계할 때는 Superset 설정만 보지 말고 ClickHouse Mart의 테이블 설계, Trino 또는 Dremio의 Iceberg 연결, SQL 권한, 쿼리 비용을 함께 고려해야 한다.

이 장에서는 ClickHouse Mart를 기본 시각화 대상 데이터로 사용한다. Iceberg 테이블은 Superset이 직접 읽는 것이 아니라 Trino 또는 Dremio 같은 SQL 엔진을 경유해 조회한다. 마지막에는 Dataset 접근 권한, Row Level Security, 쿼리 캐시, 로컬 Docker Compose 환경을 구성한다.

## 이 장의 기준과 버전 주의사항

첨부된 최신 조사본의 권장 기준은 Apache Superset 6.1.0이다. 사용자가 처음 제시한 조사본에는 Superset 4.1.2가 기준으로 기재되어 있으므로, 원고 전체의 기준 버전을 6.1.0으로 통일할지 먼저 확정해야 한다. 아래 본문은 최신 조사본을 우선해 Superset 6.1.0을 기준으로 작성한다.

| 구성 요소 | 이 장의 기준 | 역할 |
|---|---:|---|
| Apache Superset | 6.1.0 | Dataset·차트·대시보드·SQL Lab |
| ClickHouse | 26.6~26.7 | Serving Mart 조회 |
| clickhouse-connect | 0.9.x 예시 | ClickHouse SQLAlchemy 연결 |
| Apache Iceberg | 1.11.0 | 레이크하우스 테이블 |
| Trino | 477 이상 예시 | Iceberg SQL 조회 엔진 |
| Dremio | 24.x 예시 | Iceberg SQL·Arrow Flight SQL 경유 |
| PostgreSQL | 17.11 예시 | Superset Metadata Database |
| Redis | 7.4 예시 | 쿼리 결과 캐시·비동기 처리 보조 |

[검토 필요: 기존 보고서의 Superset 4.1.2와 최신 조사본의 Superset 6.1.0 중 출판 기준 버전을 확정하고, 그 버전에 맞는 UI 메뉴·기능 플래그·SQLAlchemy URI·Docker 이미지 태그를 최종 검증해야 합니다.]

조사본은 Superset 4.1.2 미만에서 ClickHouse 관련 SQL injection 취약점 CVE-2026-23969가 보고되었으므로 4.1.2 이상을 사용해야 한다고 기록한다. 그러나 최신 조사본의 기준은 6.1.0이다. 이 보안 관련 문장은 출판 전에 NVD와 Superset 공식 보안 공지를 다시 확인한 뒤 확정한다 (출처: [NVD CVE-2026-23969](https://nvd.nist.gov/vuln/detail/CVE-2026-23969)).

## 학습 목표

이 장을 마치면 다음 작업을 수행할 수 있다.

- Superset, ClickHouse, Trino, Dremio, Iceberg의 역할을 구분할 수 있다.
- ClickHouse Mart를 Superset 데이터베이스와 Dataset으로 등록할 수 있다.
- Trino 또는 Dremio를 경유해 Iceberg 테이블을 조회할 수 있다.
- 물리 Dataset과 가상 Dataset의 차이를 설명하고 SQL Lab 결과를 재사용할 수 있다.
- 차트와 대시보드, 네이티브 필터, 크로스 필터를 구성할 수 있다.
- 역할·데이터베이스·Dataset·행 수준 조건을 분리해 접근 제어를 설계할 수 있다.
- Docker Compose로 Superset과 PostgreSQL, Redis를 실행하고 연결 상태를 점검할 수 있다.

## 1. Superset은 무엇을 담당하는가

### 1.1 시각화 계층의 위치

Superset은 데이터 플랫폼의 마지막 사용 계층이다. 사용자가 대시보드에서 기간이나 지역을 선택하면 Superset은 해당 조건을 SQL에 반영해 연결된 데이터베이스에 질의한다. Superset 자체에 데이터가 복사되는 것이 아니라, 일반적으로 연결된 엔진이 쿼리를 실행하고 Superset이 결과를 화면에 표시한다.

| 구성 요소 | 주된 책임 |
|---|---|
| Flink CDC | 원천 데이터베이스의 변경 이벤트 수집 |
| Iceberg·Polaris·Ozone | 레이크하우스 테이블과 메타데이터·파일 관리 |
| Spark | 변환·백필·Iceberg 유지보수 |
| ClickHouse | 대시보드에 적합한 Serving Mart와 집계 쿼리 |
| Trino·Dremio | Iceberg를 SQL로 조회하는 쿼리 엔진 |
| Apache Superset | Dataset·차트·대시보드·권한·SQL 사용 화면 |

Superset에서 쿼리가 성공했다는 뜻은 연결된 데이터베이스가 요청을 처리하고 결과를 반환했다는 뜻이다. 원천 데이터가 최신인지, Mart의 중복이 없는지, Iceberg Snapshot이 올바른지는 앞 장의 파이프라인과 검증 Task가 책임진다.

### 1.2 데이터 흐름

ClickHouse Mart를 사용하는 일반적인 흐름은 다음과 같다.

~~~mermaid
flowchart TD
    A["Flink CDC"] --> B["Iceberg"]
    B --> C["Spark transform"]
    C --> D["ClickHouse Mart"]
    D --> E["Superset"]
~~~

Iceberg를 직접 조회하는 분석 화면은 Trino 또는 Dremio를 중간에 둔다.

~~~mermaid
flowchart TD
    A["Iceberg table"] --> B["Trino or Dremio"]
    B --> C["Superset Dataset"]
    C --> D["Chart and Dashboard"]
~~~

여기서 직접 조회라는 표현은 Superset이 Iceberg 파일을 직접 읽는다는 의미가 아니다. Superset이 Iceberg 전용 저장소 API를 호출하는 것이 아니라, Iceberg 커넥터가 구성된 SQL 엔진에 SQL을 전송한다는 의미로 사용한다.

### 1.3 Superset과 ClickHouse의 경계

Superset에서 모든 계산을 수행하려고 하면 대시보드가 복잡해지고 쿼리 비용을 예측하기 어려워진다. 반복적으로 사용하는 지표는 ClickHouse Mart에서 미리 집계하고, Superset에서는 기간·차원·필터를 조합해 조회하는 구조가 관리하기 쉽다.

| 질문 | 판단 계층 |
|---|---|
| 원천 변경을 어떻게 수집하는가? | Flink CDC |
| 정제와 백필을 어떻게 수행하는가? | Spark |
| 대시보드용 집계를 어디에 둘 것인가? | ClickHouse Mart |
| 사용자가 어떤 지표를 보는가? | Superset Dataset·Chart |
| 누가 어떤 행을 볼 수 있는가? | Superset 권한·RLS와 데이터베이스 권한 |

## 2. Superset 로컬 구성과 Metadata Database

### 2.1 Metadata Database의 역할

Superset은 사용자가 만든 Dashboard, Chart, Dataset, Database 연결, 사용자·역할·권한 같은 애플리케이션 메타데이터를 저장해야 한다. 이 저장소를 Superset Metadata Database라고 한다. 이 장의 로컬 예제에서는 PostgreSQL을 사용한다.

Metadata Database는 ClickHouse Mart나 Iceberg 데이터 자체를 저장하는 장소가 아니다. Superset이 재시작되어도 대시보드 정의와 연결 정보가 유지되도록 별도의 PostgreSQL 볼륨을 둔다.

| 저장소 | 저장 내용 |
|---|---|
| Superset PostgreSQL | Superset 사용자·역할·Dataset·Chart·Dashboard·설정 메타데이터 |
| ClickHouse | Mart 데이터 |
| Iceberg Catalog | 테이블 메타데이터와 Snapshot 참조 |
| Ozone 또는 객체 저장소 | Iceberg 데이터·Manifest·파일 |
| Redis | 결과 캐시와 비동기 처리 보조 데이터 |

Superset의 Metadata Database는 Airflow의 Metadata Database와도 별개의 논리 구성 요소다. 같은 PostgreSQL 서버를 사용할 수는 있지만, 초보자 실습에서는 데이터베이스와 사용자 계정을 분리해 책임을 명확히 한다.

### 2.2 Docker Compose 예시

다음 예시는 Superset, PostgreSQL, Redis의 최소 관계를 보여 준다. 비밀번호와 Secret Key는 예시 문자열을 그대로 사용하지 않고 .env에서 주입한다.

~~~yaml
services:
  postgres:
    image: postgres:17.11
    environment:
      POSTGRES_USER: superset
      POSTGRES_PASSWORD: change-this-password
      POSTGRES_DB: superset
    volumes:
      - superset_postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U superset -d superset"]
      interval: 10s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7.4
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5

  superset:
    image: apache/superset:6.1.0
    environment:
      SUPERSET_SECRET_KEY: change-this-secret-key
      SUPERSET_LOAD_EXAMPLES: "no"
      DATABASE_URL: postgresql+psycopg2://superset:change-this-password@postgres:5432/superset
      REDIS_HOST: redis
      REDIS_PORT: 6379
      CELERY_BROKER_URL: redis://redis:6379/0
      CELERY_RESULT_BACKEND: redis://redis:6379/0
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    ports:
      - "8088:8088"
    volumes:
      - superset_home:/app/superset_home

volumes:
  superset_postgres_data:
  redis_data:
  superset_home:
~~~

위 Compose 예시는 조사 자료에 포함된 로컬 학습용 구성이다. 운영 환경에서는 Secret Key, 비밀번호, TLS, 네트워크 노출, 백업, 장애 조치와 사용자 인증을 별도로 설계한다.

[검토 필요: Superset 6.1.0 공식 이미지가 위 환경 변수와 서비스 구성을 그대로 지원하는지, Metadata Database 초기화와 관리자 계정 생성에 필요한 공식 명령이 무엇인지 최종 이미지로 검증해야 합니다.]

### 2.3 Windows 11과 Apple Silicon Mac

Windows 11에서는 Docker Desktop의 WSL 2 기반 엔진, 파일 공유, 프로젝트 디렉터리 권한을 확인한다. Apple Silicon Mac에서는 ARM64 이미지가 제공되는지 확인하고, 이미지가 amd64 전용이면 에뮬레이션으로 인한 성능 저하나 호환성 문제를 고려한다.

두 운영체제 모두 컨테이너 간 통신에는 Compose 서비스 이름을 사용한다. Superset 컨테이너에서 ClickHouse를 연결할 때 ClickHouse가 Compose 서비스 이름 clickhouse라면 호스트 주소는 localhost가 아니라 clickhouse다. 호스트 운영체제에서 실행 중인 서비스에 접근할 때만 host.docker.internal 사용을 검토한다.

## 3. ClickHouse 데이터베이스 연결

### 3.1 ClickHouse 연결 구조

Superset은 ClickHouse를 SQL 데이터베이스로 등록하고 SQLAlchemy URI를 통해 연결한다. 조사본은 ClickHouse 연결에 clickhouse-connect 드라이버를 사용한다고 정리한다 (출처: [Superset ClickHouse Database](https://superset.apache.org/user-docs/databases/supported/clickhouse/), [ClickHouse and Superset](https://clickhouse.com/docs/integrations/connectors/data-visualization/superset-and-clickhouse)).

로컬 ClickHouse HTTP 포트는 일반적으로 8123을 사용하고, TLS를 적용한 환경은 8443 등을 사용한다. 실제 포트 매핑은 앞 장의 Compose 파일과 일치시킨다.

### 3.2 SQLAlchemy URI

조사본에서 제시한 기본 URI 형식은 다음과 같다.

~~~text
clickhousedb://default:password@clickhouse:8123/mart
~~~

clickhousedb+connect 형식으로 드라이버를 명시하는 예시도 사용할 수 있다.

~~~text
clickhousedb+connect://default:password@clickhouse:8123/mart
~~~

비밀번호에 URI 예약 문자가 들어가면 URL 인코딩이 필요하다. 연결 문자열을 문서에 기록할 때 실제 비밀번호를 노출하지 않는다.

TLS 연결을 사용하는 예시는 다음과 같다.

~~~text
clickhousedb+connect://user:password@clickhouse:8443/mart?secure=true
~~~

[검토 필요: Superset 6.1.0과 clickhouse-connect 0.9.x 조합에서 기본 dialect와 clickhousedb+connect URI가 모두 동작하는지, TLS Query Parameter의 정확한 이름과 연결 포트를 실제 이미지에서 확인해야 합니다.]

### 3.3 UI에서 ClickHouse 연결

1. Superset에 로그인한다.
2. Data → Databases로 이동한다.
3. + Database를 선택한다.
4. ClickHouse Connect 또는 버전에 맞는 ClickHouse 연결 유형을 선택한다.
5. Display Name에 ClickHouse Mart를 입력한다.
6. SQLAlchemy URI에 연결 문자열을 입력한다.
7. Test Connection으로 연결을 확인한다.
8. Connect를 눌러 저장한다.

연결이 성공해도 Dataset을 자동으로 생성한 것은 아니다. 다음 절에서 mart.daily_orders 같은 테이블을 Dataset으로 등록한다.

### 3.4 ClickHouse 연결 옵션

조사본은 Advanced 설정의 connect_args 예시로 다음 항목을 제시한다.

~~~json
{
  "connect_args": {
    "secure": false,
    "verify": false,
    "connect_timeout": 30,
    "send_receive_timeout": 300,
    "compression": "zstd",
    "query_limit": 100000
  }
}
~~~

이 설정은 로컬 실습의 예시다. verify: false는 TLS 인증서 검증을 끄는 설정이므로 운영 환경에서 그대로 사용하지 않는다. 쿼리 결과 제한은 대시보드가 실수로 대량의 상세 행을 반환하는 것을 줄이는 데 도움이 되지만, 업무 요구에 맞는 값으로 조정해야 한다.

| 옵션 | 예시 값 | 의미 |
|---|---:|---|
| secure | false | TLS 사용 여부 |
| verify | false | TLS 인증서 검증 여부 |
| connect_timeout | 30 | 연결 대기 시간 |
| send_receive_timeout | 300 | 요청·응답 대기 시간 |
| compression | zstd | 통신 압축 방식 |
| query_limit | 100000 | 결과 행 수 제한 예시 |

## 4. Trino·Dremio 경유 Iceberg 조회

### 4.1 Superset은 Iceberg 네이티브 커넥터가 아니다

Superset은 Iceberg 파일과 Snapshot을 직접 해석하는 저장소 엔진이 아니다. Iceberg 커넥터와 Polaris Catalog 설정을 가진 Trino나 Dremio에 SQL을 전달하고, 그 결과를 Dataset과 차트에 사용한다.

이 구조는 역할을 분리한다.

| 계층 | 책임 |
|---|---|
| Superset | SQL 작성·차트·대시보드·권한 |
| Trino 또는 Dremio | SQL 계획·실행·Iceberg 커넥터 |
| Polaris | Iceberg Catalog |
| Ozone 또는 객체 저장소 | 데이터·메타데이터 파일 |

### 4.2 Trino 연결

조사본은 Trino Iceberg Catalog를 다음과 같은 형태로 Superset에 등록하는 예시를 제시한다.

~~~text
Database name: Trino Iceberg
SQLAlchemy URI: trino://user:password@trino-coordinator:8080/iceberg_catalog
Allow DML: No
~~~

Compose 네트워크 안에서는 trino-coordinator가 Trino 서비스 이름이어야 한다. Trino에서 Catalog 이름과 Schema 이름이 어떻게 구성되었는지 먼저 확인한 뒤 URI를 작성한다.

~~~sql
SELECT
    order_date,
    customer_id,
    total_amount,
    status
FROM iceberg_catalog.raw.orders
WHERE order_date >= DATE '2026-08-01';
~~~

대시보드 전용 Superset 연결에서는 Allow DML을 허용하지 않는 것을 기본으로 한다. Superset 사용자가 SQL Lab에서 INSERT, UPDATE, DELETE를 실행해야 하는 특별한 이유가 없다면 조회 전용 연결을 만든다.

[검토 필요: Superset 6.1.0에서 Trino SQLAlchemy dialect의 정확한 URI, 인증 방식, Catalog·Schema 표기 방법, DML 제한 설정을 사용하는 Provider 버전으로 확인해야 합니다.]

### 4.3 Dremio 연결

Dremio를 사용하는 경우 조사본은 Arrow Flight SQL 연결 URI의 예시를 제시한다.

~~~text
dremio+flight://data.dremio.cloud:443/?Token=token-value&UseEncryption=true
~~~

토큰을 URI에 직접 기록하면 로그나 Metadata Database에 노출될 수 있다. 실습에서도 토큰은 환경 변수 또는 Superset의 보안 저장 기능을 사용하고, 실제 토큰을 Markdown 원고나 Compose 파일에 넣지 않는다.

Dremio 연결은 Trino 연결과 동일한 목적을 가지지만 URI와 인증 방식이 다르다. 두 엔진의 SQL dialect, Schema 탐색, 시간 함수와 Iceberg 기능 지원이 같다고 가정하지 않는다.

[검토 필요: Superset 6.1.0에서 Dremio Arrow Flight dialect의 공식 지원 상태, dremio+flight URI 형식, 토큰 전달 위치, TLS 옵션 이름을 공식 문서와 실제 Provider에서 확인해야 합니다.]

### 4.4 Iceberg 조회 방식을 선택하는 기준

| 선택 | 장점 | 주의점 |
|---|---|---|
| ClickHouse Mart | 대시보드용 응답 예측과 집계에 유리 | Mart 갱신 파이프라인 필요 |
| Trino 경유 Iceberg | Iceberg 테이블을 SQL로 직접 분석 | 쿼리 비용과 동시성 관리 필요 |
| Dremio 경유 Iceberg | Dremio의 SQL·Arrow Flight 환경 활용 | 연결 Provider와 인증 검증 필요 |

초보자 실습에서는 먼저 ClickHouse Mart 연결을 성공시킨 뒤 Trino 또는 Dremio 경유 Iceberg Dataset을 추가한다. 두 경로를 동시에 구성하면 연결 오류가 Dataset 오류인지, SQL 엔진 오류인지 구분하기 어렵다.

## 5. Dataset 생성과 스키마

### 5.1 물리 Dataset

물리 Dataset은 연결된 데이터베이스의 실제 테이블을 Superset Dataset으로 등록한 것이다. ClickHouse의 mart.daily_orders가 대표적인 예다.

1. Data → Datasets로 이동한다.
2. + Dataset을 선택한다.
3. Database에서 ClickHouse Mart를 선택한다.
4. Schema 또는 ClickHouse Database에서 mart를 선택한다.
5. Table에서 daily_orders를 선택한다.
6. Add를 눌러 Dataset을 생성한다.

등록 후 Dataset 설정에서 컬럼, 시간 컬럼, Metrics, 필터·그룹 대상 컬럼을 확인한다. 데이터베이스에서 컬럼을 변경했다면 Superset Dataset의 스키마 정보를 새로 읽어야 할 수 있다.

### 5.2 가상 Dataset

가상 Dataset은 SQL Lab에서 작성한 SQL을 재사용 가능한 Dataset으로 저장한 것이다. 물리 테이블을 직접 노출하지 않고, 대시보드에서 필요한 컬럼과 필터만 제공하고 싶을 때 유용하다.

~~~sql
SELECT
    order_date,
    customer_id,
    order_count,
    total_revenue
FROM mart.daily_orders
WHERE order_date >= today() - INTERVAL 30 DAY;
~~~

SQL Lab에서 쿼리를 실행한 뒤 결과 화면의 Explore 또는 Save dataset 기능을 사용해 가상 Dataset으로 저장한다. 메뉴 이름은 Superset 버전에 따라 달라질 수 있으므로 UI에서 현재 표시되는 이름을 확인한다.

가상 Dataset은 원본 데이터를 복사하는 테이블이 아니다. 저장되는 것은 SQL 정의와 Dataset 메타데이터이며, 차트 실행 시 SQL이 연결된 데이터베이스에서 실행된다.

### 5.3 물리 Dataset과 가상 Dataset 비교

| 구분 | 물리 Dataset | 가상 Dataset |
|---|---|---|
| 기반 | 실제 테이블·뷰 | SQL 쿼리 |
| 데이터 복사 | 하지 않음 | 하지 않음 |
| 재사용 | 테이블 구조 재사용 | SQL 로직 재사용 |
| 장점 | 단순하고 탐색하기 쉬움 | 필요한 컬럼·조건을 캡슐화 |
| 주의점 | 원본 스키마 변경 영향 | SQL dialect·쿼리 비용 관리 |

가상 Dataset은 보안 경계가 아니다. 사용자가 SQL Lab에서 원본 테이블을 직접 조회할 권한을 가지고 있다면 가상 Dataset으로 감싼 SQL을 우회할 수 있다. 접근 제어는 Dataset 권한, 데이터베이스 권한, RLS를 함께 설계한다.

### 5.4 Dataset 스키마와 Metrics

Dataset 설정에서 다음 항목을 확인한다.

| 항목 | 의미 |
|---|---|
| Column | Dataset이 반환하는 컬럼 |
| Time Column | 시간 범위 필터와 시계열 차트에 사용할 컬럼 |
| Metric | COUNT, SUM, AVG 등 재사용 가능한 집계 |
| Filterable Column | 필터에서 사용할 컬럼 |
| Groupable Column | 차원으로 그룹화할 컬럼 |

예를 들어 mart.daily_orders에 order_count와 total_revenue가 이미 일자·고객 단위로 집계되어 있다면, Superset에서는 이 값을 다시 합산하는 지표를 정의할 수 있다. 단, 같은 데이터를 중복 집계하지 않도록 Mart의 grain을 먼저 문서화한다.

## 6. 차트 만들기

### 6.1 Explore와 차트 생성

Superset의 Explore는 코드를 직접 작성하지 않고 Dataset을 차트로 구성하는 화면이다.

1. Data → Datasets에서 Dataset을 선택한다.
2. Create Chart를 누른다.
3. 차트 유형을 선택한다.
4. 시간 컬럼과 차원을 지정한다.
5. Metric을 선택한다.
6. 필터와 정렬을 설정한다.
7. Run 또는 Update chart로 결과를 확인한다.
8. Save를 눌러 차트 이름과 저장할 Dashboard를 지정한다.

차트 정의에는 Dataset, 차트 유형, 차원, Metrics, 필터, 정렬, 시간 범위가 함께 저장된다. 차트가 느리면 시각화 설정만 보지 말고 생성된 SQL을 확인한다.

### 6.2 차트 유형 선택

| 차트 유형 | 적합한 질문 | ClickHouse Mart와의 조합 |
|---|---|---|
| Big Number | 핵심 KPI가 얼마인가? | 단일 집계에 적합 |
| Big Number with Trendline | KPI가 시간에 따라 어떻게 변했는가? | 시계열 집계에 적합 |
| Bar Chart | 범주별 차이가 무엇인가? | GROUP BY 집계에 적합 |
| Stacked Bar Chart | 범주별 구성비가 어떻게 되는가? | 차원·합계 조합 |
| Line Chart | 시간 흐름이 어떠한가? | 시간 컬럼과 집계 |
| Pie 또는 Donut | 소수 범주의 비율은 어떠한가? | 범주 수를 제한해야 함 |
| Heatmap | 두 차원의 밀도나 분포는 어떠한가? | 행·열 차원 집계 |
| Pivot Table | 여러 차원을 교차해 비교할 수 있는가? | 다차원 집계 |
| Table | 상세 행을 확인해야 하는가? | LIMIT와 정렬 필수 |

조사본은 Superset이 30개 이상의 차트 유형을 제공한다고 정리하지만, 정확한 개수와 차트 목록은 버전과 플러그인에 따라 달라질 수 있다. 차트 개수 자체보다 질문에 맞는 차트와 반환 행 수를 선택하는 것이 중요하다.

### 6.3 ClickHouse Mart 예제

mart.daily_orders가 일자·고객 단위로 집계되어 있다고 가정한다.

~~~sql
SELECT
    order_date,
    sum(order_count) AS orders,
    sum(total_revenue) AS revenue
FROM mart.daily_orders
WHERE order_date >= today() - INTERVAL 30 DAY
GROUP BY order_date
ORDER BY order_date;
~~~

이 결과는 Line Chart의 시간 축과 두 개의 Metrics를 구성하는 기초가 된다. 지역이나 상품 차원을 비교하려면 해당 차원이 Mart에 포함되어 있는지 먼저 확인한다. Superset 차트에서 없는 차원을 만들어낼 수는 없다.

## 7. Dashboard·필터·크로스 필터

### 7.1 Dashboard 구성

Dashboard는 여러 Chart를 하나의 화면에 배치하고 공통 필터와 설명을 제공하는 단위다.

1. 저장한 Chart를 연다.
2. Save에서 새 Dashboard를 선택하거나 기존 Dashboard를 선택한다.
3. Dashboard로 이동한다.
4. Edit mode에서 Chart를 배치한다.
5. 제목·설명·레이아웃을 정리한다.
6. 필터를 추가하고 적용 대상 Chart를 지정한다.
7. 저장 후 일반 사용자 권한으로 화면을 확인한다.

대시보드에는 무엇을 보여 주는가뿐 아니라 어떤 기준일과 집계 grain인가도 표시한다. 같은 revenue라는 이름이라도 주문일 기준인지 결제일 기준인지 명시하지 않으면 차트의 숫자를 잘못 해석할 수 있다.

### 7.2 네이티브 필터

네이티브 필터는 Dashboard 수준에서 여러 Chart에 공통 조건을 적용한다. 조사본은 시간 범위, 값, 숫자 범위, 계층 필터를 주요 유형으로 제시한다.

| 필터 | 예시 |
|---|---|
| Time Range | 최근 30일 |
| Value | 국가가 KR |
| Numerical Range | 매출이 100 이상 1000 이하 |
| 계층 필터 | Country 선택 후 City 후보 제한 |

필터를 추가할 때 Dataset, Column, 기본값, 적용할 Chart 범위를 지정한다. 서로 다른 Dataset의 컬럼 이름과 의미가 다르면 하나의 필터가 모든 Chart에 동일하게 적용되지 않을 수 있다.

### 7.3 크로스 필터

크로스 필터는 한 Chart에서 막대나 영역을 선택했을 때 선택 조건을 Dashboard의 다른 Chart에 전달하는 방식이다. 예를 들어 지역별 Bar Chart에서 KR을 선택하면 같은 Dashboard의 Line Chart와 Table에 국가 조건이 적용될 수 있다.

조사본은 DASHBOARD_CROSS_FILTERS 기능 플래그와 Chart 수준의 Allow cross-filtering 설정을 제시한다.

~~~python
FEATURE_FLAGS = {
    "DASHBOARD_CROSS_FILTERS": True,
}
~~~

기능 플래그의 이름과 기본 활성화 여부는 Superset 버전에 따라 달라질 수 있다. 설정을 변경한 뒤에는 Superset 서비스 재시작과 실제 Chart 조작으로 동작을 확인한다.

크로스 필터 지원 여부도 차트 유형마다 다르다.

| 차트 | 조사본 기준 |
|---|---|
| Bar Chart | 지원 |
| Pie Chart | 지원 |
| Line Chart | 제한 조건에서 지원 |
| Heatmap | 지원 |
| Choropleth Map | 지원 |
| Big Number | 지원하지 않음으로 조사됨 |
| Pivot Table | 제한적 |

[검토 필요: Superset 6.1.0에서 크로스 필터의 기능 플래그, 차트별 지원 범위, Chart Configuration 메뉴 이름을 실제 UI와 공식 문서로 확인해야 합니다.]

### 7.4 Drill-down

Drill-down은 월별 집계를 클릭해 일별 집계로 내려가는 것처럼 상세 수준으로 이동하는 사용 경험이다. 조사본은 ENABLE_DRILL_BY 기능 플래그를 예시로 제시한다.

~~~python
FEATURE_FLAGS = {
    "ENABLE_DRILL_BY": True,
}
~~~

이 설정은 버전별로 이름이나 지원 형태가 바뀔 수 있다. 원고에서는 특정 차트 클릭으로 하위 수준을 조회할 수 있다는 개념과, 실제 기능 플래그·메뉴 설정을 분리해서 설명한다.

## 8. SQL Lab과 Virtual Dataset

### 8.1 SQL Lab의 역할

SQL Lab은 데이터베이스를 선택하고 SQL을 작성·실행·저장하는 인터페이스다. Explore가 Dataset 중심의 시각화 도구라면 SQL Lab은 SQL을 먼저 검증하는 작업 공간이다.

일반적인 흐름은 다음과 같다.

1. SQL Lab을 연다.
2. Database와 Schema를 선택한다.
3. SQL을 작성한다.
4. Run으로 실행한다.
5. 결과와 실행 시간을 확인한다.
6. Save Query로 쿼리를 저장하거나 Explore로 이동한다.
7. 필요한 경우 Virtual Dataset으로 저장한다.

### 8.2 ClickHouse Mart SQL

~~~sql
SELECT
    toDate(order_date) AS event_date,
    sum(order_count) AS total_orders,
    sum(total_revenue) AS total_revenue
FROM mart.daily_orders
WHERE order_date >= today() - INTERVAL 30 DAY
GROUP BY event_date
ORDER BY event_date;
~~~

SQL Lab에서 쿼리를 실행할 때는 먼저 제한된 기간으로 테스트한다. SELECT *로 대량 데이터를 다운로드하지 않고 필요한 컬럼과 기간을 명시한다. 실행 결과를 차트로 만들기 전, 시간 컬럼의 타입과 집계 수준을 확인한다.

### 8.3 Virtual Dataset 저장

Virtual Dataset을 저장하면 동일한 SQL을 여러 Chart에서 재사용할 수 있다. 그러나 다음 사항은 계속 원본 데이터베이스에서 관리해야 한다.

- 원본 테이블의 컬럼 변경
- SQL dialect의 함수 호환성
- 쿼리 실행 비용
- 원본 Dataset과 Virtual Dataset의 권한
- 데이터 최신성

가상 Dataset이 편리하다는 이유로 모든 업무 SQL을 한 곳에 쌓으면 의존성이 숨겨질 수 있다. 이름과 설명에 대상 테이블, 집계 grain, 기준일, 갱신 책임자를 기록한다.

### 8.4 SQL Lab 권한

조사본은 sql_lab 역할이 SQL Lab 접근 권한과 관련된다고 정리한다. Admin, Alpha, Gamma의 기본 권한은 Superset 버전과 역할 설정에 따라 달라질 수 있으므로, 역할 이름만 믿지 말고 실제 Permission과 데이터베이스 접근 권한을 확인한다 (출처: [Superset Security](https://superset.apache.org/admin-docs/security/)).

| 역할 | 일반적인 성격 | 반드시 확인할 것 |
|---|---|---|
| Admin | 관리와 전체 설정 | 모든 데이터베이스 접근이 기본인지 |
| Alpha | Dataset·차트 관리 범위가 넓음 | 데이터베이스별 접근 권한 |
| Gamma | 제한된 조회 중심 | 허용된 Dataset과 Dashboard |
| sql_lab 관련 권한 | SQL Lab 기능 | 쿼리 실행·저장·다운로드 권한 |

## 9. 사용자·역할·권한·RLS

### 9.1 권한을 네 층으로 나누기

Superset 접근 제어는 한 가지 설정으로 끝나지 않는다. 다음 네 층을 분리해 설계한다.

1. Superset에 로그인할 수 있는가?
2. 어떤 데이터베이스와 Dataset을 볼 수 있는가?
3. 어떤 Chart와 Dashboard를 볼 수 있는가?
4. 같은 Dataset 안에서 어떤 행을 볼 수 있는가?

마지막 항목이 Row Level Security다. RLS는 Dataset에 SQL 조건을 적용해 사용자나 역할에 따라 조회 행을 제한한다.

### 9.2 역할과 데이터베이스 접근

Superset의 인증과 권한 관리는 Flask AppBuilder 기반으로 동작한다는 내용이 조사본에 포함되어 있다. Admin, Alpha, Gamma 같은 기본 역할은 편의상 사용할 수 있지만, 조직의 업무 권한을 기본 역할에 그대로 의존하지 않고 별도의 역할을 설계하는 편이 명확하다.

예를 들어 다음 역할을 구분할 수 있다.

| 역할 | 접근 대상 |
|---|---|
| platform_admin | 모든 Superset 설정과 연결 |
| analytics_engineer | Dataset·Chart 생성과 수정 |
| business_analyst | 허용된 Dataset으로 Chart 생성 |
| dashboard_viewer | 승인된 Dashboard 조회 |
| country_kr_viewer | 한국 행만 조회 |

역할 이름은 예시이며, 실제 권한은 Superset의 Permission과 데이터베이스 권한을 함께 설정해야 한다.

### 9.3 RLS의 Regular와 Base

조사본은 RLS 필터를 Regular와 Base로 구분한다.

| 유형 | 조사본 기준 의미 |
|---|---|
| Regular | 지정한 역할에 필터 적용 |
| Base | 지정한 역할을 제외한 사용자에게 필터 적용 |

Regular는 특정 역할의 데이터 범위를 좁힐 때 사용한다. Base는 기본적으로 모든 사용자에게 적용할 제한을 만들고 특정 예외 역할을 제외할 때 사용한다. Base 필터를 잘못 설정하면 관리자나 데이터 검증 계정까지 제한될 수 있으므로 예외 역할을 명확히 테스트한다.

조사본의 Tenant-aware는 사용자별 동적 조건을 설명하는 응용 패턴으로 다룬다. 이것을 Regular·Base와 같은 Superset 기본 필터 유형이라고 단정하지 않는다. 사용자 식별자를 SQL 템플릿에 넣는 방식과 실제 Superset 지원 범위는 버전별 검증이 필요하다.

### 9.4 RLS 규칙 만들기

조사본의 UI 흐름은 다음과 같다.

1. Settings → Row Level Security로 이동한다.
2. + Rule을 선택한다.
3. Type에서 Regular 또는 Base를 선택한다.
4. 적용할 Dataset을 선택한다.
5. 적용할 Role을 선택한다.
6. SQL clause에 행 조건을 입력한다.
7. Save를 누른다.
8. 해당 역할의 테스트 계정으로 SQL Lab과 Dashboard를 각각 확인한다.

한국 사용자에게 국가 조건을 적용하는 간단한 예는 다음과 같다.

~~~sql
country = 'KR'
~~~

RLS 조건은 Dataset에 생성되는 SQL에 반영된다. 그러나 SQL Lab에서 사용자가 원본 데이터베이스나 다른 Dataset을 직접 조회할 권한을 가진 경우에는 의도한 제한을 우회할 수 있다. 따라서 RLS는 데이터베이스의 계정·권한과 함께 검토한다.

### 9.5 RLS 검증 시나리오

RLS를 저장한 뒤 관리자 계정만 확인해서는 안 된다. 최소한 다음 계정으로 같은 질문을 실행한다.

| 테스트 계정 | 기대 결과 |
|---|---|
| 관리자 | 전체 행 또는 정책에 정의된 범위 |
| 한국 역할 | country = KR 행만 |
| 다른 국가 역할 | 해당 국가 행만 |
| 일반 Viewer | 허용된 Dataset·Dashboard만 |
| 권한 없는 계정 | Dataset 또는 Database 접근 거부 |

검증은 Dashboard뿐 아니라 SQL Lab, CSV 다운로드, Virtual Dataset, API 호출 경로까지 고려한다. 화면에 보이지 않는다고 데이터 접근이 차단되었다고 단정하지 않는다.

### 9.6 RLS REST API

조사본에는 RLS 규칙 조회 API 예시가 포함되어 있다.

~~~http
GET /api/v1/rowlevelsecurity/
~~~

특정 Dataset의 규칙을 조회하는 쿼리 예시는 다음과 같은 형태다.

~~~http
GET /api/v1/rowlevelsecurity/?q=(filters:!((col:tables,opr:rel_m_m,value:<dataset_id>)))
~~~

API 경로·필터 문법·인증 방식은 Superset 버전과 API 문서에서 확인한다. 이 예시는 API를 통해 RLS 상태를 자동 점검할 수 있다는 개념을 보여 주는 용도다.

[검토 필요: Superset 6.1.0에서 RLS API의 정확한 Endpoint, Dataset 필드명, 인증 토큰 발급과 필터 문법을 실제 API로 확인해야 합니다.]

## 10. 쿼리 성능과 캐싱

### 10.1 Dashboard 쿼리의 비용

Dashboard 하나는 여러 Chart를 포함하고, 각 Chart는 하나 이상의 SQL을 실행할 수 있다. 사용자가 필터를 변경할 때마다 여러 쿼리가 다시 실행되면 ClickHouse와 Superset 모두에 부하가 생긴다.

성능 문제는 다음 순서로 분석한다.

1. 어떤 Chart의 SQL이 느린가?
2. 쿼리가 반환하는 행 수는 얼마인가?
3. 시간 범위와 필터가 WHERE 절에 반영되는가?
4. ClickHouse Mart의 정렬 키와 파티션 설계가 쿼리와 맞는가?
5. 같은 쿼리가 반복 실행되고 있는가?
6. Superset 결과 캐시를 적용할 수 있는가?

Chart 화면의 로딩 시간만 측정하면 원인을 놓칠 수 있다. Superset이 생성한 SQL, ClickHouse의 실행 시간과 읽은 데이터량, 반환 행 수를 함께 확인한다.

### 10.2 ClickHouse Mart를 우선 사용하기

Iceberg 원본을 매 Chart에서 직접 읽으면 최신성은 얻을 수 있지만, 대시보드의 동시성·응답 시간·쿼리 비용을 통제하기 어려워진다. 반복되는 KPI와 기간 집계는 ClickHouse Mart로 옮기고, Superset은 제한된 범위의 필터와 시각화에 집중한다.

| 상황 | 권장 |
|---|---|
| 매일 반복되는 KPI | ClickHouse Mart에 사전 집계 |
| 대화형 탐색과 임시 분석 | Trino·Dremio 경유 Iceberg |
| 상세 행 확인 | 제한된 기간과 결과 행 수 |
| 대시보드 공통 지표 | 승인된 Dataset과 Metric 재사용 |

### 10.3 연결 타임아웃과 결과 제한

조사본은 connect_timeout, send_receive_timeout, compression, query_limit을 ClickHouse 연결 옵션의 예로 제시한다. 이 값들은 성능을 자동으로 보장하는 설정이 아니다. 타임아웃은 실패를 빨리 알려 주는 제한이고, 결과 제한은 응답 크기를 제한하는 정책이다.

대시보드가 느릴 때 타임아웃을 무조건 늘리면 긴 쿼리가 더 오래 자원을 점유할 수 있다. 먼저 WHERE 조건, 집계 범위, Mart 설계를 점검한다.

### 10.4 결과 캐시

조사본은 Redis 기반 결과 캐시 예시를 제시한다.

~~~python
CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_HOST": "redis",
    "CACHE_REDIS_PORT": 6379,
    "CACHE_REDIS_DB": 1,
}
~~~

캐시는 동일한 쿼리 결과를 재사용해 데이터베이스의 반복 계산을 줄일 수 있다. 그러나 데이터가 갱신된 뒤에도 이전 결과가 일정 시간 표시될 수 있다. Mart 갱신 주기와 캐시 유효 시간을 함께 정한다.

| 선택 | 장점 | 대가 |
|---|---|---|
| 짧은 캐시 | 최신 결과에 가까움 | ClickHouse 쿼리 증가 |
| 긴 캐시 | 빠른 반복 조회 | 최신성 저하 |
| 캐시 없음 | 원본에 가까운 결과 | 동시 쿼리 비용 증가 |

Redis 캐시 설정의 정확한 키와 Superset 6.1.0 기능 플래그는 이미지와 공식 문서에서 검증한다.

### 10.5 비동기 쿼리

긴 SQL을 웹 요청이 끝날 때까지 기다리지 않고 별도 작업으로 처리하는 비동기 쿼리 방식은 Dashboard와 SQL Lab의 긴 작업을 다루는 방법이다. 조사본은 ENABLE_ASYNC_QUERY 기능 플래그를 예로 제시한다.

~~~python
FEATURE_FLAGS = {
    "ENABLE_ASYNC_QUERY": True,
}

SQLLAB_TIMEOUT = 300
~~~

비동기 쿼리를 활성화하려면 결과를 저장하고 조회하는 실행 구성, Worker, Redis 또는 Celery 관련 설정이 함께 필요할 수 있다. 기능 플래그만 추가하고 끝나는 것으로 설명하지 않는다.

[검토 필요: Superset 6.1.0의 비동기 쿼리 구성에 필요한 Celery·Redis·Worker 설정, 정확한 기능 플래그, SQL Lab timeout 설정명을 공식 문서와 실제 Compose 환경에서 확인해야 합니다.]

## 11. 단계별 로컬 실습

### 11.1 실습 목표

이번 실습에서는 다음 결과를 만든다.

1. Superset과 PostgreSQL, Redis를 Docker Compose로 시작한다.
2. Superset에서 ClickHouse Mart를 연결한다.
3. mart.daily_orders를 물리 Dataset으로 등록한다.
4. Big Number, Bar, Line Chart를 만든다.
5. Dashboard에 차트를 배치한다.
6. 시간 범위와 값 필터를 추가한다.
7. SQL Lab에서 Virtual Dataset을 만든다.
8. RLS를 적용하고 역할별 결과를 확인한다.

### 11.2 환경 파일

프로젝트 루트의 .env에 실습용 값을 둔다. 실제 비밀번호나 운영 Secret Key를 저장소에 커밋하지 않는다.

~~~dotenv
SUPERSET_DB_PASSWORD=change-this-password
SUPERSET_SECRET_KEY=change-this-secret-key
CLICKHOUSE_PASSWORD=change-this-clickhouse-password
~~~

SUPERSET_SECRET_KEY는 임의 문자열을 반복 사용하지 않고, 실제 환경의 비밀 관리 원칙에 따라 생성한다. 이 예제의 값은 학습용 자리 표시자다.

### 11.3 컨테이너 시작

~~~bash
docker compose config
docker compose up -d postgres redis superset
docker compose ps
~~~

웹 브라우저에서 http://localhost:8088을 연다. 첫 실행에 Metadata Database 마이그레이션과 관리자 계정 생성이 필요할 수 있다.

[추가 자료 조사 필요: Superset 6.1.0 공식 Docker 이미지에서 첫 실행 시 필요한 db upgrade, 관리자 계정 생성, 초기화 스크립트의 정확한 순서를 확인해야 합니다.]

서비스 로그는 다음과 같이 확인한다.

~~~bash
docker compose logs --tail=100 postgres
docker compose logs --tail=100 redis
docker compose logs --tail=100 superset
~~~

### 11.4 ClickHouse 연결

Superset UI에서 Data → Databases → + Database로 이동한다. ClickHouse Connect를 선택하고 다음 정보를 입력한다.

| 항목 | 실습 값 |
|---|---|
| Display Name | ClickHouse Mart |
| Host | Compose 서비스 이름 clickhouse |
| Port | 8123 |
| Database | mart |
| 사용자 | 앞 장에서 만든 조회 전용 사용자 |
| TLS | 로컬에서는 미사용 예시 |

SQLAlchemy URI 예시는 다음과 같다.

~~~text
clickhousedb://default:password@clickhouse:8123/mart
~~~

Test Connection을 먼저 실행한다. 실패하면 Superset 컨테이너에서 ClickHouse 이름이 해석되는지, ClickHouse 포트가 컨테이너 내부 포트인지, 사용자에게 mart 조회 권한이 있는지 확인한다.

### 11.5 Dataset과 첫 Chart

1. Data → Datasets → + Dataset을 선택한다.
2. Database에서 ClickHouse Mart를 선택한다.
3. Schema 또는 Database에서 mart를 선택한다.
4. daily_orders 테이블을 선택한다.
5. Add를 선택한다.
6. Create Chart를 선택한다.
7. Big Number에서 sum(order_count)를 Metric으로 설정한다.
8. Save하여 Dashboard에 추가한다.

다음으로 Bar Chart를 만든다. 차원은 customer_id 또는 Mart에 포함된 업무 차원을 사용하고, Metric은 sum(total_revenue)로 설정한다. 데이터가 많다면 Top N이나 기간 필터를 추가한다.

Line Chart에서는 order_date를 시간 컬럼으로 선택하고, sum(total_revenue)를 Metric으로 설정한다. 시간 범위는 최근 30일처럼 제한된 값으로 시작한다.

### 11.6 SQL Lab과 Virtual Dataset

~~~sql
SELECT
    order_date,
    sum(order_count) AS total_orders,
    sum(total_revenue) AS total_revenue
FROM mart.daily_orders
WHERE order_date >= today() - INTERVAL 30 DAY
GROUP BY order_date
ORDER BY order_date;
~~~

SQL Lab에서 실행한 뒤 결과가 의도한 grain인지 확인한다. Explore로 이동해 Virtual Dataset을 저장하고, 다시 Dataset 목록에서 저장된 이름을 확인한다.

### 11.7 Dashboard 필터

Dashboard Edit mode에서 Add/Edit Filters를 선택한다.

1. Time Range 필터를 추가한다.
2. 시간 컬럼으로 order_date를 지정한다.
3. 적용할 Chart를 선택한다.
4. 기본 기간을 최근 30일로 설정한다.
5. Value 필터가 필요하면 Mart에 존재하는 차원을 선택한다.
6. 저장한 뒤 필터 변경에 따라 모든 Chart의 SQL이 달라지는지 확인한다.

필터가 특정 Chart에만 적용되면 해당 Chart Dataset의 컬럼명과 시간 컬럼 설정이 다른지 확인한다.

### 11.8 RLS 실습

1. country 컬럼이 포함된 Dataset을 준비한다.
2. country_kr_viewer 역할을 만든다.
3. RLS에서 Regular Rule을 만든다.
4. Dataset과 country_kr_viewer 역할을 연결한다.
5. SQL clause에 다음 조건을 입력한다.

~~~sql
country = 'KR'
~~~

6. 한국 역할의 테스트 사용자를 생성한다.
7. SQL Lab과 Dashboard에서 한국 행만 표시되는지 확인한다.
8. CSV 다운로드 결과에도 같은 제한이 적용되는지 확인한다.

컬럼이 문자열이 아닌 경우 조건의 따옴표와 타입을 데이터베이스 dialect에 맞게 조정한다. 실제 테이블의 country 값이 KR인지 KOR인지도 확인한다.

## 12. 장애 시나리오별 점검

| 증상 | 우선 확인할 곳 | 가능한 원인 |
|---|---|---|
| Superset UI가 열리지 않음 | docker compose ps, Superset 로그 | 컨테이너 종료, 초기화 실패, 포트 충돌 |
| Metadata Database 연결 실패 | PostgreSQL 상태·URI | 서비스 이름 오류, 비밀번호 오류, healthcheck 미통과 |
| ClickHouse Test Connection 실패 | 네트워크·포트·사용자 | localhost 사용, 포트 매핑 혼동, 권한 부족 |
| Dataset 목록이 비어 있음 | Database·Schema·Table | 잘못된 Catalog, 조회 권한, 연결 dialect |
| Chart가 실행되지 않음 | Chart SQL과 데이터베이스 로그 | 함수 dialect 차이, 컬럼 타입, SQL 오류 |
| Dashboard가 느림 | 생성 SQL·ClickHouse 쿼리 | 범위 미제한, Mart 미사용, 과도한 Chart 수 |
| 캐시 결과가 오래됨 | 캐시 유효 시간·Mart 갱신 | TTL이 갱신 주기보다 김 |
| RLS가 적용되지 않음 | 역할·Dataset·SQL Lab | 잘못된 역할, 다른 Dataset 조회, 규칙 미할당 |
| RLS 후 관리자도 제한됨 | Base Rule·예외 역할 | Base 필터의 적용 범위 오류 |
| Virtual Dataset이 실패함 | 원본 SQL·권한 | 원본 테이블 변경, SQL dialect, 접근 권한 |
| 크로스 필터가 작동하지 않음 | 기능 플래그·Chart 설정 | 버전 차이, 차트 미지원, 적용 범위 설정 |
| Mac에서 이미지가 실행되지 않음 | 이미지 플랫폼·메모리 | ARM64 미지원, 에뮬레이션, 메모리 부족 |
| Windows에서 파일 반영이 늦음 | Docker Desktop·WSL 2 | 마운트 성능, 권한, 공유 설정 |

장애를 Superset 하나의 문제로 단정하지 않는다. Chart가 실패하면 먼저 Superset이 생성한 SQL을 확인하고, 같은 SQL을 ClickHouse·Trino·Dremio에서 직접 실행해 어느 계층에서 오류가 발생하는지 분리한다.

## 13. 운영 설계 체크리스트

### 연결과 비밀

- ClickHouse와 Trino·Dremio 연결은 조회 전용 계정으로 시작한다.
- URI에 실제 비밀번호·토큰을 직접 기록하지 않는다.
- TLS 환경에서는 인증서 검증을 끄지 않는다.
- SUPERSET_SECRET_KEY를 고정된 예시 값으로 운영하지 않는다.

### Dataset과 지표

- Dataset마다 원본 테이블과 집계 grain을 기록한다.
- Metric의 정의와 기준일을 설명에 적는다.
- 같은 이름의 revenue가 서로 다른 의미를 가지지 않게 한다.
- Virtual Dataset의 SQL 변경 책임자를 지정한다.

### Dashboard와 쿼리

- 시간 범위와 결과 행 수를 제한한다.
- ClickHouse Mart에서 반복 집계를 사전 계산한다.
- Dashboard Chart 수와 공통 필터 수를 과도하게 늘리지 않는다.
- 캐시 유효 시간과 Mart 갱신 주기를 비교한다.

### 권한과 RLS

- Database, Dataset, Dashboard 권한을 별도로 점검한다.
- RLS는 관리자·일반 사용자·국가별 사용자로 검증한다.
- SQL Lab, CSV 다운로드, API 경로에서 정책이 우회되지 않는지 확인한다.
- RLS를 데이터베이스 계정 권한의 대체물로 보지 않는다.

## 14. 핵심 정리

- Superset은 데이터를 저장하는 엔진이 아니라 SQL 데이터소스를 Chart와 Dashboard로 표현하는 시각화 계층이다.
- ClickHouse는 Serving Mart를 제공하고 Superset은 그 Mart를 Dataset으로 소비한다.
- Iceberg는 Superset이 직접 읽는 것이 아니라 Trino 또는 Dremio 같은 SQL 엔진을 경유해 조회한다.
- 물리 Dataset은 실제 테이블 기반이고, Virtual Dataset은 SQL 정의 기반이다.
- Explore는 Chart를 만들고 SQL Lab은 SQL을 검증·저장·재사용하는 공간이다.
- Dashboard 필터와 크로스 필터는 여러 Chart의 SQL과 결과를 바꿀 수 있으므로 대상 Dataset과 컬럼을 확인해야 한다.
- RLS는 행 단위 접근을 제한하지만 Database·Dataset·Dashboard 권한과 함께 설계해야 한다.
- Superset Metadata Database는 PostgreSQL에 저장하고, ClickHouse·Iceberg 데이터와 분리한다.
- 결과 캐시와 비동기 쿼리는 성능 보조 수단이며, 먼저 Mart 설계와 생성 SQL을 점검한다.
- Superset 버전과 Provider 버전에 따라 URI, 메뉴, 기능 플래그, API가 달라질 수 있으므로 출판 직전 실행 검증이 필요하다.

## 확인 문제

1. Superset과 ClickHouse의 역할을 각각 설명해 보세요.
2. Superset이 Iceberg 파일을 직접 읽지 않고 Trino 또는 Dremio를 경유하는 이유는 무엇인가요?
3. ClickHouse 연결 URI에서 Compose 네트워크의 호스트로 localhost 대신 서비스 이름을 사용하는 이유는 무엇인가요?
4. 물리 Dataset과 Virtual Dataset의 차이를 설명해 보세요.
5. Virtual Dataset이 보안 경계가 될 수 없는 이유는 무엇인가요?
6. Big Number, Bar, Line Chart를 각각 어떤 질문에 사용하는지 설명해 보세요.
7. Dashboard의 네이티브 필터와 크로스 필터의 차이는 무엇인가요?
8. SQL Lab에서 SELECT *와 제한 없는 기간 조회를 피해야 하는 이유는 무엇인가요?
9. RLS의 Regular와 Base 필터는 어떻게 다른가요?
10. RLS를 적용한 뒤 Dashboard뿐 아니라 SQL Lab과 CSV 다운로드도 검증해야 하는 이유는 무엇인가요?
11. Superset Metadata Database와 ClickHouse Mart에 저장되는 데이터는 어떻게 다른가요?
12. Dashboard가 느릴 때 Superset 설정 외에 ClickHouse에서 확인할 항목은 무엇인가요?
13. Windows 11과 Apple Silicon Mac에서 Docker Compose 실습 시 각각 확인할 환경 요소는 무엇인가요?

## 이 장에서 자료가 부족했거나 검증이 필요한 부분

- 기존 조사본의 Superset 4.1.2와 최신 조사본의 Superset 6.1.0 중 최종 기준 버전
- CVE-2026-23969의 영향 버전·수정 버전·Superset 공식 보안 공지
- Superset 6.1.0 공식 Docker 이미지의 Metadata Database 초기화·관리자 생성 절차
- Superset 6.1.0과 clickhouse-connect 0.9.x의 정확한 URI와 Advanced 설정
- Trino 477 이상과 Dremio 24.x의 Superset SQLAlchemy dialect·인증·Catalog 표기
- DASHBOARD_CROSS_FILTERS, ENABLE_DRILL_BY, ENABLE_ASYNC_QUERY의 Superset 6.1.0 지원 여부와 기본값
- RLS REST API의 Superset 6.1.0 Endpoint·필터 문법·인증 방식
- RLS의 사용자별 동적 조건을 구현하는 공식 패턴과 템플릿 사용 범위
- Apple Silicon ARM64와 Windows 11 WSL 2 환경의 Superset·ClickHouse·Trino·Dremio 이미지 재현성
- ClickHouse Mart의 실제 Schema·Table·조회 전용 계정·집계 grain

## 참고 자료

- [Apache Superset 공식 문서](https://superset.apache.org/user-docs/)
- [Superset ClickHouse Database](https://superset.apache.org/user-docs/databases/supported/clickhouse/)
- [Superset Security Configurations](https://superset.apache.org/admin-docs/security/)
- [Creating Your First Dashboard](https://superset.apache.org/user-docs/using-superset/creating-your-first-dashboard/)
- [ClickHouse와 Apache Superset 연동](https://clickhouse.com/docs/integrations/connectors/data-visualization/superset-and-clickhouse)
- [ClickHouse Connect Python Client](https://clickhouse.com/docs/integrations/language-clients/python)
- [Apache Iceberg 공식 문서](https://iceberg.apache.org/docs/latest/)
- [Trino 공식 문서](https://trino.io/docs/current/)
- [Apache Polaris 공식 문서](https://polaris.apache.org/)
- [NVD CVE-2026-23969](https://nvd.nist.gov/vuln/detail/CVE-2026-23969)
