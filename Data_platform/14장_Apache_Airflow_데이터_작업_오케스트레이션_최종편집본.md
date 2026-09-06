# 14장. Apache Airflow로 데이터 작업 오케스트레이션하기

11장부터 13장까지는 Flink CDC, Spark, ClickHouse를 각각 실행 엔진으로 사용했다. Flink는 변경 이벤트를 수집하고, Spark는 백필과 Iceberg 유지보수를 수행하며, ClickHouse는 Serving Mart를 제공한다. 이제 이 작업들을 언제 실행하고, 어떤 작업이 끝난 뒤 다음 작업을 시작하며, 실패했을 때 어디서 재시도할지를 정의해야 한다.

Apache Airflow는 이 순서를 관리하는 오케스트레이션 플랫폼이다. Airflow가 데이터를 직접 변환하는 것이 아니라, DAG에 작업과 의존성을 선언하고 각 작업을 실행할 엔진을 호출한다. 이 역할을 분리해야 Spark 작업의 실패를 Airflow의 스케줄링 문제로 오해하지 않고, Flink Job의 상태와 ClickHouse 적재 결과를 별도로 확인할 수 있다.

## 이 장의 기준과 버전 주의사항

조사 자료의 권장 기준은 Apache Airflow 3.3.0이다. 현재 공식 문서가 3.3.1 기준으로 제공되는 부분이 있으므로, 아래 예제는 Airflow 3.x의 공통 공개 API를 중심으로 작성하고 3.3.0과 3.3.1 사이의 차이는 실행 전에 확인한다.

| 구성 요소 | 이 장의 기준 | 역할 |
|---|---|---|
| Apache Airflow | 3.3.0 | DAG 스케줄링과 작업 실행 |
| Metadata Database | PostgreSQL 17.11 예시 | DAG·Task·실행 상태 저장 |
| Apache Spark | 3.5.4 | 백필·변환·Iceberg 유지보수 |
| Apache Flink | 2.2.x | CDC Job 실행 |
| ClickHouse | 26.7 | Serving Mart 갱신 |
| Apache Iceberg | 1.11.0 | 테이블과 Snapshot |
| Apache Polaris | 1.7.0 | Iceberg REST Catalog |

[검토 필요: Airflow 3.3.0과 현재 공식 문서 3.3.1의 Task SDK, Asset, Provider API 차이와 각 Provider의 정확한 버전을 최종 실행 검증해야 합니다.]

Airflow 공식 Docker Compose quick start는 학습·탐색용 구성이며 운영 보안 보장을 제공하지 않는다. 이 장의 Docker Compose 실습도 로컬 학습 범위로 한정한다 (출처: [Airflow Running in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)).

## 학습 목표

이 장을 마치면 다음 작업을 수행할 수 있습니다.

- Airflow 3.x의 Scheduler, Dag Processor, API Server, Metadata Database의 역할을 설명할 수 있습니다.
- DAG, Task, Operator, Sensor, Asset의 관계를 이해하고 기본 DAG를 작성할 수 있습니다.
- Spark·Flink·ClickHouse 작업을 Airflow에서 호출하고 의존성을 연결할 수 있습니다.
- Retry, Timeout, Trigger Rule, Backfill, 재실행 정책을 설계할 수 있습니다.
- Docker Compose 로컬 환경에서 Airflow를 실행하고 로그와 상태를 점검할 수 있습니다.

## 1. Airflow는 무엇을 담당하는가

### 1.1 실행 엔진과 오케스트레이터의 구분

Airflow는 Spark나 Flink의 대체품이 아니다. Airflow는 작업을 정의하고 실행 시점을 결정하며, 실행 결과를 기록하고 실패한 작업을 다시 시도한다. 실제 데이터 처리는 Spark, Flink, ClickHouse 또는 SQL 엔진이 담당한다.

| 구성 요소 | 실행하는 일 |
|---|---|
| Airflow | 일정, 의존성, 재시도, 타임아웃, 실행 이력 관리 |
| Flink CDC | Source 데이터베이스 변경 수집과 스트리밍 처리 |
| Spark | 백필, 변환, Compaction, Snapshot 유지보수 |
| ClickHouse | Serving Mart와 분석 쿼리 |
| PostgreSQL | Airflow Metadata Database |

Airflow Task가 성공했다는 의미는 Airflow가 호출한 명령 또는 Operator가 성공 상태를 반환했다는 뜻이다. Iceberg Snapshot이 올바르게 커밋되었는지, ClickHouse Mart의 행 수가 맞는지는 별도의 검증 Task가 확인해야 한다.

### 1.2 DAG와 Task

DAG는 Directed Acyclic Graph의 약자로, 작업과 작업 사이의 방향성 있는 의존성을 표현한다. Task는 DAG 안에서 실제로 실행되는 하나의 작업이다. 같은 DAG는 여러 실행 시점에 반복될 수 있으므로, 각 실행의 논리적 날짜와 실행 ID를 구분해야 한다.

Airflow 공식 문서는 DAG를 작업 순서의 표현으로, Task를 실행 단위로 설명한다. Operator는 미리 정의된 작업 템플릿이고, Sensor는 외부 조건을 기다리는 특수한 Task다 (출처: [Airflow Architecture Overview](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html), [Airflow Tasks](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html)).

~~~mermaid
flowchart TD
    A["DAG"] --> B["Task"]
    B --> C["Operator"]
    B --> D["Sensor"]
    B --> E["TaskFlow 함수"]
~~~

### 1.3 이 책의 전체 흐름

이 책의 주요 데이터 흐름을 Airflow 관점에서 표현하면 다음과 같다.

~~~mermaid
flowchart TD
    A["Flink CDC"] --> B["Iceberg raw"]
    B --> C["Spark transform"]
    C --> D["Iceberg clean"]
    D --> E["ClickHouse Mart"]
    F["Airflow"] --> A
    F --> C
    F --> E
~~~

Airflow는 화살표의 실제 데이터 처리를 수행하지 않고, Flink Job 제출, Spark 실행, ClickHouse SQL 실행과 결과 확인을 조정한다.

## 2. Airflow 3.x 아키텍처

### 2.1 주요 구성 요소

Airflow 3.x의 기본 구성은 다음과 같다.

| 구성 요소 | 책임 |
|---|---|
| Scheduler | 스케줄과 Task 의존성을 평가하고 실행할 Task를 제출 |
| Dag Processor | DAG 파일을 읽고 DAG 정의를 Metadata Database에 반영 |
| API Server | REST API와 Web UI 제공, DAG·Task 상태 관리 |
| Metadata Database | DAG, Task, Variable, Connection, 실행 상태 저장 |
| Executor | Task를 어떤 방식으로 실행할지 결정 |
| Triggerer | Deferrable Task의 비동기 대기 처리 |
| Worker | 분산 Executor에서 실제 Task 실행 |

Airflow 3.x에서는 Dag Processor가 독립된 구성 요소로 분리되고, API Server가 UI와 API를 제공한다. LocalExecutor를 사용하는 단일 머신에서는 일부 실행 역할이 같은 환경에 배치될 수 있지만, 개념적 책임은 구분해서 이해한다 (출처: [Airflow Architecture Overview](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html)).

~~~mermaid
flowchart TD
    A["Dag Processor"] --> B["Metadata Database"]
    B --> C["Scheduler"]
    C --> D["Executor"]
    D --> E["Task Runtime"]
    F["API Server"] --> B
    F --> G["Web UI"]
~~~

### 2.2 Task SDK와 공개 API

Airflow 3.x의 DAG 작성에서는 airflow.sdk 네임스페이스를 공개 인터페이스로 사용하는 방향이 제시되었다. 내부 모듈을 직접 import하면 업그레이드 때 변경 영향을 크게 받을 수 있으므로, 본문의 DAG 예제는 가능한 한 Task SDK를 사용한다.

Airflow 공식 문서는 Airflow 3.0부터 DAG 작성과 Task 실행의 기본 공개 인터페이스로 airflow.sdk를 사용하도록 안내한다 (출처: [Airflow Public Interface](https://airflow.apache.org/docs/apache-airflow/stable/public-airflow-interface.html)).

### 2.3 Task 실행 격리

Task는 Worker 또는 실행 환경에서 별도로 실행된다. Task 코드가 Metadata Database에 직접 접속하는 구조를 기본으로 만들지 않는다. Connection, Variable, XCom, Task 상태는 Airflow가 제공하는 공개 API와 Task Context를 사용한다.

이 분리는 보안과 운영 측면에서 중요하다. Spark나 ClickHouse의 비밀번호를 DAG 소스에 기록하지 않고 Airflow Connection 또는 외부 비밀 관리 방식으로 전달해야 한다.

## 3. DAG, Operator, Sensor, TaskFlow

### 3.1 Operator

Operator는 특정 종류의 작업을 실행하기 위한 템플릿이다. 예를 들어 PythonOperator는 Python 함수를 실행하고, BashOperator는 쉘 명령을 실행하며, SparkSubmitOperator는 spark-submit으로 Spark Job을 제출한다.

| Operator 또는 Sensor | 용도 |
|---|---|
| Python TaskFlow | 짧은 Python 로직과 상태 전달 |
| BashOperator | Flink CLI 등 쉘 명령 호출 |
| SparkSubmitOperator | Spark Job 제출 |
| ClickHouseOperator | ClickHouse SQL 실행 |
| SqlExecuteQueryOperator | Provider가 지원하는 SQL 실행 |
| ExternalTaskSensor | 다른 DAG 또는 Task 완료 대기 |
| FileSensor | 파일 생성 대기 |
| HttpSensor | HTTP endpoint 상태 대기 |

Provider가 설치되지 않은 Operator를 import하면 DAG Processor가 DAG를 읽지 못할 수 있다. Provider의 이름과 버전은 Airflow 이미지에 설치된 패키지 목록으로 확인한다.

### 3.2 TaskFlow API 예제

다음은 Airflow 3.x의 공개 SDK를 사용하는 간단한 DAG 예제다.

~~~python
from datetime import datetime, timezone

from airflow.sdk import dag, task


@dag(
    dag_id="basic_data_pipeline",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    tags=["tutorial"],
)
def basic_data_pipeline():

    @task
    def extract(logical_date=None):
        return {
            "source": "postgres",
            "logical_date": str(logical_date),
        }

    @task
    def transform(payload):
        return {
            "source": payload["source"],
            "transformed": True,
            "logical_date": payload["logical_date"],
        }

    @task
    def load(payload):
        print(f"load result: {payload}")

    load(transform(extract()))


basic_data_pipeline()
~~~

예제의 핵심은 함수 호출 순서가 아니라 반환값과 Task 의존성이다. extract가 반환한 값이 transform으로 전달되고, transform의 결과가 load로 전달된다. 큰 데이터셋을 Python 반환값으로 옮기지 않고, 실제 데이터는 Iceberg·Ozone·ClickHouse 같은 외부 저장소에 두며 XCom에는 작은 메타데이터만 전달한다.

### 3.3 Sensor

Sensor는 외부 조건이 충족될 때까지 기다린다. 예를 들어 이전 DAG의 완료, 파일 생성, HTTP 상태를 기다릴 수 있다. 대기 시간이 긴 Sensor가 Worker 슬롯을 계속 점유하지 않도록 poke와 reschedule 모드의 차이를 확인한다.

| 모드 | 동작 | 자원 관점 |
|---|---|---|
| poke | 같은 Worker가 주기적으로 조건 확인 | 대기 중 슬롯 점유 |
| reschedule | 확인 후 Worker를 반환하고 다음 시점에 재예약 | 대기 중 슬롯 절약 |

Sensor를 사용하기 전에 이벤트를 Asset으로 표현할 수 있는지 검토한다. 데이터 생성 완료를 기준으로 downstream DAG를 시작하는 경우 Asset 기반 스케줄링이 더 직접적인 표현일 수 있다.

## 4. Spark·Flink·ClickHouse 작업 연결

Airflow에서 외부 처리 엔진을 연결할 때 가장 먼저 결정할 것은 “어떤 프로그램이 데이터를 처리하는가”와 “어떤 프로그램이 그 처리를 지시하는가”를 분리하는 것이다. Airflow Task는 Spark 애플리케이션을 제출하거나 Flink 명령을 실행하고, ClickHouse에 SQL을 전달할 수 있다. 그러나 Task가 성공했다고 해서 데이터 품질까지 자동으로 보장되는 것은 아니다. 처리 엔진의 종료 코드와 별도로 행 수, Snapshot, 체크포인트, 쿼리 결과를 검증하는 Task를 둔다.

### 4.1 SparkSubmitOperator로 Spark Job 제출

Spark Job을 별도의 애플리케이션으로 관리한다면 SparkSubmitOperator가 적합하다. DAG에는 애플리케이션 경로, 인자, Spark 설정만 선언하고, 실제 Iceberg 테이블을 읽고 쓰는 코드는 Spark 애플리케이션에 둔다.

~~~python
from datetime import datetime, timezone

from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    dag_id="spark_iceberg_transform",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spark", "iceberg"],
)
def spark_iceberg_transform():
    transform = SparkSubmitOperator(
        task_id="transform_orders",
        application="/opt/airflow/jobs/orders_transform.py",
        application_args=[
            "--source", "lakehouse.raw.orders",
            "--target", "lakehouse.clean.orders",
        ],
        conf={
            "spark.sql.extensions": (
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            ),
        },
        verbose=True,
    )

    transform


spark_iceberg_transform()
~~~

이 예제에서 `/opt/airflow/jobs/orders_transform.py`는 Airflow 컨테이너에서 접근할 수 있어야 한다. Spark가 별도의 컨테이너나 클러스터에서 실행된다면 애플리케이션 경로와 Iceberg·Polaris 접속 설정이 Spark 실행 환경에도 전달되는지 확인한다. Airflow 이미지에 Spark Provider가 없으면 DAG Processor에서 import 오류가 발생하므로 Provider 패키지와 버전을 함께 고정한다.

[검토 필요: 이 장의 기준 Airflow 이미지에 포함할 Apache Spark Provider의 정확한 패키지명·버전, Spark 3.5.4와 Spark 4.1.2 중 책 전체에서 사용할 기준 버전, Polaris 접속 설정 및 Iceberg Runtime JAR 조합을 실행 환경에서 확정해야 합니다.]

### 4.2 Flink CDC Job 호출

Flink CDC Job은 장시간 실행되는 경우가 많다. 따라서 Airflow가 Flink 프로세스를 직접 붙잡고 있는지, Job 제출만 하고 종료 상태를 별도로 감시하는지부터 정한다. 로컬 실습에서는 BashOperator로 Flink CLI를 호출하는 단순한 방법을 사용한다.

~~~python
from datetime import datetime, timezone

from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator


@dag(
    dag_id="flink_cdc_submit",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["flink", "cdc"],
)
def flink_cdc_submit():
    submit = BashOperator(
        task_id="submit_flink_cdc",
        bash_command=(
            "flink run -d "
            "-c com.example.cdc.OrdersCdcJob "
            "/opt/flink-jobs/orders-cdc-job.jar "
            "--checkpoint-interval 60000"
        ),
        append_env=True,
    )

    submit


flink_cdc_submit()
~~~

`-d`는 Flink Job을 제출한 뒤 CLI가 바로 반환되는 형태를 가정한 예시다. 제출 성공은 CDC 파이프라인이 정상적으로 계속 실행 중이라는 뜻과 다르다. 실제 운영형 DAG에서는 Job ID를 저장하고, Flink REST API나 별도의 상태 확인 Task로 RUNNING·FAILED 상태를 확인한다.

FlinkKubernetesOperator를 사용하면 Kubernetes 환경에 Job을 제출하는 형태로 확장할 수 있지만, 이 장의 로컬 Docker Compose 실습은 Kubernetes를 전제로 하지 않는다. Flink Provider의 공식 지원 범위, Operator 이름, Airflow 이미지에 설치할 버전은 별도 검증이 필요하다.

[추가 자료 조사 필요: Flink 2.2.1과 호환되는 Airflow 공식 Flink Provider의 제공 여부, Flink Kubernetes Operator와 Airflow Provider의 역할 차이, 로컬 Docker 네트워크에서 Job 상태를 조회하는 구체적인 명령을 확인해야 합니다.]

### 4.3 ClickHouseOperator로 Mart 갱신

ClickHouse Mart 갱신은 SQL을 실행하는 작업이다. 원천 테이블에서 최근 범위를 다시 계산해 적재하는 경우에는 중복 적재가 생기지 않도록 대상 테이블의 엔진과 적재 전략을 함께 설계한다.

~~~python
from datetime import datetime, timezone

from airflow.sdk import dag
from airflow.providers.clickhouse.operators.clickhouse import ClickHouseOperator


@dag(
    dag_id="clickhouse_mart_refresh",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="0 */6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["clickhouse", "mart"],
)
def clickhouse_mart_refresh():
    refresh = ClickHouseOperator(
        task_id="refresh_daily_orders",
        clickhouse_conn_id="clickhouse_default",
        sql="""
        INSERT INTO mart.daily_orders
        SELECT
            toDate(order_date) AS order_date,
            toUInt64(customer_id) AS customer_id,
            count() AS order_count,
            sum(total_amount) AS total_revenue
        FROM iceberg_lakehouse.clean.orders
        WHERE order_date >= today() - INTERVAL 7 DAY
        GROUP BY order_date, customer_id
        """,
    )

    refresh


clickhouse_mart_refresh()
~~~

이 SQL을 그대로 여러 번 실행하면 Mart에 같은 기간 데이터가 중복될 수 있다. 실습에서는 대상 기간을 먼저 삭제한 뒤 다시 넣거나, 날짜별 교체 테이블을 사용하거나, 중복을 허용하지 않는 ClickHouse 테이블 설계를 선택한다. Airflow의 재시도 설정만으로 중복 문제가 해결되지는 않는다.

[검토 필요: 사용 중인 ClickHouse Provider가 `clickhouse_conn_id`와 SQL 실행 인자를 어떤 이름으로 제공하는지, Iceberg 카탈로그를 ClickHouse에서 조회하는 설정, `mart.daily_orders`의 중복 방지 전략을 최종 버전에서 검증해야 합니다.]

### 4.4 세 엔진을 하나의 흐름으로 연결하기

실시간 CDC Job의 제출, Spark 변환, ClickHouse Mart 갱신을 하나의 DAG에 모두 넣을 수 있지만, 장시간 실행되는 Flink Job과 주기적으로 끝나는 배치 작업을 무조건 같은 DAG Run에 묶으면 운영이 어려워진다. 다음과 같이 책임을 나누는 편이 이해하기 쉽다.

| DAG | 시작 조건 | 주요 작업 | 완료 판단 |
|---|---|---|---|
| `flink_cdc_submit` | 수동 또는 배포 이벤트 | Flink CDC Job 제출 | 제출 결과와 Job 상태 |
| `spark_iceberg_transform` | 시간 또는 raw Asset | Spark 변환·Iceberg commit | Spark 종료 코드와 Snapshot |
| `clickhouse_mart_refresh` | clean Asset 또는 시간 | Mart 갱신 SQL | SQL 성공과 품질 검증 |

장시간 실행되는 Flink를 매시간 시작하는 DAG에 넣으면 매 실행마다 중복 Job이 생길 수 있다. CDC Job은 배포·복구 DAG로 분리하고, 데이터 도착 이벤트는 Asset이나 별도의 상태 확인 Task로 전달한다.

~~~mermaid
flowchart LR
    A["Flink CDC Job"] --> B["Iceberg raw Asset"]
    B --> C["Spark transform"]
    C --> D["Iceberg clean Asset"]
    D --> E["ClickHouse Mart"]
~~~

## 5. Retry·Timeout·Trigger Rule 설계

실패한 Task를 다시 실행하는 정책은 데이터 파이프라인의 신뢰성을 높이지만, 모든 오류를 재시도하면 장애를 늦게 발견하거나 데이터를 중복 처리할 수 있다. 재시도는 일시적인 오류에만 적용하고, 권한 오류·스키마 오류·잘못된 SQL처럼 즉시 수정해야 하는 오류는 빠르게 실패하도록 설계한다.

### 5.1 재시도와 시간 제한

| 설정 | 의미 | 적용 예 |
|---|---|---|
| `retries` | 최초 실패 후 추가 실행 횟수 | 네트워크 일시 오류 |
| `retry_delay` | 재시도 전 대기 시간 | 5분 후 재시도 |
| `retry_exponential_backoff` | 재시도할 때 대기 시간을 점진적으로 증가 | 외부 서비스 부하 완화 |
| `max_retry_delay` | 지수 백오프의 최대 대기 시간 | 지나치게 긴 지연 방지 |
| `execution_timeout` | 개별 Task의 최대 실행 시간 | Spark Job 정체 감지 |
| `dagrun_timeout` | 하나의 DAG Run이 유지될 수 있는 최대 시간 | 전체 파이프라인 정체 방지 |

~~~python
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator


@dag(
    dag_id="retry_timeout_example",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    dagrun_timeout=timedelta(hours=2),
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
    },
)
def retry_timeout_example():
    run_job = BashOperator(
        task_id="run_job",
        bash_command="/opt/airflow/scripts/run_job.sh",
        execution_timeout=timedelta(minutes=45),
    )

    run_job


retry_timeout_example()
~~~

`execution_timeout`은 Task 하나의 실행 시간을 제한하고, `dagrun_timeout`은 DAG Run 전체의 시간을 제한한다. Spark나 Flink가 내부적으로 재시도하는 경우 Airflow 재시도와 중첩될 수 있으므로, 어느 계층이 몇 번 재시도할지 먼저 정한다.

### 5.2 Trigger Rule

기본 의존성은 upstream Task가 모두 성공해야 downstream Task를 실행하는 방식이다. 그러나 실패 여부를 기록하는 정리 Task나 알림 Task는 upstream 실패와 관계없이 실행해야 할 수 있다. 이때 Trigger Rule을 사용한다.

~~~python
from datetime import datetime, timezone

from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator


@dag(
    dag_id="trigger_rule_example",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
)
def trigger_rule_example():
    process = BashOperator(
        task_id="process",
        bash_command="/opt/airflow/scripts/process.sh",
    )

    notify = BashOperator(
        task_id="notify_or_cleanup",
        bash_command="/opt/airflow/scripts/notify_or_cleanup.sh",
        trigger_rule="all_done",
    )

    process >> notify


trigger_rule_example()
~~~

`all_done`은 upstream의 성공·실패·스킵 여부와 관계없이 조건을 평가하는 대표적인 예다. 실패한 데이터 적재 뒤에 성공 알림을 보내는 실수가 생기지 않도록 Task 이름과 알림 내용도 함께 구분한다. 정확한 Trigger Rule 이름과 Airflow 3.x SDK 노출 방식은 사용하는 버전의 공식 문서에서 확인한다.

## 6. Backfill·재실행·멱등성

### 6.1 논리적 날짜와 실행 날짜

배치 DAG의 “2026년 8월 30일 처리분”은 작업을 실제로 실행한 시각이 아니라 처리 대상 기간을 가리키는 경우가 많다. Airflow는 DAG Run의 논리적 날짜와 데이터 간격을 구분한다. 데이터 필터는 가능하면 현재 시스템 시각을 직접 읽지 말고 DAG Run의 논리적 날짜를 기준으로 계산한다.

~~~python
from datetime import datetime, timezone

from airflow.sdk import dag, task


@dag(
    dag_id="logical_date_example",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
)
def logical_date_example():
    @task
    def show_partition(**context):
        logical_date = context["logical_date"]
        print(f"처리 대상 논리 날짜: {logical_date.date()}")

    show_partition()


logical_date_example()
~~~

데이터 처리 애플리케이션에는 다음과 같이 날짜를 인자로 전달한다.

~~~python
from airflow.sdk import get_current_context


def build_partition_argument():
    context = get_current_context()
    logical_date = context["logical_date"]
    return logical_date.strftime("%Y-%m-%d")
~~~

현재 시각을 의미하는 `now()`가 필요한 운영 기록과, 데이터 파티션을 결정하는 논리 날짜를 혼동하지 않는다. 두 값이 필요한 경우 각각의 용도를 변수명에 드러낸다.

### 6.2 Backfill의 목적

Backfill은 과거 기간을 동일한 DAG 정의로 다시 처리하는 작업이다. 원천 데이터가 늦게 도착했거나 변환 로직을 수정했을 때 유용하다. Backfill을 실행하기 전 다음을 확인한다.

1. 대상 기간의 원천 데이터가 실제로 존재하는가?
2. 결과 테이블에 이미 같은 기간 데이터가 있는가?
3. 재실행해도 같은 결과가 나오는가?
4. 여러 날짜를 동시에 처리해도 Source·Catalog·Target이 감당할 수 있는가?
5. 실패한 날짜만 다시 실행할 수 있는가?

Airflow UI와 CLI에서 제공하는 Backfill 옵션은 Airflow 버전에 따라 달라질 수 있다. 이 책에서는 UI에서 날짜 범위를 선택하는 방식으로 설명하고, CLI 명령은 설치된 Airflow CLI에서 `airflow dags backfill --help`로 먼저 확인한다.

[검토 필요: Airflow 3.3.0에서 Backfill CLI의 정확한 옵션과 UI 명칭은 실행 이미지 기준으로 확인해야 합니다.]

### 6.3 멱등성 패턴

멱등성이란 같은 입력 기간을 한 번 또는 여러 번 처리해도 최종 결과가 같도록 만드는 성질이다. Airflow는 네트워크 오류나 Worker 종료 뒤 Task를 재실행할 수 있으므로, 외부 작업을 멱등적으로 만드는 것이 중요하다.

| 처리 대상 | 권장 패턴 | 주의점 |
|---|---|---|
| Iceberg 파티션 | 같은 파티션을 교체하거나 MERGE | Snapshot commit 충돌 처리 필요 |
| ClickHouse Mart | 대상 기간 삭제 후 INSERT 또는 교체 테이블 | 재시도 중 동시 실행 제한 |
| 파일 산출물 | 임시 경로에 쓴 뒤 성공 시 원자적 이동 | 부분 파일을 소비하지 않기 |
| Flink Job | Job ID·외부화 체크포인트로 중복 제출 방지 | 제출과 상태 확인 분리 |

`max_active_runs=1`은 동일 DAG의 동시 실행을 줄이는 데 도움을 주지만, 그것만으로 멱등성이 생기지는 않는다. 예를 들어 이전 Run이 실패한 뒤 재시도되면 같은 SQL이 다시 실행될 수 있으므로, 대상 기간의 정리·교체 전략이 필요하다.

## 7. Asset 기반 스케줄링

### 7.1 시간 중심에서 데이터 중심으로

시간 기반 스케줄은 “매일 03시에 실행”을 표현한다. Asset 기반 스케줄은 “특정 데이터가 갱신되면 실행”을 표현한다. Airflow 3.x 문서에서 Dataset을 계승한 개념으로 Asset을 설명하며, 생산 DAG가 Asset 업데이트를 기록하면 소비 DAG의 스케줄 조건으로 사용할 수 있다 (출처: [Airflow Assets](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html)).

Iceberg raw 테이블을 갱신하는 Flink 또는 Spark 작업이 끝난 뒤 clean 변환을 시작해야 한다면 Asset이 의도를 잘 드러낸다. 다만 외부 엔진이 Iceberg에 commit했다고 해서 Airflow가 자동으로 그 사실을 알게 되는 것은 아니다. Producer Task가 성공하면서 Asset 업데이트를 기록하도록 연결해야 한다.

### 7.2 Producer와 Consumer DAG

Asset URI는 실제 데이터를 저장하는 위치를 가리키는 식별자다. 이 책에서는 Polaris 카탈로그의 논리 테이블을 표현하기 위해 사용자 정의 URI를 사용한다. Airflow Asset URI에 특정 스킴의 의미를 부여할 때는 해당 Provider의 동작을 확인한다.

~~~python
from datetime import datetime, timezone

from airflow.sdk import Asset, dag, task


raw_orders = Asset(
    uri="x-iceberg://polaris/raw/orders",
    name="iceberg_raw_orders",
)


@dag(
    dag_id="raw_orders_producer",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
)
def raw_orders_producer():
    @task(outlets=[raw_orders])
    def submit_or_check_cdc():
        # 실제 환경에서는 Flink 제출 또는 상태 확인 명령을 실행한다.
        print("raw orders Asset 업데이트를 기록할 준비 완료")

    submit_or_check_cdc()


raw_orders_producer()
~~~

다음 Consumer DAG는 같은 Asset을 `schedule`에 선언한다.

~~~python
from datetime import datetime, timezone

from airflow.sdk import Asset, dag, task


raw_orders = Asset(
    uri="x-iceberg://polaris/raw/orders",
    name="iceberg_raw_orders",
)


@dag(
    dag_id="raw_orders_consumer",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=[raw_orders],
    catchup=False,
)
def raw_orders_consumer():
    @task
    def transform_after_asset_update():
        print("raw orders 갱신 뒤 Spark 변환을 시작합니다.")

    transform_after_asset_update()


raw_orders_consumer()
~~~

Producer와 Consumer에서 URI와 이름이 다르면 같은 Asset으로 인식되지 않을 수 있다. Asset 선언을 공통 Python 모듈로 분리해 두 DAG가 같은 정의를 import하도록 만들되, 공통 모듈이 DAG 파일로 인식되지 않도록 DAG 폴더 구조와 설정을 확인한다.

### 7.3 Asset 파티션

Asset 전체가 아니라 날짜 파티션이나 특정 키 범위가 갱신되었다는 사실을 downstream에 전달하면, 변경된 범위만 유지보수할 수 있다. Airflow 3.2 이후 Asset partitioning 관련 기능이 추가되었지만, 파티션 클래스와 이벤트 기록 방식은 Airflow 3.3.x와 Provider 조합에 따라 확인해야 한다.

~~~python
from airflow.sdk import Asset


daily_orders = Asset(
    uri="x-iceberg://polaris/raw/orders",
    name="iceberg_raw_orders_daily",
)
~~~

위 코드는 Asset 식별자의 기본 형태만 보인 것이다. 날짜 파티션 값을 실제 이벤트에 기록하고 Consumer Task에서 읽는 완전한 예제는 사용 중인 Airflow 버전의 Asset partition API가 확정된 뒤 작성한다.

[추가 자료 조사 필요: Airflow 3.3.0·3.3.1에서 Asset partition 정의, AssetPartition import 경로, 파티션 이벤트 기록 API, 외부 Flink·Spark commit을 Airflow Asset 이벤트로 변환하는 공식 패턴을 확인해야 합니다.]

~~~mermaid
sequenceDiagram
    participant P as Producer DAG
    participant A as Asset 상태
    participant C as Consumer DAG
    P->>A: raw 테이블 갱신 이벤트 기록
    A->>C: 스케줄 조건 만족
    C->>C: Spark·ClickHouse 작업 실행
~~~

## 8. Iceberg 유지보수 DAG 설계

Iceberg 유지보수는 “명령을 실행한다”보다 “어떤 순서와 안전 조건으로 실행한다”가 중요하다. 이 장에서는 Spark 애플리케이션이 Iceberg의 유지보수 프로시저를 호출하고, Airflow가 그 순서와 실행 이력을 관리하는 구조를 사용한다.

### 8.1 권장 순서

자료 수집본에서 제시한 기본 순서는 다음과 같다.

1. Compaction: 작은 데이터 파일을 병합한다.
2. Manifest rewrite: Manifest를 재구성해 메타데이터 탐색 비용을 줄인다.
3. Snapshot expiration: 보존 기간이 지난 Snapshot을 만료시킨다.
4. Orphan cleanup: 더 이상 참조되지 않는 파일을 정리한다.

이 순서는 테이블의 쓰기 패턴, 동시 Reader, 보존 정책에 따라 조정될 수 있다. 특히 orphan cleanup은 참조 상태를 잘못 판단하면 아직 유효한 파일을 삭제할 위험이 있으므로, 가장 보수적으로 실행한다.

~~~mermaid
flowchart LR
    A["Compaction"] --> B["Manifest rewrite"]
    B --> C["Snapshot expiration"]
    C --> D["Orphan cleanup"]
~~~

| 단계 | 대표 작업 | 확인할 결과 |
|---|---|---|
| Compaction | `rewrite_data_files` | 작은 파일 수와 대상 파티션 |
| Manifest rewrite | `rewrite_manifests` | Manifest 수와 메타데이터 크기 |
| Snapshot expiration | `expire_snapshots` | 보존 Snapshot과 Reader 영향 |
| Orphan cleanup | `remove_orphan_files` | 삭제 후보와 최소 파일 연령 |

### 8.2 Spark 유지보수 애플리케이션

아래 애플리케이션은 명령행 인자로 작업과 테이블을 받아 Spark SQL 프로시저를 호출하는 교육용 골격이다. 실제 실행에는 Iceberg Spark Extensions, Polaris REST Catalog, Ozone 또는 객체 저장소 연결 설정이 필요하다.

~~~python
import argparse

from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--operation", required=True)
    parser.add_argument("--target-file-size-bytes", default="268435456")
    parser.add_argument("--older-than-days", type=int, default=7)
    parser.add_argument("--retain-last", type=int, default=10)
    return parser.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("IcebergMaintenance").getOrCreate()
    table = args.table.replace("'", "''")

    if args.operation == "rewrite_data_files":
        spark.sql(f"""
            CALL system.rewrite_data_files(
                table => '{table}',
                strategy => 'binpack',
                options => map(
                    'target-file-size-bytes',
                    '{args.target_file_size_bytes}'
                )
            )
        """)
    elif args.operation == "rewrite_manifests":
        spark.sql(f"CALL system.rewrite_manifests(table => '{table}')")
    elif args.operation == "expire_snapshots":
        spark.sql(f"""
            CALL system.expire_snapshots(
                table => '{table}',
                older_than => current_timestamp() - INTERVAL {args.older_than_days} DAYS,
                retain_last => {args.retain_last}
            )
        """)
    elif args.operation == "remove_orphan_files":
        spark.sql(f"""
            CALL system.remove_orphan_files(
                table => '{table}',
                older_than => current_timestamp() - INTERVAL {args.older_than_days} DAYS
            )
        """)
    else:
        raise ValueError(f"unsupported operation: {args.operation}")

    spark.stop()


if __name__ == "__main__":
    main()
~~~

테이블 이름을 사용자 입력으로 직접 이어 붙이는 방식은 운영 환경에서 위험할 수 있다. 위 예제는 교육을 위해 단순화했으며, 운영 코드에서는 허용된 테이블 목록과 작업 목록을 코드로 제한한다. 또한 Snapshot expiration과 orphan cleanup의 보존 기간이 Reader와 백업 정책보다 짧지 않은지 확인한다.

### 8.3 유지보수 DAG

~~~python
from datetime import datetime, timezone

from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    dag_id="iceberg_maintenance",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["iceberg", "maintenance"],
)
def iceberg_maintenance():
    common = {
        "application": "/opt/airflow/jobs/iceberg_maintenance.py",
        "conf": {
            "spark.sql.extensions": (
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            ),
        },
    }

    compact = SparkSubmitOperator(
        task_id="compact_raw_orders",
        application_args=[
            "--table", "lakehouse.raw.orders",
            "--operation", "rewrite_data_files",
            "--target-file-size-bytes", "268435456",
        ],
        **common,
    )

    rewrite_manifests = SparkSubmitOperator(
        task_id="rewrite_raw_orders_manifests",
        application_args=[
            "--table", "lakehouse.raw.orders",
            "--operation", "rewrite_manifests",
        ],
        **common,
    )

    expire_snapshots = SparkSubmitOperator(
        task_id="expire_raw_orders_snapshots",
        application_args=[
            "--table", "lakehouse.raw.orders",
            "--operation", "expire_snapshots",
            "--older-than-days", "7",
            "--retain-last", "10",
        ],
        **common,
    )

    remove_orphans = SparkSubmitOperator(
        task_id="remove_raw_orders_orphans",
        application_args=[
            "--table", "lakehouse.raw.orders",
            "--operation", "remove_orphan_files",
            "--older-than-days", "3",
        ],
        **common,
    )

    compact >> rewrite_manifests >> expire_snapshots >> remove_orphans


iceberg_maintenance()
~~~

`remove_orphan_files`의 최소 연령은 Snapshot 보존 기간보다 충분히 길어야 한다. 자료 수집본에 제시된 3일·7일 값은 설명을 위한 예시이지, 모든 환경에 적용할 운영 기본값이 아니다. Flink CDC가 아직 파일을 쓰거나 커밋 중인 상태에서 orphan cleanup이 실행되지 않도록 Producer와 유지보수 DAG의 실행 창도 분리한다.

[검토 필요: 사용 중인 Iceberg·Spark 버전에서 `rewrite_data_files`, `rewrite_manifests`, `expire_snapshots`, `remove_orphan_files` 프로시저의 인자와 동작을 각각 실행 검증해야 합니다.]

## 9. PostgreSQL Metadata Database

### 9.1 사용자 데이터와 메타데이터의 분리

Airflow Metadata Database는 Airflow가 DAG, Task Instance, Connection, Variable, 실행 이력을 관리하기 위한 저장소다. PostgreSQL의 사용자 업무 데이터나 Iceberg 테이블 데이터를 이곳에 넣는다는 뜻이 아니다. 이 분리를 지키면 Airflow를 재설치하거나 DAG를 변경할 때 데이터 플랫폼의 원천·분석 데이터와 오케스트레이터 상태를 독립적으로 관리할 수 있다.

| 저장소 | 저장 내용 | 이 장의 예시 |
|---|---|---|
| Airflow Metadata DB | DAG·Task 상태, Connection, Variable | PostgreSQL 17.11 |
| Iceberg Catalog | 테이블 메타데이터와 Snapshot 참조 | Apache Polaris |
| Ozone | 데이터·메타데이터 파일 저장 | Apache Ozone |
| ClickHouse | Serving Mart와 분석용 데이터 | ClickHouse 26.x |

Airflow 공식 문서는 Metadata Database로 PostgreSQL 또는 MySQL 계열을 사용할 수 있도록 설명하며, 지원 버전은 Airflow 릴리스별 설치 문서에서 확인해야 한다 (출처: [Setting up a Database](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html)).

### 9.2 PostgreSQL Compose 서비스

다음은 Airflow Metadata Database를 위한 PostgreSQL 서비스의 핵심 설정이다. 비밀번호는 예시 문자열을 그대로 사용하지 말고 `.env` 또는 비밀 관리 방식으로 주입한다.

~~~yaml
services:
  postgres:
    image: postgres:17.11
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: ${AIRFLOW_DB_PASSWORD}
      POSTGRES_DB: airflow
    volumes:
      - postgres-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U airflow -d airflow"]
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  postgres-data:
~~~

Airflow 서비스의 SQLAlchemy 연결 문자열은 이 서비스 이름을 호스트로 사용한다. 예를 들어 Compose 네트워크 안에서 PostgreSQL 서비스 이름이 `postgres`라면 호스트 주소는 `localhost`가 아니라 `postgres`다.

~~~text
postgresql+psycopg2://airflow:${AIRFLOW_DB_PASSWORD}@postgres:5432/airflow
~~~

컨테이너 내부 주소와 호스트 운영체제에서 접속할 때의 주소를 구분한다. Windows 11과 Apple Silicon Mac에서 모두 Docker Compose 서비스 간 통신은 Compose 서비스 이름을 사용하고, 호스트의 데이터베이스나 파일에 접근할 때만 `host.docker.internal` 사용 여부를 검토한다.

### 9.3 공식 Docker Compose quick start 사용

Airflow 공식 문서는 Docker Compose quick start 파일을 제공한다. 로컬 실습에서는 문서의 해당 릴리스 파일을 내려받고, `airflow-init`으로 데이터베이스 초기화와 관리자 계정 초기화를 수행한 뒤 서비스를 시작한다.

~~~bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.3.1/docker-compose.yaml'
mkdir -p ./dags ./logs ./plugins ./config
docker compose up airflow-init
docker compose up -d
docker compose ps
~~~

이 책의 기준 버전은 Airflow 3.3.0이므로 URL의 버전과 Docker 이미지 태그를 일치시킨다. 공식 quick start는 학습·탐색용으로 제공되며, 운영 배포의 보안·고가용성·비밀 관리 기준으로 사용하지 않는다. Windows 11에서는 Docker Desktop의 WSL 2 엔진과 파일 공유 설정을 확인하고, Apple Silicon Mac에서는 ARM64 호환 이미지와 Docker Desktop 메모리 할당을 확인한다.

Airflow 3.x quick start 구성에서 Scheduler, Dag Processor, API Server, Worker, Triggerer, Metadata Database 등이 어떤 서비스로 표현되는지 파일을 직접 확인한다. 이전 2.x 예제의 `webserver` 서비스 이름과 Airflow 3.x 공식 Compose 파일의 서비스 구성을 섞지 않는다.

[검토 필요: Airflow 3.3.0 공식 Compose 파일의 서비스명, Executor, 초기화 명령, 기본 계정, PostgreSQL 이미지 태그는 최종 배포 시점의 공식 파일과 대조해야 합니다.]

## 10. 단계별 로컬 실습

### 10.1 디렉터리 준비

책의 앞 장에서 사용한 Compose 프로젝트 루트에 Airflow 디렉터리를 추가한다.

~~~text
data-platform/
├── docker-compose.yml
├── .env
├── dags/
│   ├── basic_data_pipeline.py
│   ├── iceberg_maintenance.py
│   └── clickhouse_mart_refresh.py
├── jobs/
│   ├── orders_transform.py
│   └── iceberg_maintenance.py
├── scripts/
│   ├── run_job.sh
│   ├── verify_iceberg.sh
│   └── refresh_clickhouse.sh
├── logs/
├── plugins/
└── config/
~~~

DAG 파일은 빠르게 읽히도록 유지하고, Spark·Flink 애플리케이션은 `jobs/`의 별도 파일로 둔다. 로컬 Compose에서는 `dags/`, `jobs/`, `scripts/`가 Airflow 컨테이너에 실제로 마운트되는지 확인한다.

### 10.2 시작과 종료

~~~bash
docker compose config
docker compose up airflow-init
docker compose up -d
docker compose ps
~~~

`docker compose config`는 YAML 병합과 환경 변수 치환 결과를 확인하는 단계다. 초기화가 실패하면 서비스 전체를 반복해서 시작하지 말고 PostgreSQL 로그와 `airflow-init` 로그를 먼저 확인한다.

~~~bash
docker compose logs --tail=100 postgres
docker compose logs --tail=100 airflow-init
docker compose logs --tail=100 airflow-scheduler
~~~

서비스를 중지할 때는 컨테이너만 중지하고 데이터 볼륨은 남겨 두는 방식과, 실습을 처음부터 다시 만드는 방식을 구분한다. 데이터 볼륨 삭제는 Airflow 실행 이력과 PostgreSQL 데이터를 제거하므로 필요한 경우에만 수행한다.

### 10.3 DAG 확인

웹 UI에서 DAG가 보이지 않으면 다음 순서로 확인한다.

1. `dags/`가 컨테이너에 마운트되었는가?
2. DAG 파일의 Python import 오류가 없는가?
3. Provider 패키지가 설치되었는가?
4. `airflow.sdk`와 현재 Airflow 이미지 버전이 맞는가?
5. Dag Processor 로그에 파일 구문 분석 오류가 없는가?

~~~bash
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags show basic_data_pipeline
docker compose exec airflow-scheduler airflow dags trigger basic_data_pipeline
~~~

Airflow 3.x에서 CLI 명령의 세부 옵션은 릴리스별로 달라질 수 있다. 명령이 실패하면 해당 이미지에서 `airflow dags --help`를 실행해 지원되는 명령을 확인한다.

### 10.4 통합 DAG 예제

다음 DAG는 처리 엔진을 직접 구현하지 않고, 단계별 실행과 검증을 표현하는 로컬 실습용 골격이다.

~~~python
from datetime import datetime, timezone

from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator


@dag(
    dag_id="local_data_platform_pipeline",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["local", "end-to-end"],
)
def local_data_platform_pipeline():
    transform = BashOperator(
        task_id="spark_transform",
        bash_command=(
            "spark-submit /opt/airflow/jobs/orders_transform.py "
            "--source lakehouse.raw.orders "
            "--target lakehouse.clean.orders"
        ),
    )

    verify_iceberg = BashOperator(
        task_id="verify_iceberg",
        bash_command="/opt/airflow/scripts/verify_iceberg.sh",
    )

    refresh_clickhouse = BashOperator(
        task_id="refresh_clickhouse",
        bash_command="/opt/airflow/scripts/refresh_clickhouse.sh",
    )

    transform >> verify_iceberg >> refresh_clickhouse


local_data_platform_pipeline()
~~~

이 코드는 `spark-submit`과 검증 스크립트가 Airflow 컨테이너에서 실행된다는 가정만 보여 준다. Spark와 ClickHouse가 별도 컨테이너라면 네트워크 주소, 실행 파일 위치, 인증 정보, 결과 로그 위치를 컨테이너별로 맞춘다. 실행 엔진이 Airflow 컨테이너에 없으면 SparkSubmitOperator와 ClickHouseOperator 또는 전용 실행 서비스로 구조를 바꾼다.

### 10.5 상태와 로그 확인

Airflow UI의 Grid 또는 Task Instance 화면에서 다음을 확인한다.

| 확인 항목 | 질문 |
|---|---|
| DAG Run | 어느 논리 날짜의 실행인가? |
| Task 상태 | queued, running, success, failed 중 무엇인가? |
| 시도 횟수 | 재시도가 몇 번 발생했는가? |
| 로그 | 외부 엔진의 종료 코드와 오류 메시지는 무엇인가? |
| XCom | 실제 대용량 데이터를 저장하고 있지 않은가? |
| Asset | Producer의 갱신 이벤트가 기록되었는가? |

공식 Docker Compose 문서는 상태 점검을 위한 Health endpoint도 안내한다. 로컬 포트와 API 인증 설정에 따라 다음 요청이 동작하는지 확인한다.

~~~bash
curl http://localhost:8080/api/v2/monitor/health
~~~

응답이 정상이더라도 특정 Worker나 Provider가 정상이라는 뜻은 아니다. API Server, Scheduler, Dag Processor, Worker, Triggerer, PostgreSQL을 각각 점검한다.

## 11. 장애 시나리오별 점검

| 증상 | 우선 확인할 곳 | 가능한 원인 |
|---|---|---|
| DAG가 UI에 나타나지 않음 | Dag Processor 로그 | Python 구문 오류, Provider import 오류, 마운트 누락 |
| Task가 queued에 오래 머묾 | Scheduler·Executor·Worker | Worker 부족, Executor 설정, 리소스 부족 |
| Spark 제출 실패 | Spark 로그와 경로 | JAR 누락, Polaris 주소 오류, 네트워크 분리 |
| Flink Job이 중복 실행됨 | Flink Job 목록과 DAG Run | 장기 실행 Job을 주기 DAG에 포함 |
| ClickHouse 적재 행 수가 증가함 | SQL·테이블 엔진 | 재시도 시 중복 INSERT, 기간 삭제 누락 |
| Asset Consumer가 실행되지 않음 | Asset URI·Producer Task | URI 불일치, Asset 이벤트 미기록 |
| Snapshot 유지보수 실패 | Spark SQL·Catalog | 프로시저 인자, 권한, 동시 commit 충돌 |
| Orphan cleanup 뒤 조회 실패 | 삭제 후보·보존 정책 | 너무 짧은 최소 연령, 동시 쓰기 |
| Airflow 재시작 뒤 상태가 사라짐 | PostgreSQL volume | Metadata DB 볼륨 미설정 또는 초기화 반복 |
| Mac에서 이미지가 실행되지 않음 | Docker 이미지 플랫폼 | ARM64 미지원 이미지, 메모리 부족 |
| Windows에서 파일 변경이 늦음 | Docker Desktop·WSL 2 | 파일 공유·마운트 성능 또는 권한 |

장애를 “Airflow 문제”로 한 번에 분류하지 않는다. 먼저 Task 로그에서 외부 명령의 종료 코드와 실제 대상 시스템의 상태를 함께 확인한다. Spark Job이 성공했지만 Iceberg Snapshot이 예상과 다르면 Catalog와 저장소를 점검하고, ClickHouse SQL이 성공했지만 중복이 생기면 Mart 적재 설계를 점검한다.

## 12. 핵심 정리

- Airflow는 데이터 처리 엔진이 아니라 일정·의존성·실행 상태를 관리하는 오케스트레이터다.
- Airflow 3.x DAG는 가능한 한 `airflow.sdk` 공개 인터페이스를 사용한다.
- Spark·Flink·ClickHouse는 각자의 실행 환경과 상태를 가지며, Airflow Task 성공과 데이터 품질 검증을 분리한다.
- Retry와 Timeout은 일시 오류를 다루기 위한 도구이며, 멱등성 없는 작업의 중복 실행을 자동으로 해결하지 않는다.
- Backfill은 논리적 날짜를 기준으로 설계하고, 동일 기간을 여러 번 처리해도 결과가 안정적인지 확인한다.
- Asset은 “데이터가 갱신되면 실행”이라는 의도를 표현하는 방법이다. 외부 엔진의 commit을 Airflow Asset 이벤트와 연결하는 방식은 별도로 검증해야 한다.
- Iceberg 유지보수는 Compaction, Manifest rewrite, Snapshot expiration, Orphan cleanup의 순서와 보존 정책을 함께 설계한다.
- Airflow Metadata Database는 PostgreSQL에 분리하고, Docker Compose quick start는 로컬 학습 범위로 사용한다.

## 확인 문제

1. Airflow와 Spark의 역할을 각각 한 문장으로 설명해 보세요.
2. DAG, Task, Operator, Sensor의 관계를 설명해 보세요.
3. `execution_timeout`과 `dagrun_timeout`의 차이는 무엇인가요?
4. 재시도 가능한 오류와 즉시 수정해야 하는 오류의 예를 하나씩 들어 보세요.
5. Backfill에서 논리적 날짜가 필요한 이유는 무엇인가요?
6. ClickHouse Mart에 동일 기간을 재적재할 때 생길 수 있는 문제와 멱등성 해결 방법을 설명해 보세요.
7. Flink CDC Job을 매시간 DAG에서 무조건 새로 제출하면 어떤 문제가 생기나요?
8. Asset 기반 스케줄링이 시간 기반 스케줄링보다 잘 표현하는 상황은 무엇인가요?
9. Iceberg 유지보수 작업의 순서를 적고, Orphan cleanup을 보수적으로 실행해야 하는 이유를 설명해 보세요.
10. Airflow Metadata Database와 Iceberg Catalog의 역할은 어떻게 다른가요?
11. DAG가 UI에 나타나지 않을 때 확인할 항목을 세 가지 이상 적어 보세요.
12. Windows 11과 Apple Silicon Mac에서 Docker Compose 실습 시 확인할 환경 차이는 무엇인가요?

## 이 장에서 자료가 부족했거나 검증이 필요한 부분

- Airflow 3.3.0과 현재 공식 문서 3.3.1 사이의 Task SDK·Asset·CLI 차이
- Spark 3.5.4와 Spark 4.1.2 중 책 전체의 기준 버전 및 Iceberg·Polaris Runtime 조합
- Spark Provider, ClickHouse Provider, Standard Provider의 정확한 패키지명·버전·Operator 인자
- Flink 2.2.x에 대한 공식 Airflow Provider 지원 여부와 로컬 실행·상태 확인 방식
- Asset partitioning의 Airflow 3.3.x 정확한 API와 외부 엔진 commit 연동 방식
- Iceberg 유지보수 프로시저의 버전별 인자 및 Ozone·Polaris 환경에서의 실제 실행 결과
- Airflow 3.x 공식 Docker Compose quick start의 서비스명·Executor·초기화 명령
- PostgreSQL 17.11과 Airflow 3.3.0 조합의 최종 호환성
- Windows 11 WSL 2와 Apple Silicon ARM64에서 동일 예제를 재현하기 위한 이미지·마운트 검증

## 참고 자료

- [Airflow Architecture Overview](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html)
- [Airflow Public Interface](https://airflow.apache.org/docs/apache-airflow/stable/public-airflow-interface.html)
- [Airflow Tasks](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html)
- [Airflow Operators](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/operators.html)
- [Airflow Assets](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html)
- [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- [Setting up a Database](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html)
- [Airflow Installation](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html)
- [Airflow Provider Packages](https://airflow.apache.org/registry/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [Apache Polaris Documentation](https://polaris.apache.org/)
- [ClickHouse Documentation](https://clickhouse.com/docs/)
