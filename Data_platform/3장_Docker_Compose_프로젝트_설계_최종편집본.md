# 3장. Docker Compose 프로젝트 설계

## 이 장의 목표

이 장에서는 여러 컨테이너를 하나의 데이터 플랫폼 프로젝트로 구성하는 방법을 설계합니다.

다음 작업을 수행할 수 있어야 합니다.

- Compose 네트워크와 서비스 이름 기반 통신 구성
- 서비스 간 의존 관계와 실제 readiness 구분
- 데이터베이스별 healthcheck 작성
- .env, .env.example, Secret의 역할 구분
- profiles를 사용한 계층별 실행
- 서비스별 named volume과 bind mount 설계
- 로그와 재시작 정책 구성
- 단일 호스트 실습 환경의 한계 설명

2장에서 Docker Desktop과 Compose의 기본 실행을 확인했다면, 이 장에서는 데이터 플랫폼에 필요한 여러 서비스를 하나의 프로젝트로 조직합니다.

---

## 1. 프로젝트 구조

데이터 플랫폼 프로젝트는 Compose 파일 하나만으로 구성하지 않습니다. 실행 설정, 초기화 스크립트, 환경 변수, 로그와 데이터 보존 정책을 함께 관리해야 합니다.

이 책의 기본 프로젝트 구조는 다음과 같습니다.

~~~text
data-platform/
├── compose.yaml
├── .env.example
├── .env
├── .gitignore
├── init/
│   ├── mysql/
│   ├── postgres/
│   └── mongodb/
├── scripts/
│   ├── start.ps1
│   ├── start.sh
│   └── check-environment.sh
├── airflow/
│   └── dags/
├── flink/
│   ├── conf/
│   └── jobs/
├── kafka/
│   └── config/
├── ozone/
├── polaris/
├── clickhouse/
│   └── init/
└── README.md
~~~

각 디렉터리의 역할은 다음과 같습니다.

| 경로 | 역할 |
|---|---|
| compose.yaml | 서비스, 네트워크, 볼륨, 의존 관계 정의 |
| .env.example | 공유 가능한 환경 변수 템플릿 |
| .env | 로컬 실행에 사용하는 실제 값 |
| init | 데이터베이스와 서비스 초기화 파일 |
| scripts | 운영체제별 실행·검증 스크립트 |
| airflow/dags | Airflow DAG 파일 |
| flink/jobs | Flink 작업 파일 |
| kafka/config | Kafka 설정 파일 |
| ozone, polaris | 레이크하우스 관련 설정 |
| clickhouse/init | ClickHouse 초기화 SQL |
| README.md | 실행 순서와 문제 해결 안내 |

실제 비밀번호와 API 키를 포함하는 .env 파일은 Git 저장소에 커밋하지 않습니다.

---

## 2. Compose 네트워크와 서비스 이름

### 2.1 기본 네트워크

Compose 프로젝트의 서비스는 기본 네트워크를 통해 서로 통신할 수 있습니다. 컨테이너 안에서 다른 서비스를 호출할 때는 호스트 컴퓨터의 IP 주소가 아니라 Compose 서비스 이름을 사용합니다.

예를 들어 다음과 같은 서비스가 있다고 가정합니다.

~~~yaml
services:
  application:
    image: example/application:1.0

  database:
    image: postgres:17.11
~~~

application 컨테이너에서 PostgreSQL에 연결할 때 데이터베이스 호스트는 database가 됩니다.

~~~text
DATABASE_HOST=database
DATABASE_PORT=5432
~~~

서비스 이름은 Compose 네트워크에서 내부 DNS 이름으로 사용됩니다. 컨테이너가 재생성되어 IP 주소가 바뀌더라도 서비스 이름을 사용하면 연결 설정을 변경할 필요가 없습니다.

### 2.2 호스트 포트와 컨테이너 포트

다음 두 포트를 구분해야 합니다.

- 컨테이너 포트: 컨테이너 내부 서비스가 수신하는 포트
- 호스트 포트: Windows 또는 macOS에서 접근하기 위해 공개하는 포트

~~~yaml
services:
  postgres:
    image: postgres:17.11
    ports:
      - "5432:5432"
~~~

Compose 프로젝트 내부의 다른 서비스는 호스트 포트가 아니라 컨테이너 포트와 서비스 이름으로 연결합니다.

~~~text
postgresql://postgres:secret@postgres:5432/mydb
~~~

호스트에서 직접 접속할 때는 localhost와 호스트 포트를 사용합니다.

~~~text
postgresql://postgres:secret@localhost:5432/mydb
~~~

컨테이너 간 통신과 호스트에서의 개발자 접속을 혼동하면 연결 오류가 발생할 수 있습니다.

### 2.3 네트워크 확인

~~~powershell
docker compose up -d
docker network ls
docker compose ps
~~~

Compose가 생성한 네트워크의 상세 정보를 확인할 수 있습니다.

~~~powershell
docker network inspect 프로젝트명_default
~~~

컨테이너 안에서 서비스 이름이 해결되는지 확인할 때는 해당 컨테이너에 네트워크 진단 명령이 포함되어 있는지 먼저 확인합니다.

---

## 3. 멀티 네트워크 설계

모든 서비스를 하나의 네트워크에 배치하면 구성이 단순하지만, 서비스 간 접근 범위를 구분하기 어렵습니다.

다음과 같이 네트워크를 분리할 수 있습니다.

~~~yaml
networks:
  frontend:
  backend:
  streaming:
  lakehouse:

services:
  superset:
    image: apache/superset:6.1.0
    networks:
      - frontend
      - backend

  clickhouse:
    image: clickhouse/clickhouse-server:26.6
    networks:
      - backend
      - lakehouse

  kafka:
    image: apache/kafka:4.3.1
    networks:
      - streaming

  flink:
    image: apache/flink:2.2.1
    networks:
      - streaming
      - lakehouse
~~~

네트워크 분리의 목적은 보안 경계와 책임 경계를 명확히 하는 것입니다.

| 네트워크 | 연결을 허용할 계층 |
|---|---|
| frontend | Superset과 외부 접근 계층 |
| backend | Superset과 ClickHouse |
| streaming | Kafka와 Flink |
| lakehouse | Flink, Ozone, Polaris, ClickHouse |

서비스가 어떤 네트워크에 연결되어 있는지에 따라 접근 가능한 서비스 이름의 범위가 달라집니다.

### 3.1 내부 전용 네트워크

외부 연결이 필요하지 않은 내부 네트워크는 internal 속성을 검토할 수 있습니다.

~~~yaml
networks:
  backend:
    internal: true
~~~

다만 특정 서비스가 이미지 풀, 외부 카탈로그, 라이선스 확인, 패키지 다운로드를 위해 인터넷 연결을 필요로 할 수 있습니다. internal 네트워크를 적용하기 전에 서비스 초기화 절차를 확인해야 합니다.

### 3.2 외부 네트워크

Compose 외부에서 만든 네트워크에 연결하려면 external 네트워크를 선언할 수 있습니다.

~~~yaml
networks:
  shared_network:
    external: true
~~~

외부 네트워크는 다른 Compose 프로젝트와 통신해야 할 때 사용할 수 있지만, 네트워크의 생성과 삭제 주체가 Compose 프로젝트 밖에 있다는 점을 문서에 기록해야 합니다.

---

## 4. 서비스 이름 기반 통신 실습

다음 예제는 PostgreSQL과 애플리케이션을 같은 네트워크에 배치합니다.

~~~yaml
services:
  database:
    image: postgres:17.11
    environment:
      POSTGRES_PASSWORD: secret
    networks:
      - backend

  application:
    image: alpine:latest
    command: ["sh", "-c", "sleep infinity"]
    networks:
      - backend
    depends_on:
      - database

networks:
  backend:
~~~

application에서 database라는 이름을 사용해야 합니다. localhost는 application 컨테이너 자신을 가리키므로 PostgreSQL에 연결되지 않습니다.

이 규칙은 다음 서비스에도 동일하게 적용됩니다.

| 목적지 | Compose 내부 호스트 이름 예 |
|---|---|
| Percona Server for MySQL | mysql |
| PostgreSQL | postgres |
| MongoDB | mongodb |
| Kafka | kafka |
| Flink | flink |
| ClickHouse | clickhouse |
| Polaris | polaris |
| Ozone | ozone |

서비스 이름은 프로젝트의 논리적 이름으로 관리하고, 컨테이너 IP 주소를 설정 파일에 직접 기록하지 않습니다.

---

## 5. depends_on과 실제 readiness

### 5.1 short syntax

다음은 가장 간단한 의존 관계 표현입니다.

~~~yaml
services:
  application:
    image: example/application:1.0
    depends_on:
      - database

  database:
    image: postgres:17.11
~~~

short syntax는 database 컨테이너가 먼저 시작되도록 요청하지만, PostgreSQL이 실제 연결을 받을 준비가 되었는지 확인하지는 않습니다.

가능한 실행 순서는 다음과 같습니다.

1. database 컨테이너 시작
2. PostgreSQL 초기화 진행
3. application 컨테이너 시작
4. application이 database에 연결 시도
5. PostgreSQL이 아직 준비되지 않아 연결 실패

애플리케이션에 자체 재시도 로직이 없다면 컨테이너가 종료될 수 있습니다.

### 5.2 long syntax와 condition

healthcheck와 long syntax를 사용하면 의존 서비스의 상태를 조건으로 표현할 수 있습니다.

~~~yaml
services:
  database:
    image: postgres:17.11
    environment:
      POSTGRES_PASSWORD: secret
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 30s

  application:
    image: example/application:1.0
    depends_on:
      database:
        condition: service_healthy
~~~

Compose에서 사용하는 주요 조건은 다음과 같습니다.

| 조건 | 의미 | 사용 사례 |
|---|---|---|
| service_started | 컨테이너가 시작됨 | 단순한 시작 순서 |
| service_healthy | healthcheck 성공 | 데이터베이스와 메시지 시스템 |
| service_completed_successfully | 일회성 작업이 성공 종료 | 마이그레이션과 초기화 |

### 5.3 readiness의 범위

healthcheck가 성공했다고 해서 애플리케이션 수준의 준비가 모두 끝났다고 볼 수는 없습니다.

예를 들어 데이터베이스가 연결을 허용하더라도 다음 작업은 아직 끝나지 않았을 수 있습니다.

- 애플리케이션 스키마 생성
- 사용자와 권한 초기화
- Kafka 토픽 생성
- 외부 저장소 연결
- CDC 커넥터의 이벤트 소비 시작

따라서 Compose의 healthcheck와 애플리케이션의 재시도·readiness 로직을 함께 설계해야 합니다.

---

## 6. healthcheck 설계

### 6.1 healthcheck 필드

~~~yaml
healthcheck:
  test: ["CMD-SHELL", "검사 명령"]
  interval: 10s
  timeout: 5s
  retries: 5
  start_period: 30s
~~~

| 필드 | 역할 |
|---|---|
| test | 컨테이너 내부에서 실행할 검사 명령 |
| interval | 검사 사이의 간격 |
| timeout | 한 번의 검사에 허용하는 시간 |
| retries | 실패를 허용하는 횟수 |
| start_period | 초기 기동 중 실패를 유예하는 시간 |

초기화 시간이 긴 데이터베이스나 JVM 서비스에는 start_period를 충분히 설정해야 합니다.

### 6.2 데이터베이스별 예

PostgreSQL:

~~~yaml
healthcheck:
  test: ["CMD-SHELL", "pg_isready -U postgres"]
  interval: 10s
  timeout: 5s
  retries: 5
  start_period: 30s
~~~

Percona Server for MySQL:

~~~yaml
healthcheck:
  test: ["CMD-SHELL", "mysqladmin ping -h localhost -u root -p$$MYSQL_ROOT_PASSWORD"]
  interval: 10s
  timeout: 5s
  retries: 5
  start_period: 30s
~~~

MongoDB:

~~~yaml
healthcheck:
  test: ["CMD-SHELL", "mongosh --quiet --eval 'db.adminCommand({ ping: 1 })'"]
  interval: 10s
  timeout: 5s
  retries: 5
  start_period: 30s
~~~

Kafka:

~~~yaml
healthcheck:
  test: ["CMD-SHELL", "kafka-broker-api-versions.sh --bootstrap-server localhost:9092"]
  interval: 10s
  timeout: 5s
  retries: 5
  start_period: 60s
~~~

ClickHouse:

~~~yaml
healthcheck:
  test: ["CMD-SHELL", "clickhouse-client --query 'SELECT 1'"]
  interval: 10s
  timeout: 5s
  retries: 5
  start_period: 30s
~~~

검사 명령이 이미지 안에 실제로 존재하는지 확인해야 합니다. 이미지마다 실행 파일 이름과 기본 인증 설정이 다를 수 있습니다.

### 6.3 상태 확인

~~~powershell
docker compose ps
docker inspect --format '{{.State.Health.Status}}' 컨테이너명
~~~

healthcheck 상태는 일반적으로 다음 단계로 진행됩니다.

~~~text
컨테이너 시작
    ↓
starting
    ↓
healthy
또는
unhealthy
~~~

unhealthy 상태가 되었다면 다음을 확인합니다.

1. 검사 명령이 이미지에 존재하는가?
2. 인증 정보가 필요한가?
3. 검사 대상 주소가 localhost인가?
4. 초기화 시간이 start_period보다 긴가?
5. timeout이 지나치게 짧은가?
6. 서비스 로그에 초기화 오류가 있는가?

---

## 7. .env와 .env.example

### 7.1 .env의 역할

Compose에서 .env 파일은 Compose 파일의 환경 변수 값을 관리하는 데 사용할 수 있습니다. 그러나 .env에 적힌 값이 모든 컨테이너의 환경 변수로 자동 주입되는 것은 아닙니다.

다음 세 가지 방식을 구분해야 합니다.

| 방식 | 주요 역할 |
|---|---|
| .env | Compose 파일의 변수 보간과 실행 기본값 |
| env_file | 컨테이너에 환경 변수 주입 |
| environment | Compose 파일에 환경 변수를 직접 선언 |

예를 들어 env_file을 사용하면 다음과 같습니다.

~~~yaml
services:
  application:
    image: example/application:1.0
    env_file:
      - .env.application
~~~

environment를 사용하면 다음과 같습니다.

~~~yaml
services:
  application:
    image: example/application:1.0
    environment:
      NODE_ENV: development
      DATABASE_HOST: postgres
~~~

### 7.2 .env.example

.env.example은 다른 사람이 프로젝트를 실행할 때 필요한 변수 이름과 예시 값을 제공하는 템플릿입니다.

~~~text
POSTGRES_DB=platform
POSTGRES_USER=platform
POSTGRES_PASSWORD=change-me
KAFKA_BROKER_HOST=kafka
KAFKA_BROKER_PORT=9092
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=8123
~~~

실제 값은 로컬 .env 파일에 작성합니다.

~~~text
POSTGRES_DB=platform
POSTGRES_USER=platform
POSTGRES_PASSWORD=local-password
KAFKA_BROKER_HOST=kafka
KAFKA_BROKER_PORT=9092
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=8123
~~~

.gitignore에는 민감 정보가 포함될 수 있는 파일을 추가합니다.

~~~text
.env
.env.*.local
*.secret
~~~

### 7.3 Secret과 환경 변수

환경 변수는 편리하지만 비밀번호와 API 키가 프로세스 정보나 진단 출력에 노출될 가능성이 있습니다.

환경 변수에 적합한 값:

- 서비스 호스트
- 포트
- 로그 레벨
- 실행 모드
- 기능 플래그

Secret으로 분리해야 하는 값:

- 데이터베이스 비밀번호
- API 키
- TLS 개인 키
- 암호화 키

Docker Compose secrets를 사용하는 경우 Secret이 컨테이너 내부 파일로 전달되는 구조를 확인해야 합니다. 애플리케이션이 환경 변수만 읽는다면 Secret 파일을 읽도록 애플리케이션 설정도 함께 변경해야 합니다.

로컬 개발에서는 Git에 커밋하지 않는 별도의 Secret 파일을 사용할 수 있습니다. 운영 환경에서는 조직의 Secret Manager와 배포 플랫폼 정책을 따릅니다.

---

## 8. Compose profiles를 이용한 단계별 실행

### 8.1 profiles의 목적

전체 데이터 플랫폼을 한 번에 실행하면 로컬 컴퓨터의 자원을 많이 사용할 수 있습니다. profiles를 사용하면 계층별로 필요한 서비스를 선택해 실행할 수 있습니다.

이 책의 profiles 구분은 다음과 같습니다.

| profile | 포함 계층 |
|---|---|
| source-db | Percona MySQL, PostgreSQL, MongoDB |
| streaming | Kafka |
| processing | Flink |
| lakehouse | Ozone, Polaris |
| serving | ClickHouse, Superset |
| orchestration | Airflow |

### 8.2 Compose 파일 예

~~~yaml
services:
  mysql:
    image: percona/percona-server:8.4.11
    profiles: ["source-db"]

  postgres:
    image: postgres:17.11
    profiles: ["source-db"]

  mongodb:
    image: mongodb/mongodb-community-server:8.0-ubi8-slim
    profiles: ["source-db"]

  kafka:
    image: apache/kafka:4.3.1
    profiles: ["streaming"]

  flink:
    image: apache/flink:2.2.1
    profiles: ["processing"]

  ozone:
    image: apache/ozone:2.2.0
    profiles: ["lakehouse"]

  polaris:
    image: apache/polaris:1.7.0
    profiles: ["lakehouse"]

  clickhouse:
    image: clickhouse/clickhouse-server:26.6
    profiles: ["serving"]

  superset:
    image: apache/superset:6.1.0
    profiles: ["serving"]

  airflow:
    image: apache/airflow:3.3.1
    profiles: ["orchestration"]
~~~

### 8.3 단계별 실행

~~~powershell
docker compose --profile source-db up -d
docker compose --profile streaming up -d
docker compose --profile processing up -d
docker compose --profile lakehouse up -d
docker compose --profile serving up -d
docker compose --profile orchestration up -d
~~~

여러 profiles를 함께 실행할 수도 있습니다.

~~~powershell
docker compose \
  --profile source-db \
  --profile streaming \
  --profile processing \
  --profile lakehouse \
  --profile serving \
  --profile orchestration \
  up -d
~~~

PowerShell에서는 여러 줄 명령의 줄 연결 문법이 셸 설정에 따라 달라질 수 있으므로, 초보자는 한 줄로 실행하거나 운영체제별 스크립트를 사용하는 편이 안전합니다.

### 8.4 profiles와 depends_on의 관계

depends_on은 서비스의 의존 관계를 표현하지만, 의존 서비스가 다른 profile에 속해 있으면 profile 조합이 실제로 실행 가능한지 확인해야 합니다.

예를 들어 flink가 processing profile에 있고 kafka가 streaming profile에 있다면, processing만 활성화했을 때 kafka가 함께 실행되는지 Compose 버전과 profile 정의를 기준으로 확인해야 합니다.

가장 명확한 방법은 의존 계층을 함께 활성화하는 것입니다.

~~~powershell
docker compose --profile streaming --profile processing up -d
~~~

실행 전 구성 결과를 확인합니다.

~~~powershell
docker compose config
~~~

### 8.5 기본 실행의 의미

프로파일을 지정하지 않고 실행하면 프로파일이 지정되지 않은 서비스가 기본 실행 대상이 됩니다. 프로파일이 지정된 서비스까지 실행하려면 해당 profile을 명시해야 합니다.

~~~powershell
docker compose up -d
~~~

이 명령이 모든 서비스의 실행을 의미한다고 가정하지 않습니다.

---

## 9. 서비스별 named volume

### 9.1 데이터와 설정의 분리

데이터베이스 데이터와 개발자가 편집하는 설정 파일은 저장 방식이 달라야 합니다.

| 대상 | 권장 방식 |
|---|---|
| MySQL 데이터 디렉터리 | named volume |
| PostgreSQL 데이터 디렉터리 | named volume |
| MongoDB 데이터 디렉터리 | named volume |
| Kafka 데이터 | named volume |
| Ozone 데이터 | named volume |
| SQL 초기화 파일 | bind mount |
| Flink 작업 파일 | bind mount |
| Airflow DAG 파일 | bind mount |
| ClickHouse 설정 파일 | bind mount |

### 9.2 데이터베이스 볼륨 예

~~~yaml
services:
  mysql:
    image: percona/percona-server:8.4.11
    volumes:
      - mysql_data:/var/lib/mysql

  postgres:
    image: postgres:17.11
    volumes:
      - postgres_data:/var/lib/postgresql/data

  mongodb:
    image: mongodb/mongodb-community-server:8.0-ubi8-slim
    volumes:
      - mongodb_data:/data/db

volumes:
  mysql_data:
  postgres_data:
  mongodb_data:
~~~

컨테이너를 삭제해도 named volume을 삭제하지 않으면 데이터가 남아 있을 수 있습니다.

### 9.3 설정 파일 bind mount

~~~yaml
services:
  clickhouse:
    image: clickhouse/clickhouse-server:26.6
    volumes:
      - ./clickhouse/init:/docker-entrypoint-initdb.d:ro
~~~

읽기 전용인 설정 파일에는 ro 옵션을 사용하는 것을 고려합니다.

### 9.4 볼륨 관리 명령

~~~powershell
docker volume ls
docker volume inspect mysql_data
docker compose down
~~~

다음 명령은 볼륨까지 삭제할 수 있습니다.

~~~powershell
docker compose down -v
~~~

실습 데이터가 필요하다면 down과 down -v를 구분해야 합니다. down -v는 데이터베이스와 레이크하우스 실습 데이터를 제거할 수 있습니다.

### 9.5 볼륨 백업의 위치

로컬 named volume은 자동으로 백업되지 않습니다. 필요한 경우 별도의 백업 절차를 마련해야 합니다.

백업을 설계할 때는 다음을 구분합니다.

- 데이터베이스 논리 백업
- 파일 기반 볼륨 복사
- Compose 설정 파일 백업
- 버전 매트릭스와 이미지 태그 보존
- 복원 후 초기화 순서

이 책의 로컬 환경은 학습용이므로 백업과 복원은 기본 원리만 설명하고, 운영 수준의 백업 정책은 별도 설계 대상으로 둡니다.

---

## 10. 로그와 재시작 정책

### 10.1 restart 정책

서비스의 성격에 따라 재시작 정책을 선택합니다.

| 정책 | 동작 | 적합한 예 |
|---|---|---|
| no | 종료해도 재시작하지 않음 | 일회성 작업 |
| on-failure | 비정상 종료 시 재시작 | 배치 작업 |
| always | 항상 재시작 | 지속 실행 서비스 |
| unless-stopped | 수동 중지 전까지 재시작 | 로컬 장기 실행 서비스 |

로컬 실습에서는 지속 실행되는 데이터베이스와 처리 엔진에 unless-stopped를 사용할 수 있습니다.

~~~yaml
services:
  postgres:
    image: postgres:17.11
    restart: unless-stopped
~~~

배치나 마이그레이션 컨테이너에 무조건 always를 적용하면 실패 작업이 반복 실행될 수 있습니다.

### 10.2 로그 확인

~~~powershell
docker compose logs
docker compose logs postgres
docker compose logs -f kafka
docker compose logs --tail=100
docker compose logs -t
~~~

장애를 분석할 때는 컨테이너 상태와 로그를 함께 확인합니다.

~~~powershell
docker compose ps
docker compose logs --tail=200 postgres
docker inspect 컨테이너명
~~~

### 10.3 로그 보존

Docker 로그가 계속 쌓이면 호스트 저장 공간을 사용할 수 있습니다. 로그 드라이버와 보존 정책을 구성할 때는 다음을 고려합니다.

- 로그 파일 최대 크기
- 로그 파일 최대 개수
- 로그 보존 기간
- 표준 출력과 애플리케이션 파일 로그의 중복
- 개인정보와 비밀번호의 로그 노출
- 중앙 로그 시스템으로의 전송 여부

예시 설정은 다음과 같습니다.

~~~yaml
services:
  application:
    image: example/application:1.0
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
~~~

이 값은 학습용 예시입니다. 운영 환경에서는 조직의 로그 보존 정책을 적용합니다.

---

## 11. 전체 Compose 설계 원칙

### 원칙 1. 서비스 이름을 사용합니다

컨테이너 IP 주소를 설정 파일에 기록하지 않고 Compose 서비스 이름을 사용합니다.

### 원칙 2. 호스트 공개 포트를 최소화합니다

외부에서 직접 접속해야 하는 UI와 개발용 포트만 호스트에 공개합니다. 서비스 간 통신은 Compose 내부 네트워크를 사용합니다.

### 원칙 3. readiness를 별도로 설계합니다

depends_on만으로 서비스가 사용할 수 있다고 가정하지 않습니다. healthcheck와 애플리케이션 재시도 로직을 함께 사용합니다.

### 원칙 4. 데이터베이스 데이터는 named volume에 보존합니다

호스트 경로와 컨테이너 경로의 차이, 파일 권한, 데이터 지속성을 고려해 데이터 디렉터리를 관리합니다.

### 원칙 5. 설정과 비밀 정보를 분리합니다

설정 파일과 실제 Secret을 같은 파일에 저장하지 않습니다. .env.example에는 실제 비밀번호를 기록하지 않습니다.

### 원칙 6. 계층별 profiles를 사용합니다

필요한 서비스만 실행하여 로컬 자원 사용량과 문제 발생 범위를 줄입니다.

### 원칙 7. 이미지 버전을 고정합니다

latest 대신 책의 버전 매트릭스에 정의한 구체적인 이미지 태그를 사용합니다.

---

## 12. 단일 노드 실습 환경의 한계

이 책의 모든 서비스는 하나의 Windows 11 또는 Apple Silicon Mac에서 실행됩니다. 따라서 다음과 같은 한계가 있습니다.

- 호스트 장애 시 전체 서비스 중단
- CPU와 메모리 경합
- 디스크 장애 시 데이터 손실 가능
- 단일 네트워크 인터페이스 의존
- 진정한 클러스터 장애 조정 미제공
- 운영 수준의 자동 확장 미제공
- 고가용성 보장 없음

컨테이너를 여러 개 실행한다고 해서 물리적 고가용성이 생기는 것은 아닙니다. 세 개의 컨테이너가 모두 같은 노트북에서 실행된다면 노트북 자체가 장애 지점입니다.

### 12.1 실습 환경과 운영 환경 비교

| 항목 | 이 책의 실습 환경 | 운영 환경의 일반적 고려 |
|---|---|---|
| 실행 기반 | 단일 호스트 Docker Compose | 여러 호스트 또는 관리형 플랫폼 |
| Kafka | 단일 브로커 학습 구성 | 다중 브로커와 컨트롤러 쿼럼 |
| 데이터베이스 | 단일 인스턴스 중심 | 복제, 장애 조치, 백업 |
| 저장소 | 로컬 named volume | 복제된 스토리지와 객체 저장소 |
| 카탈로그 | 단일 Polaris 인스턴스 가능 | 가용성과 접근 제어 고려 |
| 네트워크 | 로컬 Docker 네트워크 | 방화벽, 암호화, 네트워크 분리 |
| 복구 | 수동 재기동과 복원 | 자동화된 장애 조치와 복구 절차 |

이 장의 Compose 파일을 운영 환경에 그대로 배포하지 않습니다. 운영 환경에는 별도의 보안, 모니터링, 백업, 장애 복구, 접근 제어 설계가 필요합니다.

---

## 13. 실습 절차

다음 순서로 3장의 설계를 확인합니다.

### 1단계: Compose 파일 검사

~~~powershell
docker compose config
~~~

### 2단계: 소스 데이터베이스 실행

~~~powershell
docker compose --profile source-db up -d
docker compose ps
~~~

### 3단계: healthcheck 확인

~~~powershell
docker compose ps
docker compose logs --tail=100 mysql
docker compose logs --tail=100 postgres
docker compose logs --tail=100 mongodb
~~~

### 4단계: 스트리밍 계층 실행

~~~powershell
docker compose --profile streaming up -d
docker compose ps
docker compose logs --tail=100 kafka
~~~

### 5단계: 처리 계층 실행

~~~powershell
docker compose --profile processing up -d
docker compose ps
docker compose logs --tail=100 flink
~~~

### 6단계: 레이크하우스와 서빙 계층 실행

~~~powershell
docker compose --profile lakehouse --profile serving up -d
docker compose ps
~~~

### 7단계: 전체 상태 확인

~~~powershell
docker compose ps
docker compose logs --tail=100
~~~

서비스가 시작되지 않으면 다음 순서로 원인을 확인합니다.

1. 이미지 태그가 존재하는가?
2. 호스트 포트가 이미 사용 중인가?
3. 이미지의 CPU 아키텍처가 호스트와 맞는가?
4. healthcheck 명령이 이미지 안에 존재하는가?
5. 의존 서비스가 실행 중인가?
6. 볼륨 권한 또는 초기화 오류가 있는가?
7. 컨테이너 로그에 설정 오류가 있는가?

---

## 장 요약

1. Compose 프로젝트는 Compose 파일뿐 아니라 환경 변수, 초기화 파일, 스크립트와 데이터 정책을 함께 관리해야 합니다.
2. 컨테이너 간 통신에서는 IP 주소 대신 Compose 서비스 이름을 사용합니다.
3. 호스트 포트와 컨테이너 포트는 서로 다른 목적을 가집니다.
4. 멀티 네트워크는 서비스 접근 범위와 보안 경계를 구분하는 데 사용합니다.
5. depends_on은 시작 순서를 표현하지만 실제 readiness를 완전히 보장하지 않습니다.
6. service_healthy를 사용하려면 의존 서비스에 healthcheck가 필요합니다.
7. .env, env_file, environment는 역할이 다릅니다.
8. Secret은 일반 환경 변수와 분리하여 관리해야 합니다.
9. profiles를 이용하면 데이터베이스, 스트리밍, 처리, 레이크하우스, 서빙 계층을 단계적으로 실행할 수 있습니다.
10. 데이터베이스 데이터에는 named volume을, 설정과 개발 파일에는 bind mount를 사용하는 것이 적합합니다.
11. restart 정책은 서비스의 성격에 따라 선택해야 합니다.
12. 로그에는 보존 한계와 민감 정보 노출 가능성을 고려해야 합니다.
13. 여러 컨테이너를 하나의 호스트에서 실행하는 것만으로 고가용성이 보장되지는 않습니다.

다음 장에서는 Percona Server for MySQL, PostgreSQL, MongoDB를 CDC의 원천 데이터베이스로 준비하고 각 데이터베이스의 변경 정보 수집 조건을 구성합니다.

## 확인 문제

1. Compose 네트워크에서 다른 서비스에 연결할 때 IP 주소 대신 서비스 이름을 사용해야 하는 이유는 무엇인가요?
2. 호스트 포트와 컨테이너 포트의 차이는 무엇인가요?
3. 멀티 네트워크를 사용하면 어떤 접근 경계를 만들 수 있나요?
4. depends_on short syntax의 한계는 무엇인가요?
5. service_healthy를 사용하려면 무엇이 필요하나요?
6. .env와 env_file은 어떻게 다른가요?
7. .env.example을 Git 저장소에 포함하는 이유는 무엇인가요?
8. profiles를 사용하면 어떤 문제를 줄일 수 있나요?
9. named volume과 bind mount의 용도를 구분해 보세요.
10. restart 정책을 배치 작업에 무조건 적용하면 안 되는 이유는 무엇인가요?
11. Docker 로그 보존 정책을 설정해야 하는 이유는 무엇인가요?
12. 단일 호스트의 여러 컨테이너가 고가용성을 제공하지 못하는 이유는 무엇인가요?

<!--
편집부 확인 메모
- 최종 Compose 파일에 사용할 이미지 태그와 플랫폼을 2장 버전 매트릭스와 대조합니다.
- 각 최종 이미지에서 healthcheck 명령을 실제 실행합니다.
- profiles와 depends_on 조합을 최종 Compose 파일에서 실제 검증합니다.
- Secret의 로컬 파일 방식과 운영 환경 Secret Manager 방식은 구분하여 설명합니다.
- 전체 스택의 CPU·메모리·디스크 권장값은 별도 실험 또는 공식 근거 확보 후 추가합니다.
-->

