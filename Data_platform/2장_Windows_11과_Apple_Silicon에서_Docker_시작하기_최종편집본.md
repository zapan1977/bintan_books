# 2장. Windows 11과 Apple Silicon에서 Docker 시작하기

## 이 장의 목표

이 장에서는 Windows 11과 Apple Silicon Mac에서 데이터 플랫폼 실습을 시작하기 위한 기본 환경을 준비합니다.

다음 작업을 수행할 수 있어야 합니다.

- Docker Desktop의 실행 상태 확인
- Windows 11에서 WSL2 기반 컨테이너 환경 확인
- Apple Silicon에서 ARM64 이미지 확인
- Docker 이미지와 컨테이너의 차이 이해
- 데이터베이스용 named volume 구성
- Docker Compose V2 기본 명령 실행
- Compose profiles와 healthcheck 사용
- Windows PowerShell과 macOS zsh 명령 구분
- 이미지 아키텍처와 버전 확인

이 장의 목적은 전체 데이터 플랫폼을 한 번에 실행하는 것이 아닙니다. 전체 Compose 프로젝트의 디렉터리 구조, 네트워크, Secret, 재시작 정책은 3장에서 설계합니다.

---

## 1. 실습 환경의 전체 구조

이 책은 별도의 가상 머신이나 클라우드 계정 없이 개인용 컴퓨터에서 데이터 플랫폼을 실행합니다.

| 환경 | 기본 실행 방식 | 우선 사용할 컨테이너 아키텍처 |
|---|---|---|
| Windows 11 | Docker Desktop + WSL2 백엔드 | 호스트 환경에 맞는 Linux 이미지 |
| Apple Silicon Mac | Docker Desktop + macOS 가상화 환경 | linux/arm64 네이티브 이미지 |

Docker Desktop은 운영체제에 Docker Engine과 Compose 실행 환경을 제공합니다. 데이터 플랫폼의 각 구성 요소는 컨테이너로 실행하며, 실행 구성은 Docker Compose 파일로 관리합니다.

컨테이너 이미지는 CPU 아키텍처에 따라 별도로 제공될 수 있습니다. 따라서 Windows와 Apple Silicon에서 동일한 이미지 태그를 사용하더라도 실제로 선택되는 이미지가 다를 수 있습니다.

---

## 2. 이 책의 버전 기준

### 2.1 최종 집필 기준안

다음 표는 2026년 8월 29일 기준으로 전달된 피드백 반영 검증 자료를 우선 적용한 기준안입니다.

| 구성 요소 | 기준 버전 | 이미지 또는 사용 방식 | 상태 |
|---|---:|---|---|
| Windows | 11 24H2, build 26100 | 해당 없음 | 확정 |
| macOS | 15 Sequoia, build 24A5327a | 해당 없음 | 확정 |
| Docker Desktop | 4.42.0 | 해당 없음 | 확정 |
| Docker Compose | V2.29.7 | docker compose 명령 | 확정 |
| Percona Server for MySQL | 8.4.11 | percona/percona-server:8.4.11 | 확정 |
| PostgreSQL | 17.11 | postgres:17.11 | 확정 |
| MongoDB Community Server | 8.0 | mongodb/mongodb-community-server:8.0-ubi8-slim | 확정 |
| Apache Kafka | 4.3.1 | apache/kafka:4.3.1 | 확정 |
| Apache Flink | 2.2.1 | apache/flink:2.2.1 | 확정 |
| Apache Spark | 3.5.4 | apache/spark:3.5.4 | 조건부 |
| Apache Iceberg | 1.11.0 | JVM 라이브러리 | 확정 |
| Apache Polaris | 1.7.0 | apache/polaris:1.7.0 | 확정 |
| Apache Ozone | 2.2.0 | apache/ozone:2.2.0 | 조건부 |
| Apache Airflow | 3.3.1 | apache/airflow:3.3.1 | 확정 |
| ClickHouse | 26.6 | clickhouse/clickhouse-server:26.6 | 조건부 |
| Apache Superset | 6.1.0 | apache/superset:6.1.0 | 확정 |

Spark, Ozone, ClickHouse는 ARM64 공식 이미지와 멀티 아키텍처 매니페스트 확인이 완전히 끝나지 않았으므로, 최종 Compose 파일에 넣기 전에 다시 확인해야 합니다.

### 2.2 버전을 고정하는 이유

Docker 이미지를 latest 태그로 지정하면 시간이 지나면서 다른 버전이 내려받아질 수 있습니다. 그러면 같은 책을 따라 해도 독자의 실행 결과가 달라질 수 있습니다.

따라서 다음처럼 구체적인 버전을 사용합니다.

~~~yaml
services:
  postgres:
    image: postgres:17.11
~~~

버전을 고정하면 다음 문제를 줄일 수 있습니다.

- 이미지 내부 기본 설정의 변화
- 이미지 아키텍처의 변화
- 커넥터와 데이터베이스의 호환성 변화
- 책의 장별 실습 버전 불일치
- 재현 과정에서 발생하는 예기치 않은 결과

### 2.3 버전 파일과 Compose 파일의 관계

실습 저장소에서는 버전 정보를 한 곳에서 관리하는 것이 좋습니다.

~~~text
data-platform/
├── compose.yaml
├── .env.example
├── versions.env
├── scripts/
├── init/
└── README.md
~~~

versions.env의 예는 다음과 같습니다.

~~~text
POSTGRES_VERSION=17.11
KAFKA_VERSION=4.3.1
FLINK_VERSION=2.2.1
CLICKHOUSE_VERSION=26.6
~~~

Compose 파일에서 환경 변수를 사용하는 방식은 3장에서 자세히 설명합니다. 이 장에서는 모든 이미지 태그를 고정해야 한다는 원칙만 기억합니다.

---

## 3. Windows 11에서 Docker Desktop 준비하기

### 3.1 Windows 환경의 논리적 흐름

이 책의 Windows 환경은 Docker Desktop과 WSL2 백엔드를 조합합니다.

~~~mermaid
flowchart TD
    A["Windows 11"] --> B["WSL2"]
    B --> C["Docker Desktop"]
    C --> D["Docker Engine"]
    D --> E["Linux 컨테이너"]
~~~

WSL2는 Windows에서 Linux 실행 환경을 제공하고, Docker Desktop은 이 환경과 연결되어 Linux 컨테이너를 실행합니다.

### 3.2 설치 전 확인

설치 전 다음 항목을 확인합니다.

- Windows 11 버전
- WSL2 사용 가능 여부
- 하드웨어 가상화 활성화 여부
- Docker Desktop 설치 가능 여부
- 실습 데이터 저장 공간
- 방화벽과 보안 프로그램의 컨테이너 네트워크 차단 여부

재조사 자료에서는 Windows 11 23H2 이상과 WSL2 2.1.5 이상을 기준으로 제시했으며, 본문의 기준 환경은 Windows 11 24H2입니다.

정확한 설치 조건은 [Docker Desktop 공식 설치 문서](https://docs.docker.com/desktop/install/windows-install/)와 [Microsoft WSL 문서](https://learn.microsoft.com/en-us/windows/wsl/)를 기준으로 확인합니다.

### 3.3 Docker Desktop 설치 후 확인

PowerShell을 열고 다음 명령을 실행합니다.

~~~powershell
docker --version
docker compose version
docker info
~~~

| 명령 | 확인 내용 |
|---|---|
| docker --version | Docker CLI 버전 |
| docker compose version | Compose V2 버전 |
| docker info | Docker Engine 연결 상태와 실행 정보 |

docker info가 정상적으로 결과를 반환하지 않으면 Docker Desktop이 실행 중인지 확인합니다.

### 3.4 WSL2 프로젝트 경로

Windows 경로는 WSL2 내부에서 /mnt/c와 같은 형태로 보일 수 있습니다. 데이터베이스 파일을 Windows 호스트 경로에 직접 bind mount하면 파일 공유 경계와 권한, 파일 I/O 특성을 함께 고려해야 합니다.

이 책에서는 다음 원칙을 사용합니다.

- 데이터베이스 데이터: named volume 우선
- SQL·설정·DAG 파일: bind mount
- 대규모 실습 프로젝트: 가능하면 WSL2 내부 경로에 보관
- Windows와 WSL2 사이의 경로를 명령에 혼용하지 않기

---

## 4. Apple Silicon Mac에서 Docker Desktop 준비하기

### 4.1 ARM64 컨테이너 환경

Apple Silicon Mac은 ARM64 CPU를 사용합니다. ARM64 이미지를 제공하는 서비스는 네이티브 방식으로 실행하는 것이 기본입니다.

~~~mermaid
flowchart TD
    A["Apple Silicon Mac"] --> B["Docker Desktop"]
    B --> C["linux/arm64 이미지"]
    B -. 필요 시 .-> D["linux/amd64 이미지 에뮬레이션"]
~~~

AMD64 이미지를 실행할 수 있다고 해서 모든 서비스가 같은 방식으로 동작하거나 같은 성능을 보장하는 것은 아닙니다. 데이터베이스, JVM 기반 엔진, 파일 시스템을 사용하는 서비스는 이미지와 네이티브 라이브러리의 지원 여부를 확인해야 합니다.

### 4.2 설치 후 확인

터미널에서 다음 명령을 실행합니다.

~~~zsh
docker --version
docker compose version
docker info
uname -m
~~~

이미지의 아키텍처는 다음 명령으로 확인할 수 있습니다.

~~~zsh
docker image inspect postgres:17.11 --format '{{.Architecture}}'
~~~

### 4.3 플랫폼을 지정하여 이미지 내려받기

필요한 경우 이미지 풀 단계에서 플랫폼을 지정합니다.

~~~zsh
docker pull --platform linux/arm64 postgres:17.11
docker pull --platform linux/amd64 postgres:17.11
~~~

두 번째 명령은 ARM64 Mac에서 AMD64 이미지를 선택하는 예입니다. 기본 실습에서는 네이티브 ARM64 이미지를 우선합니다.

특정 이미지에 ARM64가 제공되지 않을 때만 AMD64 에뮬레이션을 검토합니다. 에뮬레이션 성능 저하율은 워크로드와 실행 환경에 따라 달라지므로 고정 수치로 제시하지 않습니다.

---

## 5. Docker의 기본 개념

### 5.1 이미지와 컨테이너

이미지는 컨테이너를 실행하는 데 필요한 파일 시스템과 실행 구성을 묶은 배포 단위입니다. 컨테이너는 이미지를 기반으로 실행된 프로세스입니다.

| 개념 | 의미 |
|---|---|
| 이미지 | 실행에 필요한 파일과 설정의 패키지 |
| 컨테이너 | 이미지로부터 실행된 인스턴스 |
| 레지스트리 | 이미지를 저장하고 배포하는 저장소 |
| 볼륨 | 컨테이너 외부에 데이터를 보존하는 저장 영역 |
| 네트워크 | 컨테이너 사이의 통신 경로 |

컨테이너 내부에만 저장한 데이터는 컨테이너를 삭제할 때 사라질 수 있습니다. 데이터베이스와 같이 지속성이 필요한 서비스는 볼륨을 사용해야 합니다.

### 5.2 기본 Docker 실행

다음은 PostgreSQL 컨테이너의 실행을 확인하는 간단한 예입니다.

~~~powershell
docker pull postgres:17.11
docker run -d --name book-postgres -e POSTGRES_PASSWORD=secret postgres:17.11
docker ps
docker logs book-postgres
docker exec -it book-postgres psql -U postgres
docker stop book-postgres
docker rm book-postgres
~~~

macOS에서도 Docker 명령은 동일합니다.

~~~zsh
docker pull postgres:17.11
docker run -d --name book-postgres -e POSTGRES_PASSWORD=secret postgres:17.11
docker ps
docker logs book-postgres
docker stop book-postgres
docker rm book-postgres
~~~

비밀번호를 명령행에 직접 입력하는 방식은 학습용 예제에만 사용합니다. 실제 프로젝트의 환경 변수와 Secret 관리는 3장에서 다룹니다.

---

## 6. Docker 볼륨

### 6.1 named volume

named volume은 Docker가 관리하는 이름 있는 볼륨입니다. 컨테이너를 다시 만들더라도 볼륨을 유지하면 데이터가 보존됩니다.

~~~yaml
services:
  postgres:
    image: postgres:17.11
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
~~~

데이터베이스 파일을 호스트의 특정 경로와 직접 연결하지 않아도 되므로 Windows와 macOS 간의 경로 차이를 줄일 수 있습니다.

### 6.2 bind mount

bind mount는 호스트의 파일 또는 디렉터리를 컨테이너에 연결합니다.

~~~yaml
services:
  app:
    image: alpine:latest
    volumes:
      - ./scripts:/opt/scripts:ro
~~~

호스트에서 직접 편집해야 하는 설정 파일, SQL 파일, 작업 파일에 적합합니다.

### 6.3 선택 기준

| 사용 대상 | 권장 방식 | 이유 |
|---|---|---|
| 데이터베이스 데이터 디렉터리 | named volume | 데이터 지속성과 호스트 경로 차이 완화 |
| SQL·설정·DAG 파일 | bind mount | 호스트에서 직접 편집 가능 |
| 읽기 전용 설정 | bind mount + ro | 컨테이너의 변경 방지 |
| 일회성 테스트 | 임시 파일 또는 named volume | 테스트 목적에 따라 선택 |

Windows와 macOS의 파일 공유 방식, 권한, Docker Desktop 설정에 따라 동작이 달라질 수 있습니다. 따라서 named volume이 항상 몇 배 빠르다고 일반화하지 않습니다.

### 6.4 볼륨 확인

~~~powershell
docker volume ls
docker volume inspect postgres_data
~~~

다음 명령은 Compose가 관리하는 볼륨까지 삭제할 수 있습니다.

~~~powershell
docker compose down -v
~~~

실습 데이터가 필요한 상태에서는 이 명령을 사용하지 않습니다.

---

## 7. Docker Compose V2

### 7.1 Compose를 사용하는 이유

데이터 플랫폼은 여러 서비스로 구성됩니다. 각 서비스를 docker run으로 실행할 수도 있지만, 환경 변수·볼륨·네트워크·의존 관계를 반복해서 입력해야 합니다.

Compose는 여러 컨테이너의 실행 구성을 YAML 파일로 관리합니다.

이 책에서는 하이픈이 없는 다음 명령을 사용합니다.

~~~text
docker compose
~~~

Compose V2와 Compose Specification의 세부 내용은 [Docker Compose 공식 문서](https://docs.docker.com/compose/)를 기준으로 합니다.

### 7.2 첫 번째 compose.yaml

다음 파일을 compose.yaml로 저장합니다.

~~~yaml
services:
  postgres:
    image: postgres:17.11
    environment:
      POSTGRES_PASSWORD: secret
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
~~~

파일이 있는 디렉터리에서 실행합니다.

~~~powershell
docker compose config
docker compose up -d
docker compose ps
docker compose logs --tail=50 postgres
~~~

macOS에서는 동일한 명령을 사용할 수 있습니다.

~~~zsh
docker compose config
docker compose up -d
docker compose ps
docker compose logs --tail=50 postgres
~~~

### 7.3 기본 명령

| 명령 | 역할 |
|---|---|
| docker compose config | Compose 파일 해석 결과 확인 |
| docker compose up -d | 백그라운드로 서비스 시작 |
| docker compose ps | 서비스 상태 확인 |
| docker compose logs -f | 로그를 계속 출력 |
| docker compose exec 서비스명 명령 | 실행 중인 컨테이너 내부 명령 실행 |
| docker compose stop | 컨테이너 중지 |
| docker compose down | 컨테이너와 네트워크 제거 |
| docker compose down -v | 볼륨까지 삭제할 수 있음 |

실제 서비스 실행 전에는 항상 docker compose config로 YAML의 구조와 환경 변수 보간 결과를 확인합니다.

---

## 8. Compose profiles

Compose profiles는 여러 서비스를 선택적으로 실행하는 기능입니다. 데이터 플랫폼의 모든 서비스를 항상 실행하지 않고 데이터베이스, 스트리밍, 분석 계층을 나누어 실행할 수 있습니다.

~~~yaml
services:
  postgres:
    image: postgres:17.11
    profiles: ["db"]

  kafka:
    image: apache/kafka:4.3.1
    profiles: ["streaming"]

  clickhouse:
    image: clickhouse/clickhouse-server:26.6
    profiles: ["analytics"]
~~~

실행 예시는 다음과 같습니다.

~~~powershell
docker compose --profile db up -d
docker compose --profile streaming up -d
docker compose --profile analytics up -d
~~~

여러 profiles를 함께 활성화할 수도 있습니다.

~~~powershell
docker compose --profile db --profile streaming up -d
~~~

프로파일이 지정된 서비스는 해당 프로파일을 활성화해야 실행됩니다. 프로파일이 지정되지 않은 서비스는 기본 실행 대상이 될 수 있습니다.

실행 구성을 변경한 뒤에는 다음 명령으로 실제 해석 결과를 확인합니다.

~~~text
docker compose config
~~~

Compose profiles의 상세 동작과 의존 서비스 처리 규칙은 3장에서 전체 프로젝트 구조와 함께 설명합니다.

---

## 9. depends_on과 healthcheck

### 9.1 depends_on의 한계

depends_on은 서비스 사이의 시작 순서를 표현합니다.

~~~yaml
services:
  flink:
    image: apache/flink:2.2.1
    depends_on:
      - kafka

  kafka:
    image: apache/kafka:4.3.1
~~~

그러나 컨테이너가 시작되었다고 해서 애플리케이션이 요청을 받을 준비를 마쳤다는 뜻은 아닙니다. Kafka나 데이터베이스가 초기화 중일 수 있습니다.

### 9.2 service_healthy

healthcheck와 service_healthy 조건을 함께 사용하면 의존 서비스의 healthcheck가 성공한 뒤 다음 서비스가 시작되도록 구성할 수 있습니다.

~~~yaml
services:
  postgres:
    image: postgres:17.11
    environment:
      POSTGRES_PASSWORD: secret
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 30s

  flink:
    image: apache/flink:2.2.1
    depends_on:
      postgres:
        condition: service_healthy
~~~

Compose의 조건은 다음과 같습니다.

| 조건 | 의미 |
|---|---|
| service_started | 의존 컨테이너가 시작됨 |
| service_healthy | 의존 서비스의 healthcheck가 성공함 |
| service_completed_successfully | 일회성 서비스가 성공적으로 종료됨 |

service_healthy를 사용하려면 의존 서비스에 healthcheck가 있어야 합니다.

### 9.3 데이터베이스별 healthcheck 예

~~~yaml
services:
  mysql:
    image: percona/percona-server:8.4.11
    environment:
      MYSQL_ROOT_PASSWORD: secret
    healthcheck:
      test: ["CMD-SHELL", "mysqladmin ping -h localhost -u root -p$$MYSQL_ROOT_PASSWORD"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 30s

  mongodb:
    image: mongodb/mongodb-community-server:8.0-ubi8-slim
    healthcheck:
      test: ["CMD-SHELL", "mongosh --quiet --eval 'db.adminCommand({ ping: 1 })'"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 30s
~~~

healthcheck는 컨테이너 안에서 실행됩니다. 테스트 명령이 해당 이미지에 포함되어 있는지 확인해야 합니다.

### 9.4 healthcheck의 한계

healthcheck가 성공해도 다음 작업이 모두 완료되었다고 보장할 수는 없습니다.

- 사용자와 권한 초기화
- 애플리케이션 스키마 생성
- Kafka 토픽 생성
- 외부 저장소 연결
- CDC 커넥터의 이벤트 소비 시작

따라서 컨테이너 수준 healthcheck와 애플리케이션 수준 readiness를 구분해야 합니다. 서비스별 readiness 설계는 3장에서 다룹니다.

---

## 10. Windows PowerShell과 macOS zsh

Docker 명령 대부분은 두 운영체제에서 동일하지만, 셸의 기본 명령은 다릅니다.

### 10.1 환경 변수

| 작업 | PowerShell | macOS zsh 또는 bash |
|---|---|---|
| 값 설정 | $env:VAR = "value" | export VAR=value |
| 값 확인 | $env:VAR | echo $VAR |
| 값 삭제 | Remove-Item Env:VAR | unset VAR |
| 전체 목록 | Get-ChildItem Env: | env 또는 printenv |

### 10.2 파일과 디렉터리

| 작업 | PowerShell | macOS zsh 또는 bash |
|---|---|---|
| 현재 디렉터리 | Get-Location 또는 pwd | pwd |
| 디렉터리 이동 | Set-Location ./path 또는 cd ./path | cd ./path |
| 디렉터리 생성 | New-Item -ItemType Directory ./data | mkdir -p ./data |
| 파일 생성 | New-Item file.txt | touch file.txt |
| 파일 목록 | Get-ChildItem 또는 ls | ls -la |
| 파일 삭제 | Remove-Item file.txt | rm file.txt |

### 10.3 포트와 프로세스

| 작업 | PowerShell | macOS zsh 또는 bash |
|---|---|---|
| 포트 확인 | Get-NetTCPConnection -LocalPort 5432 | lsof -i :5432 |
| 포트 확인 대안 | netstat -ano \| findstr :5432 | netstat -an \| grep :5432 |
| 프로세스 목록 | Get-Process | ps aux |
| 프로세스 종료 | Stop-Process -Id PID | kill PID |

컨테이너가 사용할 호스트 포트가 이미 사용 중이면 Compose가 컨테이너를 시작하지 못할 수 있습니다.

### 10.4 Docker 공통 명령

| 작업 | PowerShell | macOS zsh 또는 bash |
|---|---|---|
| Docker 버전 | docker --version | docker --version |
| Compose 버전 | docker compose version | docker compose version |
| 이미지 풀 | docker pull postgres:17.11 | docker pull postgres:17.11 |
| 컨테이너 시작 | docker start 컨테이너명 | docker start 컨테이너명 |
| 컨테이너 중지 | docker stop 컨테이너명 | docker stop 컨테이너명 |
| 상태 확인 | docker compose ps | docker compose ps |
| 로그 확인 | docker compose logs | docker compose logs |

---

## 11. 이미지 아키텍처 확인

### 11.1 이미지 검사

이미지를 내려받은 뒤 다음 명령으로 아키텍처를 확인합니다.

~~~powershell
docker image inspect postgres:17.11 --format '{{.Architecture}}'
docker image inspect mongodb/mongodb-community-server:8.0-ubi8-slim --format '{{.Architecture}}'
docker image inspect apache/kafka:4.3.1 --format '{{.Architecture}}'
docker image inspect apache/flink:2.2.1 --format '{{.Architecture}}'
~~~

멀티 아키텍처 매니페스트를 확인할 때는 다음 명령을 사용할 수 있습니다.

~~~powershell
docker manifest inspect postgres:17.11
docker manifest inspect mongodb/mongodb-community-server:8.0-ubi8-slim
docker manifest inspect apache/kafka:4.3.1
docker manifest inspect apache/flink:2.2.1
~~~

### 11.2 플랫폼 불일치

다음과 같은 상황이 발생할 수 있습니다.

- 호스트와 이미지의 CPU 아키텍처가 다름
- 에뮬레이션 실행이 필요함
- 해당 태그에 원하는 플랫폼의 매니페스트가 없음
- 이미지 이름 또는 태그가 존재하지 않음

확인 순서는 다음과 같습니다.

1. 공식 배포 태그 확인
2. 호스트 아키텍처 확인
3. 이미지 아키텍처 확인
4. 네이티브 이미지 사용 가능 여부 확인
5. 필요한 경우 플랫폼을 명시
6. 공식 지원이 없으면 버전 또는 이미지 변경 검토

무조건 AMD64를 강제하기보다 네이티브 이미지의 사용 가능 여부를 먼저 확인합니다.

---

## 12. Apache Ozone과 Apache Polaris

### 12.1 Apache Ozone

재조사 자료에서는 Apache Ozone 2.2.0과 ARM64 별도 태그가 제시되어 있습니다.

~~~zsh
docker pull apache/ozone:2.2.0
docker pull apache/ozone:2.2.0-linuxarm64
~~~

다만 Apache Ozone 공식 Docker Hub의 멀티 아키텍처 매니페스트는 최종 확인이 필요합니다. 따라서 최종 Compose 파일에 반영하기 전에 공식 배포 경로와 실제 실행 결과를 확인해야 합니다.

### 12.2 Apache Polaris

Apache Polaris는 Iceberg 테이블의 카탈로그 계층으로 사용합니다.

~~~zsh
docker pull apache/polaris:1.7.0
docker image inspect apache/polaris:1.7.0 --format '{{.Architecture}}'
~~~

이 장에서는 이미지의 풀과 아키텍처 확인까지만 수행합니다. 카탈로그 초기화와 Iceberg 테이블 접근은 후속 장에서 구성합니다.

---

## 13. 성능 수치를 일반화하지 않기

WSL2와 macOS 가상화 환경, named volume과 bind mount, ARM64 네이티브 실행과 AMD64 에뮬레이션 사이에는 차이가 있을 수 있습니다.

그러나 특정 환경의 측정 결과를 모든 환경에 적용할 수는 없습니다.

결과에 영향을 주는 요소는 다음과 같습니다.

- 호스트 CPU와 메모리
- 저장 장치
- 파일 시스템
- Docker Desktop 버전
- WSL2 또는 macOS 가상화 설정
- 파일 크기와 파일 수
- 컨테이너의 CPU·메모리 제한
- 보안 프로그램과 네트워크

따라서 본문에서는 “몇 배 빠르다”와 같은 고정된 수치를 사용하지 않습니다.

### 13.1 선택 실습: named volume 쓰기

~~~zsh
docker volume create book_test_volume
docker run --rm \
  -v book_test_volume:/data \
  alpine:latest \
  sh -c 'dd if=/dev/zero of=/data/test.bin bs=1M count=128 conv=fdatasync'
~~~

실험 결과를 기록할 때는 다음 정보를 함께 남깁니다.

- 운영체제와 버전
- CPU 아키텍처
- Docker Desktop 버전
- 볼륨 종류
- 파일 크기
- 반복 횟수
- 평균과 표준편차

이 실습의 결과를 책의 일반 성능 기준으로 사용하지 않습니다.

---

## 14. 2장과 3장의 범위

| 주제 | 2장 | 3장 |
|---|---|---|
| Docker Desktop 설치 | 포함 | 제외 |
| Windows WSL2 | 포함 | 제외 |
| Apple Silicon 환경 | 포함 | 제외 |
| ARM64 이미지 확인 | 포함 | 제외 |
| Docker 기본 명령 | 포함 | 제외 |
| Compose V2 기본 명령 | 포함 | 보강 |
| named volume·bind mount 개념 | 포함 | 서비스별 설계 |
| Compose profiles 소개 | 포함 | 전체 profiles 설계 |
| depends_on·healthcheck 소개 | 포함 | 서비스별 readiness |
| Compose 네트워크 | 개념만 | 상세 설계 |
| 서비스 이름 기반 통신 | 제외 | 포함 |
| .env와 Secret | 개념만 | 포함 |
| 로그와 재시작 정책 | 개념만 | 포함 |
| 전체 Compose 파일 | 제외 | 포함 |

2장은 실행 환경을 준비하는 장이고, 3장은 여러 서비스를 하나의 데이터 플랫폼 프로젝트로 설계하는 장입니다.

---

## 15. 실습 확인 절차

### Windows 11 PowerShell

~~~powershell
docker --version
docker compose version
docker run --rm hello-world
docker pull postgres:17.11
docker image inspect postgres:17.11 --format '{{.Architecture}}'
~~~

### macOS zsh

~~~zsh
docker --version
docker compose version
docker run --rm hello-world
docker pull postgres:17.11
docker image inspect postgres:17.11 --format '{{.Architecture}}'
~~~

Compose 파일이 있는 디렉터리에서 다음을 실행합니다.

~~~text
docker compose config
docker compose up -d
docker compose ps
docker compose logs --tail=50
docker compose down
~~~

config 단계에서 오류가 발생하면 컨테이너를 시작하기 전에 YAML 구조, 들여쓰기, 이미지 태그와 환경 변수 이름을 확인합니다.

---

## 장 요약

1. Windows 11에서는 Docker Desktop과 WSL2를 기반으로 Linux 컨테이너를 실행합니다.
2. Apple Silicon Mac에서는 ARM64 이미지를 네이티브 방식으로 사용하는 것을 우선합니다.
3. ARM64 이미지가 없을 때 AMD64 에뮬레이션을 검토할 수 있지만, 호환성과 성능을 별도로 확인해야 합니다.
4. 컨테이너 내부 데이터는 삭제될 수 있으므로 데이터베이스에는 볼륨이 필요합니다.
5. named volume은 데이터베이스 데이터에, bind mount는 개발 파일과 설정 파일에 적합합니다.
6. Docker Compose V2에서는 docker compose 명령을 사용합니다.
7. profiles를 이용하면 데이터 플랫폼의 서비스 계층을 선택적으로 실행할 수 있습니다.
8. depends_on은 시작 순서를 표현하지만 애플리케이션의 실제 readiness를 보장하지 않습니다.
9. service_healthy를 사용하려면 의존 서비스에 healthcheck가 필요합니다.
10. PowerShell과 zsh는 환경 변수와 파일 명령의 문법이 다릅니다.
11. 이미지 태그는 재현성을 위해 구체적으로 고정해야 합니다.
12. ARM64 지원과 최종 버전이 확인되지 않은 이미지는 Compose에 반영하기 전에 다시 검증해야 합니다.

다음 장에서는 디렉터리 구조, Compose 네트워크, 환경 변수, 볼륨, profiles, Secret, 로그와 재시작 정책을 포함한 전체 데이터 플랫폼 프로젝트를 설계합니다.

## 확인 문제

1. Docker 이미지와 컨테이너의 차이는 무엇인가요?
2. Windows 11에서 WSL2 백엔드를 사용하는 이유는 무엇인가요?
3. Apple Silicon Mac에서 ARM64 이미지를 우선해야 하는 이유는 무엇인가요?
4. named volume과 bind mount는 각각 어떤 파일에 적합한가요?
5. docker compose config 명령은 언제 사용하나요?
6. depends_on만으로 readiness를 보장할 수 없는 이유는 무엇인가요?
7. service_healthy 조건을 사용하려면 무엇이 필요하나요?
8. PowerShell에서 환경 변수를 설정하는 방법은 무엇인가요?
9. 이미지의 CPU 아키텍처를 확인하는 명령은 무엇인가요?
10. 성능 수치를 모든 독자 환경에 일반화하면 안 되는 이유는 무엇인가요?

<!--
편집부 확인 메모
- Apache Spark 3.5.4 공식 ARM64 이미지 확인 필요
- Apache Ozone 2.2.0 공식 Docker 이미지와 ARM64 매니페스트 확인 필요
- ClickHouse 26.6 공식 ARM64 이미지 확인 필요
- 전체 스택 동시 실행을 위한 최소·권장 CPU·메모리·디스크 용량 확인 필요
- 최종 Compose 저장소에서 사용할 이미지 digest 확정 필요
- healthcheck 명령이 각 최종 이미지에서 실제로 실행되는지 확인 필요
-->

