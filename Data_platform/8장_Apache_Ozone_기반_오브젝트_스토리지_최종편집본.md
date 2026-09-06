# 8장. Apache Ozone 기반 오브젝트 스토리지

## 이 장의 목표

이 장에서는 앞 장에서 Kafka와 Flink CDC를 통해 만든 이벤트와 원본 데이터를 데이터 플랫폼의 저장 계층으로 보내는 방법을 배운다. 저장 대상은 단순한 파일 시스템이 아니라, Iceberg 테이블의 데이터 파일과 메타데이터를 담을 수 있는 오브젝트 스토리지다.

실습은 Windows 11 또는 Apple Silicon Mac의 Docker Compose 환경을 기준으로 한다. Apache Ozone의 핵심 구성 요소, Volume·Bucket·Key 계층, S3 Gateway, Iceberg 웨어하우스 경로, 복제와 Erasure Coding의 차이를 이해한 뒤 AWS CLI로 객체를 생성하고 확인한다.

- 난이도: 초급에서 초중급
- 선수 지식: Docker Compose, Kafka 토픽, Flink CDC 이벤트의 기본 개념
- 이 장의 범위: Apache Ozone 2.2.0 계열을 기준으로 한 로컬 학습 환경
- 다음 장과의 연결: 다음 장에서 다룰 Apache Iceberg가 사용할 객체 저장 위치를 준비한다

> 버전과 이미지의 ARM64 지원 여부는 배포 시점에 다시 확인해야 한다. 이 장의 조사 자료에도 Ozone 2.2.0의 ARM64 태그와 공식 multi-arch manifest에 관한 상충된 기록이 있다. 본문에서 확정하지 않은 내용은 별도로 표시한다.

## 1. 데이터 플랫폼에서 Ozone의 역할

지금까지의 데이터 흐름을 저장 관점에서 다시 보자. 소스 데이터베이스에서 변경이 발생하면 Flink CDC가 변경 이벤트를 만들고, Kafka가 이벤트를 전달한다. 이 이벤트를 장기 보관하고 테이블 형태로 분석하려면, 데이터 파일을 안정적으로 저장할 공간이 필요하다.

이 책에서는 그 저장 계층을 Apache Ozone으로 구성한다. Ozone은 내부적으로 여러 구성 요소를 사용하지만, 애플리케이션 입장에서는 S3 호환 API를 제공하는 오브젝트 스토리지로 접근할 수 있다.

~~~mermaid
flowchart LR
    DB["소스 DB<br/>MySQL · PostgreSQL · MongoDB"] --> CDC["Flink CDC"]
    CDC --> K["Apache Kafka"]
    K --> F["Flink 처리"]
    F --> S3["Ozone S3 Gateway"]
    S3 --> W["Iceberg Warehouse"]
    W --> CH["ClickHouse 분석"]
~~~

이 구조에서 Ozone이 담당하는 일은 다음과 같다.

| 계층 | 담당 역할 | 이 책에서의 의미 |
|---|---|---|
| Ozone | 객체와 파일의 저장 | Raw 이벤트와 Iceberg 파일을 보관 |
| S3 Gateway | S3 API 제공 | AWS CLI와 Flink가 Ozone에 접근하는 통로 |
| Iceberg | 테이블 메타데이터와 데이터 파일 관리 | 다음 장에서 본격적으로 다룸 |
| ClickHouse | 분석 쿼리와 집계 | 저장된 데이터를 소비하는 분석 계층 |

Ozone 자체가 Kafka나 Flink의 역할을 대신하는 것은 아니다. Ozone은 이벤트를 생성하거나 변환하지 않는다. 이미 생성된 파일 또는 객체를 저장하고, 클라이언트가 다시 읽을 수 있도록 제공한다.

이 구분이 중요한 이유는 저장 장애와 처리 장애를 다르게 진단해야 하기 때문이다. Kafka에 이벤트가 쌓이지 않는 문제는 메시징 계층의 문제이고, Flink가 파일을 만들었지만 S3 Gateway로 업로드하지 못하는 문제는 저장 접근 경로의 문제다.

## 2. Apache Ozone의 핵심 구성 요소

Ozone은 하나의 단일 서버 프로세스가 아니다. 네임스페이스를 관리하는 구성 요소와 실제 데이터를 저장하는 구성 요소가 나뉜다.

### 2.1 구성 요소별 역할

| 구성 요소 | 약어 | 주요 책임 |
|---|---:|---|
| Ozone Manager | OM | Volume·Bucket·Key 네임스페이스와 메타데이터 관리 |
| Storage Container Manager | SCM | 컨테이너, 블록 할당, 데이터노드와 저장 파이프라인 관리 |
| Datanode | DN | 컨테이너와 실제 데이터 블록 저장 |
| S3 Gateway | S3G | S3 REST 요청을 Ozone 네이티브 연산으로 변환 |
| Recon | - | Ozone 상태와 운영 정보를 모니터링하는 선택 구성 요소 |

OM은 “어떤 객체가 존재하는가”를 관리한다. 예를 들어 /lakehouse/warehouse/events/data.parquet라는 키가 어느 버킷에 속하는지, 메타데이터가 무엇인지와 같은 네임스페이스 정보를 담당한다.

SCM은 데이터가 저장될 컨테이너와 블록의 배치를 조정한다. 복제 또는 Erasure Coding을 사용하는 경우에도 저장 정책을 적용하는 중심 구성 요소가 SCM이다.

Datanode는 데이터를 실제로 보관한다. 애플리케이션이 직접 Datanode의 파일을 조작하는 방식이 아니라, OM과 SCM을 거쳐 관리되는 저장 단위를 사용한다.

S3 Gateway는 외부 클라이언트가 가장 자주 만나는 진입점이다. AWS CLI와 같은 S3 클라이언트가 PUT 또는 GET 요청을 보내면 S3 Gateway가 이를 Ozone의 내부 연산으로 변환한다. S3 Gateway는 상태 비저장(stateless) 서비스로 설명되므로, 본문 데이터의 영속적인 저장 책임은 Ozone의 저장 계층에 있다.

~~~mermaid
flowchart TB
    C["S3 Client<br/>AWS CLI · Flink"] --> G["S3 Gateway"]
    G --> O["Ozone Manager<br/>namespace"]
    O --> S["SCM<br/>containers · pipelines"]
    S --> D1["Datanode<br/>blocks"]
    S --> D2["Datanode<br/>blocks"]
~~~

로컬 학습 환경에서는 이 구성 요소들을 여러 Docker 컨테이너로 실행할 수 있다. 다만 컨테이너 수가 늘어났다고 해서 물리적으로 여러 서버를 구성한 것은 아니다. 이 차이는 6절에서 다시 설명한다.

### 2.2 학습자가 기억할 호출 흐름

객체 하나를 업로드하는 과정을 단순화하면 다음과 같다.

1. 클라이언트가 S3 Gateway에 객체 업로드 요청을 보낸다.
2. S3 Gateway가 요청을 Ozone 네이티브 API 호출로 변환한다.
3. OM이 버킷과 키의 네임스페이스를 확인한다.
4. SCM이 저장할 컨테이너와 블록을 배정한다.
5. Datanode가 실제 데이터를 저장한다.
6. 클라이언트에 업로드 결과가 반환된다.

이 흐름을 이해하면 로그를 볼 때도 위치를 추정할 수 있다. 버킷을 찾지 못했다면 OM 또는 네임스페이스 설정을 먼저 확인하고, 저장 파이프라인 오류가 발생했다면 SCM과 Datanode 상태를 확인한다. S3 요청 자체가 도달하지 않았다면 S3 Gateway의 포트와 endpoint를 확인한다.

## 3. Volume, Bucket, Key 계층

Ozone의 기본 네임스페이스는 Volume, Bucket, Key로 구성된다. AWS S3의 Bucket과 Object에 익숙한 독자라면 Bucket과 Key는 비교적 쉽게 이해할 수 있지만, Ozone에서는 그 위에 Volume이 존재한다.

~~~mermaid
flowchart TB
    V["Volume<br/>/lakehouse"] --> B["Bucket<br/>/warehouse"]
    B --> K1["Key<br/>events/data.parquet"]
    B --> K2["Key<br/>events/metadata.json"]
~~~

### 3.1 Volume

Volume은 버킷을 묶는 상위 논리 공간이다. 조직, 테넌트 또는 저장 영역을 나누는 단위로 사용할 수 있다.

Ozone 네이티브 CLI를 사용하면 다음과 같이 Volume을 만들고 확인할 수 있다.

~~~bash
# 컨테이너 이름은 이 책의 로컬 예시를 따른다.
docker exec ozone ozone sh volume create /lakehouse

# Volume 목록 확인
docker exec ozone ozone sh volume list /

# Volume 정보 확인
docker exec ozone ozone sh volume info /lakehouse
~~~

이 책에서는 다음과 같은 이름을 사용한다.

| 목적 | 예시 경로 |
|---|---|
| Iceberg 관련 저장 영역 | /lakehouse |
| S3 Gateway의 기본 Volume 예시 | /s3v |
| 웨어하우스 Bucket | /lakehouse/warehouse |
| S3 방식으로 접근할 Bucket | /s3v/iceberg-bucket |

Volume 이름과 Bucket 이름은 이후 설정 파일과 Flink 작업의 warehouse 경로에 반복해서 등장한다. 따라서 처음부터 의미가 분명한 이름을 사용한다.

### 3.2 Bucket

Bucket은 데이터를 담는 논리적인 저장 공간이다. S3 Bucket과 유사한 역할을 하며, Key가 Bucket 아래에 위치한다.

~~~bash
# FILE_SYSTEM_OPTIMIZED 레이아웃을 사용하는 예시
docker exec ozone ozone sh bucket create /lakehouse/warehouse

# S3 접근을 위한 OBJECT_STORE 레이아웃 예시
docker exec ozone ozone sh bucket create /s3v/iceberg-bucket --layout OBJECT_STORE

# Bucket 정보 확인
docker exec ozone ozone sh bucket info /lakehouse/warehouse
~~~

버킷 레이아웃은 생성 시점의 중요한 선택 사항이다. 조사 자료에서는 FILE_SYSTEM_OPTIMIZED와 OBJECT_STORE가 구분되어 있으며, S3용 버킷에는 OBJECT_STORE를 사용하는 예시가 제시되어 있다.

실습에서 버킷을 만들 때는 경로를 정확히 구분한다.

- 네이티브 CLI 경로: /lakehouse/warehouse
- S3 URL 표현: s3://iceberg-bucket/...
- S3 Gateway 내부의 기본 Volume: /s3v

S3 클라이언트가 Volume을 일반 S3 Bucket처럼 직접 노출한다고 단정해서는 안 된다. S3 Gateway는 별도의 설정과 매핑 규칙을 사용한다. 이 장에서는 S3 클라이언트가 접근할 Bucket과 Ozone 네이티브 CLI가 접근할 경로를 구분해 사용한다.

### 3.3 Key

Key는 Bucket 안에 저장되는 객체 이름이다. 로컬 파일 시스템의 파일과 비슷하게 보이지만, 오브젝트 스토리지에서 슬래시는 계층처럼 보이는 이름의 일부로 취급될 수 있다.

Iceberg를 사용하면 하나의 테이블 아래에 다음과 같은 Key가 생길 수 있다.

~~~text
warehouse/
└── events/
    ├── data/
    │   ├── 00000-0-12345-00001.parquet
    │   └── 00001-1-12346-00002.parquet
    └── metadata/
        ├── v1.metadata.json
        └── snap-12345-1-abcde.avro
~~~

여기서 Parquet 파일은 데이터 파일이고, metadata JSON과 Manifest Avro 파일은 테이블의 상태와 파일 목록을 설명하는 메타데이터 파일이다. 구체적인 Iceberg 스냅샷 구조와 파일 포맷은 다음 장에서 다룬다.

## 4. S3 Gateway로 Ozone 접근하기

### 4.1 S3 Gateway의 역할

S3 Gateway는 S3 REST API를 받아 Ozone의 네이티브 저장 연산으로 연결하는 프로토콜 어댑터다. 따라서 애플리케이션은 Ozone 내부 구현을 직접 알지 않아도 S3 클라이언트 방식으로 객체를 업로드하고 다운로드할 수 있다.

조사 자료에서는 AWS CLI, AWS SDK, s3fs, MinIO Client 등 S3 클라이언트 계열의 접근 예시를 제시한다. 다만 S3 호환 구현은 제품마다 지원 범위가 다를 수 있으므로, 운영에 사용하기 전에는 사용하는 API와 기능을 직접 검증해야 한다.

S3 Gateway의 기본 실습 endpoint는 다음과 같이 정리한다.

| 항목 | 로컬 실습 값 |
|---|---|
| HTTP endpoint | http://localhost:9878 |
| HTTPS endpoint 예시 | 9879 |
| 기본 S3 Gateway Volume 예시 | s3v |
| 버킷 레이아웃 예시 | OBJECT_STORE |
| 주소 지정 방식 | path-style 권장 또는 필요 |

HTTP는 로컬 실습을 단순하게 만들기 위한 설정이다. 운영 환경에서는 TLS, 인증, 키 관리 정책을 별도로 구성해야 한다.

### 4.2 설정 파일에서 확인할 항목

조사 자료에는 다음과 같은 Ozone 설정 항목이 제시되어 있다.

~~~xml
<!-- ozone-site.xml의 개념 예시 -->
<property>
  <name>ozone.s3g.volume.name</name>
  <value>s3v</value>
</property>

<property>
  <name>ozone.s3g.default.bucket.layout</name>
  <value>OBJECT_STORE</value>
</property>
~~~

설정 파일의 실제 위치는 사용하는 이미지와 Docker Compose 구성에 따라 달라질 수 있다.

[추가 자료 조사 필요: Apache Ozone 2.2.0 공식 Docker 이미지에서 설정 파일을 마운트하는 권장 경로와 멀티 컨테이너 초기화 순서]

초보자는 설정 파일을 수정한 뒤 반드시 컨테이너 재기동 여부를 확인해야 한다. 설정 변경이 실행 중인 프로세스에 자동 반영된다고 가정하면 안 된다.

### 4.3 AWS CLI 설정

AWS CLI에서는 endpoint를 매번 명시하면 실습 환경의 동작을 확인하기 쉽다. 아래 예시는 로컬 S3 Gateway에 임시 자격 증명으로 접근하는 형태다.

~~~bash
# 실습용 임시 자격 증명
aws configure set aws_access_key_id ozone
aws configure set aws_secret_access_key ozone-secret
aws configure set default.region us-east-1

# Ozone S3 Gateway endpoint 확인
aws s3 ls --endpoint-url http://localhost:9878
~~~

path-style 접근이 필요한 환경에서는 다음 설정도 함께 사용한다.

~~~bash
aws configure set default.s3.addressing_style path
~~~

AWS CLI 버전과 설정 키 이름에 따라 이 옵션의 표시 방식이 달라질 수 있다. 따라서 명령이 실패하면 endpoint와 인증 오류를 구분해서 확인한다.

### 4.4 객체 생성·업로드·다운로드

먼저 S3 버킷을 만든다.

~~~bash
# 버킷 생성
aws s3 mb s3://iceberg-bucket \
  --endpoint-url http://localhost:9878

# 버킷 목록 확인
aws s3 ls \
  --endpoint-url http://localhost:9878
~~~

로컬 파일을 업로드하고 다시 목록을 확인한다.

~~~bash
# 테스트 파일 생성
echo "ozone-lab" > ozone-test.txt

# 객체 업로드
aws s3 cp ozone-test.txt \
  s3://iceberg-bucket/lab/ozone-test.txt \
  --endpoint-url http://localhost:9878

# 객체 목록 확인
aws s3 ls s3://iceberg-bucket/lab/ \
  --endpoint-url http://localhost:9878
~~~

다운로드와 삭제도 같은 endpoint를 사용한다.

~~~bash
# 객체 다운로드
aws s3 cp \
  s3://iceberg-bucket/lab/ozone-test.txt \
  ozone-test-downloaded.txt \
  --endpoint-url http://localhost:9878

# 객체 삭제
aws s3 rm \
  s3://iceberg-bucket/lab/ozone-test.txt \
  --endpoint-url http://localhost:9878
~~~

Windows PowerShell에서도 명령의 기본 형태는 동일하다. 다만 파일 생성은 다음처럼 작성할 수 있다.

~~~powershell
"ozone-lab" | Set-Content -Path .\ozone-test.txt
~~~

명령 결과를 확인할 때는 다음 세 가지를 함께 기록한다.

1. 요청 endpoint가 http://localhost:9878인지
2. 버킷 이름과 Key 경로가 일치하는지
3. 업로드 전후의 목록 결과가 달라졌는지

이 세 가지를 확인하면 “업로드 명령은 성공했지만 다른 버킷을 보고 있는” 실수를 줄일 수 있다.

## 5. Iceberg 웨어하우스 저장 경로

### 5.1 Ozone과 Iceberg의 관계

Iceberg는 테이블의 데이터 파일과 메타데이터 파일을 객체 저장소에 둘 수 있다. Ozone은 그 파일을 저장하는 공간을 제공한다.

개념적인 웨어하우스 경로는 다음과 같이 표현할 수 있다.

~~~text
s3://iceberg-bucket/warehouse/
~~~

또는 Hadoop S3A 설정을 사용하는 작업에서는 다음과 같은 형태를 사용한다.

~~~text
s3a://iceberg-bucket/warehouse/
~~~

어떤 URI 스킴을 사용할지는 Flink, Spark, Iceberg Catalog, S3 파일 I/O 구현에 따라 달라진다. 따라서 URI를 무조건 s3와 s3a 중 하나로 고정하기보다, 해당 실행 엔진의 설정과 함께 관리해야 한다.

웨어하우스 아래에는 일반적으로 테이블 데이터와 메타데이터가 함께 저장된다.

| 경로 예시 | 의미 |
|---|---|
| warehouse/events/data/ | Parquet 데이터 파일 |
| warehouse/events/metadata/ | Iceberg 메타데이터 JSON |
| warehouse/events/metadata/ | Manifest 및 스냅샷 관련 파일 |

Ozone을 직접 확인할 때는 파일이 “일반적인 로컬 디렉터리”가 아니라 Bucket 안의 Key라는 점을 기억한다. 데이터 파일의 목록만 봐서는 테이블이 정상인지 판단할 수 없고, Iceberg 메타데이터까지 함께 확인해야 한다.

### 5.2 Flink와 S3A 설정의 핵심

조사 자료에서는 Flink 또는 Spark가 Ozone S3 Gateway를 사용하도록 endpoint, path-style access, SSL 여부를 설정하는 예시를 제시한다.

~~~properties
# 개념적인 S3A 설정
fs.s3a.endpoint=http://ozone-s3g:9878
fs.s3a.path.style.access=true
fs.s3a.connection.ssl.enabled=false
fs.s3a.access.key=ozone
fs.s3a.secret.key=ozone-secret
~~~

여기서 ozone-s3g는 Docker Compose 네트워크 안에서 S3 Gateway 서비스에 접근할 때의 이름 예시다. 호스트에서 실행하는 AWS CLI는 localhost:9878을 사용하지만, 다른 컨테이너 안에서 실행되는 Flink는 Compose 서비스 이름과 컨테이너 포트를 사용해야 한다.

이 차이를 표로 정리하면 다음과 같다.

| 클라이언트 위치 | endpoint 예시 |
|---|---|
| Windows 또는 Mac 호스트의 AWS CLI | http://localhost:9878 |
| Flink 컨테이너 | http://ozone-s3g:9878 |
| 별도 Docker 네트워크의 애플리케이션 | Compose DNS로 해석되는 S3 Gateway 서비스명 |

[추가 자료 조사 필요: 이 책의 전체 Compose 파일에서 사용할 실제 Ozone S3 Gateway 서비스명, Flink 이미지의 S3A 의존성, Iceberg Catalog 설정값]

### 5.3 Polaris Catalog를 사용할 때의 경계

조사 자료에는 Apache Polaris를 이용해 Ozone을 저장소로 사용하는 로컬 레이크하우스 예시가 포함되어 있다. Polaris는 Iceberg Catalog 역할을 담당하는 별도 구성 요소이고, Ozone은 데이터 파일과 메타데이터가 저장되는 객체 저장소 역할을 한다.

두 역할을 혼동하지 않도록 다음처럼 나눈다.

| 구성 요소 | 관리 대상 |
|---|---|
| Ozone | 객체, Parquet 파일, Iceberg 메타데이터 파일 |
| Polaris | Iceberg Catalog와 테이블 등록 정보 |
| Flink 또는 Spark | 데이터 처리와 테이블 읽기·쓰기 |

이 책의 전체 스택을 “Apache 솔루션 중심”으로 유지하려면 Polaris를 본문 기본 구성에 포함할지 결정해야 한다.

[검토 필요: Apache-only 원칙에서 Apache Polaris를 Iceberg Catalog 기본값으로 채택할지 여부]

이 장에서는 Ozone의 저장 경로와 접근 방식을 이해하는 데 집중하고, Catalog의 설치와 테이블 생성은 다음 장의 범위로 넘긴다.

## 6. 복제와 Erasure Coding

오브젝트 스토리지에서 데이터 보호 방식은 저장 용량과 장애 대응 방식에 영향을 준다. Ozone 조사 자료에서는 복제와 Erasure Coding을 비교한다.

### 6.1 복제

복제는 동일한 데이터를 여러 개 보관하는 방식이다. RF는 Replication Factor를 의미한다.

| 설정 | 논리적 저장 배수 | 계산상 사용 가능 비율 | 자료에 제시된 장애 허용 개념 |
|---|---:|---:|---|
| RF=1 | 1배 | 100% | 복제본 없음 |
| RF=2 | 2배 | 50% | 1개 노드 장애 |
| RF=3 | 3배 | 약 33% | 2개 노드 장애 |

RF=3은 저장 공간을 세 배 사용하지만, 복제본을 여러 물리적 노드에 배치할 수 있을 때 장애 대응에 유리하다. 그러나 같은 호스트 안에서 Datanode 컨테이너 세 개를 실행하는 것만으로는 호스트 장애를 보호하지 못한다.

### 6.2 Erasure Coding

Erasure Coding(EC)은 원본 데이터를 데이터 조각과 패리티 조각으로 나누어 저장한다. 예를 들어 EC-6-3은 데이터 조각 6개와 패리티 조각 3개를 사용하는 프로파일로 설명된다.

| 프로파일 | 데이터 조각 | 패리티 조각 | 계산상 저장 배수 | 자료에 제시된 장애 허용 개념 |
|---|---:|---:|---:|---|
| EC-2-1 | 2 | 1 | 1.5배 | 1개 조각 손실 |
| EC-4-2 | 4 | 2 | 1.5배 | 2개 조각 손실 |
| EC-6-3 | 6 | 3 | 1.5배 | 3개 조각 손실 |
| EC-8-3 | 8 | 3 | 약 1.375배 | 3개 조각 손실 |
| EC-10-4 | 10 | 4 | 1.4배 | 4개 조각 손실 |

복제는 읽기와 복구 구조가 상대적으로 단순하고, 자주 읽고 쓰는 데이터에 적합한 선택으로 설명된다. EC는 저장 공간 효율이 높지만, 조각 계산과 복구가 필요하므로 데이터 특성과 접근 패턴을 확인해야 한다.

### 6.3 학습 환경과 운영 환경을 분리해서 판단하기

조사 자료에는 단일 노드 실습 환경에서 RF=3을 사용하라는 권장과, 단일 호스트에서는 복제본이 물리적으로 분산되지 않는다는 설명이 함께 있다. 두 문장은 서로 다른 목표를 말한다.

- 논리적 복제 동작을 관찰하는 실습: 여러 Datanode 컨테이너로 RF 개념을 확인할 수 있다.
- 호스트 장애를 견디는 운영 설계: 서로 다른 물리 노드와 네트워크 장애 도메인이 필요하다.

따라서 이 책의 기본 실습에서는 저장 공간과 재현성을 우선해 단일 호스트 프로파일을 사용하고, RF=3은 “복제 정책을 실험하는 선택 프로파일”로 설명한다.

[검토 필요: 단일 호스트 실습의 기본 RF 값을 RF=1로 고정할지, 여러 Datanode 컨테이너를 사용한 개념 검증을 위해 RF=3을 선택할지]

운영 설계의 예시로는 다음과 같이 정리할 수 있다.

| 상황 | 우선 고려할 선택 |
|---|---|
| 로컬 초급 실습 | RF=1 또는 저장 공간을 고려한 최소 구성 |
| 복제 동작 학습 | 여러 Datanode와 RF=3을 별도 실험 |
| 읽기 빈도가 높은 데이터 | 복제 방식 검토 |
| 장기 보관·콜드 데이터 | EC 프로파일 검토 |
| 호스트 장애 대응 | 물리적으로 분리된 다중 노드 |

## 7. Windows 11과 Apple Silicon의 로컬 실행 제약

### 7.1 로컬 구성은 운영 클러스터가 아니다

이 책의 Ozone은 Docker Compose로 하나의 호스트에서 실행한다. 운영 환경과 비교하면 다음과 같은 차이가 있다.

| 구성 요소 | 운영 환경의 일반적 요구 방향 | 이 책의 학습 환경 |
|---|---|---|
| OM | 여러 인스턴스와 쿼럼 고려 | 1개 |
| SCM | HA 구성 고려 | 1개 |
| Datanode | 여러 물리 노드 | 여러 컨테이너 또는 최소 구성 |
| S3 Gateway | 2개 이상 HA 고려 | 1개 |
| 장애 도메인 | 물리 서버·랙·네트워크 분리 | 하나의 Windows 또는 Mac 호스트 |

컨테이너를 여러 개 실행해도 CPU, 메모리, 디스크, 운영체제 커널은 같은 호스트를 공유한다. 따라서 호스트가 중단되면 모든 컨테이너가 함께 영향을 받는다.

본문에서 로컬 환경을 다음처럼 표현한다.

> 이 장의 환경은 Ozone의 구성 요소와 데이터 흐름을 학습하기 위한 단일 호스트 Docker Compose 환경이다. 고가용성과 호스트 장애 내성을 제공하는 운영 클러스터가 아니다.

### 7.2 리소스 계획

Ozone은 JVM 기반 구성 요소를 포함한다. 조사 자료는 여러 Datanode 컨테이너를 실행하는 로컬 환경에서 메모리 8GB 이상과 CPU 4코어 이상을 권장한다.

이 값은 “실습이 가능하기 위한 참고 기준”으로 사용한다. 실제 필요한 리소스는 실행하는 구성 요소 수, JVM 힙 설정, 동시에 기동하는 Kafka·Flink·ClickHouse의 수에 따라 달라진다.

실행 전 Docker Desktop의 리소스 설정에서 다음을 점검한다.

- Docker Desktop에 할당한 메모리
- 사용할 CPU 코어 수
- Ozone 데이터 볼륨의 디스크 위치
- Kafka와 Flink를 동시에 실행할 때의 메모리 여유
- Apple Silicon에서 이미지 아키텍처가 일치하는지

### 7.3 Ozone 이미지 아키텍처 확인

조사 자료에는 다음 두 가지 주장이 모두 있다.

1. apache/ozone:2.2.0이 Apple Silicon에서 ARM64로 실행될 수 있다는 기록
2. 공식 multi-arch manifest가 확인되지 않았고, 별도의 linuxarm64 태그를 사용해야 한다는 기록

이 때문에 책의 최종 배포본에서는 Docker Hub의 공식 이미지 manifest와 Apache Ozone 2.2.0 릴리스 정보를 배포 직전에 확인해야 한다.

~~~bash
# Apple Silicon에서 실행 아키텍처를 명시하는 확인 예시
docker pull --platform linux/arm64 apache/ozone:2.2.0

# 별도 ARM64 태그가 실제로 제공되는지 확인한 뒤 사용할 수 있다.
docker pull apache/ozone:2.2.0-linuxarm64

# 내려받은 이미지의 정보를 확인하는 예시
docker image inspect apache/ozone:2.2.0
~~~

[추가 자료 조사 필요: Apache Ozone 공식 Docker Hub의 2.2.0 multi-arch manifest, linuxarm64 태그의 공식성, Windows amd64와 Apple Silicon arm64에서의 실제 Compose 기동 결과]

확인되지 않은 이미지 태그를 책의 기본값으로 확정하면 독자가 첫 단계에서 막힐 수 있다. 최종 출간 시점에는 다음 정책 중 하나를 선택해야 한다.

- 공식 multi-arch 이미지가 확인되면 하나의 태그를 공통 사용
- 플랫폼별 공식 태그가 확인되면 운영체제별 변수를 Compose에 적용
- 공식 이미지가 특정 플랫폼을 지원하지 않으면 대체 실행 방식과 제한을 별도 장으로 분리

## 8. 인증, Access Key와 ACL

### 8.1 실습 인증과 운영 인증

조사 자료는 실습 환경에서 Simple 모드를 사용하고, 운영 환경에서 Kerberos 등을 고려하는 방식으로 구분한다.

| 방식 | 특징 | 적용 범위 |
|---|---|---|
| Simple | 별도 인증을 사용하지 않는 단순 모드 | 격리된 로컬 실습 |
| Kerberos | 티켓 기반 인증 | 운영 보안 환경 |
| S3 Access Key | AWS 스타일 Access Key와 Secret Key | S3 Gateway 클라이언트 |

Simple 모드는 로컬 학습을 쉽게 해 주지만, 네트워크에 공개된 환경에서 사용하면 안 된다. 실습 endpoint를 외부에 노출하지 않고, Docker 내부 또는 로컬 호스트에서만 접근하도록 제한한다.

S3 클라이언트에는 다음과 같은 실습용 값을 사용할 수 있다.

~~~bash
export AWS_ACCESS_KEY_ID=ozone
export AWS_SECRET_ACCESS_KEY=ozone-secret
~~~

PowerShell에서는 다음처럼 설정한다.

~~~powershell
$env:AWS_ACCESS_KEY_ID = "ozone"
$env:AWS_SECRET_ACCESS_KEY = "ozone-secret"
~~~

이 값은 책의 학습용 예시일 뿐이다. 운영 환경에서는 소스 코드, Compose 파일, 셸 히스토리에 Secret Key를 직접 넣지 않는다.

### 8.2 ACL의 계층

Ozone의 접근 권한은 Volume, Bucket, Key 계층과 함께 이해해야 한다.

| 권한 | 의미 |
|---|---|
| READ | 읽기 |
| WRITE | 생성·수정·삭제와 관련된 쓰기 |
| READ_ACL | ACL 읽기 |
| WRITE_ACL | ACL 변경 |
| ALL | 위 권한을 포함한 전체 권한 |

조사 자료의 ACL 명령 예시는 다음과 같다.

~~~bash
# Bucket ACL 추가
docker exec ozone ozone sh bucket acl add \
  /lakehouse/warehouse user:alice:rwxy

# Bucket ACL 확인
docker exec ozone ozone sh bucket acl get \
  /lakehouse/warehouse

# Bucket ACL 제거
docker exec ozone ozone sh bucket acl remove \
  /lakehouse/warehouse user:alice
~~~

rwxy와 같은 ACL 문자열의 문자별 의미는 Ozone 버전과 CLI 문서에서 다시 확인해야 한다.

[추가 자료 조사 필요: Ozone 2.2.0 ACL 문자열의 문자별 매핑과 Simple 모드에서 AWS CLI 요청에 적용되는 권한 규칙]

운영 환경에서는 중앙 권한 관리와 감사 로그가 필요할 수 있다. 조사 자료에는 Apache Ranger와의 통합이 운영 선택지로 제시되어 있지만, 이 책의 로컬 실습 범위에서는 다루지 않는다.

## 9. 데이터 삭제와 볼륨 초기화

### 9.1 객체 삭제가 곧바로 디스크 회수는 아니다

Ozone 네이티브 CLI로 Key를 삭제하는 예시는 다음과 같다.

~~~bash
docker exec ozone ozone sh key delete \
  /lakehouse/warehouse/events/data/example.parquet
~~~

S3 API로는 다음과 같이 삭제한다.

~~~bash
aws s3 rm \
  s3://iceberg-bucket/warehouse/events/data/example.parquet \
  --endpoint-url http://localhost:9878
~~~

조사 자료는 스냅샷이 존재하는 경우 삭제된 데이터가 즉시 물리적으로 제거되지 않고 pendingDeleteSnapshotBytes에 반영될 수 있다고 설명한다. 따라서 사용량을 확인할 때는 활성 데이터와 삭제 대기 데이터가 함께 표시될 수 있다.

이 차이를 다음과 같이 기억한다.

- 논리 삭제: 사용자가 객체 목록에서 더 이상 사용하지 않도록 표시
- 물리 회수: 스냅샷 보존 정책과 내부 정리 과정이 끝난 후 저장 공간 회수
- 사용량 보고: 활성 데이터와 삭제 대기 데이터가 함께 고려될 수 있음

### 9.2 실습 버킷 초기화

버킷을 삭제하려면 먼저 내부 Key를 삭제해야 한다. 버킷이 비어 있지 않으면 삭제가 실패할 수 있다.

~~~bash
# S3 객체 전체 삭제 예시
aws s3 rm s3://iceberg-bucket \
  --recursive \
  --endpoint-url http://localhost:9878

# 버킷 삭제
aws s3 rb s3://iceberg-bucket \
  --endpoint-url http://localhost:9878

# 남은 버킷 확인
aws s3 ls \
  --endpoint-url http://localhost:9878
~~~

Ozone 네이티브 경로를 사용할 때도 순서를 지킨다.

~~~bash
# 버킷 삭제 후 Volume 삭제
docker exec ozone ozone sh bucket delete /lakehouse/warehouse
docker exec ozone ozone sh volume delete /lakehouse
~~~

실습 전체를 처음부터 다시 시작해야 한다면 Docker Compose의 named volume 삭제를 사용할 수 있다.

~~~bash
# 주의: Ozone을 포함한 Compose 데이터가 삭제된다.
docker compose down -v

# 실습 프로파일 재기동 예시
docker compose --profile lakehouse up -d
~~~

down -v는 해당 Compose 프로젝트의 영속 데이터를 제거할 수 있으므로, 운영 데이터나 보존할 실습 결과가 있는 상태에서 실행하지 않는다.

## 10. Docker Compose 실습 절차

이 절에서는 지금까지의 내용을 하나의 확인 순서로 묶는다. 기존 책의 Compose 프로젝트에서 Ozone 서비스를 기동한다는 전제로 작성한다.

### 10.1 1단계: 이미지와 플랫폼 확인

Windows 11의 일반적인 Docker Desktop 환경에서는 amd64 이미지 사용 여부를 확인한다. Apple Silicon에서는 arm64 이미지 또는 Docker의 플랫폼 변환 실행 여부를 확인한다.

~~~bash
# 현재 Docker 정보 확인
docker version
docker info

# 이미지 확인
docker image ls apache/ozone

# Compose 파일의 설정 검증
docker compose config
~~~

Apple Silicon에서 플랫폼이 맞지 않거나 이미지가 없다는 오류가 발생하면, 이미지 manifest와 플랫폼별 태그를 먼저 확인한다. 이미지 문제와 Ozone 설정 문제를 한 번에 해결하려고 하지 않는다.

### 10.2 2단계: Ozone 기동

조사 자료에서는 Ozone을 lakehouse 또는 ozone 프로파일로 기동하는 예시를 사용한다.

~~~bash
docker compose --profile lakehouse up -d
~~~

실제 Compose 파일의 서비스명이 다르면 서비스명을 확인한다.

~~~bash
docker compose ps
docker compose logs --tail=100 ozone
~~~

[추가 자료 조사 필요: 이 책의 최종 Compose 파일과 일치하는 Ozone 서비스명, 프로파일명, healthcheck, OM·SCM·Datanode·S3 Gateway 기동 명령]

### 10.3 3단계: 핵심 구성 요소 상태 확인

조사 자료에 제시된 Ozone 관리 명령을 이용해 상태를 확인한다.

~~~bash
# 컨테이너 상태
docker compose ps

# OM 상태
docker exec ozone ozone admin om status

# SCM 상태
docker exec ozone ozone admin scm status

# Datanode 목록
docker exec ozone ozone admin datanode list

# S3 Gateway 상태
docker exec ozone ozone admin s3g status
~~~

명령 이름은 Ozone 이미지의 CLI 버전에 따라 다를 수 있다. 명령이 존재하지 않는다면 먼저 컨테이너 안에서 도움말을 확인한다.

~~~bash
docker exec ozone ozone --help
docker exec ozone ozone admin --help
docker exec ozone ozone sh --help
~~~

### 10.4 4단계: 네이티브 Volume과 Bucket 생성

~~~bash
# Volume 생성
docker exec ozone ozone sh volume create /lakehouse

# Bucket 생성
docker exec ozone ozone sh bucket create /lakehouse/warehouse

# 결과 확인
docker exec ozone ozone sh volume list /
docker exec ozone ozone sh bucket list /lakehouse
~~~

이미 같은 이름이 존재한다면 생성 명령이 실패할 수 있다. 이때는 삭제 후 재생성하기보다 먼저 info 명령으로 기존 설정과 데이터를 확인한다.

### 10.5 5단계: S3 Bucket과 테스트 객체 생성

~~~bash
# S3 Bucket 생성
aws s3 mb s3://iceberg-bucket \
  --endpoint-url http://localhost:9878

# 테스트 객체 업로드
echo "ozone-lab" > ozone-test.txt
aws s3 cp ozone-test.txt \
  s3://iceberg-bucket/lab/ozone-test.txt \
  --endpoint-url http://localhost:9878

# 업로드 결과 확인
aws s3 ls s3://iceberg-bucket/lab/ \
  --endpoint-url http://localhost:9878
~~~

이 단계가 성공하면 S3 Gateway endpoint, 버킷 생성, 객체 PUT, 객체 목록 조회의 기본 경로가 연결된 것이다.

### 10.6 6단계: 웨어하우스 경로 확인

다음 장의 Iceberg 실습을 위해 사용할 경로를 문서에 고정한다.

~~~text
s3://iceberg-bucket/warehouse/
~~~

Flink 컨테이너에서 접근할 때는 Compose 네트워크의 S3 Gateway 이름을 사용한다.

~~~properties
fs.s3a.endpoint=http://ozone-s3g:9878
fs.s3a.path.style.access=true
fs.s3a.connection.ssl.enabled=false
~~~

호스트에서 localhost를 사용하고 컨테이너 안에서도 localhost를 사용하면, 컨테이너 자기 자신을 가리키게 될 수 있다. Docker 네트워크 안의 주소와 호스트의 주소를 구분하는 것이 이 실습의 핵심 점검 항목이다.

## 11. 장애 상황별 점검 순서

초보자가 자주 겪는 오류를 계층별로 나누어 확인한다.

| 증상 | 우선 확인할 항목 |
|---|---|
| 이미지 pull 실패 | 이미지 태그, 플랫폼, Docker Hub 접근 |
| 컨테이너가 즉시 종료 | Ozone 로그, 설정 파일, JVM 리소스 |
| localhost:9878 연결 실패 | S3 Gateway 기동 상태, 포트 매핑 |
| NoSuchBucket | 버킷 이름, S3 endpoint, path-style 설정 |
| AccessDenied | Access Key, Simple/Security 모드, ACL |
| 업로드 성공 후 목록이 비어 있음 | 다른 endpoint 또는 다른 버킷을 조회하는지 |
| Volume 또는 Bucket 생성 실패 | 동일 이름 존재 여부, 경로 형식 |
| 삭제 후 사용량이 줄지 않음 | 스냅샷과 pending delete 상태 |

점검 순서는 항상 클라이언트에서 Ozone 내부로 들어간다.

1. 명령에 사용한 endpoint와 경로 확인
2. S3 Gateway 포트와 로그 확인
3. OM의 Volume·Bucket·Key 확인
4. SCM과 Datanode 상태 확인
5. Docker 볼륨과 호스트 리소스 확인

이 순서를 지키면, 저장 문제를 Kafka나 Flink 문제로 잘못 분류하는 일을 줄일 수 있다.

## 12. 이 장의 정리

Apache Ozone은 이 책의 데이터 플랫폼에서 Kafka와 Flink가 만든 결과를 객체 형태로 보관하는 저장 계층이다. Ozone Manager는 네임스페이스를 관리하고, SCM은 저장 단위와 파이프라인을 조정하며, Datanode는 실제 데이터를 저장한다. S3 Gateway는 AWS CLI와 Flink 같은 외부 클라이언트가 Ozone을 사용하는 진입점이다.

핵심 계층은 Volume → Bucket → Key다. Iceberg를 사용할 때는 Parquet 데이터 파일과 메타데이터 파일이 Bucket 아래의 Key로 저장된다. 호스트에서 실행하는 클라이언트는 localhost endpoint를 사용하지만, Flink 컨테이너는 Compose 네트워크의 서비스명과 컨테이너 포트를 사용한다.

복제와 Erasure Coding은 저장 효율과 장애 대응을 바꾸는 정책이다. 그러나 한 호스트 안의 여러 컨테이너는 여러 물리 서버가 아니다. 이 책의 로컬 환경은 구조를 학습하기 위한 환경이며, HA나 호스트 장애 내성을 제공하지 않는다.

다음 장에서는 이 장에서 준비한 Ozone 저장 경로 위에 Apache Iceberg 테이블과 Parquet 파일을 구성한다.

## 확인 문제

1. Ozone Manager와 Datanode의 책임은 어떻게 다른가?
2. Volume, Bucket, Key의 관계를 예시 경로로 표현해 보라.
3. 호스트에서 실행하는 AWS CLI와 Flink 컨테이너에서 사용하는 endpoint가 다른 이유는 무엇인가?
4. S3 Gateway는 데이터를 영구 저장하는 구성 요소인가, 아니면 어떤 역할을 하는가?
5. 단일 호스트에서 Datanode 컨테이너 세 개를 실행할 때 호스트 장애를 보호할 수 없는 이유는 무엇인가?
6. RF=3과 EC-6-3은 저장 공간과 장애 대응 측면에서 어떻게 다른가?
7. 객체를 삭제했는데 사용량이 즉시 줄지 않을 수 있는 이유는 무엇인가?

## 이 장에서 자료가 부족했거나 검증이 필요한 부분

- Apache Ozone 2.2.0 공식 Docker 이미지의 multi-arch manifest와 ARM64 전용 태그의 공식성
- Windows amd64와 Apple Silicon arm64에서의 실제 Docker Compose 기동 결과
- Apache Ozone 2.2.0의 공식 멀티 컨테이너 Compose 예제와 초기화 순서
- 이 책의 전체 Compose 파일에서 사용할 실제 S3 Gateway 서비스명과 healthcheck
- Ozone CLI 관리 명령의 버전별 차이
- ACL 문자열의 문자별 의미와 Simple 모드의 실제 권한 적용 방식
- 단일 호스트 실습에서 기본으로 채택할 RF 값
- Apache-only 원칙에서 Apache Polaris를 기본 Catalog로 포함할지 여부
- Flink Iceberg 커넥터와 Ozone S3A 의존성의 최종 버전 조합

## 참고 자료

아래 자료는 이 장의 개념과 실습 항목을 확인하기 위한 조사 기준이다.

- [Apache Ozone 공식 문서](https://ozone.apache.org/docs/)
- [Apache Ozone S3 Gateway](https://ozone.apache.org/docs/core-concepts/architecture/s3-gateway/)
- [Apache Ozone S3A 클라이언트 인터페이스](https://ozone.apache.org/docs/next/user-guide/client-interfaces/s3a)
- [Apache Ozone Volume·Bucket 관련 문서](https://ozone.apache.org/docs/core-concepts/namespace/buckets/overview/)
- [Apache Ozone Quota 운영 문서](https://ozone.apache.org/docs/next/administrator-guide/operations/quota/)
- [Apache Ozone Hardware and Sizing](https://ozone.apache.org/docs/next/administrator-guide/installation/hardware-and-sizing/)
- [Apache Ozone Erasure Coding 관련 공식 자료](https://ozone.apache.org/blog/2026/01/30/apache-ozone-best-practices-at-didi/)
- [Apache Polaris의 Ozone 기반 로컬 레이크하우스 예시](https://polaris.apache.org/blog/2026/04/04/build-a-local-open-data-lakehouse-with-k3d-apache-ozone-apache-polaris-and-trino/)

