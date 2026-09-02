**『Docker Compose로 만드는 Apache 오픈소스 데이터 플랫폼』**

부제:

**Windows 11과 Apple Silicon Mac에서 Percona MySQL·MongoDB·PostgreSQL·Kafka·Flink·Iceberg·ClickHouse를 단계별로 구축하기**

## 서적 컨셉

이 책은 데이터 플랫폼을 처음 접하는 데이터 엔지니어 입문자와 주니어 엔지니어를 대상으로 합니다. 독자는 Python·SQL·관계형 데이터베이스의 기본 개념과 간단한 Docker 사용 경험은 있지만, Kafka, Flink, Iceberg, Airflow, ClickHouse를 실제로 연결해 본 경험은 없다고 가정합니다.

책의 핵심은 개별 솔루션 사용법을 나열하는 것이 아니라, 세 가지 서로 다른 소스 데이터베이스에서 발생한 데이터를 CDC로 수집하고, Kafka를 거쳐 Flink로 처리한 뒤 Apache Iceberg와 Apache Ozone에 저장하고, ClickHouse와 Superset에서 분석하는 전체 흐름을 직접 완성하는 것입니다.

이 책은 Kubernetes, 클라우드 매니지드 서비스, 대규모 운영 클러스터 구축을 목표로 하지 않습니다. 대신 Windows 11과 Apple Silicon Mac에서 Docker Compose로 재현 가능한 단일 호스트 실습 환경을 제공하며, 데이터 생성·수집·변환·저장·조회·재처리·장애 복구까지 데이터 플랫폼의 전체 생명주기를 경험하도록 설계합니다.

## 핵심 독자

* 데이터 엔지니어 입문자와 주니어 데이터 엔지니어
* 애플리케이션 개발자에서 데이터 엔지니어로 전환하려는 독자
* 관계형 DB는 알고 있지만 데이터 레이크하우스는 처음인 DBA
* 로컬에서 Kafka·Flink·Iceberg를 실습하고 싶은 학습자
* 클라우드 도입 전 오픈소스 데이터 플랫폼을 검토하는 기술 담당자

## 독자에게 요구하는 사전 지식

* 기본적인 SQL
* 테이블, 인덱스, 트랜잭션의 개념
* Python 기초 문법
* 기본적인 터미널 사용
* Docker 이미지와 컨테이너의 기초 개념

Kafka, Flink, Spark, Iceberg, Airflow 운영 경험은 요구하지 않습니다.
