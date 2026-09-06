# HashiCorp Boundary 기반 서버 접근 제어 실무 가이드
## Docker Local Lab으로 배우는 Boundary·Vault·Consul 통합과 Zero Trust 접근 아키텍처

> **최종 통합 기획안** — 기존 기획안과 비교하여 전문가용 실무서, 통합 프로젝트, 400~500쪽이라는 세 조건을 동시에 만족하도록 재설계합니다.

---

# 1. 기획 편집 판단

두 기획안의 핵심 방향은 모두 타당합니다. 공통적으로 Boundary를 중심 제품으로 두고 Vault와 Consul을 연계하며 Docker Local Lab에서 실습하도록 한 점은 유지할 가치가 있습니다. 타 기획안은 프로젝트형으로 범위를 줄여 실습 완주 가능성을 높였다는 점에서 기존안보다 출판성이 높습니다. 반면 3~7년 경력이라는 독자 정의는 이번에 확정한 전문가용 독자와 맞지 않습니다. 또한 Consul과 Vault 연동을 실제 공식 지원 기능처럼 단정하지 않고, **공식 통합 / 공식 API 활용 / 사용자 자동화**를 구분해야 합니다.

따라서 최종안은 **타 기획안의 프로젝트형 구조를 채택하고, 기존안의 전문가 수준 아키텍처·운영·장애 대응 깊이를 결합**합니다. 24개 장과 10개 Lab은 줄이고, 18개 Chapter와 6개 통합 Lab으로 재구성합니다.

## 최종 포지셔닝

> Boundary 기능을 배우는 책이 아니라, Boundary를 중심으로 기업의 서버 접근 제어 체계를 설계하고 Local Docker Lab에서 검증하는 책

---

# 2. 기획안 비교

| 항목 | 기존 기획안 | 타 기획안 | 최종안 |
|---|---|---|---|
| Boundary 중심성 | 높음 | 높음 | 유지 |
| 프로젝트 완주성 | 보통 | 높음 | 타안 채택 |
| 전문가 깊이 | 높음 | 중~고급 | 기존안의 깊이 유지 |
| Lab | 10개 | 6~7개 | 6개 통합 |
| Vault | 독립+통합 | 통합 중심 | 통합 중심 |
| Consul | 독립+통합 | 통합 중심 | 통합 중심 |
| Terraform | 핵심 | 핵심 | 핵심 |
| 고급 주제 | 광범위 | 설계 중심 | 설계 중심 |
| 운영/장애 | 충분 | 충분 | 강화 |
| 독자 | 전문가 | 3~7년 경력 | **전문가 역량 기준** |
| 분량 | 400~500쪽 목표 | 380~450쪽 | **420~470쪽** |
| 차별점 | 통합 아키텍처 | 프로젝트 | **통합 프로젝트+운영 아키텍처** |

---

# 3. 기존안에서 수정할 핵심 문제

### 3.1 장 수 과다

24개 Chapter는 제품별 설명이 반복되고 400~500쪽을 쉽게 초과합니다. 전문가용 책에서는 제품의 모든 기능을 나열하기보다 접근 제어라는 문제를 해결하는 데 필요한 기능을 깊게 다루는 편이 낫습니다.

### 3.2 Lab 과다

10개의 작은 Lab은 학습 단계로는 유용하지만 책의 중심 흐름을 분산시킵니다. 6개의 통합 프로젝트로 합칩니다.

### 3.3 Consul 연동 가정 주의

`Consul → Discovery → Boundary`를 하나의 기본 기능처럼 기술하지 않습니다. 정보 수집 단계에서 공식 지원 범위와 API/Provider/스크립트 기반 자동화를 먼저 검증합니다.

### 3.4 Vault 연동 가정 주의

`Vault Dynamic Credential → Boundary → SSH` 역시 기획 단계에서 사실로 확정하지 않습니다. Boundary Credential Store/Library, Vault Secret Engine, SSH Credential 전달 방식, Community/Enterprise 차이를 조사한 뒤 실습 경로를 확정합니다.

### 3.5 독자 정의 개선

경력 연수 대신 사전 지식과 업무 역량으로 정의합니다.

- Senior DevOps/SRE
- Platform/Infrastructure Engineer
- Security Engineer
- PAM/IAM 담당자
- DBA/DBRE
- Cloud Engineer
- 접근 제어 아키텍트 및 기술 리더

Linux, TCP/IP, SSH, Docker, TLS, 인증·인가의 기본 개념은 알고 있다고 가정합니다.

---

# 4. 서적 컨셉

이 책은 기업 환경의 서버 접근 제어를 **Identity, Authorization, Connectivity, Credential, Discovery, Audit**의 관점에서 재설계하고, HashiCorp Boundary를 중심으로 실제 접근 제어 환경을 구축하는 전문가용 실무서입니다.

독자는 Docker Local Lab에서 Controller, Worker, Target을 구성하고, 사용자·그룹·Role 기반 접근 정책을 구현합니다. 이어 Vault의 Credential 관리 모델과 Boundary의 접근 제어를 연결하고, Consul을 동적 대상 관리에 활용하며, Terraform으로 설정을 코드화합니다. 마지막으로 감사, 장애 대응, 고가용성, 긴급 접근, 기존 Bastion/VPN 환경의 전환까지 연결합니다.

## 핵심 질문

> **누가, 어떤 조건으로, 어떤 서버에, 어떤 경로를 통해, 어떤 Credential을 사용하여 접근할 수 있으며, 그 접근을 어떻게 통제하고 증명할 것인가?**

Boundary는 중심이지만 목적은 Boundary 자체가 아니라 **기업의 접근 제어 운영 모델 완성**입니다.

---

# 5. 기술 범위

## 핵심

- Boundary
- Vault
- Consul
- Docker / Docker Compose
- Terraform
- Linux SSH
- TCP Target
- TLS
- Authentication / Authorization
- RBAC
- Credential Lifecycle
- Session
- Audit
- Troubleshooting
- Migration

## 고급 설계

- OIDC
- LDAP
- MFA
- Kubernetes
- Cloud VM
- Dynamic Infrastructure
- SIEM
- HA / DR
- Enterprise 기능

## 제외

- Vault 전체 기능 레퍼런스
- Consul 전체 운영 가이드
- Kubernetes 전문서
- Cloud별 네트워크 전문서
- 전사 IAM 구축 전문서
- 법률·컴플라이언스 해석
- 대규모 Production 클러스터의 완전한 배포 매뉴얼

---

# 6. 버전 정책

2026년 출간 기준으로 정보 수집 단계에서 최신 안정 버전을 확정합니다.

버전 매트릭스에는 다음을 기록합니다.

| 항목 | 확인 내용 |
|---|---|
| Product | Boundary / Vault / Consul |
| Version | 출간 기준 최신 안정 버전 |
| Edition | Community / Enterprise |
| CLI | 호환성 |
| API | 변경사항 |
| Terraform | Provider 호환성 |
| Docker | 공식 이미지/태그 |
| Support | 지원 정책 |
| Deprecated | 폐기 기능 |
| Security | CVE/Advisory |
| Lab | 실습 가능 여부 |

기획 단계에서 임의의 버전을 확정하지 않습니다. 정보 수집가가 공식 릴리스·지원 정책·CLI/API·Provider·Docker 이미지를 확인한 후 고정합니다.

---

# 7. 최종 목차

## Part I. 문제 정의와 아키텍처

### Chapter 1. 서버 접근 제어를 다시 설계해야 하는 이유
**학습 목표:** SSH/VPN/Bastion의 구조와 한계를 이해하고 Identity 기반 접근 제어 요구사항을 정의합니다.  
**핵심 개념:** SSH, Bastion, VPN, PAM, IAM, Zero Trust, Least Privilege, JIT  
**난이도:** 중급~고급  
**분량:** 20~25쪽

### Chapter 2. Boundary·Vault·Consul의 책임 경계
**학습 목표:** 세 제품의 책임을 분리하고 Authentication, Authorization, Connectivity, Credential, Discovery, Audit의 관계를 설계합니다.  
**핵심 개념:** Boundary, Vault, Consul, Secret, Discovery, Audit  
**난이도:** 고급  
**분량:** 20~25쪽

### Chapter 3. 기업형 접근 제어 아키텍처 설계
**학습 목표:** User에서 Target까지의 제어·데이터 흐름과 네트워크 경계를 설계합니다.  
**핵심 개념:** Control Plane, Data Plane, Controller, Worker, Target, Session, Trust Boundary  
**난이도:** 고급  
**분량:** 25~30쪽

## Part II. Boundary 핵심

### Chapter 4. Boundary 핵심 구조와 리소스 모델
**학습 목표:** Scope, Target, Host Catalog, Host Set, Role 등의 관계를 이해하고 기업 환경의 객체 모델을 설계합니다.  
**핵심 개념:** Controller, Worker, Scope, Organization, Project, Target, Host Catalog, Host Set, Session  
**난이도:** 고급  
**분량:** 30~35쪽

### Chapter 5. 인증과 RBAC 기반 접근 정책
**학습 목표:** 사용자 인증과 Target 인가를 분리하고 최소 권한 정책을 설계합니다.  
**핵심 개념:** Auth Method, User, Group, Role, Grant, RBAC, OIDC, LDAP  
**난이도:** 고급  
**분량:** 25~30쪽

### Chapter 6. Target·Worker·Session 설계
**학습 목표:** 접근 대상을 모델링하고 Worker 경로와 Session 생성·종료 흐름을 이해합니다.  
**핵심 개념:** Target, Host, Host Catalog, Host Set, Worker, Session, SSH, TCP  
**난이도:** 고급  
**분량:** 25~30쪽

## Part III. Credential과 통합

### Chapter 7. Vault와 Credential 관리 아키텍처
**학습 목표:** Vault의 Secret 관리와 Boundary의 접근 제어 책임을 분리하고 Credential Lifecycle을 설계합니다.  
**핵심 개념:** Secret Engine, Policy, Token, Dynamic Secret, TTL, Rotation, Revocation  
**난이도:** 고급  
**분량:** 30~35쪽

### Chapter 8. Boundary와 Vault 통합 설계
**학습 목표:** 공식 통합 범위와 Credential 전달 방식을 검증하고 Static/Dynamic Credential의 적용 조건을 판단합니다.  
**핵심 개념:** Credential Store, Credential Library, Vault, SSH Credential, Dynamic Credential  
**난이도:** 고급  
**분량:** 25~30쪽

### Chapter 9. Consul과 동적 Target 관리
**학습 목표:** Service Discovery와 Health Check를 동적 접근 대상 관리에 연결하는 방법을 설계합니다.  
**핵심 개념:** Service Registration, Discovery, Catalog, Health Check, API, Automation  
**난이도:** 고급  
**분량:** 25~30쪽

> **편집 주의:** Consul의 Boundary 연계는 공식 지원 기능인지, API/Provider 기반 자동화인지 조사 결과에 따라 본문 표현을 결정합니다.

## Part IV. Docker 통합 프로젝트

### Chapter 10. Docker Local Lab 전체 환경 구축
**학습 목표:** Boundary, Vault, Consul, Target Server를 하나의 재현 가능한 Local Lab으로 구성합니다.  
**핵심 개념:** Docker, Compose, Network, Volume, Healthcheck, TLS, DNS  
**난이도:** 중급~고급  
**분량:** 25~30쪽

### Chapter 11. Boundary 접근 제어 프로젝트
**학습 목표:** Controller/Worker/Target을 구성하고 실제 접근 성공·실패를 검증합니다.  
**핵심 개념:** Controller, Worker, Target, Host Catalog, Host Set, Role, Grant, SSH Session  
**난이도:** 고급  
**분량:** 30~35쪽

### Chapter 12. Vault·Consul 통합 프로젝트
**학습 목표:** Credential과 동적 대상 관리를 기존 Boundary 환경에 통합합니다.  
**핵심 개념:** Vault, Credential, Consul, Discovery, Dynamic Target, TTL, Health Check  
**난이도:** 고급  
**분량:** 30~35쪽

### Chapter 13. Terraform 기반 전체 환경 자동화
**학습 목표:** 접근 정책과 구성을 IaC로 관리하고 State의 민감정보 문제를 다룹니다.  
**핵심 개념:** Provider, Resource, Data Source, State, Import, Drift, Policy as Code, CI/CD  
**난이도:** 고급  
**분량:** 30~35쪽

## Part V. 기업 운영

### Chapter 14. 개발자·DBA·운영자의 접근 정책
**학습 목표:** 역할·환경·서버 등급에 따른 실제 접근 정책을 설계합니다.  
**핵심 개념:** Developer, DBA, Operations, Production, Environment Isolation, RBAC  
**난이도:** 고급  
**분량:** 25~30쪽

### Chapter 15. 감사·Session·보안 운영
**학습 목표:** 접근 기록을 감사와 보안 분석에 활용할 수 있는 운영 모델을 설계합니다.  
**핵심 개념:** Audit, Session, Access Log, Monitoring, SIEM, Forensics  
**난이도:** 고급  
**분량:** 25~30쪽

### Chapter 16. 장애 대응과 고가용성
**학습 목표:** Controller, Worker, Vault, Consul, Target의 장애를 계층별로 진단하고 HA/DR을 설계합니다.  
**핵심 개념:** Failure Domain, HA, DR, TLS Failure, Credential Failure, Troubleshooting  
**난이도:** 전문가  
**분량:** 30~35쪽

## Part VI. 고급 설계와 전환

### Chapter 17. Identity·Kubernetes·Cloud 확장
**학습 목표:** Local Lab에서 검증한 접근 모델을 OIDC/LDAP, Kubernetes, Cloud 환경으로 확장하는 설계 원칙을 이해합니다.  
**핵심 개념:** OIDC, LDAP, MFA, Kubernetes, Cloud VM, Ephemeral Infrastructure  
**난이도:** 전문가  
**분량:** 25~30쪽

### Chapter 18. Bastion/VPN에서 Boundary로의 전환
**학습 목표:** 기존 접근 인프라를 단계적으로 전환하고 병행 운영·Cutover·Rollback까지 설계합니다.  
**핵심 개념:** Migration, Bastion, VPN, Access Inventory, Parallel Operation, Cutover, Rollback, Governance  
**난이도:** 전문가  
**분량:** 25~30쪽

---

# 8. 통합 Hands-on Lab

10개의 작은 Lab 대신 다음 6개 프로젝트로 구성합니다.

## Lab 1. 직접 SSH 환경 분석
- 직접 SSH 접근
- SSH Key
- 계정 및 권한
- IP/Port 노출
- 접근 이력의 한계

## Lab 2. Boundary 접근 제어 구축
- Controller
- Worker
- Target
- Host Catalog/Host Set
- Role/Grant
- SSH Session

## Lab 3. 사용자·환경별 RBAC
- Developer
- DBA
- Operator
- Development / Production
- 허용 테스트와 거부 테스트

## Lab 4. Vault Credential 통합
- Static Credential
- Dynamic Credential
- TTL
- Rotation
- Revocation
- Credential 노출 경로

> 실제 Boundary-Vault 통합 방식은 공식 지원 범위 검증 후 확정합니다.

## Lab 5. Consul 기반 동적 대상 관리
- Service Registration
- Discovery
- Health Check
- 신규 서버 추가
- 서버 제거
- 장애 대상 처리
- Boundary 반영 자동화

> 공식 통합이 아닌 경우 API/스크립트/Provider 기반 자동화로 명확히 표시합니다.

## Lab 6. Terraform + 운영 통합
- IaC
- RBAC
- Credential
- Dynamic Target
- Break-Glass
- Audit
- Failure Testing

---

# 9. 대표 운영 시나리오

### Scenario A. 개발자의 Production 접근 차단
Developer는 Development만 접근할 수 있고 Production Target은 명시적으로 거부됩니다.

### Scenario B. DBA의 Database 접근
DBA Role에 Database Target만 허용하고 Application Server 접근을 차단합니다.

### Scenario C. 신규 서버 자동 반영
Consul에 신규 서버가 등록될 때 접근 대상에 반영되는 전체 흐름을 검증합니다.

### Scenario D. Health Check 실패
서비스 상태 변화가 접근 대상 관리에 어떤 영향을 주어야 하는지 검증합니다.

### Scenario E. 긴급 Production 접근

```text
Normal User
    │
    X
Production
    │
Emergency Approval
    ↓
Temporary Access
    ↓
Production Session
    ↓
Expiration / Revocation
```

### Scenario F. 장애 대응

- Worker 장애
- Controller 장애
- Vault 접근 실패
- Consul 접근 실패
- Target 장애
- TLS 오류
- Credential 만료

각 시나리오는 `증상 → 계층 분류 → 확인 위치 → 원인 → 조치 → 재발 방지`로 구성합니다.

---

# 10. 자료 조사 브리프

정보 수집가는 다음 자료를 우선 확보합니다.

## Boundary
- 최신 안정 버전
- Community/Enterprise
- Controller/Worker
- Scope
- Target
- Host Catalog/Host Set
- Auth Method
- Role/Grant
- Session
- Credential Store/Library
- CLI/API
- Terraform Provider
- Security Advisory
- Deprecated 기능

## Vault
- 최신 안정 버전
- Auth/Policy/Token
- Secret Engine
- KV
- SSH 관련 기능
- Dynamic Secret
- TTL/Rotation/Revocation
- Audit/TLS
- Boundary 통합
- Community/Enterprise

## Consul
- 최신 안정 버전
- Service Registration
- Discovery
- Catalog
- Health Check
- ACL
- API
- Terraform Provider
- Boundary 공식 통합 여부

## Docker
- Engine/Desktop
- Compose
- Network
- Volume
- Healthcheck
- DNS
- TLS
- Secret
- Linux/macOS/Windows
- Apple Silicon
- 공식 HashiCorp 이미지

## Terraform
- Boundary Provider
- Vault Provider
- Consul Provider
- Resource/Data Source
- 인증
- State 민감정보
- Import
- Drift
- CI/CD

## Zero Trust
- NIST 관련 기준
- Identity-centric Access
- Least Privilege
- Continuous Verification
- IAM/PAM 관계
- JIT Access
- Break-Glass

## 보안
출간 직전 Boundary, Vault, Consul, Docker의 CVE/Security Advisory, 인증 방식 및 TLS 변경, Deprecated 기능을 다시 확인합니다.

---

# 11. 자료의 증거 등급

정보 수집가는 모든 자료를 다음과 같이 분류합니다.

1. **공식 제품 문서 / API / CLI**
2. **공식 Release Notes / Security Advisory**
3. **공식 기술 블로그 / Architecture**
4. **NIST 등 공신력 있는 표준**
5. **공식 컨퍼런스 발표**
6. **검증 가능한 기업 사례**
7. **기술 논문**
8. **기타 공신력 자료**

또한 모든 자료에 다음 메타데이터를 기록합니다.

- 제목
- 발행 주체
- URL/식별자
- 발행·수정일
- 제품 버전
- Community/Enterprise
- 관련 Chapter
- Fact / Recommendation / Case / Design Decision
- 실습 검증 여부
- Deprecated 여부

---

# 12. 집필 원칙

## 반드시 구분할 것

- 제품 기능
- 공식 권장사항
- 운영 패턴
- 책의 참조 설계
- 사용자가 구현하는 자동화

## 모든 실습에 포함할 것

```text
사전 조건
    ↓
구성
    ↓
실행
    ↓
성공 검증
    ↓
실패 검증
    ↓
원인 분석
    ↓
정리
```

특히 전문가용 책이므로 **성공하는 명령만 제시하지 않고 의도적인 접근 거부, Credential 만료, Worker 장애, Discovery 변경 등의 실패 검증을 포함**합니다.

---

# 13. 분량 정책

최종 목표는 **420~470쪽**입니다.

구성 원칙:

- 핵심 Boundary/아키텍처: 약 25%
- Vault/Consul 통합: 약 20%
- Docker Hands-on: 약 25%
- 기업 운영/장애: 약 20%
- 고급 확장/부록: 약 10%

페이지가 초과하면 다음 순서로 줄입니다.

1. 일반적인 제품 개론
2. 반복적인 CLI 설명
3. Kubernetes/Cloud 상세 설명
4. Enterprise 세부 기능

반대로 분량이 부족하면 다음을 보강합니다.

1. 실패 검증
2. 장애 대응
3. 운영 정책
4. Migration
5. 실습 검증

---

# 14. 부록

## Appendix A. Docker Local Lab Repository
- compose.yaml
- Dockerfile
- Configuration
- Network
- Scripts
- Policies
- Certificates

## Appendix B. Boundary CLI 핵심 명령
- Auth
- Target
- Host
- Host Catalog
- Host Set
- Role
- User
- Group
- Session

## Appendix C. Vault CLI 핵심 명령
- Login
- Policy
- Secret Engine
- Token
- Credential
- Audit

## Appendix D. Consul CLI/API
- Members
- Services
- Catalog
- Health
- ACL
- API

## Appendix E. Terraform
- Boundary Provider
- Vault Provider
- Consul Provider
- 주요 Resource
- State 보안

## Appendix F. Troubleshooting Matrix

```text
증상
 ↓
Boundary
 ↓
Worker
 ↓
Credential
 ↓
Vault
 ↓
Consul
 ↓
Target
 ↓
원인 확정
```

---

# 15. 최종 편집 결정

## 채택
- 타 기획안의 **프로젝트형 구조**
- 6개 통합 Hands-on
- 고급 주제의 설계 중심 처리
- Boundary 중심 포지셔닝
- 2026년 최신 안정 버전 조사 후 버전 고정

## 유지
- 기존안의 전문가 수준
- Vault/Consul 책임 분리
- Terraform
- 장애 대응
- Migration
- Enterprise 고려
- Fact / Recommendation / Case / Design Decision 구분

## 수정
- 24 Chapter → **18 Chapter**
- 10 Lab → **6 통합 Lab**
- 제품별 독립 설명 → **통합 프로젝트 중심**
- 3~7년 경력 → **전문가 역량 기준**
- Consul 직접 연동 가정 → **공식 기능/자동화 검증 후 확정**
- Vault Dynamic Credential 가정 → **공식 지원 범위 검증 후 확정**
- 목표 분량 → **420~470쪽**

---

# 16. 최종 서적 정의

> **HashiCorp Boundary 기반 서버 접근 제어 실무 가이드**는 Boundary의 CLI와 기능을 설명하는 제품 사용서가 아니다.
>
> 이 책은 기업 환경에서 `누가 → 무엇에 → 어떤 조건으로 → 어떤 경로를 통해 → 어떤 Credential로 접근하는가`를 설계하고, 이를 Boundary·Vault·Consul·Terraform으로 구현하며, Docker Local Lab에서 성공과 실패를 검증하는 **전문가용 서버 접근 제어 실무서**다.
>
> 최종 학습 경로는 다음과 같다.

```text
문제 정의
   ↓
접근 제어 아키텍처
   ↓
Boundary 핵심 모델
   ↓
RBAC / Target / Session
   ↓
Vault Credential
   ↓
Consul Discovery
   ↓
Docker 통합 Lab
   ↓
Terraform 자동화
   ↓
Audit / 장애 대응
   ↓
HA / DR
   ↓
Bastion / VPN Migration
   ↓
Enterprise 운영 모델
```

이 기획을 **정보 수집가 → 집필가 → 기술 검수 → 최종 편집**의 공통 기준 문서로 사용합니다.
