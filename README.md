# CSED332 Team Indigo

## :pushpin: Description
Project Repository of Team Indigo for POSTECH CSED332 Lecture

## :sparkles: Development Environment
1. 개발환경
  - **JDK**  ```22```
  - **Scala**  ```3.5.2```
  - **sbt**  ```1.10.5```
2. 자료 공유 툴
  - [**Notion Page**](https://sly-look-d40.notion.site/SD-Project-124f77d7c97e8011a6cbef480b7c2a03?pvs=4)

## How to Compile and Install
1. Compile
```shell
sbt compile
```
332project 경로에서 명령어 실행을 통해 코드 컴파일

2. Install
```shell
sbt stage
```
컴파일 후 명령어 실행을 통해 실행 파일 생성 --> target/universal/stage/bin 경로에 master, worker 파일 생성됨

## How to Run
컴파일과 실행 파일 생성 이후 실행 파일이 생성된 경로로 이동
```shell
cd target/universal/stage/bin
```
1. master
```shell
./master <# of workers>
```
연결할 worker의 개수를 인자로 전달, 입력 이후 연결할 ip 주소 출력됨

2. worker
```shell
./worker <master IP> -I <input directory> -O <output directory>
```
연결할 모든 worker에 대해 각각 실행, 인자로 앞서 master에서 출력된 ip 주소(포트를 포함)와 입력 데이터의 경로, 출력 데이터를 저장할 경로 전달. 아래와 같이 사용. 
```shell
./worker 10.1.25.21:33465 -I /home/dataset/big -O /home/indigo/output
```
모든 worker에서 실행시 master 실행창에는 연결된 worker들의 ip 주소가 출력되고, 이후 실행이 완료될때까지 대기하면 worker 실행 파일에 전달한 출력 데이터 저장 경로에 정렬된 데이터 생성. 

## :hammer_and_wrench: Git Convention
1. Commit / Pull Request
    - `Feat` : 새로운 기능 추가
    - `Fix` : 오류 및 버그 수정
    - `Docs` : 문서 (README, 메뉴얼 등)
    - `Test` : 테스트용 코드
    - `Chore` : 빌드, 설정 파일
    - `Comment` : 주석 추가
    - `Refactor` : 기존 코드에서 기능 변화 없이 성능 개선
2. Git Branch Naming
    - `main` : 기본 Branch
    - `docs` : 문서 작업 Branch
    - `feat/[feat_name]` : 각 기능을 작업하는 Branch

## :people_hugging: Authors

- [@황찬기](https://github.com/DevMizeKR) | [@이다민](https://www.github.com/ldm0902) | [@안재영](https://github.com/2nter21)
  
## :books: Documentation

- [Weekly Report](./Weekly%20Report/)
