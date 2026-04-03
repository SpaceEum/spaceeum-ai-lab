# 1. 베이스 이미지 선택 (Python 3.10 슬림 버전 사용)
FROM python:3.10-slim

# 2. 작업 디렉토리 설정
WORKDIR /app

# 3. 시스템 패키지 업데이트 및 Chrome, ChromeDriver, 기타 필요 도구 설치
#    - apt-get update: 패키지 목록 업데이트
#    - apt-get install -y ...: 필요한 패키지 설치 (-y는 확인 없이 설치)
#    - wget, unzip: ChromeDriver 다운로드 및 압축 해제에 필요할 수 있음 (여기서는 apt로 설치)
#    - google-chrome-stable: 구글 크롬 브라우저
#    - chromedriver: 크롬 드라이버
#    - --no-install-recommends: 추천 패키지 제외하여 이미지 크기 줄임
#    - rm -rf /var/lib/apt/lists/*: 패키지 목록 캐시 삭제하여 이미지 크기 줄임
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    google-chrome-stable \
    chromedriver \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# 4. requirements.txt 파일을 컨테이너의 /app 디렉토리로 복사
COPY requirements.txt .

# 5. 파이썬 의존성 설치
#    - pip install: 파이썬 패키지 설치
#    - --no-cache-dir: pip 캐시 사용 안 함 (이미지 크기 줄임)
#    - -r requirements.txt: requirements.txt 파일에 명시된 패키지 설치
RUN pip install --no-cache-dir -r requirements.txt

# 6. 현재 폴더의 모든 파일(Dockerfile, .dockerignore 제외)을 컨테이너의 /app 디렉토리로 복사
#    이 단계에서 autotrade.py, strategy.txt 등이 복사됩니다.
#    .env 파일은 보안상 이미지에 포함하지 않고 실행 시점에 주입합니다.
COPY . .

# 7. 데이터베이스 파일이 저장될 디렉토리 생성 (선택적이지만 권장)
#    데이터베이스 파일을 코드와 분리하여 관리하기 위함
RUN mkdir /app/data

# 8. 환경 변수 설정 (autotrade.py의 create_driver 함수 로직 활용)
#    스크립트가 '/usr/bin/chromedriver' 경로를 사용하도록 'ec2' 환경으로 설정
ENV ENVIRONMENT=ec2

# 9. 컨테이너가 시작될 때 실행될 기본 명령어 설정
#    python으로 autotrade.py 스크립트를 실행합니다.
CMD ["python", "autotrade.py"]
