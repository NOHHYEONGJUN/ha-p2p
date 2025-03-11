FROM python:3.9-slim

WORKDIR /app

# 필요한 패키지 설치
COPY requirements.txt .
RUN pip install --no-cache-dir --timeout=100 -r requirements.txt

# 애플리케이션 코드 복사
COPY *.py /app/

# 기본 실행 명령
ENTRYPOINT ["python", "run.py"]
