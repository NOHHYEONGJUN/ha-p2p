FROM python:3.9-slim

WORKDIR /app

# 의존성 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 코드 복사
COPY *.py /app/
COPY startup.sh /app/

RUN chmod +x /app/startup.sh

# startup.sh가 컨테이너의 엔트리포인트로 동작
ENTRYPOINT ["/app/startup.sh"]

