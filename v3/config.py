# 시스템 설정 파일
import os

# RAFT 타이밍 관련 설정 (단위: 초)
HEARTBEAT_INTERVAL = 0.2  # 200ms
MIN_ELECTION_TIMEOUT = 1.0  # 1초
MAX_ELECTION_TIMEOUT = 2.5  # 2.5초

# 장애 감지 설정
MAX_HEARTBEAT_MISS = 3  # 이 횟수만큼 heartbeat가 누락되면 장애로 판단
HEALTH_CHECK_INTERVAL = 0.5  # 상태 확인 주기 (500ms)

# 노드 연결 재시도 간격 및 횟수
CONNECTION_RETRY_INTERVAL = 1.0
MAX_CONNECTION_RETRIES = 5

# ETCD 설정
ETCD_HOST = os.environ.get('ETCD_HOST', 'localhost')
ETCD_PORT = int(os.environ.get('ETCD_PORT', 2379))

# 노드 포트 설정 (호스트 네트워크 사용으로 중요)
NODE_PORT = int(os.environ.get('NODE_PORT', 5000))

# 디버그 모드
DEBUG = os.environ.get('DEBUG', 'false').lower() == 'true'
