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

# 노드 포트 설정
NODE_PORT = int(os.environ.get('NODE_PORT', 5000))

# 디버그 모드
DEBUG = os.environ.get('DEBUG', 'false').lower() == 'true'

# ETCD 클러스터 설정
# 각 클러스터별 ETCD 엔드포인트 목록 (각 클러스터당 하나의 etcd 만 사용)
ETCD_CLUSTER_ENDPOINTS = {
    "1": ["localhost:2371"],
    "2": ["localhost:2381"],
    "3": ["localhost:2391"]
}

# 클러스터 백업 구성 (환형 구조: 1->2->3->1)
CLUSTER_BACKUP_MAP = {
    '1': '2',  # 클러스터 1은 클러스터 2가 백업
    '2': '3',  # 클러스터 2는 클러스터 3이 백업
    '3': '1',  # 클러스터 3은 클러스터 1이 백업
}

# 리더 선출 타임아웃 관련 설정
LEADER_ELECTION_MIN_TIMEOUT = MIN_ELECTION_TIMEOUT * 2
LEADER_ELECTION_MAX_TIMEOUT = MAX_ELECTION_TIMEOUT * 2

# 클러스터 간 연결 설정
INTER_CLUSTER_HEARTBEAT_INTERVAL = 1.0  # 클러스터 간 heartbeat 주기

# ETCD 이벤트 감시 관련 설정 
ETCD_WATCH_PREFIX = "/p2p"  # ETCD에서 감시할 키 프리픽스

# 노드 정보 저장 경로 상수
NODE_INFO_PREFIX = "/p2p/nodes"
MANAGER_INFO_PREFIX = "/p2p/managers"
CLUSTER_INFO_PREFIX = "/p2p/clusters"
BACKUP_INFO_PREFIX = "/p2p/backups"

# 클러스터 상태 체크 간격 (초)
CLUSTER_HEALTH_CHECK_INTERVAL = 3.0

# 백업 클러스터 활성화 타임아웃 (초)
# 이 시간 동안 기본 클러스터에서 응답이 없으면 백업 클러스터가 활성화됨
BACKUP_ACTIVATION_TIMEOUT = 5.0

