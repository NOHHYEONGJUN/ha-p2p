import time
import json
import random
import threading
import logging
import socket
import etcd3

from config import *

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger('p2p_node')

class Node:
    """모든 P2P 노드의 기본 클래스"""
    
    def __init__(self, node_id, cluster_id, etcd_client=None):
        self.id = node_id
        self.cluster_id = cluster_id
        self.status = "INITIALIZING"
        self.type = "UNKNOWN"
        self.last_updated = time.time()
        
        # ETCD 클라이언트 초기화
        if etcd_client:
            self.etcd_client = etcd_client
        else:
            self.etcd_client = etcd3.client(host=ETCD_HOST, port=ETCD_PORT)
        
        # 연결 관리
        self.connections = {}
        self.connection_lock = threading.Lock()
        
        # 노드 메타데이터
        self.metadata = {
            "node_id": self.id,
            "cluster_id": self.cluster_id,
            "status": self.status,
            "type": self.type,
            "last_updated": self.last_updated
        }
        
    def register_to_etcd(self):
        """노드 정보를 ETCD에 등록"""
        self.metadata["last_updated"] = time.time()
        self.etcd_client.put(f"/nodes/{self.cluster_id}/{self.id}", 
                             json.dumps(self.metadata))
        # 클러스터별 노드 목록에도 추가
        self.etcd_client.put(f"/nodes/cluster_{self.cluster_id}/{self.id}", 
                             json.dumps(self.metadata))
        logger.info(f"Node {self.id} registered to etcd")
        
    def update_status(self, status):
        """노드 상태 업데이트"""
        self.status = status
        self.metadata["status"] = status
        self.metadata["last_updated"] = time.time()
        self.register_to_etcd()
        
    def connect_to_node(self, target_node_id):
        """다른 노드에 연결 (실제 구현에서는 소켓/gRPC 등으로 구현)"""
        with self.connection_lock:
            if target_node_id in self.connections:
                return self.connections[target_node_id]
            
            # 연결 시도 (여기서는 모의 구현)
            connection = {
                "node_id": target_node_id,
                "status": "CONNECTED",
                "established_at": time.time(),
                "last_message": time.time()
            }
            
            self.connections[target_node_id] = connection
            logger.info(f"Node {self.id} connected to {target_node_id}")
            return connection
            
    def disconnect_from_node(self, target_node_id):
        """노드 연결 해제"""
        with self.connection_lock:
            if target_node_id in self.connections:
                del self.connections[target_node_id]
                logger.info(f"Node {self.id} disconnected from {target_node_id}")
                
    def send_message(self, target_node_id, message_type, payload=None):
        """다른 노드에 메시지 전송"""
        # 실제 구현에서는 소켓/gRPC 등을 통해 메시지 전송
        # 여기서는 로깅만 수행
        if payload is None:
            payload = {}
            
        logger.debug(f"Node {self.id} sending {message_type} to {target_node_id}: {payload}")
        
        # 연결이 없으면 연결 시도
        if target_node_id not in self.connections:
            self.connect_to_node(target_node_id)
            
        # 연결 갱신
        self.connections[target_node_id]["last_message"] = time.time()
        
        # 메시지 처리는 실제 구현에서 구현
        return True
        
    def heartbeat_loop(self):
        """주기적으로 상태 업데이트 및 heartbeat 전송"""
        while self.status != "STOPPED":
            try:
                self.register_to_etcd()
                time.sleep(HEARTBEAT_INTERVAL)
            except Exception as e:
                logger.error(f"Error in heartbeat loop: {e}")
                time.sleep(1)  # 오류 시 더 긴 간격으로 재시도
                    
    # 포트 관련 부분 수정
    def start(self):
        """노드 시작"""
        # NODE_PORT 환경변수 사용
        self.port = NODE_PORT
        self.update_status("ACTIVE")
        
        logger.info(f"Node {self.id} started on {ETCD_HOST}:{self.port}")
        
        # heartbeat 스레드 시작
        self.heartbeat_thread = threading.Thread(target=self.heartbeat_loop)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()
        
        logger.info(f"Node {self.id} started")
        
    def stop(self):
        """노드 중지"""
        self.update_status("STOPPED")
        logger.info(f"Node {self.id} stopped")


class ManagementNode(Node):
    """P2P 관리 노드 클래스"""
    
    def __init__(self, node_id, cluster_id, is_primary=False, backup_for=None, desired_secondary=None, etcd_client=None):
        super().__init__(node_id, cluster_id, etcd_client)
        self.type = "MANAGER"
        self.is_primary = is_primary
        self.backup_for = backup_for  # 이 노드가 백업 역할을 하는 클러스터 ID
        self.desired_secondary = desired_secondary  # 이 클러스터의 백업을 담당하는 노드 ID
        
        # RAFT 관련 상태
        self.current_term = 0
        self.voted_for = None
        self.election_timeout = random.uniform(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT)
        self.last_heartbeat = time.time()
        self.raft_state = "FOLLOWER"  # FOLLOWER, CANDIDATE, LEADER
        
        # 관리 중인 참여 노드 목록
        self.participant_nodes = {}
        
        # 백업 관련 상태
        if backup_for:
            logger.info(f"[BACKUP CONFIG] Node {self.id} will backup cluster cluster_{backup_for}")
            self.backup_primary_info = None
            logger.info(f"[BACKUP CONFIG] Backup cluster cluster_{backup_for} original primary is None")
        else:
            logger.info(f"[BACKUP CONFIG] Node {self.id} is NOT configured as backup for any cluster")
            
        # 추가 스레드
        self.discovery_thread = None
        self.election_thread = None
        self.backup_check_thread = None
        
    def initialize(self, desired_primary=None, desired_secondary=None, backup_for=None):
        """관리 노드 초기화"""
        logger.info(f"Node {self.id} initialized with cluster_id={self.cluster_id}, "
                    f"desired_primary={desired_primary}, desired_secondary={desired_secondary}, "
                    f"backup_for={backup_for}")
                    
        # 기본값 설정
        if desired_primary == self.id:
            self.is_primary = True
            self.raft_state = "LEADER"
            
        self.backup_for = backup_for
        self.desired_secondary = desired_secondary
        
        # ETCD에 등록
        self.register_as_manager()
        
    def register_as_manager(self):
        """관리 노드로 ETCD에 등록"""
        role = "PRIMARY" if self.is_primary else "SECONDARY"
        
        if self.is_primary:
            # Primary 관리자로 등록
            manager_info = {
                "node_id": self.id,
                "role": role,
                "timestamp": time.time()
            }
            self.etcd_client.put(f"/managers/{self.cluster_id}/primary", json.dumps(manager_info))
            logger.info(f"Node {self.id} registered as PRIMARY manager in cluster {self.cluster_id}.")
            
        else:
            # Secondary 관리자로 등록
            manager_info = {
                "node_id": self.id,
                "role": role,
                "timestamp": time.time()
            }
            self.etcd_client.put(f"/managers/{self.cluster_id}/secondary/{self.id}", 
                                json.dumps(manager_info))
            logger.info(f"Node {self.id} registered as SECONDARY (default) manager in cluster {self.cluster_id}.")
            
        # 백업 역할 등록
        if self.backup_for:
            backup_info = {
                "node_id": self.id,
                "original_cluster": self.cluster_id,
                "timestamp": time.time()
            }
            self.etcd_client.put(f"/managers/cluster_{self.backup_for}/backup/{self.id}", 
                                json.dumps(backup_info))
            logger.info(f"Node {self.id} from cluster {self.cluster_id} registered as BACKUP manager for cluster cluster_{self.backup_for}.")
        else:
            logger.info(f"Node {self.id} has no backup responsibilities.")
        
        # 클러스터 간 연결 정보 등록
        if self.desired_secondary:
            connection_info = {
                "primary_cluster": self.cluster_id,
                "secondary_cluster": self.desired_secondary.split('_')[0] if '_' in self.desired_secondary else None,
                "timestamp": time.time()
            }
            if connection_info["secondary_cluster"]:
                self.etcd_client.put(f"/cluster_connections/{self.cluster_id}/{connection_info['secondary_cluster']}", 
                                    json.dumps(connection_info))
                
    def start(self):
        """관리 노드 시작"""
        super().start()
        
        # 노드 탐색 스레드 시작
        self.discovery_thread = threading.Thread(target=self.discovery_loop)
        self.discovery_thread.daemon = True
        self.discovery_thread.start()
        
        # 선거 스레드 시작 (RAFT)
        self.election_thread = threading.Thread(target=self.election_loop)
        self.election_thread.daemon = True
        self.election_thread.start()
        
        # 백업 관리자 책임이 있는 경우 백업 체크 스레드 시작
        if self.backup_for:
            self.backup_check_thread = threading.Thread(target=self.backup_check_loop)
            self.backup_check_thread.daemon = True
            self.backup_check_thread.start()
            
    def discovery_loop(self):
        """주기적으로 클러스터 내 노드 탐색"""
        while self.status != "STOPPED":
            try:
                # 모든 노드 목록 가져오기
                nodes = self.get_all_nodes()
                
                # 클러스터 내 참여 노드 탐색
                cluster_nodes = [n for n in nodes if n["cluster_id"] == self.cluster_id]
                for node in cluster_nodes:
                    if node["node_id"] != self.id and node["type"] != "MANAGER":
                        self.participant_nodes[node["node_id"]] = node
                        logger.info(f"Discovered new node: {node['node_id']}")
                        
                # 다른 클러스터의 관리 노드 탐색
                for node in nodes:
                    if node["node_id"] != self.id and node["type"] == "MANAGER" and node["cluster_id"] != self.cluster_id:
                        logger.info(f"Discovered external manager node: {node['node_id']} from cluster {node['cluster_id']}")
                
                # 1초마다 탐색 (기존 5초에서 단축)
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error in discovery loop: {e}")
                time.sleep(1)
    
    def get_all_nodes(self):
        """ETCD에서 모든 노드 정보 가져오기"""
        nodes = []
        try:
            # 모든 노드 키 가져오기
            for result in self.etcd_client.get_prefix("/nodes/"):
                if result[0]:  # 값이 있는 경우
                    node_data = json.loads(result[0].decode('utf-8'))
                    nodes.append(node_data)
        except Exception as e:
            logger.error(f"Error getting nodes from etcd: {e}")
        return nodes
        
    def election_loop(self):
        """RAFT 선거 루프"""
        while self.status != "STOPPED":
            try:
                current_time = time.time()
                
                # FOLLOWER 상태일 때 타임아웃 체크
                if self.raft_state == "FOLLOWER":
                    if current_time - self.last_heartbeat > self.election_timeout:
                        # 타임아웃 발생, CANDIDATE로 상태 변경
                        self.become_candidate()
                
                # CANDIDATE 상태일 때 투표 요청
                elif self.raft_state == "CANDIDATE":
                    # 선거 진행 (실제 구현에서는 다른 노드에 투표 요청 전송)
                    # 여기서는 단순화를 위해 항상 당선되도록 함
                    self.become_leader()
                
                # LEADER 상태일 때 heartbeat 전송
                elif self.raft_state == "LEADER":
                    if current_time - self.last_heartbeat > HEARTBEAT_INTERVAL:
                        self.send_heartbeat()
                        self.last_heartbeat = current_time
                
                # 선거 타임아웃 범위 내에서 랜덤하게 대기
                # 기존 고정 5초에서 election_timeout 사용으로 변경
                wait_time = random.uniform(MIN_ELECTION_TIMEOUT / 2, self.election_timeout / 2)
                time.sleep(wait_time)
                
                # 새로운 타임아웃 값 설정 (RAFT 권장사항)
                self.election_timeout = random.uniform(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT)
                
            except Exception as e:
                logger.error(f"Error in election loop: {e}")
                time.sleep(1)
    
    def become_candidate(self):
        """CANDIDATE 상태로 전환"""
        self.raft_state = "CANDIDATE"
        self.current_term += 1
        self.voted_for = self.id  # 자신에게 투표
        logger.info(f"Node {self.id} became CANDIDATE for term {self.current_term}")
        
        # 실제 구현에서는 여기서 다른 노드에 투표 요청 전송
        
    def become_leader(self):
        """LEADER 상태로 전환"""
        self.raft_state = "LEADER"
        self.is_primary = True
        logger.info(f"Node {self.id} became LEADER for term {self.current_term}")
        
        # ETCD에 리더 정보 업데이트
        self.register_as_manager()
        
        # 즉시 heartbeat 전송
        self.send_heartbeat()
        self.last_heartbeat = time.time()
        
    def send_heartbeat(self):
        """모든 Follower에게 heartbeat 전송"""
        # 실제 구현에서는 여기서 모든 Follower에게 AppendEntries RPC 전송
        if self.raft_state == "LEADER":
            # Primary 정보 갱신
            self.register_as_manager()
            
            # 여기서는 로깅만 수행 (실제로는 RPC 전송)
            logger.debug(f"Leader {self.id} sending heartbeat for term {self.current_term}")
    
    def receive_heartbeat(self, sender_id, term):
        """다른 노드로부터 heartbeat 수신"""
        if term < self.current_term:
            # 더 낮은 term의 heartbeat는 무시
            return False
            
        if term > self.current_term:
            # 더 높은 term을 발견하면 follower로 전환
            self.current_term = term
            self.raft_state = "FOLLOWER"
            self.voted_for = None
            self.is_primary = False
            
        # heartbeat 시간 갱신
        self.last_heartbeat = time.time()
        
        # LEADER로부터 온 경우 primary로 인정
        if sender_id != self.id:
            primary_info = self.get_primary_info()
            if primary_info and primary_info["node_id"] == sender_id:
                self.is_primary = False
                
        return True
        
    def backup_check_loop(self):
        """백업 관리자로서 primary 상태 체크"""
        while self.status != "STOPPED" and self.backup_for:
            try:
                logger.info(f"[heartbeat_loop] Node {self.id} is backup for cluster cluster_{self.backup_for}")
                
                # 백업 대상 클러스터의 primary 정보 확인
                primary_info = self.get_backup_primary_info()
                
                if primary_info:
                    primary_id = primary_info["node_id"]
                    logger.info(f"[heartbeat_loop] Backup cluster cluster_{self.backup_for} original primary: {primary_id}")
                    self.backup_primary_info = primary_info
                else:
                    logger.info(f"[heartbeat_loop] Backup cluster cluster_{self.backup_for} original primary: unknown")
                    
                    # PRIMARY가 없는 경우 장애 판단 및 TEMPORARY PRIMARY 승격
                    self.check_backup_primary()
                
                # 주기적 체크
                time.sleep(HEARTBEAT_INTERVAL * 3)
                
            except Exception as e:
                logger.error(f"Error in backup check loop: {e}")
                time.sleep(1)
    
    def get_backup_primary_info(self):
        """백업 대상 클러스터의 primary 정보 가져오기"""
        try:
            primary_key = f"/managers/{self.backup_for}/primary"
            result = self.etcd_client.get(primary_key)
            
            if result[0]:
                primary_info = json.loads(result[0].decode('utf-8'))
                # 타임스탬프 확인: 오래된 정보인지 체크
                if time.time() - primary_info.get("timestamp", 0) > MAX_ELECTION_TIMEOUT * 2:
                    # 오래된 정보는 무시
                    return None
                return primary_info
        except Exception as e:
            logger.error(f"Error getting backup primary info: {e}")
        
        return None
        
    def check_backup_primary(self):
        """백업 대상 클러스터의 PRIMARY가 없거나 장애인 경우 처리"""
        primary_info = self.get_backup_primary_info()
        
        # PRIMARY가 없는 경우
        if not primary_info:
            logger.info(f"[check_backup_primary] *** No existing primary for backup cluster cluster_{self.backup_for}. Node {self.id} taking over! ***")
            self.become_temporary_primary_for_backup()
            return True
            
        # PRIMARY가 있지만 상태가 불량한 경우 (최근 업데이트가 없음)
        primary_id = primary_info["node_id"]
        primary_timestamp = primary_info.get("timestamp", 0)
        
        if time.time() - primary_timestamp > MAX_ELECTION_TIMEOUT * 2:
            logger.info(f"[check_backup_primary] *** Primary {primary_id} for backup cluster cluster_{self.backup_for} is not responsive. Node {self.id} taking over! ***")
            self.become_temporary_primary_for_backup()
            return True
            
        return False
        
    def become_temporary_primary_for_backup(self):
        """백업 클러스터의 임시 PRIMARY로 승격"""
        # 이제 더 이상 step_down 타이머를 설정하지 않음 (자동 강등 제거)
        logger.info(f"*** Node {self.id} PROMOTED to TEMPORARY PRIMARY for cluster cluster_{self.backup_for} due to missing primary! ***")
        
        # ETCD에 백업 클러스터의 새 PRIMARY로 등록
        backup_primary_info = {
            "node_id": self.id,
            "role": "TEMPORARY_PRIMARY",
            "original_cluster": self.cluster_id,
            "timestamp": time.time()
        }
        
        self.etcd_client.put(f"/managers/{self.backup_for}/primary", json.dumps(backup_primary_info))
        
        # 백업 클러스터의 참여 노드에게 새 Primary 정보 알림 (이벤트 발행)
        self.notify_backup_nodes_of_new_primary()
    
    def notify_backup_nodes_of_new_primary(self):
        """백업 클러스터의 참여 노드에게 새 PRIMARY 정보 알림"""
        # 백업 클러스터의 모든 참여 노드 가져오기
        backup_nodes = self.get_backup_cluster_nodes()
        
        # 각 노드에게 새 PRIMARY 정보 알림
        for node in backup_nodes:
            # 실제 구현에서는 여기서 각 노드에게 메시지 전송
            logger.info(f"Notifying node {node['node_id']} of new temporary primary {self.id} for cluster {self.backup_for}")
    
    def get_backup_cluster_nodes(self):
        """백업 클러스터의 모든 참여 노드 가져오기"""
        nodes = []
        try:
            # 백업 클러스터의 모든 노드 키 가져오기
            for result in self.etcd_client.get_prefix(f"/nodes/cluster_{self.backup_for}/"):
                if result[0]:  # 값이 있는 경우
                    node_data = json.loads(result[0].decode('utf-8'))
                    if node_data["type"] != "MANAGER":  # 관리 노드가 아닌 참여 노드만 포함
                        nodes.append(node_data)
        except Exception as e:
            logger.error(f"Error getting backup cluster nodes: {e}")
        return nodes
        
    def get_primary_info(self):
        """현재 클러스터의 PRIMARY 정보 가져오기"""
        try:
            primary_key = f"/managers/{self.cluster_id}/primary"
            result = self.etcd_client.get(primary_key)
            
            if result[0]:
                return json.loads(result[0].decode('utf-8'))
        except Exception as e:
            logger.error(f"Error getting primary info: {e}")
        
        return None


class ParticipantNode(Node):
    """P2P 참여 노드 클래스"""
    
    def __init__(self, node_id, cluster_id, etcd_client=None):
        super().__init__(node_id, cluster_id, etcd_client)
        self.type = "PARTICIPANT"
        
        # 관리자 노드 참조
        self.primary_manager_id = None
        self.secondary_manager_id = None
        self.primary_manager_connection = None
        self.secondary_manager_connection = None
        
        # 연결 상태 확인 타이머
        self.connection_check_timer = None
        
    def start(self):
        """참여 노드 시작"""
        super().start()
        
        # 관리자 노드 탐색 및 연결
        self.find_managers()
        
        # 관리자 노드 연결 상태 확인 타이머 시작
        self.connection_check_timer = threading.Timer(HEALTH_CHECK_INTERVAL, self.check_manager_connections)
        self.connection_check_timer.daemon = True
        self.connection_check_timer.start()
        
        logger.info(f"Participant node {self.id} is running")
        
    def find_managers(self):
        """클러스터의 관리자 노드 탐색 및 연결"""
        try:
            # PRIMARY 관리자 찾기
            primary_key = f"/managers/{self.cluster_id}/primary"
            primary_result = self.etcd_client.get(primary_key)
            
            if primary_result[0]:
                primary_info = json.loads(primary_result[0].decode('utf-8'))
                self.primary_manager_id = primary_info["node_id"]
                
                # PRIMARY 관리자에 연결
                self.connect_to_primary_manager()
            else:
                logger.warning(f"No primary manager found for cluster {self.cluster_id}")
                # PRIMARY가 없는 경우 10초 후 자신을 관리자로 승격 (클러스터 최초 구성 시)
                self.promotion_timer = threading.Timer(10, self.promote_to_manager)
                self.promotion_timer.daemon = True
                self.promotion_timer.start()
            
            # SECONDARY 관리자 찾기 (여러 개 있을 수 있음)
            for result in self.etcd_client.get_prefix(f"/managers/{self.cluster_id}/secondary/"):
                if result[0]:
                    secondary_info = json.loads(result[0].decode('utf-8'))
                    self.secondary_manager_id = secondary_info["node_id"]
                    
                    # SECONDARY 관리자에 연결
                    self.connect_to_secondary_manager()
                    # 한 개의 SECONDARY 연결만 유지 (여러 개 있더라도)
                    break
            
        except Exception as e:
            logger.error(f"Error finding managers: {e}")
    
    def connect_to_primary_manager(self):
        """PRIMARY 관리자에 연결"""
        if not self.primary_manager_id:
            return False
            
        try:
            self.primary_manager_connection = self.connect_to_node(self.primary_manager_id)
            
            # 관리자에 등록
            self.register_to_manager(self.primary_manager_id, is_primary=True)
            
            logger.info(f"Connected to primary manager {self.primary_manager_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to primary manager {self.primary_manager_id}: {e}")
            return False
    
    def connect_to_secondary_manager(self):
        """SECONDARY 관리자에 연결"""
        if not self.secondary_manager_id:
            return False
            
        try:
            self.secondary_manager_connection = self.connect_to_node(self.secondary_manager_id)
            
            # 관리자에 등록
            self.register_to_manager(self.secondary_manager_id, is_primary=False)
            
            logger.info(f"Connected to secondary manager {self.secondary_manager_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to secondary manager {self.secondary_manager_id}: {e}")
            return False
    
    def register_to_manager(self, manager_id, is_primary=True):
        """관리자 노드에 등록"""
        # 실제 구현에서는 여기서 관리자에게 등록 메시지 전송
        logger.info(f"Registering to {'primary' if is_primary else 'secondary'} manager {manager_id}")
        
    def check_manager_connections(self):
        """관리자 노드 연결 상태 확인 및 필요시 재연결"""
        try:
            # PRIMARY 연결 확인
            if self.primary_manager_id:
                if self.primary_manager_id not in self.connections:
                    # 연결이 끊어진 경우 재연결 시도
                    logger.warning(f"Primary manager connection lost. Attempting to reconnect to {self.primary_manager_id}")
                    self.connect_to_primary_manager()
                    
            # SECONDARY 연결 확인
            if self.secondary_manager_id:
                if self.secondary_manager_id not in self.connections:
                    # 연결이 끊어진 경우 재연결 시도
                    logger.warning(f"Secondary manager connection lost. Attempting to reconnect to {self.secondary_manager_id}")
                    self.connect_to_secondary_manager()
                    
            # PRIMARY가 없는 경우 SECONDARY를 PRIMARY로 승격
            if not self.primary_manager_id and self.secondary_manager_id:
                # ETCD에서 다시 PRIMARY 확인 (다른 노드가 이미 PRIMARY가 되었을 수 있음)
                primary_key = f"/managers/{self.cluster_id}/primary"
                primary_result = self.etcd_client.get(primary_key)
                
                if not primary_result[0]:  # PRIMARY가 여전히 없음
                    # SECONDARY를 PRIMARY로 사용
                    self.primary_manager_id = self.secondary_manager_id
                    self.primary_manager_connection = self.secondary_manager_connection
                    logger.info(f"No primary manager available. Using secondary manager {self.secondary_manager_id} as primary")
                else:
                    # 새 PRIMARY 발견
                    primary_info = json.loads(primary_result[0].decode('utf-8'))
                    self.primary_manager_id = primary_info["node_id"]
                    self.connect_to_primary_manager()
            
            # 관리자가 모두 없는 경우 새로 찾기
            if not self.primary_manager_id and not self.secondary_manager_id:
                logger.warning("No managers available. Searching for new managers...")
                self.find_managers()
                
            # 타이머 재시작
            self.connection_check_timer = threading.Timer(HEALTH_CHECK_INTERVAL, self.check_manager_connections)
            self.connection_check_timer.daemon = True
            self.connection_check_timer.start()
            
        except Exception as e:
            logger.error(f"Error checking manager connections: {e}")
            # 오류 발생 시에도 타이머 재시작
            self.connection_check_timer = threading.Timer(HEALTH_CHECK_INTERVAL, self.check_manager_connections)
            self.connection_check_timer.daemon = True
            self.connection_check_timer.start()
            
    def promote_to_manager(self):
        """관리자 노드가 없는 경우 자신을 관리자로 승격"""
        try:
            # 다시 한번 PRIMARY 확인 (다른 노드가 이미 승격되었을 수 있음)
            primary_key = f"/managers/{self.cluster_id}/primary"
            primary_result = self.etcd_client.get(primary_key)
            
            if primary_result[0]:  # PRIMARY가 있음
                primary_info = json.loads(primary_result[0].decode('utf-8'))
                self.primary_manager_id = primary_info["node_id"]
                self.connect_to_primary_manager()
                return
                
            # PRIMARY가 10초 이상 없는 경우 자신을 관리자로 승격
            logger.info(f"No primary detected in cluster {self.cluster_id} for 10 seconds. Promoting self.")
            logger.info(f"Promoting participant node {self.id} to management node.")
            
            # 실제 구현에서는 여기서 관리자 노드로 변환
            # 여기서는 SECONDARY 관리자로 등록만 수행
            manager_info = {
                "node_id": self.id,
                "role": "SECONDARY",
                "timestamp": time.time()
            }
            
            self.etcd_client.put(f"/managers/{self.cluster_id}/secondary/{self.id}", 
                                json.dumps(manager_info))
            
            # 필요시 관리자 노드 객체 생성 및 시작
            # 실제 구현에서는 이 부분이 더 복잡할 수 있음
            
        except Exception as e:
            logger.error(f"Error promoting to manager: {e}")
