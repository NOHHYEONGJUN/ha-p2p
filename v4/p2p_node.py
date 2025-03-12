import time
import json
import random
import threading
import logging
import uuid
from typing import Dict, List, Any, Optional, Tuple, Set

from etcd_client import EtcdClient
from config import (
    HEARTBEAT_INTERVAL, MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT,
    NODE_INFO_PREFIX, MANAGER_INFO_PREFIX, CLUSTER_INFO_PREFIX, BACKUP_INFO_PREFIX,
    HEALTH_CHECK_INTERVAL, MAX_HEARTBEAT_MISS, NODE_PORT,
    CLUSTER_BACKUP_MAP, CLUSTER_HEALTH_CHECK_INTERVAL, BACKUP_ACTIVATION_TIMEOUT
)

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger('p2p_node')

class Node:
    """모든 P2P 노드의 기본 클래스"""
    def __init__(self, node_id: str, cluster_id: str, 
                 etcd_endpoints: Optional[List[str]] = None,
                 backup_etcd_endpoints: Optional[List[str]] = None):
        self.id = node_id
        self.cluster_id = cluster_id
        self.status = "INITIALIZING"
        self.type = "UNKNOWN"
        self.last_updated = time.time()
        
        # ETCD 클라이언트 초기화
        self.etcd_client = EtcdClient(
            cluster_id=cluster_id,
            endpoints=etcd_endpoints,
            backup_endpoints=backup_etcd_endpoints
        )
        
        self.connections = {}
        self.connection_lock = threading.Lock()
        
        self.metadata = {
            "node_id": self.id,
            "cluster_id": self.cluster_id,
            "status": self.status,
            "type": self.type,
            "last_updated": self.last_updated
        }
        
        self.uuid = str(uuid.uuid4())
        self.heartbeat_thread = None
        self.stop_event = threading.Event()
    
    def register_to_etcd(self):
        """노드 정보를 ETCD에 등록"""
        self.metadata["last_updated"] = time.time()
        node_key = f"{NODE_INFO_PREFIX}/{self.cluster_id}/{self.id}"
        self.etcd_client.put(node_key, self.metadata)
        cluster_node_key = f"{CLUSTER_INFO_PREFIX}/{self.cluster_id}/nodes/{self.id}"
        self.etcd_client.put(cluster_node_key, self.metadata)
        logger.debug(f"Node {self.id} registered to etcd")
    
    def update_status(self, status):
        """노드 상태 업데이트"""
        self.status = status
        self.metadata["status"] = status
        self.metadata["last_updated"] = time.time()
        self.register_to_etcd()
    
    def connect_to_node(self, target_node_id):
        """다른 노드에 연결 (모의 구현)"""
        with self.connection_lock:
            if target_node_id in self.connections:
                return self.connections[target_node_id]
            connection = {
                "node_id": target_node_id,
                "status": "CONNECTED",
                "established_at": time.time(),
                "last_message": time.time()
            }
            self.connections[target_node_id] = connection
            logger.info(f"Node {self.id} connected to {target_node_id}")
            return connection
    
    def send_message(self, target_node_id, message_type, payload=None):
        """메시지 전송 (모의 구현)"""
        if payload is None:
            payload = {}
        logger.debug(f"Node {self.id} sending {message_type} to {target_node_id}: {payload}")
        if target_node_id not in self.connections:
            self.connect_to_node(target_node_id)
        self.connections[target_node_id]["last_message"] = time.time()
        return True
    
    def heartbeat_loop(self):
        """주기적으로 heartbeat를 보내고 상태 업데이트"""
        while not self.stop_event.is_set():
            try:
                self.register_to_etcd()
                self.check_cluster_status()
                if not self.stop_event.wait(HEARTBEAT_INTERVAL):
                    continue
                else:
                    break
            except Exception as e:
                logger.error(f"Error in heartbeat loop: {e}")
                if not self.stop_event.wait(1.0):
                    continue
                else:
                    break
    
    def check_cluster_status(self):
        """클러스터 상태 확인 (하위 클래스에서 구현)"""
        pass
    
    def start(self):
        """노드 시작"""
        self.port = NODE_PORT
        self.update_status("ACTIVE")
        if not self.etcd_client.is_cluster_healthy():
            logger.warning("ETCD 클러스터 상태가 정상이 아닙니다. 제한된 기능으로 동작합니다.")
        logger.info(f"Node {self.id} started on port {self.port}")
        self.stop_event.clear()
        self.heartbeat_thread = threading.Thread(target=self.heartbeat_loop)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()
        logger.info(f"Node {self.id} started")
    
    def stop(self):
        """노드 중지"""
        self.stop_event.set()
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join(timeout=2.0)
        self.update_status("STOPPED")
        logger.info(f"Node {self.id} stopped")
    
    def get_cluster_nodes(self) -> List[Dict[str, Any]]:
        """현재 클러스터의 모든 노드 정보 조회"""
        nodes = []
        try:
            results = self.etcd_client.get_prefix(f"{CLUSTER_INFO_PREFIX}/{self.cluster_id}/nodes/")
            for _, node_data in results:
                nodes.append(node_data)
        except Exception as e:
            logger.error(f"Error getting cluster nodes: {e}")
        return nodes

class ManagementNode(Node):
    """P2P 관리 노드 (매니저) 클래스"""
    def __init__(self, node_id, cluster_id, is_primary=False,
                 backup_for=None, etcd_endpoints=None, backup_etcd_endpoints=None):
        super().__init__(node_id, cluster_id, etcd_endpoints, backup_etcd_endpoints)
        self.type = "MANAGER"
        self.is_primary = is_primary
        self.backup_for = backup_for
        self.participant_nodes = {}
        self.backup_mode_active = False
        self.backup_check_thread = None
        self.etcd_monitor_thread = None
        self.discovery_thread = None
    
    def initialize(self):
        logger.info(f"Management node {self.id} initializing for cluster {self.cluster_id}")
        self.register_as_manager()
        manager_event = {
            "event_type": "MANAGER_STARTED",
            "node_id": self.id,
            "cluster_id": self.cluster_id,
            "is_primary": self.is_primary,
            "timestamp": time.time()
        }
        event_key = f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/events/{int(time.time())}"
        self.etcd_client.put(event_key, manager_event)
        if self.backup_for:
            self.setup_backup_role()
    
    def register_as_manager(self):
        manager_key = f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/{self.id}"
        manager_info = {
            "node_id": self.id,
            "cluster_id": self.cluster_id,
            "is_primary": self.is_primary,
            "type": "MANAGER",
            "status": self.status,
            "last_updated": time.time()
        }
        self.etcd_client.put(manager_key, manager_info)
        cluster_key = f"{CLUSTER_INFO_PREFIX}/{self.cluster_id}/info"
        cluster_info = {
            "cluster_id": self.cluster_id,
            "manager_id": self.id,
            "is_primary": self.is_primary,
            "node_count": 0,
            "last_updated": time.time()
        }
        self.etcd_client.put(cluster_key, cluster_info)
        logger.info(f"Node {self.id} registered as {'PRIMARY' if self.is_primary else 'SECONDARY'} manager in cluster {self.cluster_id}")
    
    def setup_backup_role(self):
        if not self.backup_for:
            return
        backup_key = f"{BACKUP_INFO_PREFIX}/{self.backup_for}/managers/{self.id}"
        backup_info = {
            "node_id": self.id,
            "cluster_id": self.cluster_id,
            "backup_for": self.backup_for,
            "status": "STANDBY",
            "last_updated": time.time()
        }
        self.etcd_client.put(backup_key, backup_info)
        logger.info(f"Node {self.id} registered as backup manager for cluster {self.backup_for}")
    
    def start(self):
        super().start()
        self.initialize()
        self.discovery_thread = threading.Thread(target=self.discovery_loop)
        self.discovery_thread.daemon = True
        self.discovery_thread.start()
        self.etcd_monitor_thread = threading.Thread(target=self.monitor_etcd_status)
        self.etcd_monitor_thread.daemon = True
        self.etcd_monitor_thread.start()
        if self.backup_for:
            self.backup_check_thread = threading.Thread(target=self.backup_check_loop)
            self.backup_check_thread.daemon = True
            self.backup_check_thread.start()
        logger.info(f"Management node {self.id} started for cluster {self.cluster_id}")
    
    def stop(self):
        super().stop()
        if self.backup_mode_active and self.backup_for:
            self.deactivate_backup_mode()
        try:
            manager_event = {
                "event_type": "MANAGER_STOPPED",
                "node_id": self.id,
                "cluster_id": self.cluster_id,
                "timestamp": time.time()
            }
            event_key = f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/events/{int(time.time())}"
            self.etcd_client.put(event_key, manager_event)
        except Exception as e:
            logger.error(f"Error publishing manager stop event: {e}")
        logger.info(f"Management node {self.id} stopped")
    
    def discovery_loop(self):
        while not self.stop_event.is_set():
            try:
                nodes = self.get_cluster_nodes()
                active_count = 0
                for node in nodes:
                    if node["node_id"] != self.id:
                        node_id = node["node_id"]
                        node_type = node.get("type", "UNKNOWN")
                        node_status = node.get("status", "UNKNOWN")
                        if node_status == "ACTIVE":
                            active_count += 1
                        if node_id not in self.participant_nodes and node_type != "MANAGER":
                            self.participant_nodes[node_id] = node
                            logger.info(f"Discovered new participant node: {node_id}")
                self.update_cluster_info(active_count)
                if not self.stop_event.wait(HEALTH_CHECK_INTERVAL):
                    continue
                else:
                    break
            except Exception as e:
                logger.error(f"Error in discovery loop: {e}")
                if not self.stop_event.wait(1.0):
                    continue
                else:
                    break
    
    def update_cluster_info(self, active_node_count):
        try:
            cluster_key = f"{CLUSTER_INFO_PREFIX}/{self.cluster_id}/info"
            current_info = self.etcd_client.get(cluster_key) or {}
            current_info.update({
                "cluster_id": self.cluster_id,
                "manager_id": self.id,
                "is_primary": self.is_primary,
                "node_count": active_node_count,
                "last_updated": time.time()
            })
            self.etcd_client.put(cluster_key, current_info)
        except Exception as e:
            logger.error(f"Error updating cluster info: {e}")
    
    def monitor_etcd_status(self):
        while not self.stop_event.is_set():
            try:
                is_healthy = self.etcd_client.is_cluster_healthy()
                if not is_healthy:
                    logger.warning(f"ETCD 클러스터 상태가 비정상입니다. 클러스터: {self.cluster_id}")
                    if self.etcd_client.is_using_backup():
                        logger.info("현재 백업 ETCD 클러스터를 사용 중입니다.")
                        if random.random() < 0.2:
                            if self.etcd_client.switch_to_primary():
                                logger.info("주 ETCD 클러스터로 성공적으로 전환되었습니다.")
                    else:
                        logger.warning("주 ETCD 클러스터 상태가 비정상이지만 백업이 구성되지 않았습니다.")
                if not self.stop_event.wait(CLUSTER_HEALTH_CHECK_INTERVAL):
                    continue
                else:
                    break
            except Exception as e:
                logger.error(f"Error in ETCD monitor loop: {e}")
                if not self.stop_event.wait(1.0):
                    continue
                else:
                    break
    
    def check_cluster_status(self):
        try:
            manager_results = self.etcd_client.get_prefix(f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/")
            primary_managers = []
            for _, manager_data in manager_results:
                if manager_data["node_id"] != self.id and manager_data.get("is_primary", False):
                    primary_managers.append(manager_data)
            if self.is_primary and primary_managers:
                logger.warning(f"Multiple primary managers detected in cluster {self.cluster_id}")
                oldest_manager = min(primary_managers, key=lambda x: x.get("last_updated", float('inf')))
                if oldest_manager["last_updated"] < self.metadata["last_updated"]:
                    logger.info(f"Yielding primary role to older manager: {oldest_manager['node_id']}")
                    self.is_primary = False
                    self.register_as_manager()
        except Exception as e:
            logger.error(f"Error in check_cluster_status: {e}")
    
    def backup_check_loop(self):
        if not self.backup_for:
            return
        while not self.stop_event.is_set():
            try:
                target_cluster_key = f"{CLUSTER_INFO_PREFIX}/{self.backup_for}/info"
                target_cluster_info = self.etcd_client.get(target_cluster_key)
                current_time = time.time()
                if target_cluster_info:
                    last_updated = target_cluster_info.get("last_updated", 0)
                    time_since_update = current_time - last_updated
                    if time_since_update > BACKUP_ACTIVATION_TIMEOUT:
                        logger.warning(f"Target cluster {self.backup_for} has not updated in {time_since_update:.1f} seconds")
                        if not self.backup_mode_active:
                            logger.info(f"Activating backup mode for cluster {self.backup_for}")
                            self.activate_backup_mode()
                    elif self.backup_mode_active:
                        logger.info(f"Target cluster {self.backup_for} is back online, deactivating backup mode")
                        self.deactivate_backup_mode()
                else:
                    if not self.backup_mode_active:
                        logger.warning(f"No information for target cluster {self.backup_for}, activating backup mode")
                        self.activate_backup_mode()
                if not self.stop_event.wait(3.0):
                    continue
                else:
                    break
            except Exception as e:
                logger.error(f"Error in backup check loop: {e}")
                if not self.stop_event.wait(1.0):
                    continue
                else:
                    break
    
    def activate_backup_mode(self):
        if self.backup_mode_active or not self.backup_for:
            return
        try:
            backup_key = f"{BACKUP_INFO_PREFIX}/{self.backup_for}/managers/{self.id}"
            backup_info = {
                "node_id": self.id,
                "cluster_id": self.cluster_id,
                "backup_for": self.backup_for,
                "status": "ACTIVE",
                "activated_at": time.time(),
                "last_updated": time.time()
            }
            self.etcd_client.put(backup_key, backup_info)
            event_key = f"{BACKUP_INFO_PREFIX}/{self.backup_for}/events/{int(time.time())}"
            event_data = {
                "event_type": "BACKUP_ACTIVATED",
                "node_id": self.id,
                "cluster_id": self.cluster_id,
                "backup_for": self.backup_for,
                "timestamp": time.time()
            }
            self.etcd_client.put(event_key, event_data)
            self.backup_mode_active = True
            logger.info(f"Backup mode activated for cluster {self.backup_for} by node {self.id}")
            temp_manager_key = f"{MANAGER_INFO_PREFIX}/{self.backup_for}/temp_{self.id}"
            temp_manager_info = {
                "node_id": self.id,
                "cluster_id": self.cluster_id,
                "is_primary": True,
                "is_temporary": True,
                "type": "MANAGER",
                "status": "ACTIVE",
                "backup_source": True,
                "last_updated": time.time()
            }
            self.etcd_client.put(temp_manager_key, temp_manager_info)
        except Exception as e:
            logger.error(f"Error activating backup mode: {e}")
    
    def deactivate_backup_mode(self):
        if not self.backup_mode_active or not self.backup_for:
            return
        try:
            backup_key = f"{BACKUP_INFO_PREFIX}/{self.backup_for}/managers/{self.id}"
            backup_info = {
                "node_id": self.id,
                "cluster_id": self.cluster_id,
                "backup_for": self.backup_for,
                "status": "STANDBY",
                "deactivated_at": time.time(),
                "last_updated": time.time()
            }
            self.etcd_client.put(backup_key, backup_info)
            event_key = f"{BACKUP_INFO_PREFIX}/{self.backup_for}/events/{int(time.time())}"
            event_data = {
                "event_type": "BACKUP_DEACTIVATED",
                "node_id": self.id,
                "cluster_id": self.cluster_id,
                "backup_for": self.backup_for,
                "timestamp": time.time()
            }
            self.etcd_client.put(event_key, event_data)
            temp_manager_key = f"{MANAGER_INFO_PREFIX}/{self.backup_for}/temp_{self.id}"
            self.etcd_client.delete(temp_manager_key)
            self.backup_mode_active = False
            logger.info(f"Backup mode deactivated for cluster {self.backup_for} by node {self.id}")
        except Exception as e:
            logger.error(f"Error deactivating backup mode: {e}")

class ParticipantNode(Node):
    """P2P 참여 노드 클래스"""
    def __init__(self, node_id, cluster_id, etcd_endpoints=None, backup_etcd_endpoints=None):
        super().__init__(node_id, cluster_id, etcd_endpoints, backup_etcd_endpoints)
        self.type = "PARTICIPANT"  # 유형 명시적 설정
        self.manager_id = None
        self.manager_connection = None
        self.temp_manager_id = None
        self.temp_manager_connection = None
        self.manager_watch_ids = None
        self.promotion_check_interval = 2.0  # 2초마다 승격 확인
        self.promotion_thread = None
        self.election_watch_ids = None
    
    def start(self):
        super().start()
        self.find_manager()
        self.watch_manager_changes()
        
        # 승격 확인 스레드 시작
        self.promotion_thread = threading.Thread(target=self.promotion_check_loop)
        self.promotion_thread.daemon = True
        self.promotion_thread.start()
        
        # 선출 이벤트 감시
        self.watch_election_events()
        
        logger.info(f"Participant node {self.id} is running in cluster {self.cluster_id}")
    
    def stop(self):
        if self.manager_watch_ids:
            self.etcd_client.cancel_watch(self.manager_watch_ids)
        if self.election_watch_ids:
            self.etcd_client.cancel_watch(self.election_watch_ids)
        super().stop()
    
    def log_detailed_status(self):
        """현재 노드의 상세 상태 및 이벤트 정보 로깅"""
        logger.info(f"=== 노드 {self.id} 상세 상태 ===")
        logger.info(f"유형: {self.type}, 클러스터: {self.cluster_id}, 상태: {self.status}")
        
        # 현재 관리자 정보
        logger.info(f"현재 관리자: {self.manager_id}, 임시 관리자: {self.temp_manager_id}")
        
        # 선출 정보 확인
        try:
            election_key = f"{CLUSTER_INFO_PREFIX}/{self.cluster_id}/election"
            election_data = self.etcd_client.get(election_key)
            logger.info(f"선출 정보: {election_data}")
        except Exception as e:
            logger.info(f"선출 정보 조회 실패: {e}")
        
        # 승격 정보 확인
        try:
            promotion_key = f"{CLUSTER_INFO_PREFIX}/{self.cluster_id}/promoted_manager"
            promotion_data = self.etcd_client.get(promotion_key)
            logger.info(f"승격 정보: {promotion_data}")
        except Exception as e:
            logger.info(f"승격 정보 조회 실패: {e}")
        
        # 클러스터 정보 확인
        try:
            cluster_key = f"{CLUSTER_INFO_PREFIX}/{self.cluster_id}/info"
            cluster_info = self.etcd_client.get(cluster_key)
            logger.info(f"클러스터 정보: {cluster_info}")
        except Exception as e:
            logger.info(f"클러스터 정보 조회 실패: {e}")
        
        # 이벤트 정보 확인
        try:
            events_prefix = f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/events/"
            events = self.etcd_client.get_prefix(events_prefix)
            event_count = len(events)
            recent_events = events[-5:] if event_count > 5 else events
            logger.info(f"최근 이벤트 ({event_count}개 중 {len(recent_events)}개): {recent_events}")
        except Exception as e:
            logger.info(f"이벤트 정보 조회 실패: {e}")
    
    def watch_election_events(self):
        """선출 이벤트 감시"""
        try:
            events_prefix = f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/events/"
            
            def on_election_event(event):
                try:
                    key = event[1].key.decode('utf-8')
                    logger.info(f"선출 이벤트 감지: {key}")
                    
                    value_bytes = event[0]
                    if not value_bytes:
                        return
                        
                    value = json.loads(value_bytes.decode('utf-8'))
                    event_type = value.get("event_type")
                    
                    if event_type == "MANAGER_ELECTION_NEEDED":
                        logger.info(f"관리자 선출 요청 이벤트 감지: {key}")
                        # 즉시 승격 확인
                        self.check_promotion_status()
                        # 선출 참여
                        self.participate_in_election()
                    elif event_type == "DIRECT_PROMOTION_REQUEST" and value.get("elected_node") == self.id:
                        logger.info(f"직접 승격 요청 이벤트 감지: {key}")
                        # 즉시 승격 시도
                        self.check_promotion_status()
                    elif event_type == "MANAGER_ELECTION_COMPLETED":
                        logger.info(f"관리자 선출 완료 이벤트 감지: {key}")
                        elected_node = value.get("elected_node")
                        if elected_node == self.id:
                            logger.info(f"노드 {self.id}가 새 관리자로 선출됨")
                            # 즉시 승격 상태 확인
                            self.check_promotion_status()
                except Exception as e:
                    logger.error(f"선출 이벤트 처리 오류: {e}")
            
            # 이벤트 감시 설정
            self.election_watch_ids = self.etcd_client.watch_prefix(events_prefix, on_election_event)
            if not self.election_watch_ids:
                logger.warning("Failed to set up watch for election events")
        except Exception as e:
            logger.error(f"Error setting up watch for election events: {e}")
    
    def participate_in_election(self):
        """선출 과정에 참여"""
        try:
            # 현재 노드가 활성 상태인지 확인
            if self.status != "ACTIVE":
                logger.info(f"노드 {self.id}가 활성 상태가 아니므로 선출에 참여하지 않음")
                return False
            
            logger.info(f"노드 {self.id}가 선출 과정 참여 시도")
            self.log_detailed_status()
            
            # 이미 후보로 등록되어 있는지 확인
            election_key = f"{CLUSTER_INFO_PREFIX}/{self.cluster_id}/election"
            election_data = self.etcd_client.get(election_key)
            
            if election_data:
                logger.info(f"현재 선출 정보: {election_data}")
                
                if election_data.get("elected_node") == self.id:
                    logger.info(f"노드 {self.id}가 이미 선출 후보로 등록됨")
                    # 승격 상태 바로 확인하여 처리
                    self.check_promotion_status()
                    return True
                
                # 마지막 선출 시간 확인
                election_time = election_data.get("election_time", 0)
                current_time = time.time()
                elapsed_time = current_time - election_time
                
                if elapsed_time > 60.0:  # 60초 이상 경과했으면 선출 정보 갱신 시도
                    logger.info(f"기존 선출 정보가 {elapsed_time:.1f}초 경과로 오래됨, 새로 선출 시도")
                    
                    # 새 선출 정보 등록
                    new_election_data = {
                        "elected_node": self.id,
                        "election_time": current_time,
                        "status": "PENDING"
                    }
                    self.etcd_client.put(election_key, new_election_data)
                    logger.info(f"노드 {self.id}가 새 선출 정보 등록: {new_election_data}")
                    
                    # 승격 정보도 갱신
                    promotion_key = f"{CLUSTER_INFO_PREFIX}/{self.cluster_id}/promoted_manager"
                    promotion_data = {
                        "node_id": self.id,
                        "elected_at": current_time,
                        "status": "ELECTED"
                    }
                    self.etcd_client.put(promotion_key, promotion_data)
                    logger.info(f"노드 {self.id}가 새 승격 정보 등록: {promotion_data}")
                    
                    # 승격 상태 즉시 확인
                    self.check_promotion_status()
                    return True
            else:
                # 현재 선출 정보가 없으면 새로 등록
                logger.info(f"현재 선출 정보 없음, 노드 {self.id}가 선출 후보로 등록")
                
                # 현재 시간 기록
                current_time = time.time()
                
                # 선출 정보 등록
                new_election_data = {
                    "elected_node": self.id,
                    "election_time": current_time,
                    "status": "PENDING"
                }
                self.etcd_client.put(election_key, new_election_data)
                logger.info(f"노드 {self.id}가 선출 정보 등록: {new_election_data}")
                
                # 승격 정보도 등록
                promotion_key = f"{CLUSTER_INFO_PREFIX}/{self.cluster_id}/promoted_manager"
                promotion_data = {
                    "node_id": self.id,
                    "elected_at": current_time,
                    "status": "ELECTED"
                }
                self.etcd_client.put(promotion_key, promotion_data)
                logger.info(f"노드 {self.id}가 승격 정보 등록: {promotion_data}")
                
                # 승격 상태 즉시 확인
                self.check_promotion_status()
                return True
        except Exception as e:
            logger.error(f"선출 참여 중 오류: {e}")
        return False
    
    def find_manager(self):
        try:
            cluster_key = f"{CLUSTER_INFO_PREFIX}/{self.cluster_id}/info"
            cluster_info = self.etcd_client.get(cluster_key)
            if cluster_info and "manager_id" in cluster_info:
                manager_id = cluster_info["manager_id"]
                # 관리자가 자기 자신인지 확인 (이미 승격된 경우)
                if manager_id == self.id:
                    logger.info(f"이 노드가 이미 관리자로 등록됨: {self.id}")
                    # 승격 확인
                    self.check_promotion_status()
                    return
                    
                self.manager_id = manager_id
                logger.info(f"Found primary manager: {self.manager_id}")
                self.connect_to_manager()
            else:
                manager_results = self.etcd_client.get_prefix(f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/")
                primary_managers = []
                temp_managers = []
                for key, manager_data in manager_results:
                    # 이벤트나 임시 관리자 정보 제외
                    if "events/" not in key and "temp_" not in key and "notifications/" not in key:
                        node_id = manager_data.get("node_id", "")
                        if node_id == self.id:
                            # 자기 자신이 관리자로 등록되어 있으면 승격 확인
                            logger.info(f"이 노드가 이미 관리자로 등록됨: {self.id}")
                            self.check_promotion_status()
                            return
                        if manager_data.get("is_temporary", False):
                            temp_managers.append(manager_data)
                        elif manager_data.get("is_primary", False):
                            primary_managers.append(manager_data)
                
                # 임시 관리자 확인            
                temp_manager_results = self.etcd_client.get_prefix(f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/temp_")
                for key, temp_data in temp_manager_results:
                    temp_managers.append(temp_data)
                
                if temp_managers:
                    temp_manager = max(temp_managers, key=lambda x: x.get("last_updated", 0))
                    self.temp_manager_id = temp_manager["node_id"]
                    logger.info(f"Found temporary manager: {self.temp_manager_id}")
                    self.connect_to_temp_manager()
                elif primary_managers:
                    primary_manager = max(primary_managers, key=lambda x: x.get("last_updated", 0))
                    self.manager_id = primary_manager["node_id"]
                    logger.info(f"Found primary manager: {self.manager_id}")
                    self.connect_to_manager()
                else:
                    logger.warning(f"No manager found for cluster {self.cluster_id}")
                    
                    # 관리자가 없는 경우 이 노드를 관리자로 자체 선출 시도
                    promotion_key = f"{CLUSTER_INFO_PREFIX}/{self.cluster_id}/promoted_manager"
                    promotion_data = self.etcd_client.get(promotion_key)
                    
                    if promotion_data and promotion_data.get("node_id") == self.id:
                        logger.info(f"관리자가 없고 이 노드({self.id})가 승격 대상으로 등록됨")
                        self.check_promotion_status()
        except Exception as e:
            logger.error(f"Error finding manager: {e}")
    
    def connect_to_manager(self):
        if not self.manager_id:
            return False
        try:
            self.manager_connection = self.connect_to_node(self.manager_id)
            logger.info(f"Connected to manager {self.manager_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to manager {self.manager_id}: {e}")
            return False
    
    def connect_to_temp_manager(self):
        if not self.temp_manager_id:
            return False
        try:
            self.temp_manager_connection = self.connect_to_node(self.temp_manager_id)
            logger.info(f"Connected to temporary manager {self.temp_manager_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to temporary manager {self.temp_manager_id}: {e}")
            return False
    
    def watch_manager_changes(self):
        try:
            cluster_prefix = f"{CLUSTER_INFO_PREFIX}/{self.cluster_id}/"
            def on_cluster_change(event):
                logger.info("Cluster information changed, re-finding manager")
                self.find_manager()
                
                # 클러스터 변경 시 승격 확인
                self.check_promotion_status()
                
            self.manager_watch_ids = self.etcd_client.watch_prefix(cluster_prefix, on_cluster_change)
            if not self.manager_watch_ids:
                logger.warning("Failed to set up watch for manager changes")
        except Exception as e:
            logger.error(f"Error setting up watch for manager changes: {e}")
    
    def promotion_check_loop(self):
        """정기적으로 승격 상태 확인"""
        while not self.stop_event.is_set():
            try:
                # 승격 상태 확인
                self.check_promotion_status()
                
                if not self.stop_event.wait(self.promotion_check_interval):
                    continue
                else:
                    break
            except Exception as e:
                logger.error(f"Error in promotion check loop: {e}")
                if not self.stop_event.wait(1.0):
                    continue
                else:
                    break
    
    def promote_to_manager(self):
        """참여자 노드를 관리자 노드로 승격"""
        logger.info(f"노드 {self.id}가 관리자로 승격 시도")
        try:
            # 기존 데이터는 유지
            self.type = "MANAGER"  # 유형 명시적 설정
            self.metadata["type"] = "MANAGER"
            # 기본 관리자로 설정
            self.is_primary = True
            
            # 기존 관리자 노드 정보 확인 및 삭제
            try:
                cluster_key = f"{CLUSTER_INFO_PREFIX}/{self.cluster_id}/info"
                cluster_info = self.etcd_client.get(cluster_key)
                if cluster_info and "manager_id" in cluster_info:
                    old_manager_id = cluster_info["manager_id"]
                    if old_manager_id != self.id:
                        # 기존 관리자의 상태 확인
                        old_manager_key = f"{NODE_INFO_PREFIX}/{self.cluster_id}/{old_manager_id}"
                        old_manager_data = self.etcd_client.get(old_manager_key)
                        if old_manager_data and old_manager_data.get("status") != "ACTIVE":
                            # 비활성 상태인 경우 관리자 정보 삭제
                            old_manager_reg_key = f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/{old_manager_id}"
                            self.etcd_client.delete(old_manager_reg_key)
                            logger.info(f"기존 관리자 {old_manager_id} 정보 삭제")
            except Exception as e:
                logger.error(f"기존 관리자 정보 처리 중 오류: {e}")
            
            # 관리자 정보 등록
            manager_key = f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/{self.id}"
            manager_info = {
                "node_id": self.id,
                "cluster_id": self.cluster_id,
                "is_primary": True,
                "type": "MANAGER",
                "status": self.status,
                "last_updated": time.time(),
                "promoted_at": time.time()
            }
            result = self.etcd_client.put(manager_key, manager_info)
            logger.info(f"관리자 정보 등록 결과: {result}")
            
            # 클러스터 정보 업데이트
            cluster_key = f"{CLUSTER_INFO_PREFIX}/{self.cluster_id}/info"
            cluster_info = {
                "cluster_id": self.cluster_id,
                "manager_id": self.id,
                "is_primary": True,
                "node_count": 0,
                "last_updated": time.time()
            }
            result = self.etcd_client.put(cluster_key, cluster_info)
            logger.info(f"클러스터 정보 업데이트 결과: {result}")
            
            # 이벤트 발행
            event_key = f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/events/{int(time.time())}"
            event_data = {
                "event_type": "PARTICIPANT_PROMOTED_TO_MANAGER",
                "node_id": self.id,
                "cluster_id": self.cluster_id,
                "timestamp": time.time()
            }
            self.etcd_client.put(event_key, event_data)
            
            # 승격 상태 업데이트
            promotion_key = f"{CLUSTER_INFO_PREFIX}/{self.cluster_id}/promoted_manager"
            promotion_data = {
                "node_id": self.id,
                "promoted_at": time.time(),
                "status": "PROMOTED"
            }
            result = self.etcd_client.put(promotion_key, promotion_data)
            logger.info(f"승격 상태 업데이트 결과: {result}")
            
            # 백업 클러스터에 알림
            backup_cluster_id = CLUSTER_BACKUP_MAP.get(self.cluster_id)
            if backup_cluster_id:
                notification_key = f"{MANAGER_INFO_PREFIX}/{backup_cluster_id}/notifications/{int(time.time())}"
                notification_data = {
                    "event_type": "PRIMARY_MANAGER_PROMOTED",
                    "node_id": self.id,
                    "cluster_id": self.cluster_id,
                    "timestamp": time.time()
                }
                self.etcd_client.put(notification_key, notification_data)
            
            logger.info(f"노드 {self.id}가 관리자로 성공적으로 승격됨")
            return True
        except Exception as e:
            logger.error(f"관리자 승격 중 오류: {e}")
            return False
    
    def check_promotion_status(self):
        """선출 상태 확인 및 관리자로 승격 처리"""
        try:
            # 로그 상세화
            self.log_detailed_status()
            
            # 1. 선출 정보 확인
            election_key = f"{CLUSTER_INFO_PREFIX}/{self.cluster_id}/election"
            election_data = self.etcd_client.get(election_key)
            
            # 2. 승격 정보 확인
            promotion_key = f"{CLUSTER_INFO_PREFIX}/{self.cluster_id}/promoted_manager"
            promotion_data = self.etcd_client.get(promotion_key)
            
            # 3. 클러스터 정보 확인
            cluster_key = f"{CLUSTER_INFO_PREFIX}/{self.cluster_id}/info"
            cluster_info = self.etcd_client.get(cluster_key)
            
            # 후보 노드 선출 현황 로깅
            if election_data:
                logger.info(f"선출 정보: {election_data}")
            
            # 이미 클러스터 정보에 관리자로 등록되어 있는지 확인
            if cluster_info and cluster_info.get("manager_id") == self.id:
                if self.type != "MANAGER":
                    logger.info(f"노드 {self.id}가 이미 클러스터에 관리자로 등록되어 있음, 즉시 승격")
                    # 관리자로 승격
                    return self.convert_to_manager()
            
            # 두 정보 중 하나라도 있고 자신이 대상이면 처리
            if election_data and election_data.get("elected_node") == self.id:
                logger.info(f"노드 {self.id}가 선출 대상으로 확인됨: status={election_data.get('status', 'unknown')}")
                
                if election_data.get("status") == "PENDING":
                    # 대기 상태면 승격 진행
                    logger.info(f"노드 {self.id}가 관리자 승격 대기 상태, 승격 진행")
                    
                    # 승격 정보 갱신
                    if not promotion_data:
                        # 승격 정보가 없으면 새로 생성
                        promotion_data = {
                            "node_id": self.id,
                            "elected_at": time.time(),
                            "status": "ELECTED"
                        }
                        result = self.etcd_client.put(promotion_key, promotion_data)
                        logger.info(f"승격 정보 생성 결과: {result}")
            
            if promotion_data and promotion_data.get("node_id") == self.id:
                status = promotion_data.get("status", "")
                logger.info(f"노드 {self.id}의 승격 상태: {status}")
                
                if status == "ELECTED" or status == "PENDING":
                    logger.info(f"노드 {self.id}가 새 관리자로 선출됨, 승격 진행")
                    
                    # 관리자로 승격 진행
                    return self.convert_to_manager()
        except Exception as e:
            logger.error(f"승격 상태 확인 중 오류: {e}")
        return False
    
    def convert_to_manager(self):
        """관리자로 변환 (새로운 패턴)"""
        logger.info(f"노드 {self.id}를 관리자로 변환 시작")
        
        try:
            # 1. 관리자로 승격 처리
            success = self.promote_to_manager()
            if not success:
                logger.error(f"노드 {self.id}의 관리자 승격 실패")
                return False
            
            # 2. 필요한 ManagementNode 속성 추가
            logger.info(f"노드 {self.id}에 ManagementNode 속성 추가 중")
            self.is_primary = True
            self.participant_nodes = {}
            self.backup_mode_active = False
            self.backup_check_thread = None
            self.etcd_monitor_thread = None
            self.discovery_thread = None
            self.backup_for = CLUSTER_BACKUP_MAP.get(self.cluster_id)
            
            # 3. ManagementNode 메서드를 현재 인스턴스에 동적으로 추가
            logger.info(f"노드 {self.id}에 ManagementNode 메서드 추가 중")
            # ManagementNode 인스턴스 생성 (메서드 복사용)
            management_node = ManagementNode(
                self.id, 
                self.cluster_id, 
                is_primary=True,
                etcd_endpoints=self.etcd_client.endpoints,
                backup_etcd_endpoints=None
            )
            
            # 중요 메서드들을 현재 인스턴스에 동적으로 추가
            import types
            self.initialize = types.MethodType(management_node.initialize, self)
            self.register_as_manager = types.MethodType(management_node.register_as_manager, self)
            self.setup_backup_role = types.MethodType(management_node.setup_backup_role, self)
            self.discovery_loop = types.MethodType(management_node.discovery_loop, self)
            self.update_cluster_info = types.MethodType(management_node.update_cluster_info, self)
            self.monitor_etcd_status = types.MethodType(management_node.monitor_etcd_status, self)
            self.backup_check_loop = types.MethodType(management_node.backup_check_loop, self)
            self.activate_backup_mode = types.MethodType(management_node.activate_backup_mode, self)
            self.deactivate_backup_mode = types.MethodType(management_node.deactivate_backup_mode, self)
            self.detect_primary_manager_recovery = types.MethodType(management_node.detect_primary_manager_recovery, self)
            
            # 4. 타입 변경 및 메타데이터 업데이트
            self.type = "MANAGER"
            self.metadata["type"] = "MANAGER"
            self.register_to_etcd()
            
            # 5. 관리자 초기화
            logger.info(f"노드 {self.id}의 관리자 초기화 시작")
            self.initialize()
            
            # 6. 추가 스레드 시작
            logger.info(f"노드 {self.id}의 관리자 스레드 시작")
            self.discovery_thread = threading.Thread(target=self.discovery_loop)
            self.discovery_thread.daemon = True
            self.discovery_thread.start()
            
            self.etcd_monitor_thread = threading.Thread(target=self.monitor_etcd_status)
            self.etcd_monitor_thread.daemon = True
            self.etcd_monitor_thread.start()
            
            if self.backup_for:
                self.setup_backup_role()
                self.backup_check_thread = threading.Thread(target=self.backup_check_loop)
                self.backup_check_thread.daemon = True
                self.backup_check_thread.start()
            
            # 7. 백업 관리자에게 알림
            threading.Thread(target=self.notify_backup_managers).start()
            
            logger.info(f"노드 {self.id}가 성공적으로 관리자로 변환됨")
            return True
        except Exception as e:
            logger.error(f"관리자 변환 중 오류: {e}")
            return False
    
    def notify_backup_managers(self):
        """백업 관리자에게 승격 알림"""
        try:
            # 백업 클러스터 ID 찾기
            backup_cluster_id = CLUSTER_BACKUP_MAP.get(self.cluster_id)
            if not backup_cluster_id:
                return
                
            # 백업 관리자 정보 찾기
            backup_key = f"{BACKUP_INFO_PREFIX}/{self.cluster_id}/managers/"
            backup_results = self.etcd_client.get_prefix(backup_key)
            
            for key, backup_data in backup_results:
                backup_node_id = backup_data.get("node_id")
                backup_status = backup_data.get("status")
                
                if backup_status == "ACTIVE":
                    # 활성 백업 관리자에게 알림
                    logger.info(f"백업 관리자 {backup_node_id}에게 승격 알림 전송")
                    
                    notification_key = f"{BACKUP_INFO_PREFIX}/{self.cluster_id}/notifications/{int(time.time())}"
                    notification_data = {
                        "event_type": "PRIMARY_MANAGER_PROMOTED",
                        "primary_node_id": self.id,
                        "cluster_id": self.cluster_id,
                        "backup_node_id": backup_node_id,
                        "timestamp": time.time()
                    }
                    self.etcd_client.put(notification_key, notification_data)
        except Exception as e:
            logger.error(f"백업 관리자 알림 중 오류: {e}")
    
    def check_cluster_status(self):
        try:
            if self.manager_id and self.manager_id not in self.connections:
                logger.warning(f"Connection to manager {self.manager_id} lost, attempting to reconnect")
                self.connect_to_manager()
            if self.temp_manager_id and self.temp_manager_id not in self.connections:
                logger.warning(f"Connection to temporary manager {self.temp_manager_id} lost, attempting to reconnect")
                self.connect_to_temp_manager()
            if not self.manager_id and not self.temp_manager_id:
                logger.warning("No manager connection, attempting to find manager")
                self.find_manager()

                # 관리자가 없는 상태가 지속되면 선출 과정 시작
                if not self.manager_id and not self.temp_manager_id:
                    # 선출 정보 확인
                    election_key = f"{CLUSTER_INFO_PREFIX}/{self.cluster_id}/election"
                    election_data = self.etcd_client.get(election_key)
                    
                    # 현재 진행 중인 선출이 없으면 자신을 후보로 등록
                    if not election_data:
                        logger.info("관리자 없음 상태 감지, 선출 과정 시작")
                        self.participate_in_election()
        except Exception as e:
            logger.error(f"Error in check_cluster_status: {e}")