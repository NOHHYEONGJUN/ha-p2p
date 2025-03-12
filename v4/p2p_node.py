
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
    CLUSTER_BACKUP_MAP, CLUSTER_HEALTH_CHECK_INTERVAL, BACKUP_ACTIVATION_TIMEOUT,
    LEADER_ELECTION_MIN_TIMEOUT, LEADER_ELECTION_MAX_TIMEOUT
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
    
    def change_role(self, new_type: str, is_primary: bool = False):
        """노드 역할 변경 (참여자 -> 관리자 등)"""
        logger.info(f"Node {self.id} changing role from {self.type} to {new_type}")
        self.type = new_type
        self.metadata["type"] = new_type
        self.metadata["last_updated"] = time.time()
        
        # 특별한 역할 변경 시 처리 (참여자 -> 관리자)
        if new_type == "MANAGER":
            # 참여 노드에서 관리 노드로 변경
            self.register_to_etcd()
            manager_key = f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/{self.id}"
            manager_info = {
                "node_id": self.id,
                "cluster_id": self.cluster_id,
                "is_primary": is_primary,
                "type": "MANAGER",
                "status": self.status,
                "last_updated": time.time()
            }
            self.etcd_client.put(manager_key, manager_info)
            
            # 클러스터 정보 업데이트 (새 관리자 등록)
            if is_primary:
                cluster_key = f"{CLUSTER_INFO_PREFIX}/{self.cluster_id}/info"
                cluster_info = {
                    "cluster_id": self.cluster_id,
                    "manager_id": self.id,
                    "is_primary": is_primary,
                    "node_count": 0,
                    "last_updated": time.time()
                }
                self.etcd_client.put(cluster_key, cluster_info)
                
            # 역할 변경 이벤트 기록
            event_key = f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/events/{int(time.time())}"
            event_data = {
                "event_type": "MANAGER_PROMOTED",
                "node_id": self.id,
                "cluster_id": self.cluster_id,
                "is_primary": is_primary,
                "timestamp": time.time()
            }
            self.etcd_client.put(event_key, event_data)
            logger.info(f"Node {self.id} promoted to {'PRIMARY' if is_primary else 'SECONDARY'} MANAGER")

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
        
        # 백업 관리자로서 감시 중인 클러스터 관련 정보
        self.backup_watch_ids = None
        self.backup_election_detected = False
    
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
        
        # 백업 대상 클러스터의 관리자 변경을 감시
        self.watch_target_cluster_managers()
    
    def watch_target_cluster_managers(self):
        """백업 대상 클러스터의 관리자 변경을 감시"""
        if not self.backup_for:
            return
            
        try:
            def on_manager_change(event):
                if self.backup_mode_active:
                    # 새 관리자가 선출되었는지 확인
                    try:
                        managers_key = f"{MANAGER_INFO_PREFIX}/{self.backup_for}/"
                        results = self.etcd_client.get_prefix(managers_key)
                        
                        for key, value in results:
                            # temp_ 로 시작하지 않는 키만 처리 (실제 관리자)
                            if "temp_" not in key and "/events/" not in key:
                                manager_id = value.get("node_id")
                                is_primary = value.get("is_primary", False)
                                # 새 관리자가 활성화되고 프라이머리이며, 자신이 아니면 백업 비활성화
                                if (manager_id != self.id and 
                                    is_primary and 
                                    value.get("status") == "ACTIVE"):
                                    logger.info(f"새로운 관리자 {manager_id}가 클러스터 {self.backup_for}에서 감지됨")
                                    self.backup_election_detected = True
                                    self.deactivate_backup_mode()
                                    break
                    except Exception as e:
                        logger.error(f"관리자 변경 감시 처리 중 오류: {e}")
            
            managers_key = f"{MANAGER_INFO_PREFIX}/{self.backup_for}/"
            self.backup_watch_ids = self.etcd_client.watch_prefix(managers_key, on_manager_change)
            if self.backup_watch_ids:
                logger.info(f"클러스터 {self.backup_for}의 관리자 변경 감시 시작")
            else:
                logger.warning(f"클러스터 {self.backup_for}의 관리자 변경 감시 실패")
        except Exception as e:
            logger.error(f"백업 대상 클러스터 관리자 감시 설정 오류: {e}")
    
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
            if self.backup_watch_ids:
                self.etcd_client.cancel_watch(self.backup_watch_ids)
                
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
                manager_active = False
                current_time = time.time()
                
                # 클러스터 정보에서 매니저 확인
                if target_cluster_info:
                    manager_id = target_cluster_info.get("manager_id")
                    last_updated = target_cluster_info.get("last_updated", 0)
                    time_since_update = current_time - last_updated
                    
                    # 매니저가 있고 최근에 업데이트 되었으면 활성 상태로 간주
                    if manager_id and time_since_update < BACKUP_ACTIVATION_TIMEOUT:
                        manager_active = True
                        
                # 기본 클러스터 정보가 없거나 관리자가 비활성 상태이면 직접 관리자 확인
                if not manager_active:
                    manager_results = self.etcd_client.get_prefix(f"{MANAGER_INFO_PREFIX}/{self.backup_for}/")
                    for key, manager_data in manager_results:
                        if "/events/" not in key and "temp_" not in key:
                            last_updated = manager_data.get("last_updated", 0)
                            time_since_update = current_time - last_updated
                            status = manager_data.get("status", "UNKNOWN")
                            
                            if status == "ACTIVE" and time_since_update < BACKUP_ACTIVATION_TIMEOUT:
                                manager_active = True
                                break
                
                # 관리자가 비활성 상태이면 백업 모드 활성화
                if not manager_active and not self.backup_mode_active:
                    logger.warning(f"클러스터 {self.backup_for}의 관리자가 비활성 상태입니다. 백업 모드 활성화")
                    self.activate_backup_mode()
                # 관리자가 활성 상태이고 백업 모드가 활성화된 상태면 백업 모드 비활성화
                elif manager_active and self.backup_mode_active:
                    logger.info(f"클러스터 {self.backup_for}의 관리자가 활성 상태로 돌아왔습니다. 백업 모드 비활성화")
                    self.deactivate_backup_mode()
                
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
            self.backup_election_detected = False
            logger.info(f"Backup mode activated for cluster {self.backup_for} by node {self.id}")
            
            # 임시 관리자로 자신을 등록
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
            
            # 백업 대상 클러스터의 참여 노드들에게 선출 이벤트 트리거
            self.trigger_election_in_target_cluster()
        except Exception as e:
            logger.error(f"Error activating backup mode: {e}")
    
    def trigger_election_in_target_cluster(self):
        """백업 대상 클러스터의 참여 노드들에게 선출 프로세스 시작을 알림"""
        try:
            # 명확한 선출 트리거 메시지 생성
            election_trigger_key = f"{MANAGER_INFO_PREFIX}/{self.backup_for}/election_trigger"
            election_data = {
                "trigger_node_id": self.id,
                "trigger_cluster_id": self.cluster_id,
                "timestamp": time.time(),
                "status": "TRIGGERED",
                "force": True
            }
            self.etcd_client.put(election_trigger_key, election_data)
            
            # 모든 참여 노드에 직접 메시지 전송
            try:
                nodes = self.etcd_client.get_prefix(f"{NODE_INFO_PREFIX}/{self.backup_for}/")
                participant_nodes = []
                
                for key, node_data in nodes:
                    if node_data.get("type") == "PARTICIPANT" and node_data.get("status") == "ACTIVE":
                        participant_nodes.append(node_data.get("node_id"))
                
                logger.info(f"백업 관리자가 클러스터 {self.backup_for}의 참여 노드 {len(participant_nodes)}개에 선출 명령 전송")
                
                # 각 참여 노드에 대한 직접 트리거 메시지 생성
                for node_id in participant_nodes:
                    node_trigger_key = f"{MANAGER_INFO_PREFIX}/{self.backup_for}/election_trigger/node_{node_id}"
                    node_trigger_data = {
                        "target_node": node_id,
                        "timestamp": time.time(),
                        "action": "START_ELECTION",
                        "backup_manager": self.id
                    }
                    self.etcd_client.put(node_trigger_key, node_trigger_data)
            except Exception as e:
                logger.error(f"참여 노드 목록 조회 중 오류: {e}")
            
            logger.info(f"Election triggered in cluster {self.backup_for} by backup manager {self.id}")
        except Exception as e:
            logger.error(f"Error triggering election in target cluster: {e}")
    
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
                "election_detected": self.backup_election_detected,
                "last_updated": time.time()
            }
            self.etcd_client.put(backup_key, backup_info)
            event_key = f"{BACKUP_INFO_PREFIX}/{self.backup_for}/events/{int(time.time())}"
            event_data = {
                "event_type": "BACKUP_DEACTIVATED",
                "node_id": self.id,
                "cluster_id": self.cluster_id,
                "backup_for": self.backup_for,
                "election_detected": self.backup_election_detected,
                "timestamp": time.time()
            }
            self.etcd_client.put(event_key, event_data)
            
            # 임시 관리자 역할 제거
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
        self.type = "PARTICIPANT"
        self.metadata["type"] = "PARTICIPANT"
        self.manager_id = None
        self.manager_connection = None
        self.temp_manager_id = None
        self.temp_manager_connection = None
        self.manager_watch_ids = None
        self.election_thread = None
        self.election_in_progress = False
        self.last_manager_update = 0
        self.manager_miss_count = 0
        self.election_trigger_watch = None
    
    def start(self):
        super().start()
        self.find_manager()
        self.watch_manager_changes()
        self.watch_election_trigger()
        logger.info(f"Participant node {self.id} is running in cluster {self.cluster_id}")
    
    def stop(self):
        if self.manager_watch_ids:
            self.etcd_client.cancel_watch(self.manager_watch_ids)
        if self.election_trigger_watch:
            self.etcd_client.cancel_watch(self.election_trigger_watch)
        super().stop()
    
    def find_manager(self):
        try:
            cluster_key = f"{CLUSTER_INFO_PREFIX}/{self.cluster_id}/info"
            cluster_info = self.etcd_client.get(cluster_key)
            if cluster_info and "manager_id" in cluster_info:
                self.manager_id = cluster_info["manager_id"]
                logger.info(f"Found primary manager: {self.manager_id}")
                self.connect_to_manager()
                self.last_manager_update = time.time()
                self.manager_miss_count = 0
            else:
                manager_results = self.etcd_client.get_prefix(f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/")
                primary_managers = []
                temp_managers = []
                for _, manager_data in manager_results:
                    if manager_data.get("is_temporary", False):
                        temp_managers.append(manager_data)
                    elif manager_data.get("is_primary", False):
                        primary_managers.append(manager_data)
                if temp_managers:
                    temp_manager = max(temp_managers, key=lambda x: x.get("last_updated", 0))
                    self.temp_manager_id = temp_manager["node_id"]
                    logger.info(f"Found temporary manager: {self.temp_manager_id}")
                    self.connect_to_temp_manager()
                    self.last_manager_update = time.time()
                    self.manager_miss_count = 0
                elif primary_managers:
                    primary_manager = max(primary_managers, key=lambda x: x.get("last_updated", 0))
                    self.manager_id = primary_manager["node_id"]
                    logger.info(f"Found primary manager: {self.manager_id}")
                    self.connect_to_manager()
                    self.last_manager_update = time.time()
                    self.manager_miss_count = 0
                else:
                    logger.warning(f"No manager found for cluster {self.cluster_id}")
                    # 관리자가 없으면 선출 절차 시작 여부 확인
                    self.check_if_election_needed()
        except Exception as e:
            logger.error(f"Error finding manager: {e}")
    
    def check_if_election_needed(self):
        """관리자가 없을 때 선출 절차가 필요한지 확인"""
        if self.election_in_progress:
            return
            
        current_time = time.time()
        time_since_last_update = current_time - self.last_manager_update
        
        # 마지막 관리자 업데이트 후 일정 시간이 지났거나 manager_miss_count가 임계값을 초과했으면 선출 시작
        if time_since_last_update > BACKUP_ACTIVATION_TIMEOUT or self.manager_miss_count >= MAX_HEARTBEAT_MISS:
            # 선출 트리거가 있는지 확인
            election_trigger_key = f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/election_trigger"
            election_trigger = self.etcd_client.get(election_trigger_key)
            
            if election_trigger and election_trigger.get("status") == "TRIGGERED":
                logger.info(f"선출 트리거 감지: 백업 관리자 {election_trigger.get('trigger_node_id')}가 선출 요청")
                self.start_election()
                return
            
            # 내 노드에 대한 직접적인 선출 요청이 있는지 확인
            my_trigger_key = f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/election_trigger/node_{self.id}"
            my_trigger = self.etcd_client.get(my_trigger_key)
            
            if my_trigger and my_trigger.get("action") == "START_ELECTION":
                logger.info(f"내 노드에 대한 직접 선출 요청 감지, 백업 관리자: {my_trigger.get('backup_manager')}")
                self.start_election()
                # 처리 완료된 트리거 삭제
                self.etcd_client.delete(my_trigger_key)
                return
            
            # 이미 진행 중인 선출이 있는지 확인
            election_key = f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/election"
            election_info = self.etcd_client.get(election_key)
            if election_info and current_time - election_info.get("timestamp", 0) < LEADER_ELECTION_MAX_TIMEOUT * 2:
                # 다른 노드가 이미 선출을 시작했으면 참여만 함
                logger.info(f"Election already in progress, joining: {election_info}")
                self.join_election(election_info.get("election_id", "unknown"))
            else:
                # 새 선출 시작
                logger.info(f"No active manager for {time_since_last_update:.1f}s, starting election (missed count: {self.manager_miss_count})")
                self.start_election()
    
    def watch_election_trigger(self):
        """백업 관리자가 트리거한 선출 이벤트 감시"""
        try:
            def on_election_trigger(event):
                if event and event.events:
                    for e in event.events:
                        if e.type == "PUT":
                            # 선출 트리거 이벤트 발생
                            try:
                                key = e.key.decode('utf-8')
                                value = json.loads(e.value.decode('utf-8'))
                                
                                # 전체 클러스터에 대한 트리거 확인
                                if "/node_" not in key and value.get("status") == "TRIGGERED":
                                    logger.info(f"선출 트리거 감지 from {value.get('trigger_node_id')}")
                                    if not self.election_in_progress:
                                        self.start_election()
                                # 이 노드에 대한 직접 트리거 확인
                                elif f"node_{self.id}" in key and value.get("action") == "START_ELECTION":
                                    logger.info(f"내 노드에 대한 직접 선출 요청 감지: {value}")
                                    if not self.election_in_progress:
                                        self.start_election()
                                        # 처리 완료된 트리거 삭제
                                        self.etcd_client.delete(key)
                            except Exception as e:
                                logger.error(f"Error processing election trigger: {e}")
            
            # 전체 트리거 감시
            trigger_key = f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/election_trigger"
            global_watch = self.etcd_client.watch(trigger_key, on_election_trigger)
            
            # 이 노드에 대한 직접 트리거 감시
            node_trigger_key = f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/election_trigger/node_{self.id}"
            node_watch = self.etcd_client.watch(node_trigger_key, on_election_trigger)
            
            # 두 감시를 하나의 리스트로 결합
            self.election_trigger_watch = [global_watch, node_watch] if global_watch and node_watch else global_watch or node_watch
        except Exception as e:
            logger.error(f"Error setting up election trigger watch: {e}")
    
    def start_election(self):
        """관리자 선출 프로세스 시작"""
        if self.election_in_progress:
            return
            
        self.election_in_progress = True
        election_id = f"election_{self.cluster_id}_{int(time.time())}_{self.id}"
        
        try:
            # 선출 정보 등록
            election_key = f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/election"
            election_info = {
                "election_id": election_id,
                "initiator_id": self.id,
                "cluster_id": self.cluster_id,
                "status": "STARTED",
                "timestamp": time.time()
            }
            self.etcd_client.put(election_key, election_info)
            
            # 자신의 투표 등록
            vote_key = f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/election/votes/{self.id}"
            vote_info = {
                "node_id": self.id,
                "election_id": election_id,
                "vote_for": self.id,  # 자신에게 투표
                "timestamp": time.time()
            }
            self.etcd_client.put(vote_key, vote_info)
            
            # 선출 결과 감시 스레드 시작
            self.election_thread = threading.Thread(target=self.election_process, args=(election_id,))
            self.election_thread.daemon = True
            self.election_thread.start()
            
            logger.info(f"Node {self.id} started election process with ID {election_id}")
        except Exception as e:
            logger.error(f"Error starting election: {e}")
            self.election_in_progress = False
    
    def join_election(self, election_id):
        """진행 중인 선출에 참여"""
        if self.election_in_progress:
            return
            
        self.election_in_progress = True
        
        try:
            # 자신의 투표 등록 (현재는 단순히 자신에게 투표)
            vote_key = f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/election/votes/{self.id}"
            vote_info = {
                "node_id": self.id,
                "election_id": election_id,
                "vote_for": self.id,  # 자신에게 투표
                "timestamp": time.time()
            }
            self.etcd_client.put(vote_key, vote_info)
            
            # 선출 결과 감시 스레드 시작
            self.election_thread = threading.Thread(target=self.election_process, args=(election_id,))
            self.election_thread.daemon = True
            self.election_thread.start()
            
            logger.info(f"Node {self.id} joined election process with ID {election_id}")
        except Exception as e:
            logger.error(f"Error joining election: {e}")
            self.election_in_progress = False
    
    def election_process(self, election_id):
        """선출 프로세스 진행 및 결과 처리"""
        try:
            # 무작위 대기 시간 설정 (충돌 방지)
            wait_time = random.uniform(LEADER_ELECTION_MIN_TIMEOUT, LEADER_ELECTION_MAX_TIMEOUT)
            logger.info(f"Election waiting for {wait_time:.1f} seconds")
            time.sleep(wait_time)
            
            # 모든 투표 수집
            votes_key = f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/election/votes/"
            votes_results = self.etcd_client.get_prefix(votes_key)
            
            votes = {}
            for _, vote_data in votes_results:
                if vote_data.get("election_id") == election_id:
                    vote_for = vote_data.get("vote_for")
                    if vote_for in votes:
                        votes[vote_for] += 1
                    else:
                        votes[vote_for] = 1
            
            # 최다 득표자 결정
            if votes:
                winner_id = max(votes.items(), key=lambda x: x[1])[0]
                vote_count = votes.get(winner_id, 0)
                
                logger.info(f"Election results: {votes}, winner: {winner_id} with {vote_count} votes")
                
                # 자신이 당선되면 관리자로 역할 변경
                if winner_id == self.id:
                    logger.info(f"Node {self.id} won election, becoming primary manager")
                    
                    # 선출 결과 업데이트
                    election_key = f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/election"
                    election_result = {
                        "election_id": election_id,
                        "winner_id": self.id,
                        "votes": votes,
                        "status": "COMPLETED",
                        "timestamp": time.time()
                    }
                    self.etcd_client.put(election_key, election_result)
                    
                    # 관리자로 역할 변경
                    self.change_role("MANAGER", is_primary=True)
                    
                    # 관리자 객체로 변환하여 초기화
                    self.convert_to_manager()
                else:
                    logger.info(f"Election won by node {winner_id}")
            else:
                logger.warning("No votes collected during election")
        except Exception as e:
            logger.error(f"Error in election process: {e}")
        finally:
            self.election_in_progress = False
    
    def convert_to_manager(self):
        """참여 노드를 관리 노드로 변환"""
        try:
            # 기존 상태 저장
            node_id = self.id
            cluster_id = self.cluster_id
            etcd_endpoints = self.etcd_client.endpoints
            backup_etcd_endpoints = self.etcd_client.backup_endpoints
            
            # 기존 노드 종료 (스레드 중지)
            self.stop_event.set()
            if self.heartbeat_thread and self.heartbeat_thread.is_alive():
                self.heartbeat_thread.join(timeout=1.0)
            
            # 관리 노드 기능 초기화
            self.backup_mode_active = False
            self.participant_nodes = {}
            self.backup_check_thread = None
            self.etcd_monitor_thread = None
            self.discovery_thread = None
            
            # 백업 관리자로서의 역할 설정 (필요한 경우)
            backup_for = CLUSTER_BACKUP_MAP.get(self.cluster_id)
            
            # 관리자 이벤트 등록
            manager_event = {
                "event_type": "PARTICIPANT_PROMOTED",
                "node_id": self.id,
                "cluster_id": self.cluster_id,
                "is_primary": True,
                "timestamp": time.time()
            }
            event_key = f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/events/{int(time.time())}"
            self.etcd_client.put(event_key, manager_event)
            
            # 관리자 정보 등록
            manager_key = f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/{self.id}"
            manager_info = {
                "node_id": self.id,
                "cluster_id": self.cluster_id,
                "is_primary": True,
                "type": "MANAGER",
                "status": "ACTIVE",
                "promoted_from": "PARTICIPANT",
                "last_updated": time.time()
            }
            self.etcd_client.put(manager_key, manager_info)
            
            # 클러스터 정보 업데이트
            cluster_key = f"{CLUSTER_INFO_PREFIX}/{self.cluster_id}/info"
            cluster_info = {
                "cluster_id": self.cluster_id,
                "manager_id": self.id,
                "is_primary": True,
                "promoted": True,
                "node_count": 0,
                "last_updated": time.time()
            }
            self.etcd_client.put(cluster_key, cluster_info)
            
            # 노드 정보 업데이트
            node_key = f"{NODE_INFO_PREFIX}/{self.cluster_id}/{self.id}"
            self.metadata["type"] = "MANAGER"
            self.metadata["is_primary"] = True
            self.metadata["last_updated"] = time.time()
            self.etcd_client.put(node_key, self.metadata)
            
            # 백업이 설정된 경우 백업 역할 등록
            if backup_for:
                backup_key = f"{BACKUP_INFO_PREFIX}/{backup_for}/managers/{self.id}"
                backup_info = {
                    "node_id": self.id,
                    "cluster_id": self.cluster_id,
                    "backup_for": backup_for,
                    "status": "STANDBY",
                    "last_updated": time.time()
                }
                self.etcd_client.put(backup_key, backup_info)
            
            # 관리 노드 스레드 시작
            self.stop_event.clear()
            self.discovery_thread = threading.Thread(target=self.discovery_loop)
            self.discovery_thread.daemon = True
            self.discovery_thread.start()
            
            self.etcd_monitor_thread = threading.Thread(target=self.monitor_etcd_status)
            self.etcd_monitor_thread.daemon = True
            self.etcd_monitor_thread.start()
            
            if backup_for:
                self.backup_check_thread = threading.Thread(target=self.backup_check_loop)
                self.backup_check_thread.daemon = True
                self.backup_check_thread.start()
                self.watch_target_cluster_managers()
            
            logger.info(f"Node {self.id} successfully converted to primary manager")
        except Exception as e:
            logger.error(f"Error converting participant to manager: {e}")
    
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
            self.manager_watch_ids = self.etcd_client.watch_prefix(cluster_prefix, on_cluster_change)
            if not self.manager_watch_ids:
                logger.warning("Failed to set up watch for manager changes")
        except Exception as e:
            logger.error(f"Error setting up watch for manager changes: {e}")
    
    def check_cluster_status(self):
        try:
            current_time = time.time()
            manager_active = False
            
            # 기본 관리자 상태 확인
            if self.manager_id:
                manager_key = f"{NODE_INFO_PREFIX}/{self.cluster_id}/{self.manager_id}"
                manager_info = self.etcd_client.get(manager_key)
                
                if manager_info and manager_info.get("status") == "ACTIVE":
                    last_updated = manager_info.get("last_updated", 0)
                    time_since_update = current_time - last_updated
                    
                    if time_since_update < BACKUP_ACTIVATION_TIMEOUT:
                        manager_active = True
                        self.last_manager_update = current_time
                        self.manager_miss_count = 0
                    else:
                        self.manager_miss_count += 1
                        logger.warning(f"관리자 {self.manager_id} 응답 없음: {time_since_update:.1f}초 지남, 누적 miss: {self.manager_miss_count}")
                else:
                    self.manager_miss_count += 1
                    logger.warning(f"관리자 {self.manager_id} 비활성 상태, 누적 miss: {self.manager_miss_count}")
            # 임시 관리자 상태 확인
            elif self.temp_manager_id:
                temp_manager_key = f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/temp_{self.temp_manager_id}"
                temp_manager_info = self.etcd_client.get(temp_manager_key)
                
                if temp_manager_info:
                    last_updated = temp_manager_info.get("last_updated", 0)
                    time_since_update = current_time - last_updated
                    
                    if time_since_update < BACKUP_ACTIVATION_TIMEOUT:
                        manager_active = True
                        self.last_manager_update = current_time
                        self.manager_miss_count = 0
                    else:
                        self.manager_miss_count += 1
                        logger.warning(f"임시 관리자 {self.temp_manager_id} 응답 없음: {time_since_update:.1f}초 지남, 누적 miss: {self.manager_miss_count}")
                else:
                    self.manager_miss_count += 1
                    logger.warning(f"임시 관리자 정보가 없음, 누적 miss: {self.manager_miss_count}")
            else:
                self.manager_miss_count += 1
                logger.warning(f"관리자 없음, 누적 miss: {self.manager_miss_count}")
            
            # 관리자 부재 시 선출 검토
            # MAX_HEARTBEAT_MISS 이상 누적되면 check_if_election_needed에서 선출 시작
            if self.manager_miss_count >= MAX_HEARTBEAT_MISS and not self.election_in_progress:
                logger.warning(f"관리자 장애 감지: {self.manager_miss_count}번 연속 미응답")
                self.check_if_election_needed()
            
            # 연결 확인
            if self.manager_id and self.manager_id not in self.connections:
                logger.warning(f"Connection to manager {self.manager_id} lost, attempting to reconnect")
                self.connect_to_manager()
            if self.temp_manager_id and self.temp_manager_id not in self.connections:
                logger.warning(f"Connection to temporary manager {self.temp_manager_id} lost, attempting to reconnect")
                self.connect_to_temp_manager()
            if not self.manager_id and not self.temp_manager_id:
                logger.warning("No manager connection, attempting to find manager")
                self.find_manager()
                
        except Exception as e:
            logger.error(f"Error in check_cluster_status: {e}")