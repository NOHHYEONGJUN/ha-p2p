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
        self.type = "PARTICIPANT"
        self.manager_id = None
        self.manager_connection = None
        self.temp_manager_id = None
        self.temp_manager_connection = None
        self.manager_watch_ids = None
    
    def start(self):
        super().start()
        self.find_manager()
        self.watch_manager_changes()
        logger.info(f"Participant node {self.id} is running in cluster {self.cluster_id}")
    
    def stop(self):
        if self.manager_watch_ids:
            self.etcd_client.cancel_watch(self.manager_watch_ids)
        super().stop()
    
    def find_manager(self):
        try:
            cluster_key = f"{CLUSTER_INFO_PREFIX}/{self.cluster_id}/info"
            cluster_info = self.etcd_client.get(cluster_key)
            if cluster_info and "manager_id" in cluster_info:
                self.manager_id = cluster_info["manager_id"]
                logger.info(f"Found primary manager: {self.manager_id}")
                self.connect_to_manager()
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
                elif primary_managers:
                    primary_manager = max(primary_managers, key=lambda x: x.get("last_updated", 0))
                    self.manager_id = primary_manager["node_id"]
                    logger.info(f"Found primary manager: {self.manager_id}")
                    self.connect_to_manager()
                else:
                    logger.warning(f"No manager found for cluster {self.cluster_id}")
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
            self.manager_watch_ids = self.etcd_client.watch_prefix(cluster_prefix, on_cluster_change)
            if not self.manager_watch_ids:
                logger.warning("Failed to set up watch for manager changes")
        except Exception as e:
            logger.error(f"Error setting up watch for manager changes: {e}")
    
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
        except Exception as e:
            logger.error(f"Error in check_cluster_status: {e}")

