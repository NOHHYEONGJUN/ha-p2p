import sys
import time
import json
import subprocess
import logging
import threading
import argparse
from typing import List, Dict, Any, Optional, Tuple

from config import (
    ETCD_CLUSTER_ENDPOINTS,
    NODE_INFO_PREFIX,
    MANAGER_INFO_PREFIX,
    CLUSTER_INFO_PREFIX,
    BACKUP_INFO_PREFIX,
    CLUSTER_BACKUP_MAP,
    BACKUP_ACTIVATION_TIMEOUT
)
from etcd_client import EtcdClient

GREEN = "\033[1;32m"
RED = "\033[1;31m"
YELLOW = "\033[1;33m"
RESET = "\033[0m"

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger('test_ha')

class HATest:
    """P2P 관리 노드 고가용성 테스트 클래스"""
    
    def __init__(self, cluster_id: str, node_id: str, etcd_endpoints: Optional[List[str]] = None):
        self.cluster_id = cluster_id
        self.node_id = node_id
        
        if etcd_endpoints:
            self.etcd_endpoints = etcd_endpoints
        else:
            self.etcd_endpoints = ETCD_CLUSTER_ENDPOINTS.get(cluster_id, [])
        
        self.backup_cluster_id = CLUSTER_BACKUP_MAP.get(cluster_id)
        
        self.etcd_client = EtcdClient(cluster_id=cluster_id, endpoints=self.etcd_endpoints)
        
        self.backup_etcd_client = None
        if self.backup_cluster_id:
            backup_endpoints = ETCD_CLUSTER_ENDPOINTS.get(self.backup_cluster_id, [])
            if backup_endpoints:
                try:
                    self.backup_etcd_client = EtcdClient(
                        cluster_id=self.backup_cluster_id, 
                        endpoints=backup_endpoints
                    )
                except Exception as e:
                    logger.warning(f"Failed to initialize backup ETCD client: {e}")
        
        self.debug_mode = False
        self.stop_event = threading.Event()
        
        self.active_nodes_cache = {}
        self.active_nodes_cache_time = 0
        
    def set_debug(self, debug_mode: bool):
        self.debug_mode = debug_mode
        
    def explore_etcd_keys(self, prefix: str = "/") -> List[str]:
        if not self.debug_mode:
            return []
            
        print("=== ETCD 키 공간 탐색 ===")
        keys = []
        try:
            results = self.etcd_client.get_prefix(prefix)
            for key, _ in results:
                keys.append(key)
            
            for key in sorted(keys):
                print(f"키: {key}")
                
            return keys
        except Exception as e:
            logger.error(f"Error exploring ETCD keys: {e}")
            return []
    
    def dump_etcd_value(self, key: str) -> Optional[Dict[str, Any]]:
        if not self.debug_mode:
            return None
            
        try:
            value = self.etcd_client.get(key)
            if value:
                print(f"키: {key}, 값: {json.dumps(value, indent=2)}")
                return value
            print(f"키 {key}에 해당하는 값이 없습니다.")
            return None
        except Exception as e:
            logger.error(f"Error dumping ETCD value for key {key}: {e}")
            return None
            
    def check_all_nodes(self):
        print("=== 모든 노드 상태 확인 ===")
        
        all_nodes = {}
        try:
            results = self.etcd_client.get_prefix(f"{NODE_INFO_PREFIX}/")
            for key, node_data in results:
                node_id = node_data.get("node_id")
                if node_id:
                    all_nodes[node_id] = node_data
            
            self.active_nodes_cache = {
                node_id: node["status"] == "ACTIVE" 
                for node_id, node in all_nodes.items()
            }
            self.active_nodes_cache_time = time.time()
                    
            current_time = time.time()
            for node_id, node in sorted(all_nodes.items(), key=lambda x: x[0]):
                status_color = GREEN if node["status"] == "ACTIVE" else RED
                status_icon = "🟢" if node["status"] == "ACTIVE" else "🔴"
                last_updated_diff = current_time - node.get("last_updated", 0)
                
                print(f"노드: {node_id}, "
                      f"클러스터: {node['cluster_id']}, "
                      f"상태: {status_color}{status_icon} {node['status']}{RESET}, "
                      f"유형: {node.get('type', 'UNKNOWN')}, "
                      f"마지막 업데이트: {last_updated_diff:.1f}초 전")
                      
            cluster_nodes = [n for n in all_nodes.values() if str(n["cluster_id"]) == str(self.cluster_id)]
            active_nodes = [n for n in cluster_nodes if n["status"] == "ACTIVE"]
            node_count_color = GREEN if len(active_nodes) >= 2 else RED
            print(f"클러스터 {self.cluster_id}의 활성 노드 수: "
                  f"{node_count_color}{len(active_nodes)}{RESET}")
            
        except Exception as e:
            logger.error(f"Error checking nodes: {e}")
            
    def check_manager_status(self):
        print("=== 클러스터 관리자 현황 ===")
        
        if self.debug_mode:
            self.explore_etcd_keys(f"{MANAGER_INFO_PREFIX}/")
            
        current_time = time.time()
        max_inactive_time = 10.0
        
        for cluster_id in ETCD_CLUSTER_ENDPOINTS.keys():
            try:
                # 우선순위 1: 노드 정보에서 활성 상태의 MANAGER 유형 노드 찾기
                active_managers_from_nodes = []
                node_results = self.etcd_client.get_prefix(f"{NODE_INFO_PREFIX}/{cluster_id}/")
                for node_key, node_data in node_results:
                    if node_data.get("type") == "MANAGER" and node_data.get("status") == "ACTIVE":
                        node_id = node_data.get("node_id")
                        last_updated = node_data.get("last_updated", 0)
                        time_diff = current_time - last_updated
                        is_primary = node_data.get("is_primary", False)
                        
                        if time_diff < max_inactive_time:
                            # 노드 정보에서 찾은 활성 관리자는 최우선
                            active_managers_from_nodes.append((node_id, is_primary, time_diff, node_data))
                
                # 우선순위 2: 관리자 정보에서 활성 관리자 찾기
                active_managers_from_manager_info = []
                manager_results = self.etcd_client.get_prefix(f"{MANAGER_INFO_PREFIX}/{cluster_id}/")
                
                for key, manager_data in manager_results:
                    if "events/" not in key and "temp_" not in key and "election" not in key:
                        manager_id = manager_data.get("node_id", "unknown")
                        is_primary = manager_data.get("is_primary", False)
                        status = manager_data.get("status", "UNKNOWN")
                        last_updated = manager_data.get("last_updated", 0)
                        time_diff = current_time - last_updated
                        
                        # 최근에 업데이트된 활성 상태의 관리자 찾기
                        if status == "ACTIVE" and time_diff < max_inactive_time:
                            # 노드 상태 확인 - 관리자 정보에 있지만 실제 노드가 비활성이면 제외
                            node_is_active = self.check_node_is_active(manager_id)
                            if node_is_active:
                                active_managers_from_manager_info.append((manager_id, is_primary, time_diff, manager_data))
                
                # 우선순위 3: 클러스터 정보에서 관리자 확인
                primary_manager_id = None
                primary_manager_time = 0
                
                cluster_key = f"{CLUSTER_INFO_PREFIX}/{cluster_id}/info"
                cluster_info = self.etcd_client.get(cluster_key)
                
                if cluster_info and "manager_id" in cluster_info:
                    primary_manager_id = cluster_info.get("manager_id")
                    primary_manager_time = cluster_info.get("last_updated", 0)
                    time_diff = current_time - primary_manager_time
                    
                    # 클러스터 정보에 있는 관리자가 활성 상태인지 확인
                    if time_diff < max_inactive_time and self.check_node_is_active(primary_manager_id):
                        # 클러스터 정보에서 찾은 유효한 관리자 (가장 낮은 우선순위)
                        manager_from_cluster_info = (primary_manager_id, True, time_diff, cluster_info)
                    else:
                        manager_from_cluster_info = None
                else:
                    manager_from_cluster_info = None
                
                # 선택 로직:
                # 1. 노드 정보에서 찾은 활성 관리자 (최우선)
                # 2. 관리자 정보에서 찾은 활성 관리자
                # 3. 클러스터 정보에서 찾은 유효한 관리자
                selected_manager = None
                source = "unknown"
                
                if active_managers_from_nodes:
                    # primary 관리자가 있으면 우선 선택
                    primary_managers = [m for m in active_managers_from_nodes if m[1]]
                    if primary_managers:
                        selected_manager = min(primary_managers, key=lambda x: x[2])
                    else:
                        selected_manager = min(active_managers_from_nodes, key=lambda x: x[2])
                    source = "node_info"
                elif active_managers_from_manager_info:
                    # 관리자 정보에서 찾은 관리자 선택
                    primary_managers = [m for m in active_managers_from_manager_info if m[1]]
                    if primary_managers:
                        selected_manager = min(primary_managers, key=lambda x: x[2])
                    else:
                        selected_manager = min(active_managers_from_manager_info, key=lambda x: x[2])
                    source = "manager_info"
                elif manager_from_cluster_info:
                    # 클러스터 정보에서 찾은 관리자 (마지막 옵션)
                    selected_manager = manager_from_cluster_info
                    source = "cluster_info"
                
                # 결과 표시
                if selected_manager:
                    manager_id, is_primary, time_diff, _ = selected_manager
                    
                    role_text = "프라이머리" if is_primary else "세컨더리"
                    role_color = GREEN if is_primary else YELLOW
                    
                    node_is_active = self.check_node_is_active(manager_id)
                    
                    if self.debug_mode:
                        print(f"  소스: {source}")
                    
                    if node_is_active:
                        print(f"클러스터 {cluster_id} 관리자: "
                              f"{manager_id}, "
                              f"상태: {GREEN}🟢 {role_color}{role_text}{RESET} "
                              f"({time_diff:.1f}초 전)")
                    else:
                        print(f"클러스터 {cluster_id} 관리자: "
                              f"{manager_id}, "
                              f"상태: {RED}🔴 비활성 노드{RESET} "
                              f"({time_diff:.1f}초 전)")
                else:
                    # 선출 진행 중인지 확인
                    election_key = f"{MANAGER_INFO_PREFIX}/{cluster_id}/election"
                    election_info = self.etcd_client.get(election_key)
                    
                    if election_info and election_info.get("status") == "STARTED":
                        election_time = current_time - election_info.get("timestamp", 0)
                        print(f"클러스터 {cluster_id} 관리자: {YELLOW}선출 진행 중 ({election_time:.1f}초 전 시작){RESET}")
                    else:
                        print(f"클러스터 {cluster_id} 관리자: {RED}없음{RESET}")
                
                # 임시 관리자 표시
                temp_manager_results = self.etcd_client.get_prefix(f"{MANAGER_INFO_PREFIX}/{cluster_id}/temp_")
                temp_managers = []
                
                for key, manager_data in temp_manager_results:
                    temp_managers.append(manager_data)
                
                if temp_managers:
                    print(f"클러스터 {cluster_id} 임시 관리자:")
                    for temp_manager in temp_managers:
                        manager_id = temp_manager["node_id"]
                        original_cluster = temp_manager.get("cluster_id", "unknown")
                        last_updated_diff = current_time - temp_manager.get("last_updated", 0)
                        
                        node_is_active = self.check_node_is_active(manager_id)
                        time_is_valid = last_updated_diff < max_inactive_time
                        
                        if node_is_active and time_is_valid:
                            print(f"  - {manager_id} (클러스터 {original_cluster}에서 활성화): "
                                  f"{GREEN}활성{RESET} ({last_updated_diff:.1f}초 전)")
                        else:
                            reason = "비활성 노드" if not node_is_active else "응답 없음"
                            print(f"  - {manager_id} (클러스터 {original_cluster}에서 활성화): "
                                  f"{RED}{reason}{RESET} ({last_updated_diff:.1f}초 전)")
                
                # 디버그 모드에서 선출 정보 확인
                if self.debug_mode:
                    election_key = f"{MANAGER_INFO_PREFIX}/{cluster_id}/election"
                    election_info = self.etcd_client.get(election_key)
                    if election_info:
                        status = election_info.get("status", "UNKNOWN")
                        if status == "COMPLETED":
                            print(f"  클러스터 {cluster_id} 선출 완료: "
                                f"당선자 {election_info.get('winner_id', 'unknown')}")
                        elif status == "STARTED":
                            print(f"  클러스터 {cluster_id} 선출 진행 중: "
                                f"시작자 {election_info.get('initiator_id', 'unknown')}")
                    
                    votes_key = f"{MANAGER_INFO_PREFIX}/{cluster_id}/election/votes/"
                    votes_results = self.etcd_client.get_prefix(votes_key)
                    votes = {}
                    for _, vote_data in votes_results:
                        vote_for = vote_data.get("vote_for")
                        if vote_for in votes:
                            votes[vote_for] += 1
                        else:
                            votes[vote_for] = 1
                    
                    if votes:
                        print(f"  클러스터 {cluster_id} 투표 현황: {votes}")
                
                # 백업 관리자 정보 표시
                backup_cluster_id = CLUSTER_BACKUP_MAP.get(cluster_id)
                if backup_cluster_id:
                    try:
                        backup_etcd_client = EtcdClient(cluster_id=backup_cluster_id)
                        backup_results = backup_etcd_client.get_prefix(f"{BACKUP_INFO_PREFIX}/{cluster_id}/managers/")
                        backups = []
                        
                        for key, backup_data in backup_results:
                            backups.append(backup_data)
                        
                        if backups:
                            print(f"클러스터 {cluster_id}의 백업 관리자 (클러스터 {backup_cluster_id}에서):")
                            for backup in backups:
                                backup_id = backup["node_id"]
                                backup_status = backup.get("status", "UNKNOWN")
                                last_updated_diff = current_time - backup.get("last_updated", 0)
                                
                                status_color = GREEN if backup_status == "ACTIVE" else YELLOW
                                
                                print(f"  - {backup_id}: "
                                      f"{status_color}{backup_status}{RESET} ({last_updated_diff:.1f}초 전)")
                    except Exception as e:
                        logger.warning(f"백업 클러스터 {backup_cluster_id} 연결 실패: {e}")
                    
            except Exception as e:
                logger.error(f"Error checking manager status for cluster {cluster_id}: {e}")
                print(f"클러스터 {cluster_id} 관리자: {RED}조회 실패{RESET}")
    
    def check_node_is_active(self, node_id: str) -> bool:
        cache_age = time.time() - self.active_nodes_cache_time
        if cache_age < 1.0 and node_id in self.active_nodes_cache:
            return self.active_nodes_cache[node_id]
            
        try:
            active_status = False
            for cluster_id in ETCD_CLUSTER_ENDPOINTS.keys():
                node_key = f"{NODE_INFO_PREFIX}/{cluster_id}/{node_id}"
                node_data = self.etcd_client.get(node_key)
                
                if node_data and node_data.get("status") == "ACTIVE":
                    active_status = True
                    break
                        
            self.active_nodes_cache[node_id] = active_status
            return active_status
        except Exception as e:
            logger.error(f"Error checking node status: {e}")
            return False
    
    def check_etcd_cluster_status(self):
        print("=== ETCD 클러스터 상태 확인 ===")
        try:
            is_healthy = self.etcd_client.is_cluster_healthy()
            health_color = GREEN if is_healthy else RED
            health_text = "정상" if is_healthy else "비정상"
            
            print(f"ETCD 클러스터 상태: {health_color}{health_text}{RESET}")
            
            leader_id = self.etcd_client.get_leader_id()
            if leader_id:
                print(f"ETCD 클러스터 리더 ID: {GREEN}{leader_id}{RESET}")
            else:
                print(f"ETCD 클러스터 리더: {RED}없음{RESET}")
            
            using_backup = self.etcd_client.is_using_backup()
            if using_backup:
                print(f"{YELLOW}현재 백업 ETCD 클러스터 사용 중{RESET}")
            
            return is_healthy
        except Exception as e:
            logger.error(f"Error checking ETCD cluster status: {e}")
            print(f"ETCD 클러스터 상태: {RED}확인 실패{RESET}")
            return False
                
    def stop_node(self, node_id: str) -> bool:
        try:
            print(f"노드 {node_id} 프로세스 중지 중...")
            cmd = f"docker ps -q --filter name=p2p.*{node_id}"
            container_id = subprocess.check_output(cmd, shell=True).decode('utf-8').strip()
            
            if container_id:
                subprocess.run(["docker", "stop", container_id], check=True)
                print(f"{GREEN}노드 {node_id} 프로세스 중지 성공{RESET}")
                return True
            else:
                print(f"{RED}노드 {node_id}에 해당하는 컨테이너 찾을 수 없음{RESET}")
                return False
        except subprocess.CalledProcessError as e:
            logger.error(f"Error stopping node {node_id}: {e}")
            return False
            
    def start_node(self, node_id: str) -> bool:
        try:
            print(f"노드 {node_id} 프로세스 시작 중...")
            cmd = f"docker ps -a -q --filter name=p2p.*{node_id}"
            container_id = subprocess.check_output(cmd, shell=True).decode('utf-8').strip()
            
            if container_id:
                subprocess.run(["docker", "start", container_id], check=True)
                print(f"{GREEN}노드 {node_id} 프로세스 시작 성공{RESET}")
                return True
            else:
                print(f"{RED}노드 {node_id}에 해당하는 컨테이너 찾을 수 없음{RESET}")
                return False
        except subprocess.CalledProcessError as e:
            logger.error(f"Error starting node {node_id}: {e}")
            return False
    
    def stop_etcd_node(self, etcd_name: str) -> bool:
        try:
            print(f"ETCD 노드 {etcd_name} 프로세스 중지 중...")
            cmd = f"docker ps -q --filter name=p2p.*{etcd_name}"
            container_id = subprocess.check_output(cmd, shell=True).decode('utf-8').strip()
            
            if container_id:
                subprocess.run(["docker", "stop", container_id], check=True)
                print(f"{GREEN}ETCD 노드 {etcd_name} 프로세스 중지 성공{RESET}")
                return True
            else:
                print(f"{RED}ETCD 노드 {etcd_name}에 해당하는 컨테이너 찾을 수 없음{RESET}")
                return False
        except subprocess.CalledProcessError as e:
            logger.error(f"Error stopping ETCD node {etcd_name}: {e}")
            return False
    
    def start_etcd_node(self, etcd_name: str) -> bool:
        try:
            print(f"ETCD 노드 {etcd_name} 프로세스 시작 중...")
            cmd = f"docker ps -a -q --filter name=p2p.*{etcd_name}"
            container_id = subprocess.check_output(cmd, shell=True).decode('utf-8').strip()
            
            if container_id:
                subprocess.run(["docker", "start", container_id], check=True)
                print(f"{GREEN}ETCD 노드 {etcd_name} 프로세스 시작 성공{RESET}")
                return True
            else:
                print(f"{RED}ETCD 노드 {etcd_name}에 해당하는 컨테이너 찾을 수 없음{RESET}")
                return False
        except subprocess.CalledProcessError as e:
            logger.error(f"Error starting ETCD node {etcd_name}: {e}")
            return False
    
    def test_manager_node_failure(self):
        print(f"=== 클러스터 {self.cluster_id} 관리자 노드 장애 복구 테스트 시작 ===\n")
        
        print("[1] 초기 상태 확인")
        self.check_etcd_cluster_status()
        print()
        self.check_all_nodes()
        print()
        self.check_manager_status()
        
        cluster_key = f"{CLUSTER_INFO_PREFIX}/{self.cluster_id}/info"
        cluster_info = self.etcd_client.get(cluster_key)
        
        manager_id = None
        if cluster_info and "manager_id" in cluster_info:
            manager_id = cluster_info["manager_id"]
        else:
            manager_results = self.etcd_client.get_prefix(f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/")
            managers = []
            
            for key, manager_data in manager_results:
                if "events/" not in key and "temp_" not in key and "election" not in key:
                    managers.append(manager_data)
            
            if managers:
                latest_manager = max(managers, key=lambda x: x.get("last_updated", 0))
                manager_id = latest_manager["node_id"]
        
        if not manager_id:
            print(f"{RED}클러스터 {self.cluster_id}의 관리자 노드를 찾을 수 없습니다.{RESET}")
            return
            
        if manager_id != self.node_id:
            print(f"{YELLOW}경고: 현재 관리자({manager_id})가 중지하려는 노드({self.node_id})와 다릅니다.{RESET}")
            response = input("계속 진행하시겠습니까? (y/n): ")
            if response.lower() != 'y':
                print(f"{RED}테스트를 중단합니다.{RESET}")
                return
                
        print(f"\n[2] 관리자 노드 {self.node_id} 중지")
        if not self.stop_node(self.node_id):
            print(f"{RED}테스트를 중단합니다.{RESET}")
            return
            
        print(f"\n[3] 클러스터 상태 변화 모니터링 (최대 120초)")
        start_time = time.time()
        max_monitoring_time = 120
        
        backup_activated = False
        new_manager_elected = False
        new_manager_id = None
        while time.time() - start_time < max_monitoring_time:
            try:
                self.active_nodes_cache = {}  
                self.active_nodes_cache_time = 0
                
                # 선출된 새 관리자 확인 (관리자 테이블에서 확인)
                manager_results = self.etcd_client.get_prefix(f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/")
                active_managers = []
                for key, manager_data in manager_results:
                    if "events/" not in key and "temp_" not in key and "election" not in key and manager_data["node_id"] != self.node_id:
                        if manager_data.get("status") == "ACTIVE" and manager_data.get("is_primary", False):
                            active_managers.append(manager_data)
                
                if active_managers:
                    new_manager = sorted(active_managers, key=lambda x: x.get("last_updated", 0), reverse=True)[0]
                    new_manager_id = new_manager["node_id"]
                    new_manager_elected = True
                    if not backup_activated:
                        print(f"\n{GREEN}[*] 새 관리자가 선출됨: {new_manager_id}{RESET}")
                
                # 현재 클러스터 상태 표시
                self.check_manager_status()
                elapsed_time = time.time() - start_time
                print(f"\n=== 경과 시간: {elapsed_time:.1f}초 ===\n")
                
                # 백업 관리자 활성화 확인
                backup_cluster_id = CLUSTER_BACKUP_MAP.get(self.cluster_id)
                if backup_cluster_id:
                    try:
                        backup_etcd_client = EtcdClient(cluster_id=backup_cluster_id)
                        backup_results = backup_etcd_client.get_prefix(f"{BACKUP_INFO_PREFIX}/{self.cluster_id}/managers/")
                        for key, backup_data in backup_results:
                            if backup_data.get("status") == "ACTIVE":
                                backup_activated = True
                                backup_node_id = backup_data["node_id"]
                                print(f"\n{GREEN}[*] 백업 관리자가 활성화됨: {backup_node_id}{RESET}")
                                break
                    except Exception as e:
                        logger.warning(f"백업 클러스터 {backup_cluster_id} 연결 실패: {e}")
                
                if not backup_activated:
                    temp_manager_results = self.etcd_client.get_prefix(f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/temp_")
                    for key, manager_data in temp_manager_results:
                        backup_activated = True
                        temp_manager_id = manager_data["node_id"]
                        print(f"\n{GREEN}[*] 임시 관리자 감지: {temp_manager_id}{RESET}")
                        break
                
                # 현재 선출 상태 확인
                if not new_manager_elected:
                    election_key = f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/election"
                    election_info = self.etcd_client.get(election_key)
                    
                    if election_info:
                        status = election_info.get("status", "UNKNOWN")
                        if status == "COMPLETED":
                            new_manager_elected = True
                            new_manager_id = election_info.get("winner_id")
                            print(f"\n{GREEN}[*] 새 관리자가 선출됨: {new_manager_id}{RESET}")
                        elif status == "STARTED":
                            print(f"\n{YELLOW}[*] 선출 진행 중 (시작자: {election_info.get('initiator_id', 'unknown')}){RESET}")
                
                # 백업 관리자가 활성화되고 새 관리자가 선출되었으면 테스트 종료
                if backup_activated and new_manager_elected:
                    # 클러스터 정보에 반영되었는지 확인
                    cluster_key = f"{CLUSTER_INFO_PREFIX}/{self.cluster_id}/info"
                    cluster_info = self.etcd_client.get(cluster_key)
                    
                    if cluster_info and cluster_info.get("manager_id") == new_manager_id:
                        print(f"\n{GREEN}[+] 클러스터 정보가 새 관리자로 업데이트됨{RESET}")
                        
                        # 백업 관리자 스탠바이 확인
                        if backup_cluster_id:
                            try:
                                backup_results = backup_etcd_client.get_prefix(f"{BACKUP_INFO_PREFIX}/{self.cluster_id}/managers/")
                                for key, backup_data in backup_results:
                                    if backup_data.get("status") == "STANDBY":
                                        print(f"\n{GREEN}[+] 백업 관리자가 STANDBY 상태로 복귀{RESET}")
                                        break
                            except Exception as e:
                                logger.warning(f"백업 클러스터 상태 확인 중 오류: {e}")
                        
                        time.sleep(3)  # 상태가 완전히 갱신되도록 대기
                        break
                
                # 빠른 반복을 위한 대기 시간 설정
                wait_time = 1.0 if not backup_activated else 2.0
                # 스레드 안전한 대기 사용
                try:
                    if not self.stop_event.wait(wait_time):
                        continue
                    else:
                        break
                except:
                    # stop_event에 문제가 있는 경우 일반 sleep 사용
                    time.sleep(wait_time)
                    continue
            except KeyboardInterrupt:
                print(f"\n{YELLOW}테스트가 사용자에 의해 중단되었습니다.{RESET}")
                break
            except Exception as e:
                logger.error(f"모니터링 중 오류 발생: {e}")
                time.sleep(1.0)
                
        print(f"\n[4] 테스트 결과")
        self.check_etcd_cluster_status()
        print()
        self.check_all_nodes()
        print()
        self.check_manager_status()
        
        if backup_activated and new_manager_elected:
            print(f"\n{GREEN}[+] 테스트 성공: 백업 관리자가 활성화되고 새 관리자가 선출되었습니다.{RESET}")
        elif backup_activated:
            print(f"\n{YELLOW}[~] 테스트 부분 성공: 백업 관리자는 활성화되었지만 새 관리자 선출이 완료되지 않았습니다.{RESET}")
        else:
            print(f"\n{RED}[-] 테스트 실패: 백업 관리자가 활성화되지 않았습니다.{RESET}")
            
        response = input(f"\n[5] 원래 관리자 노드 다시 시작\n"
                         f"원래 관리자 노드 {self.node_id}를 다시 시작하시겠습니까? (y/n): ")
        if response.lower() == 'y':
            self.start_node(self.node_id)
            print(f"\n원래 관리자 노드 복구 후 상태:")
            time.sleep(5)
            self.check_etcd_cluster_status()
            print()
            self.check_all_nodes()
            print()
            self.check_manager_status()
    
    def test_etcd_node_failure(self):
        print(f"=== 클러스터 {self.cluster_id} ETCD 노드 장애 복구 테스트 시작 ===\n")
        
        print("[1] 초기 상태 확인")
        self.check_etcd_cluster_status()
        print()
        self.check_all_nodes()
        print()
        self.check_manager_status()
        
        etcd_name = f"etcd{self.cluster_id}"
        
        print(f"\n[2] ETCD 노드 {etcd_name} 중지")
        if not self.stop_etcd_node(etcd_name):
            print(f"{RED}테스트를 중단합니다.{RESET}")
            return
            
        print(f"\n[3] 클러스터 상태 변화 모니터링 (최대 60초)")
        start_time = time.time()
        max_monitoring_time = 60
        
        backup_activated = False
        while time.time() - start_time < max_monitoring_time:
            try:
                self.check_manager_status()
                elapsed_time = time.time() - start_time
                print(f"\n=== 경과 시간: {elapsed_time:.1f}초 ===\n")
                
                backup_cluster_id = CLUSTER_BACKUP_MAP.get(self.cluster_id)
                if backup_cluster_id:
                    try:
                        backup_etcd_client = EtcdClient(cluster_id=backup_cluster_id)
                        backup_results = backup_etcd_client.get_prefix(f"{BACKUP_INFO_PREFIX}/{self.cluster_id}/managers/")
                        for key, backup_data in backup_results:
                            if backup_data.get("status") == "ACTIVE":
                                backup_activated = True
                                backup_node_id = backup_data["node_id"]
                                print(f"\n{GREEN}[*] 백업 관리자가 활성화됨: {backup_node_id}{RESET}")
                                time.sleep(5)
                                break
                    except Exception as e:
                        logger.warning(f"백업 클러스터 {backup_cluster_id} 연결 실패: {e}")
                
                if not backup_activated:
                    temp_manager_results = self.etcd_client.get_prefix(f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/temp_")
                    for key, manager_data in temp_manager_results:
                        backup_activated = True
                        temp_manager_id = manager_data["node_id"]
                        print(f"\n{GREEN}[*] 임시 관리자 감지: {temp_manager_id}{RESET}")
                        time.sleep(5)
                        break
                
                if backup_activated:
                    break
                
                time.sleep(1.0)
            except KeyboardInterrupt:
                print(f"\n{YELLOW}테스트가 사용자에 의해 중단되었습니다.{RESET}")
                break
            except Exception as e:
                logger.error(f"모니터링 중 오류 발생: {e}")
                time.sleep(1.0)
                
        print(f"\n[4] 테스트 결과")
        try:
            self.check_etcd_cluster_status()
        except Exception:
            print(f"{RED}ETCD 클러스터 상태 확인 실패 (연결 오류){RESET}")
        print()
        try:
            self.check_all_nodes()
        except Exception:
            print(f"{RED}노드 상태 확인 실패 (연결 오류){RESET}")
        print()
        try:
            self.check_manager_status()
        except Exception:
            print(f"{RED}관리자 상태 확인 실패 (연결 오류){RESET}")
        
        if backup_activated:
            print(f"\n{GREEN}[+] 테스트 성공: 백업 관리자가 성공적으로 활성화되었습니다.{RESET}")
        else:
            print(f"\n{RED}[-] 테스트 실패: 백업 관리자가 활성화되지 않았습니다.{RESET}")
            
        response = input(f"\n[5] 원래 ETCD 노드 다시 시작\n"
                         f"원래 ETCD 노드 {etcd_name}를 다시 시작하시겠습니까? (y/n): ")
        if response.lower() == 'y':
            self.start_etcd_node(etcd_name)
            print(f"\nETCD 노드 복구 후 상태:")
            time.sleep(5)
            self.check_etcd_cluster_status()
            print()
            self.check_all_nodes()
            print()
            self.check_manager_status()

def parse_etcd_endpoints(endpoints_str: str) -> List[str]:
    if not endpoints_str:
        return None
    return endpoints_str.split(',')

def main():
    parser = argparse.ArgumentParser(description='P2P 관리 노드 고가용성 테스트')
    parser.add_argument('cluster_id', type=str, help='테스트할 클러스터 ID')
    parser.add_argument('node_id', type=str, help='테스트할 노드 ID (예: node_1_1)')
    parser.add_argument('--debug', action='store_true', help='디버그 정보 표시')
    parser.add_argument('--etcd-endpoints', type=str, help='ETCD 엔드포인트 목록 (콤마로 구분)')
    parser.add_argument('--test-type', choices=['manager', 'etcd'], default='manager',
                        help='테스트 유형: manager(관리자 노드 장애) 또는 etcd(ETCD 노드 장애)')
    
    args = parser.parse_args()
    
    etcd_endpoints = parse_etcd_endpoints(args.etcd_endpoints)
    
    print(f"P2P 관리 노드 고가용성 테스트 시작 - 클러스터 {args.cluster_id}, 노드 {args.node_id}")
    tester = HATest(args.cluster_id, args.node_id, etcd_endpoints)
    tester.set_debug(args.debug)
    
    if args.test_type == 'manager':
        tester.test_manager_node_failure()
    else:
        tester.test_etcd_node_failure()
    
if __name__ == "__main__":
    main()