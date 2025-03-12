import sys
import time
import json
import subprocess
import logging
import argparse
from typing import List, Dict, Any, Optional, Tuple

from config import (
    ETCD_CLUSTER_ENDPOINTS,
    NODE_INFO_PREFIX,
    MANAGER_INFO_PREFIX,
    CLUSTER_INFO_PREFIX,
    BACKUP_INFO_PREFIX,
    CLUSTER_BACKUP_MAP
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
        
        # 모든 클러스터에 대한 ETCD 클라이언트 생성
        self.etcd_clients = {}
        for cid, endpoints in ETCD_CLUSTER_ENDPOINTS.items():
            try:
                self.etcd_clients[cid] = EtcdClient(cluster_id=cid, endpoints=endpoints)
                logger.info(f"클러스터 {cid}의 ETCD 클라이언트 초기화 성공")
            except Exception as e:
                logger.warning(f"클러스터 {cid}의 ETCD 클라이언트 초기화 실패: {e}")
        
        # 현재 클러스터의 ETCD 클라이언트
        self.etcd_client = self.etcd_clients.get(cluster_id)
        if not self.etcd_client:
            logger.warning(f"현재 클러스터 {cluster_id}의 ETCD 클라이언트 초기화 실패, 기본 설정으로 재시도")
            self.etcd_client = EtcdClient(cluster_id=cluster_id, endpoints=self.etcd_endpoints)
        
        self.debug_mode = False
        
        self.active_nodes_cache = {}
        self.active_nodes_cache_time = 0
        
    def set_debug(self, debug_mode: bool):
        self.debug_mode = debug_mode
        
    def explore_etcd_keys(self, prefix: str = "/", cluster_id: Optional[str] = None) -> List[str]:
        if not self.debug_mode:
            return []
        
        client = self.etcd_clients.get(cluster_id) if cluster_id else self.etcd_client
        if not client:
            logger.warning(f"클러스터 {cluster_id or self.cluster_id}의 ETCD 클라이언트 없음")
            return []
            
        print(f"=== 클러스터 {cluster_id or self.cluster_id}의 ETCD 키 공간 탐색 ===")
        keys = []
        try:
            results = client.get_prefix(prefix)
            for key, _ in results:
                keys.append(key)
            
            for key in sorted(keys):
                print(f"키: {key}")
                
            return keys
        except Exception as e:
            logger.error(f"Error exploring ETCD keys: {e}")
            return []
    
    def dump_etcd_value(self, key: str, cluster_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        if not self.debug_mode:
            return None
        
        client = self.etcd_clients.get(cluster_id) if cluster_id else self.etcd_client
        if not client:
            logger.warning(f"클러스터 {cluster_id or self.cluster_id}의 ETCD 클라이언트 없음")
            return None
            
        try:
            value = client.get(key)
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
            # 모든 클러스터의 노드 확인
            for cid, client in self.etcd_clients.items():
                try:
                    results = client.get_prefix(f"{NODE_INFO_PREFIX}/")
                    for key, node_data in results:
                        node_id = node_data.get("node_id")
                        if node_id:
                            all_nodes[node_id] = node_data
                except Exception as e:
                    logger.warning(f"클러스터 {cid}의 노드 조회 실패: {e}")
            
            # 현재 클러스터의 노드가 없으면 현재 클라이언트로 다시 시도
            if not any(node.get("cluster_id") == self.cluster_id for node in all_nodes.values()):
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
            for cid in ETCD_CLUSTER_ENDPOINTS.keys():
                self.explore_etcd_keys(f"{MANAGER_INFO_PREFIX}/{cid}/", cid)
            
        current_time = time.time()
        max_inactive_time = 10.0
        
        for cluster_id in ETCD_CLUSTER_ENDPOINTS.keys():
            try:
                # 해당 클러스터의 ETCD 클라이언트 사용
                cluster_client = self.etcd_clients.get(cluster_id, self.etcd_client)
                
                # 클러스터 정보 조회
                cluster_key = f"{CLUSTER_INFO_PREFIX}/{cluster_id}/info"
                cluster_info = cluster_client.get(cluster_key)
                
                if cluster_info and "manager_id" in cluster_info:
                    manager_id = cluster_info["manager_id"]
                    is_primary = cluster_info.get("is_primary", False)
                    last_updated_diff = current_time - cluster_info.get("last_updated", 0)
                    
                    node_is_active = self.check_node_is_active(manager_id, cluster_id)
                    time_is_valid = last_updated_diff < max_inactive_time
                    
                    role_text = "프라이머리" if is_primary else "세컨더리"
                    role_color = GREEN if is_primary else YELLOW
                    
                    if node_is_active and time_is_valid:
                        print(f"클러스터 {cluster_id} 관리자: "
                              f"{manager_id}, "
                              f"상태: {GREEN}🟢 {role_color}{role_text}{RESET} "
                              f"({last_updated_diff:.1f}초 전)")
                    else:
                        reason = "비활성 노드" if not node_is_active else "응답 없음"
                        print(f"클러스터 {cluster_id} 관리자: "
                              f"{manager_id}, "
                              f"상태: {RED}🔴 {reason}{RESET} "
                              f"({last_updated_diff:.1f}초 전)")
                else:
                    # 관리자 정보가 없으면 직접 조회
                    manager_results = cluster_client.get_prefix(f"{MANAGER_INFO_PREFIX}/{cluster_id}/")
                    managers = []
                    
                    for key, manager_data in manager_results:
                        if "events/" not in key and "temp_" not in key and "notifications/" not in key:
                            managers.append(manager_data)
                    
                    if managers:
                        latest_manager = max(managers, key=lambda x: x.get("last_updated", 0))
                        manager_id = latest_manager["node_id"]
                        is_primary = latest_manager.get("is_primary", False)
                        last_updated_diff = current_time - latest_manager.get("last_updated", 0)
                        
                        node_is_active = self.check_node_is_active(manager_id, cluster_id)
                        time_is_valid = last_updated_diff < max_inactive_time
                        
                        role_text = "프라이머리" if is_primary else "세컨더리"
                        role_color = GREEN if is_primary else YELLOW
                        
                        if node_is_active and time_is_valid:
                            print(f"클러스터 {cluster_id} 관리자: "
                                f"{manager_id}, "
                                f"상태: {GREEN}🟢 {role_color}{role_text}{RESET} "
                                f"({last_updated_diff:.1f}초 전)")
                        else:
                            reason = "비활성 노드" if not node_is_active else "응답 없음"
                            print(f"클러스터 {cluster_id} 관리자: "
                                f"{manager_id}, "
                                f"상태: {RED}🔴 {reason}{RESET} "
                                f"({last_updated_diff:.1f}초 전)")
                    else:
                        print(f"클러스터 {cluster_id} 관리자: {RED}없음{RESET}")
                
                # 임시 관리자 확인
                temp_manager_results = cluster_client.get_prefix(f"{MANAGER_INFO_PREFIX}/{cluster_id}/temp_")
                temp_managers = []
                
                for key, manager_data in temp_manager_results:
                    temp_managers.append(manager_data)
                
                if temp_managers:
                    print(f"클러스터 {cluster_id} 임시 관리자:")
                    for temp_manager in temp_managers:
                        manager_id = temp_manager["node_id"]
                        original_cluster = temp_manager.get("cluster_id", "unknown")
                        last_updated_diff = current_time - temp_manager.get("last_updated", 0)
                        
                        node_is_active = self.check_node_is_active(manager_id, original_cluster)
                        time_is_valid = last_updated_diff < max_inactive_time
                        
                        if node_is_active and time_is_valid:
                            print(f"  - {manager_id} (클러스터 {original_cluster}에서 활성화): "
                                  f"{GREEN}활성{RESET} ({last_updated_diff:.1f}초 전)")
                        else:
                            reason = "비활성 노드" if not node_is_active else "응답 없음"
                            print(f"  - {manager_id} (클러스터 {original_cluster}에서 활성화): "
                                  f"{RED}{reason}{RESET} ({last_updated_diff:.1f}초 전)")
                
                # 승격된 관리자 정보 표시
                promotion_key = f"{CLUSTER_INFO_PREFIX}/{cluster_id}/promoted_manager"
                promotion_data = cluster_client.get(promotion_key)
                if promotion_data:
                    promoted_node = promotion_data.get("node_id")
                    status = promotion_data.get("status")
                    promoted_time = promotion_data.get("promoted_at", promotion_data.get("elected_at", 0))
                    time_diff = current_time - promoted_time
                    
                    status_color = GREEN if status == "PROMOTED" else YELLOW
                    print(f"클러스터 {cluster_id} 승격된 관리자: {promoted_node}, 상태: {status_color}{status}{RESET} ({time_diff:.1f}초 전)")
                
                # 백업 관리자 정보 표시
                backup_cluster_id = CLUSTER_BACKUP_MAP.get(cluster_id)
                if backup_cluster_id:
                    try:
                        backup_client = self.etcd_clients.get(backup_cluster_id, 
                                                   EtcdClient(cluster_id=backup_cluster_id))
                        backup_results = backup_client.get_prefix(f"{BACKUP_INFO_PREFIX}/{cluster_id}/managers/")
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
    
    def check_node_is_active(self, node_id: str, cluster_id: Optional[str] = None) -> bool:
        """특정 노드가 활성 상태인지 확인"""
        cache_age = time.time() - self.active_nodes_cache_time
        if cache_age < 1.0 and node_id in self.active_nodes_cache:
            return self.active_nodes_cache[node_id]
            
        try:
            active_status = False
            
            # 특정 클러스터를 지정한 경우
            if cluster_id:
                client = self.etcd_clients.get(cluster_id, self.etcd_client)
                node_key = f"{NODE_INFO_PREFIX}/{cluster_id}/{node_id}"
                node_data = client.get(node_key)
                
                if node_data and node_data.get("status") == "ACTIVE":
                    active_status = True
            else:
                # 모든 클러스터 확인
                for cid, client in self.etcd_clients.items():
                    try:
                        node_key = f"{NODE_INFO_PREFIX}/{cid}/{node_id}"
                        node_data = client.get(node_key)
                        
                        if node_data and node_data.get("status") == "ACTIVE":
                            active_status = True
                            break
                    except Exception:
                        continue
                        
            self.active_nodes_cache[node_id] = active_status
            return active_status
        except Exception as e:
            logger.error(f"Error checking node status: {e}")
            return False
    
    def check_etcd_cluster_status(self, cluster_id: Optional[str] = None):
        """ETCD 클러스터 상태 확인"""
        target_cluster_id = cluster_id or self.cluster_id
        client = self.etcd_clients.get(target_cluster_id, self.etcd_client)
        
        print(f"=== ETCD 클러스터 {target_cluster_id} 상태 확인 ===")
        try:
            is_healthy = client.is_cluster_healthy()
            health_color = GREEN if is_healthy else RED
            health_text = "정상" if is_healthy else "비정상"
            
            print(f"ETCD 클러스터 상태: {health_color}{health_text}{RESET}")
            
            leader_id = client.get_leader_id()
            if leader_id:
                print(f"ETCD 클러스터 리더 ID: {GREEN}{leader_id}{RESET}")
            else:
                print(f"ETCD 클러스터 리더: {RED}없음{RESET}")
            
            using_backup = client.is_using_backup()
            if using_backup:
                print(f"{YELLOW}현재 백업 ETCD 클러스터 사용 중{RESET}")
            
            return is_healthy
        except Exception as e:
            logger.error(f"Error checking ETCD cluster status: {e}")
            print(f"ETCD 클러스터 상태: {RED}확인 실패{RESET}")
            return False
                
    def stop_node(self, node_id: str) -> bool:
        """노드 중지"""
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
        """노드 시작"""
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
        """ETCD 노드 중지"""
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
        """ETCD 노드 시작"""
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
    
    def wait_for_event(self, event_name, check_function, max_wait_time=120, check_interval=1.0):
        """특정 이벤트 발생을 기다림"""
        print(f"{YELLOW}대기 중: {event_name}...{RESET}")
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            if check_function():
                print(f"{GREEN}✓ {event_name} 발생!{RESET}")
                return True
            time.sleep(check_interval)
        
        print(f"{RED}✗ {event_name} 타임아웃 ({max_wait_time}초){RESET}")
        return False
    
    def test_manager_node_failure(self):
        """관리자 노드 장애 복구 테스트"""
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
                if "events/" not in key and "temp_" not in key:
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
        promotion_activated = False
        backup_deactivated = False
        promoted_node = None
        
        while time.time() - start_time < max_monitoring_time:
            try:
                self.active_nodes_cache = {}  
                self.active_nodes_cache_time = 0
                
                self.check_manager_status()
                elapsed_time = time.time() - start_time
                print(f"\n=== 경과 시간: {elapsed_time:.1f}초 ===\n")
                
                # 백업 활성화 확인
                if not backup_activated:
                    backup_cluster_id = CLUSTER_BACKUP_MAP.get(self.cluster_id)
                    if backup_cluster_id:
                        try:
                            backup_client = self.etcd_clients.get(backup_cluster_id)
                            if not backup_client:
                                backup_client = EtcdClient(cluster_id=backup_cluster_id)
                                
                            backup_results = backup_client.get_prefix(f"{BACKUP_INFO_PREFIX}/{self.cluster_id}/managers/")
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
                
                # 새 관리자 선출 확인
                if backup_activated and not promotion_activated:
                    promotion_key = f"{CLUSTER_INFO_PREFIX}/{self.cluster_id}/promoted_manager"
                    promotion_data = self.etcd_client.get(promotion_key)
                    if promotion_data and promotion_data.get("status") == "PROMOTED":
                        promotion_activated = True
                        promoted_node = promotion_data.get("node_id")
                        print(f"\n{GREEN}[*] 새 관리자가 승격됨: {promoted_node}{RESET}")
                        time.sleep(5)
                
                # 백업 비활성화 확인
                if promotion_activated and not backup_deactivated:
                    backup_cluster_id = CLUSTER_BACKUP_MAP.get(self.cluster_id)
                    if backup_cluster_id:
                        try:
                            backup_client = self.etcd_clients.get(backup_cluster_id)
                            if not backup_client:
                                backup_client = EtcdClient(cluster_id=backup_cluster_id)
                                
                            backup_results = backup_client.get_prefix(f"{BACKUP_INFO_PREFIX}/{self.cluster_id}/managers/")
                            all_standby = True
                            for key, backup_data in backup_results:
                                if backup_data.get("status") != "STANDBY":
                                    all_standby = False
                            
                            if all_standby:
                                backup_deactivated = True
                                print(f"\n{GREEN}[*] 백업 관리자가 STANDBY 상태로 되돌아감{RESET}")
                                time.sleep(5)
                        except Exception as e:
                            logger.warning(f"백업 클러스터 {backup_cluster_id} 연결 실패: {e}")
                
                if backup_deactivated:
                    print(f"\n{GREEN}[+] 전체 복구 프로세스 완료!{RESET}")
                    break
                
                time.sleep(1.0)
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
        
        if backup_activated:
            print(f"\n{GREEN}[+] 테스트 단계 1 성공: 백업 관리자가 성공적으로 활성화되었습니다.{RESET}")
        else:
            print(f"\n{RED}[-] 테스트 단계 1 실패: 백업 관리자가 활성화되지 않았습니다.{RESET}")
        
        if promotion_activated:
            print(f"\n{GREEN}[+] 테스트 단계 2 성공: 새 관리자 {promoted_node}가 승격되었습니다.{RESET}")
        else:
            print(f"\n{RED}[-] 테스트 단계 2 실패: 새 관리자가 승격되지 않았습니다.{RESET}")
            
        if backup_deactivated:
            print(f"\n{GREEN}[+] 테스트 단계 3 성공: 백업 관리자가 STANDBY 상태로 되돌아갔습니다.{RESET}")
        else:
            print(f"\n{RED}[-] 테스트 단계 3 실패: 백업 관리자가 STANDBY 상태로 되돌아가지 않았습니다.{RESET}")
            
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
                        backup_client = self.etcd_clients.get(backup_cluster_id)
                        if not backup_client:
                            backup_client = EtcdClient(cluster_id=backup_cluster_id)
                            
                        backup_results = backup_client.get_prefix(f"{BACKUP_INFO_PREFIX}/{self.cluster_id}/managers/")
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
    
    def is_backup_active(self, cluster_id):
        """백업 관리자가 활성화 되었는지 확인"""
        backup_cluster_id = CLUSTER_BACKUP_MAP.get(cluster_id)
        if not backup_cluster_id:
            return False
            
        try:
            backup_client = self.etcd_clients.get(backup_cluster_id)
            if not backup_client:
                backup_client = EtcdClient(cluster_id=backup_cluster_id)
                
            backup_results = backup_client.get_prefix(f"{BACKUP_INFO_PREFIX}/{cluster_id}/managers/")
            for key, backup_data in backup_results:
                if backup_data.get("status") == "ACTIVE":
                    return True
        except Exception:
            pass
            
        # 임시 관리자도 확인
        try:
            temp_manager_results = self.etcd_client.get_prefix(f"{MANAGER_INFO_PREFIX}/{cluster_id}/temp_")
            for key, manager_data in temp_manager_results:
                if manager_data.get("status") == "ACTIVE":
                    return True
        except Exception:
            pass
            
        return False
        
    def is_node_promoted(self, cluster_id):
        """새 관리자가 승격되었는지 확인"""
        try:
            client = self.etcd_clients.get(cluster_id, self.etcd_client)
            promotion_key = f"{CLUSTER_INFO_PREFIX}/{cluster_id}/promoted_manager"
            promotion_data = client.get(promotion_key)
            
            if promotion_data and promotion_data.get("status") == "PROMOTED":
                return True

            # 또는 클러스터 정보에서 새 관리자 확인
            cluster_key = f"{CLUSTER_INFO_PREFIX}/{cluster_id}/info"
            cluster_info = client.get(cluster_key)
            if cluster_info and "manager_id" in cluster_info:
                manager_id = cluster_info["manager_id"]
                
                # 관리자 노드가 원래 관리자가 아닌지 확인
                if manager_id != self.node_id:
                    # 노드 상태 확인
                    node_key = f"{NODE_INFO_PREFIX}/{cluster_id}/{manager_id}"
                    node_data = client.get(node_key)
                    if node_data and node_data.get("status") == "ACTIVE" and node_data.get("type") == "MANAGER":
                        return True
        except Exception:
            pass
            
        return False
        
    def are_backups_standby(self, cluster_id):
        """모든 백업 관리자가 STANDBY 상태인지 확인"""
        backup_cluster_id = CLUSTER_BACKUP_MAP.get(cluster_id)
        if not backup_cluster_id:
            return False
            
        try:
            backup_client = self.etcd_clients.get(backup_cluster_id)
            if not backup_client:
                backup_client = EtcdClient(cluster_id=backup_cluster_id)
                
            backup_results = backup_client.get_prefix(f"{BACKUP_INFO_PREFIX}/{cluster_id}/managers/")
            
            any_active = False
            for key, backup_data in backup_results:
                if backup_data.get("status") != "STANDBY":
                    any_active = True
                    break
                    
            return not any_active
        except Exception:
            return False
    
    def get_promoted_node(self, cluster_id):
        """승격된 노드 ID 조회"""
        try:
            client = self.etcd_clients.get(cluster_id, self.etcd_client)
            
            # 승격 정보 확인
            promotion_key = f"{CLUSTER_INFO_PREFIX}/{cluster_id}/promoted_manager"
            promotion_data = client.get(promotion_key)
            if promotion_data and promotion_data.get("status") == "PROMOTED":
                return promotion_data.get("node_id")
                
            # 클러스터 정보 확인
            cluster_key = f"{CLUSTER_INFO_PREFIX}/{cluster_id}/info"
            cluster_info = client.get(cluster_key)
            if cluster_info and "manager_id" in cluster_info:
                manager_id = cluster_info["manager_id"]
                if manager_id != self.node_id:  # 원래 관리자가 아닌 경우
                    return manager_id
        except Exception:
            pass
            
        return None
    
    def trigger_election(self):
        """관리자 선출 트리거"""
        try:
            # 선출 요청 이벤트 발행
            event_key = f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/events/election_{int(time.time())}"
            event_data = {
                "event_type": "MANAGER_ELECTION_NEEDED",
                "timestamp": time.time()
            }
            self.etcd_client.put(event_key, event_data)
            print(f"{GREEN}관리자 선출 요청 이벤트 발행: {event_key}{RESET}")
            return True
        except Exception as e:
            logger.error(f"선출 요청 이벤트 발행 실패: {e}")
            return False
    
    def test_auto_recovery(self):
        """자동 복구 기능 테스트"""
        print(f"=== 클러스터 {self.cluster_id} 관리자 노드 자동 복구 테스트 시작 ===\n")
        
        # 초기 상태 확인
        print("[1] 초기 상태 확인")
        self.check_etcd_cluster_status()
        print()
        self.check_all_nodes()
        print()
        self.check_manager_status()
        
        # 관리자 노드 찾기
        cluster_key = f"{CLUSTER_INFO_PREFIX}/{self.cluster_id}/info"
        cluster_info = self.etcd_client.get(cluster_key)
        
        manager_id = None
        if cluster_info and "manager_id" in cluster_info:
            manager_id = cluster_info["manager_id"]
        else:
            manager_results = self.etcd_client.get_prefix(f"{MANAGER_INFO_PREFIX}/{self.cluster_id}/")
            managers = []
            
            for key, manager_data in manager_results:
                if "events/" not in key and "temp_" not in key:
                    managers.append(manager_data)
            
            if managers:
                latest_manager = max(managers, key=lambda x: x.get("last_updated", 0))
                manager_id = latest_manager["node_id"]
        
        if not manager_id:
            print(f"{RED}클러스터 {self.cluster_id}의 관리자 노드를 찾을 수 없습니다.{RESET}")
            return
            
        if manager_id != self.node_id:
            print(f"{YELLOW}경고: 현재 관리자({manager_id})가 테스트 노드({self.node_id})와 다릅니다.{RESET}")
            response = input("계속 진행하시겠습니까? (y/n): ")
            if response.lower() != 'y':
                print(f"{RED}테스트를 중단합니다.{RESET}")
                return
        
        # 1. 관리자 노드 중지
        print(f"\n[2] 관리자 노드 {manager_id} 중지")
        if not self.stop_node(manager_id):
            print(f"{RED}테스트를 중단합니다.{RESET}")
            return
        
        # 선출 트리거
        print(f"\n[추가] 선출 트리거 수동 발행")
        self.trigger_election()
        
        # 2. 복구 과정 모니터링
        print(f"\n[3] 자동 복구 프로세스 모니터링")
        
        # 백업 활성화 대기
        backup_activated = self.wait_for_event(
            "백업 관리자 활성화",
            lambda: self.is_backup_active(self.cluster_id),
            max_wait_time=60,
            check_interval=3.0
        )
        if backup_activated:
            self.check_manager_status()
        
        # 새 관리자 승격 대기
        node_promoted = self.wait_for_event(
            "새 관리자 승격",
            lambda: self.is_node_promoted(self.cluster_id),
            max_wait_time=120,
            check_interval=5.0
        )
        
        promoted_node = None
        if node_promoted:
            promoted_node = self.get_promoted_node(self.cluster_id)
            print(f"{GREEN}승격된 관리자 노드: {promoted_node}{RESET}")
            self.check_manager_status()
        
        # 백업 비활성화 대기
        backups_standby = False
        if node_promoted:
            backups_standby = self.wait_for_event(
                "백업 관리자 비활성화",
                lambda: self.are_backups_standby(self.cluster_id),
                max_wait_time=60,
                check_interval=5.0
            )
            if backups_standby:
                self.check_manager_status()
        
        # 테스트 결과 출력
        print("\n[4] 최종 상태:")
        self.check_etcd_cluster_status()
        print()
        self.check_all_nodes()
        print()
        self.check_manager_status()
        
        # 결과 요약
        print("\n[5] 테스트 결과 요약:")
        if backup_activated:
            print(f"{GREEN}✓ 백업 관리자 활성화: 성공{RESET}")
        else:
            print(f"{RED}✗ 백업 관리자 활성화: 실패{RESET}")
            
        if node_promoted:
            print(f"{GREEN}✓ 새 관리자 승격: 성공 - {promoted_node}{RESET}")
        else:
            print(f"{RED}✗ 새 관리자 승격: 실패{RESET}")
            
        if backups_standby:
            print(f"{GREEN}✓ 백업 관리자 비활성화: 성공{RESET}")
        else:
            print(f"{RED}✗ 백업 관리자 비활성화: 실패{RESET}")
        
        if backup_activated and node_promoted and backups_standby:
            print(f"\n{GREEN}[+] 테스트 성공: 전체 자동 복구 프로세스가 성공적으로 완료되었습니다!{RESET}")
        else:
            print(f"\n{RED}[-] 테스트 부분 실패: 일부 복구 단계가 완료되지 않았습니다.{RESET}")
        
        # 원본 관리자 노드 재시작 옵션
        response = input(f"\n[6] 원래 관리자 노드 다시 시작\n"
                         f"원래 관리자 노드 {manager_id}를 다시 시작하시겠습니까? (y/n): ")
        if response.lower() == 'y':
            self.start_node(manager_id)
            print(f"\n원래 관리자 노드 복구 후 상태:")
            time.sleep(5)
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
    parser.add_argument('--test-type', choices=['manager', 'etcd', 'auto-recovery'], default='manager',
                        help='테스트 유형: manager(관리자 노드 장애), etcd(ETCD 노드 장애), auto-recovery(자동 복구)')
    
    args = parser.parse_args()
    
    etcd_endpoints = parse_etcd_endpoints(args.etcd_endpoints)
    
    print(f"P2P 관리 노드 고가용성 테스트 시작 - 클러스터 {args.cluster_id}, 노드 {args.node_id}")
    tester = HATest(args.cluster_id, args.node_id, etcd_endpoints)
    tester.set_debug(args.debug)
    
    if args.test_type == 'manager':
        tester.test_manager_node_failure()
    elif args.test_type == 'etcd':
        tester.test_etcd_node_failure()
    elif args.test_type == 'auto-recovery':
        tester.test_auto_recovery()
    
if __name__ == "__main__":
    main()