import sys
import time
import json
import subprocess
import logging
import argparse
import etcd3
import re

from config import ETCD_HOST, ETCD_PORT

# 색상 코드 - 단순화
GREEN = "\033[1;32m"    # 성공, 활성 상태
RED = "\033[1;31m"      # 오류, 비활성 상태
YELLOW = "\033[1;33m"   # 경고, 주의
RESET = "\033[0m"       # 색상 초기화

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger('test_ha')

class HATest:
    """P2P 관리 노드 고가용성 테스트 클래스"""
    
    def __init__(self, cluster_id, node_id):
        self.cluster_id = cluster_id
        self.node_id = node_id
        self.etcd_client = etcd3.client(host=ETCD_HOST, port=ETCD_PORT)
        self.debug_mode = False
        
        # 클러스터 간 백업 관계 (docker-compose.yaml 기반)
        self.backup_relationships = {
            "1": {"primary": "node_1_1", "secondary": "node_2_1"},
            "2": {"primary": "node_2_1", "secondary": "node_3_1"},
            "3": {"primary": "node_3_1", "secondary": "node_1_1"}
        }
        
        # 활성화된 노드 캐시 (성능 최적화)
        self.active_nodes_cache = {}
        self.active_nodes_cache_time = 0
        
    def set_debug(self, debug_mode):
        """디버그 모드 설정"""
        self.debug_mode = debug_mode
        
    def explore_etcd_keys(self, prefix="/"):
        """ETCD의 키 공간 탐색"""
        if not self.debug_mode:
            return []
            
        print("=== ETCD 키 공간 탐색 ===")
        keys = []
        try:
            for result in self.etcd_client.get_prefix(prefix):
                if result[1].key:
                    key = result[1].key.decode('utf-8')
                    keys.append(key)
            
            # 키 정렬 및 출력
            for key in sorted(keys):
                print(f"키: {key}")
                
            return keys
        except Exception as e:
            logger.error(f"Error exploring ETCD keys: {e}")
            return []
    
    def dump_etcd_value(self, key):
        """특정 ETCD 키의 값 출력"""
        if not self.debug_mode:
            return None
            
        try:
            result = self.etcd_client.get(key)
            if result[0]:
                value = json.loads(result[0].decode('utf-8'))
                print(f"키: {key}, 값: {json.dumps(value, indent=2)}")
                return value
            print(f"키 {key}에 해당하는 값이 없습니다.")
            return None
        except Exception as e:
            logger.error(f"Error dumping ETCD value for key {key}: {e}")
            return None
            
    def check_all_nodes(self):
        """모든 노드 상태 확인"""
        print("=== 모든 노드 상태 확인 ===")
        
        all_nodes = {}  # 노드 ID를 키로 사용하여 중복 방지
        try:
            # 모든 노드 정보 가져오기 (중복 제거)
            for result in self.etcd_client.get_prefix("/nodes/"):
                if result[0]:
                    node_data = json.loads(result[0].decode('utf-8'))
                    node_id = node_data.get("node_id")
                    if node_id:  # 유효한 노드 ID가 있는 경우만 처리
                        all_nodes[node_id] = node_data
            
            # 캐시 업데이트
            self.active_nodes_cache = {
                node_id: node["status"] == "ACTIVE" 
                for node_id, node in all_nodes.items()
            }
            self.active_nodes_cache_time = time.time()
                    
            # 노드 정보 출력
            for node_id, node in sorted(all_nodes.items(), key=lambda x: x[0]):
                status_color = GREEN if node["status"] == "ACTIVE" else RED
                status_icon = "🟢" if node["status"] == "ACTIVE" else "🔴"
                last_updated_diff = time.time() - node.get("last_updated", 0)
                
                print(f"노드: {node_id}, "
                      f"클러스터: {node['cluster_id']}, "
                      f"상태: {status_color}{status_icon} {node['status']}{RESET}, "
                      f"유형: {node['type']}, "
                      f"마지막 업데이트: {last_updated_diff:.1f}초 전")
                      
            # 클러스터 활성 노드 수 출력
            cluster_nodes = [n for n in all_nodes.values() if str(n["cluster_id"]) == str(self.cluster_id)]
            active_nodes = [n for n in cluster_nodes if n["status"] == "ACTIVE"]
            node_count_color = GREEN if len(active_nodes) >= 2 else RED
            print(f"클러스터 {self.cluster_id}의 활성 노드 수: "
                  f"{node_count_color}{len(active_nodes)}{RESET}")
            
        except Exception as e:
            logger.error(f"Error checking nodes: {e}")
            
    def check_manager_status(self):
        """관리자 노드 상태 확인"""
        print("=== 클러스터 관리자 현황 ===")
        
        # 디버그 모드에서만 ETCD 키 탐색
        if self.debug_mode:
            self.explore_etcd_keys("/managers/")
            
        current_time = time.time()
        max_inactive_time = 10.0  # 10초 이상 업데이트 없으면 비활성으로 간주 (임시 프라이머리 대응을 위해 증가)
        
        # 모든 클러스터의 PRIMARY 확인
        for cluster_id in range(1, 4):  # 클러스터 1, 2, 3 확인
            cluster_str = str(cluster_id)
            try:
                # PRIMARY 확인
                primary_key = f"/managers/{cluster_id}/primary"
                primary_result = self.etcd_client.get(primary_key)
                
                # 임시 프라이머리 확인 (백업 노드 정보를 통해)
                backup_node_id = self.get_backup_node_id(cluster_str)
                is_in_backup_takeover = False  # 백업 노드가 take over 했는지 여부
                
                if primary_result[0]:
                    primary_info = json.loads(primary_result[0].decode('utf-8'))
                    primary_node_id = primary_info.get("node_id", "unknown")
                    last_updated_diff = current_time - primary_info.get("timestamp", 0)
                    
                    # 노드 활성 상태 확인 (캐시 활용)
                    node_is_active = self.check_node_is_active(primary_node_id)
                    time_is_valid = last_updated_diff < max_inactive_time
                    
                    # 임시 프라이머리 처리 (백업 노드가 프라이머리 역할을 하는 경우)
                    is_temp_primary = primary_info.get("role") == "TEMPORARY_PRIMARY"
                    
                    # 백업 노드가 프라이머리 역할을 하는 경우 특별 처리
                    if primary_node_id == backup_node_id:
                        is_in_backup_takeover = True
                        # 백업 노드가 프라이머리일 경우 활성 상태 확인 기준 완화
                        if self.check_node_is_active(backup_node_id):
                            node_is_active = True
                            time_is_valid = True
                    
                    if node_is_active and (time_is_valid or is_temp_primary):
                        role_text = "임시 프라이머리" if is_temp_primary else "프라이머리"
                        role_color = YELLOW if is_temp_primary else GREEN
                        
                        print(f"클러스터 {cluster_id} 프라이머리: "
                              f"{primary_node_id}, "
                              f"상태: {GREEN}🟢 {role_color}{role_text}{RESET} "
                              f"({last_updated_diff:.1f}초 전)")
                    else:
                        reason = "비활성 노드" if not node_is_active else "응답 없음"
                        print(f"클러스터 {cluster_id} 프라이머리: "
                              f"{primary_node_id}, "
                              f"상태: {RED}🔴 {reason}{RESET} "
                              f"({last_updated_diff:.1f}초 전)")
                else:
                    print(f"클러스터 {cluster_id} 프라이머리: {RED}없음{RESET}")
                
                # 백업 노드 확인 (모든 클러스터의 백업 관계 출력)
                self.check_backup_nodes(cluster_str, is_in_backup_takeover)
                    
            except Exception as e:
                logger.error(f"Error checking manager status for cluster {cluster_id}: {e}")
    
    def get_backup_node_id(self, cluster_id):
        """클러스터의 백업 노드 ID 가져오기"""
        # 미리 정의된 백업 관계에서 가져오기
        if cluster_id in self.backup_relationships:
            return self.backup_relationships[cluster_id]["secondary"]
        
        # 또는 ETCD에서 직접 가져오기 (옵션)
        backup_key = f"/managers/cluster_{cluster_id}/backup/"
        for result in self.etcd_client.get_prefix(backup_key):
            if result[0]:
                value = json.loads(result[0].decode('utf-8'))
                if "node_id" in value:
                    return value["node_id"]
                
        # 마지막 수단으로 환형 구조 기반 추정 (1->2->3->1)
        return self.get_likely_backup_node_id(cluster_id)
    
    def check_backup_nodes(self, cluster_id, is_in_backup_takeover=False):
        """클러스터의 백업 노드 정보 조회"""
        try:
            backup_nodes = []
            
            # 1. ETCD에서 백업 노드 정보 검색
            backup_key = f"/managers/cluster_{cluster_id}/backup/"
            for result in self.etcd_client.get_prefix(backup_key):
                if result[0]:
                    key = result[1].key.decode('utf-8')
                    value = json.loads(result[0].decode('utf-8'))
                    backup_nodes.append((key, value))
            
            # 2. 백업 노드 정보가 있는 경우 출력
            if backup_nodes:
                print(f"클러스터 {cluster_id} 세컨 관리 노드:")
                for key, backup_info in backup_nodes:
                    if "node_id" in backup_info:
                        backup_node_id = backup_info["node_id"]
                        last_updated_diff = time.time() - backup_info.get("timestamp", 0)
                        
                        # 백업 노드가 프라이머리 테이크오버한 경우 상태 표시 조정
                        if is_in_backup_takeover and backup_node_id == self.get_backup_node_id(cluster_id):
                            print(f"  - {backup_node_id}: "
                                  f"{YELLOW}활성 (임시 프라이머리로 승격됨){RESET} "
                                  f"({last_updated_diff:.1f}초 전)")
                        else:
                            node_is_active = self.check_node_is_active(backup_node_id)
                            if node_is_active:
                                print(f"  - {backup_node_id}: "
                                      f"{GREEN}활성 (백업 관리자){RESET} "
                                      f"({last_updated_diff:.1f}초 전)")
                            else:
                                print(f"  - {backup_node_id}: "
                                      f"{RED}비활성 (백업 관리자){RESET} "
                                      f"({last_updated_diff:.1f}초 전)")
            # 3. 백업 노드 정보가 없는 경우 docker-compose.yaml 구성 기반으로 표시 
            else:
                # 환형 백업 구조 (1->2->3->1) 기반으로 세컨더리 노드 결정
                if cluster_id in self.backup_relationships:
                    backup_node_id = self.backup_relationships[cluster_id]["secondary"]
                    node_is_active = self.check_node_is_active(backup_node_id)
                    status_color = GREEN if node_is_active else RED
                    status_text = "활성" if node_is_active else "비활성"
                    
                    # 백업 노드가 ETCD에 명시적으로 등록되어 있지 않지만 환형 구조에서 예상됨
                    print(f"클러스터 {cluster_id} 세컨 관리 노드:")
                    if is_in_backup_takeover:
                        print(f"  - {backup_node_id}: "
                              f"{YELLOW}활성 (임시 프라이머리로 승격됨){RESET}")
                    else:
                        print(f"  - {backup_node_id}: "
                              f"{status_color}{status_text} (백업 관리자){RESET}")
                else:
                    print(f"클러스터 {cluster_id} 세컨 관리 노드: {RED}없음{RESET}")
        except Exception as e:
            logger.error(f"Error checking backup nodes for cluster {cluster_id}: {e}")
            print(f"클러스터 {cluster_id} 세컨 관리 노드: {RED}조회 실패{RESET}")
    
    def get_likely_backup_node_id(self, cluster_id):
        """설정 기반으로 가능한 백업 노드 ID 추정"""
        # 클러스터 간 관계: 1→2→3→1 (환형)
        if int(cluster_id) == 1:
            return "node_2_1"
        elif int(cluster_id) == 2:
            return "node_3_1"
        elif int(cluster_id) == 3:
            return "node_1_1"
        return None
    
    def check_node_is_active(self, node_id):
        """노드가 활성 상태인지 확인"""
        # 캐시된 정보 사용 (성능 최적화)
        cache_age = time.time() - self.active_nodes_cache_time
        if cache_age < 1.0 and node_id in self.active_nodes_cache:  # 1초 이내의 캐시는 재사용
            return self.active_nodes_cache[node_id]
            
        try:
            # 노드 정보 가져오기
            active_status = False
            
            # /nodes/ 경로에서 확인
            for result in self.etcd_client.get_prefix(f"/nodes/"):
                if result[0]:
                    node_data = json.loads(result[0].decode('utf-8'))
                    if node_data.get("node_id") == node_id:
                        if node_data.get("status") == "ACTIVE":
                            active_status = True
                            break
                        
            # 캐시 업데이트
            self.active_nodes_cache[node_id] = active_status
            return active_status
        except Exception as e:
            logger.error(f"Error checking node status: {e}")
            return False
                
    def stop_node(self, node_id):
        """노드 프로세스 중지 (node_id는 예: 'node_1_1')"""
        try:
            print(f"노드 {node_id} 프로세스 중지 중...")
            # Docker 컨테이너 필터 조정 (정규식 패턴 사용)
            # 다양한 컨테이너 이름 형식 지원 (p2p-v2, p2p-v3 등)
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
            
    def start_node(self, node_id):
        """노드 프로세스 시작 (node_id는 예: 'node_1_1')"""
        try:
            print(f"노드 {node_id} 프로세스 시작 중...")
            # Docker 컨테이너 필터 조정 (정규식 패턴 사용)
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
            
    def run_primary_failure_test(self):
        """Primary 노드 장애 복구 테스트"""
        print(f"=== 클러스터 {self.cluster_id} 장애 복구 테스트 시작 ===\n")
        
        # 1. 초기 상태 확인
        print("[1] 초기 상태 확인")
        self.check_all_nodes()
        print()
        self.check_manager_status()
        
        # Primary 확인
        primary_key = f"/managers/{self.cluster_id}/primary"
        primary_result = self.etcd_client.get(primary_key)
        
        if primary_result[0]:
            primary_info = json.loads(primary_result[0].decode('utf-8'))
            primary_node_id = primary_info["node_id"]
            
            # 테스트 대상 노드가 현재 Primary인지 확인
            if primary_node_id != self.node_id:
                print(f"{YELLOW}경고: 현재 프라이머리({primary_node_id})가 중지하려는 노드({self.node_id})와 다릅니다.{RESET}")
                response = input("계속 진행하시겠습니까? (y/n): ")
                if response.lower() != 'y':
                    print(f"{RED}테스트를 중단합니다.{RESET}")
                    return
        else:
            print(f"{YELLOW}경고: 클러스터 {self.cluster_id}에 프라이머리가 없습니다.{RESET}")
            response = input("계속 진행하시겠습니까? (y/n): ")
            if response.lower() != 'y':
                print(f"{RED}테스트를 중단합니다.{RESET}")
                return
        
        # 2. Primary 노드 중지 (node_id만 전달)
        print(f"\n[2] 프라이머리 노드 {self.node_id} 중지")
        if not self.stop_node(self.node_id):
            print(f"{RED}테스트를 중단합니다.{RESET}")
            return
            
        # 3. 상태 변화 모니터링
        print(f"\n[3] 클러스터 상태 변화 모니터링 (최대 120초)")
        start_time = time.time()
        max_monitoring_time = 120  # 최대 120초 모니터링
        
        new_primary_detected = False
        while time.time() - start_time < max_monitoring_time:
            try:
                # 모니터링 주기마다 활성 노드 캐시 초기화
                self.active_nodes_cache = {}  
                self.active_nodes_cache_time = 0
                
                # 상태 확인
                self.check_manager_status()
                elapsed_time = time.time() - start_time
                print(f"\n=== 경과 시간: {elapsed_time:.1f}초 ===\n")
                
                # 모든 노드 상태 확인
                self.check_all_nodes()
                
                # 새 Primary 감지 확인
                primary_result = self.etcd_client.get(primary_key)
                if primary_result[0]:
                    primary_info = json.loads(primary_result[0].decode('utf-8'))
                    new_primary_id = primary_info.get("node_id", "")
                    
                    # 새 Primary가 이전과 다르고 TEMPORARY_PRIMARY 역할을 함
                    if new_primary_id != self.node_id and primary_info.get("role") == "TEMPORARY_PRIMARY":
                        print(f"\n{GREEN}[*] 새로운 TEMPORARY_PRIMARY 감지: {new_primary_id}{RESET}")
                        new_primary_detected = True
                        time.sleep(5)
                        break
                
                time.sleep(1.0)  # 1초마다 상태 확인
            except KeyboardInterrupt:
                print(f"\n{YELLOW}테스트가 사용자에 의해 중단되었습니다.{RESET}")
                break
            except Exception as e:
                logger.error(f"모니터링 중 오류 발생: {e}")
                time.sleep(1.0)
                
        # 4. 결과 확인
        print(f"\n[4] 테스트 결과")
        self.check_all_nodes()
        print()
        self.check_manager_status()
        
        if new_primary_detected:
            print(f"\n{GREEN}[+] 테스트 성공: 새로운 TEMPORARY_PRIMARY가 성공적으로 선출되었습니다.{RESET}")
        else:
            print(f"\n{RED}[-] 테스트 실패: 새로운 TEMPORARY_PRIMARY가 감지되지 않았습니다.{RESET}")
            
        # 5. 원래 Primary 노드 복구 (선택적)
        response = input(f"\n[5] 원래 메인 관리 노드 다시 시작\n"
                         f"원래 메인 관리 노드 {self.node_id}를 다시 시작하시겠습니까? (y/n): ")
        if response.lower() == 'y':
            self.start_node(self.node_id)
            print(f"\n원래 메인 관리 노드 복구 후 상태:")
            time.sleep(5)
            self.check_all_nodes()
            print()
            self.check_manager_status()
            
def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='P2P 관리 노드 고가용성 테스트')
    parser.add_argument('cluster_id', type=int, help='테스트할 클러스터 ID')
    parser.add_argument('node_id', type=str, help='중지할 노드 ID (예: node_1_1)')
    parser.add_argument('--debug', action='store_true', help='디버그 정보 표시')
    
    args = parser.parse_args()
    
    print(f"P2P 관리 노드 고가용성 테스트 시작 - 클러스터 {args.cluster_id}, 노드 {args.node_id}")
    tester = HATest(args.cluster_id, args.node_id)
    tester.set_debug(args.debug)
    
    tester.run_primary_failure_test()
    
if __name__ == "__main__":
    main()
