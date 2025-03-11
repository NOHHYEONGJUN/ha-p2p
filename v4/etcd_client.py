import etcd3
import json
import time
import logging
import random
import threading
from typing import List, Dict, Any, Tuple, Optional, Union

from config import (
    ETCD_CLUSTER_ENDPOINTS,
    CONNECTION_RETRY_INTERVAL,
    MAX_CONNECTION_RETRIES
)

logger = logging.getLogger('etcd_client')

class EtcdClient:
    """ETCD 클라이언트 래퍼 클래스 - 클러스터별 RAFT 지원"""
    
    def __init__(self, cluster_id: str, endpoints: Optional[List[str]] = None, 
                 backup_endpoints: Optional[List[str]] = None):
        """
        특정 클러스터의 ETCD 클라이언트 초기화
        
        Args:
            cluster_id: 클러스터 ID
            endpoints: ETCD 엔드포인트 목록 (없으면 config.py의 값 사용)
            backup_endpoints: 백업 ETCD 엔드포인트 목록 (없으면 None)
        """
        self.cluster_id = cluster_id
        self.endpoints = endpoints or ETCD_CLUSTER_ENDPOINTS.get(cluster_id, [])
        self.backup_endpoints = backup_endpoints
        self.primary_client = None
        self.backup_client = None
        self.all_clients = []
        self.client_lock = threading.RLock()
        self.using_backup = False
        
        # 연결 실패 시 재시도를 위한 설정
        self.max_retries = MAX_CONNECTION_RETRIES
        self.retry_interval = CONNECTION_RETRY_INTERVAL
        self.last_endpoint_refresh = 0
        self.endpoint_refresh_interval = 30  # 30초마다 엔드포인트 갱신
        
        # ETCD 연결 초기화
        self._init_connections()
    
    def _init_connections(self) -> None:
        """모든 ETCD 엔드포인트에 연결"""
        with self.client_lock:
            self.all_clients = []
            connection_errors = []
            
            # 메인 엔드포인트 연결 시도
            for endpoint in self.endpoints:
                try:
                    if ':' in endpoint:
                        host, port = endpoint.split(':')
                        port = int(port)
                    else:
                        host, port = endpoint, 2379
                    
                    client = etcd3.client(host=host, port=port)
                    # 간단한 연결 테스트
                    try:
                        client.status()
                        self.all_clients.append(client)
                        logger.info(f"ETCD 연결 성공: {endpoint}")
                    except Exception as e:
                        connection_errors.append(f"{endpoint} - {str(e)}")
                        logger.warning(f"ETCD 연결 실패: {endpoint} - {e}")
                except Exception as e:
                    connection_errors.append(f"{endpoint} - {str(e)}")
                    logger.warning(f"ETCD 연결 실패: {endpoint} - {e}")
            
            # 메인 클러스터 연결 실패 시 백업 시도
            if not self.all_clients and self.backup_endpoints:
                logger.warning(f"클러스터 {self.cluster_id}의 모든 ETCD 연결 실패, 백업 클러스터 연결 시도")
                for endpoint in self.backup_endpoints:
                    try:
                        if ':' in endpoint:
                            host, port = endpoint.split(':')
                            port = int(port)
                        else:
                            host, port = endpoint, 2379
                        
                        client = etcd3.client(host=host, port=port)
                        # 간단한 연결 테스트
                        try:
                            client.status()
                            self.all_clients.append(client)
                            logger.info(f"백업 ETCD 연결 성공: {endpoint}")
                            self.using_backup = True
                        except Exception as e:
                            connection_errors.append(f"백업 {endpoint} - {str(e)}")
                            logger.warning(f"백업 ETCD 연결 실패: {endpoint} - {e}")
                    except Exception as e:
                        connection_errors.append(f"백업 {endpoint} - {str(e)}")
                        logger.warning(f"백업 ETCD 연결 실패: {endpoint} - {e}")
            
            # 연결된 클라이언트가 없으면 예외 발생
            if not self.all_clients:
                error_message = f"클러스터 {self.cluster_id}의 ETCD 클러스터에 연결할 수 없습니다."
                if connection_errors:
                    error_message += "\n오류: " + "; ".join(connection_errors)
                logger.error(error_message)
                raise ConnectionError(error_message)
            
            # 기본 클라이언트 설정 (첫 번째 연결 성공한 클라이언트)
            self.primary_client = self.all_clients[0]
            self.last_endpoint_refresh = time.time()
    
    def _refresh_connections_if_needed(self) -> None:
        """필요한 경우 ETCD 연결 갱신"""
        current_time = time.time()
        if current_time - self.last_endpoint_refresh > self.endpoint_refresh_interval:
            try:
                self._init_connections()
            except Exception as e:
                logger.error(f"ETCD 연결 갱신 실패: {e}")
    
    def _execute_with_retry(self, operation: str, *args, **kwargs) -> Any:
        """재시도 로직으로 ETCD 작업 실행"""
        self._refresh_connections_if_needed()
        
        last_error = None
        for retry in range(self.max_retries):
            for client in self.all_clients:
                try:
                    with self.client_lock:
                        self.primary_client = client
                        result = getattr(client, operation)(*args, **kwargs)
                        return result
                except Exception as e:
                    last_error = e
                    logger.debug(f"ETCD 작업 실패 (재시도 {retry+1}/{self.max_retries}): {e}")
            
            # 모든 클라이언트에서 실패한 경우 잠시 대기 후 재시도
            time.sleep(self.retry_interval)
        
        # 모든 재시도 실패
        logger.error(f"ETCD 작업 최종 실패: {last_error}")
        raise last_error if last_error else RuntimeError("알 수 없는 ETCD 오류")
    
    def put(self, key: str, value: Any, lease=None) -> bool:
        """키-값 저장"""
        try:
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            
            self._execute_with_retry('put', key, value, lease=lease)
            return True
        except Exception as e:
            logger.error(f"Error putting key {key}: {e}")
            return False
    
    def get(self, key: str) -> Optional[Any]:
        """키로 값 조회"""
        try:
            result = self._execute_with_retry('get', key)
            if result and result[0]:
                return json.loads(result[0].decode('utf-8'))
            return None
        except Exception as e:
            logger.error(f"Error getting key {key}: {e}")
            return None
    
    def get_prefix(self, prefix: str) -> List[Tuple[str, Any]]:
        """prefix로 시작하는 모든 키-값 조회"""
        try:
            results = []
            for result in self._execute_with_retry('get_prefix', prefix):
                if result and result[0]:
                    value = json.loads(result[0].decode('utf-8'))
                    key = result[1].key.decode('utf-8')
                    results.append((key, value))
            return results
        except Exception as e:
            logger.error(f"Error getting prefix {prefix}: {e}")
            return []
    
    def delete(self, key: str) -> bool:
        """키 삭제"""
        try:
            self._execute_with_retry('delete', key)
            return True
        except Exception as e:
            logger.error(f"Error deleting key {key}: {e}")
            return False
    
    def create_lease(self, ttl: int) -> Optional[Any]:
        """임대(lease) 생성"""
        try:
            return self._execute_with_retry('lease', ttl=ttl)
        except Exception as e:
            logger.error(f"Error creating lease with TTL {ttl}: {e}")
            return None
    
    def watch(self, key: str, callback) -> Optional[List[Tuple]]:
        """키 변경 감시"""
        try:
            # 모든 클라이언트에서 실행 (리더가 변경될 경우 대비)
            watch_ids = []
            for client in self.all_clients:
                try:
                    watch_id = client.add_watch_callback(key, callback)
                    watch_ids.append((client, watch_id))
                except Exception as e:
                    logger.debug(f"Unable to add watch on client: {e}")
            
            if not watch_ids:
                logger.error(f"Failed to set watch on any ETCD client for key {key}")
                return None
                
            # 여러 watch ID 반환 (취소 시 모두 처리)
            return watch_ids
        except Exception as e:
            logger.error(f"Error watching key {key}: {e}")
            return None
    
    def watch_prefix(self, prefix: str, callback) -> Optional[List[Tuple]]:
        """prefix로 시작하는 모든 키 변경 감시"""
        try:
            # 모든 클라이언트에서 실행
            watch_ids = []
            for client in self.all_clients:
                try:
                    watch_id = client.add_watch_prefix_callback(prefix, callback)
                    watch_ids.append((client, watch_id))
                except Exception as e:
                    logger.debug(f"Unable to add watch prefix on client: {e}")
            
            if not watch_ids:
                logger.error(f"Failed to set watch prefix on any ETCD client for prefix {prefix}")
                return None
                
            # 여러 watch ID 반환 (취소 시 모두 처리)
            return watch_ids
        except Exception as e:
            logger.error(f"Error watching prefix {prefix}: {e}")
            return None
    
    def cancel_watch(self, watch_ids) -> bool:
        """감시 취소"""
        try:
            success = True
            for client, watch_id in watch_ids:
                try:
                    client.cancel_watch(watch_id)
                except Exception as e:
                    logger.error(f"Error canceling watch {watch_id}: {e}")
                    success = False
            return success
        except Exception as e:
            logger.error(f"Error in cancel_watch: {e}")
            return False
    
    def get_leader_id(self) -> Optional[int]:
        """현재 ETCD 클러스터 리더 ID 조회"""
        try:
            status = self._execute_with_retry('status')
            return status.leader
        except Exception as e:
            logger.error(f"Error getting leader ID: {e}")
            return None
    
    def get_cluster_members(self) -> List[Dict[str, Any]]:
        """ETCD 클러스터 멤버 정보 조회"""
        try:
            members = []
            response = self._execute_with_retry('members')
            for member in response:
                members.append({
                    'id': member.id,
                    'name': member.name,
                    'peer_urls': member.peer_urls,
                    'client_urls': member.client_urls
                })
            return members
        except Exception as e:
            logger.error(f"Error getting cluster members: {e}")
            return []
    
    def is_cluster_healthy(self) -> bool:
        """ETCD 클러스터 상태 확인"""
        try:
            # 각 엔드포인트 상태 확인
            healthy_nodes = 0
            total_nodes = len(self.all_clients)
            
            for client in self.all_clients:
                try:
                    client.status()
                    healthy_nodes += 1
                except:
                    pass
            
            # 과반수 노드가 정상이면 클러스터 정상으로 판단
            if total_nodes > 0:
                return healthy_nodes > total_nodes // 2
            return False
        except Exception as e:
            logger.error(f"Error checking cluster health: {e}")
            return False
    
    def is_using_backup(self) -> bool:
        """백업 ETCD 클러스터를 사용하고 있는지 여부 확인"""
        return self.using_backup
    
    def switch_to_primary(self) -> bool:
        """메인 ETCD 클러스터로 전환 시도"""
        if not self.using_backup:
            return True  # 이미 메인 클러스터 사용 중
            
        try:
            # 메인 클러스터 연결 테스트
            for endpoint in self.endpoints:
                try:
                    if ':' in endpoint:
                        host, port = endpoint.split(':')
                        port = int(port)
                    else:
                        host, port = endpoint, 2379
                    
                    client = etcd3.client(host=host, port=port)
                    client.status()  # 연결 테스트
                    
                    # 연결 성공, 메인 클러스터로 전환
                    logger.info(f"메인 ETCD 클러스터로 전환 성공: {endpoint}")
                    self._init_connections()  # 모든 연결 초기화
                    self.using_backup = False
                    return True
                except Exception as e:
                    logger.warning(f"메인 ETCD 연결 시도 실패: {endpoint} - {e}")
            
            # 모든 메인 엔드포인트 연결 실패
            logger.error("메인 ETCD 클러스터로 전환 실패, 백업 클러스터 유지")
            return False
        except Exception as e:
            logger.error(f"메인 ETCD 클러스터 전환 중 오류 발생: {e}")
            return False
