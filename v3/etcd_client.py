import etcd3
import json
import time
import logging

from config import ETCD_HOST, ETCD_PORT

logger = logging.getLogger('etcd_client')

class EtcdClient:
    """ETCD 클라이언트 래퍼 클래스"""
    
    def __init__(self, host=ETCD_HOST, port=ETCD_PORT):
        self.client = etcd3.client(host=host, port=port)
        
    def put(self, key, value, lease=None):
        """키-값 저장"""
        try:
            if isinstance(value, dict) or isinstance(value, list):
                value = json.dumps(value)
                
            self.client.put(key, value, lease=lease)
            return True
        except Exception as e:
            logger.error(f"Error putting key {key}: {e}")
            return False
            
    def get(self, key):
        """키로 값 조회"""
        try:
            result = self.client.get(key)
            if result[0]:
                return json.loads(result[0].decode('utf-8'))
            return None
        except Exception as e:
            logger.error(f"Error getting key {key}: {e}")
            return None
            
    def get_prefix(self, prefix):
        """prefix로 시작하는 모든 키-값 조회"""
        try:
            results = []
            for result in self.client.get_prefix(prefix):
                if result[0]:
                    value = json.loads(result[0].decode('utf-8'))
                    key = result[1].key.decode('utf-8')
                    results.append((key, value))
            return results
        except Exception as e:
            logger.error(f"Error getting prefix {prefix}: {e}")
            return []
            
    def delete(self, key):
        """키 삭제"""
        try:
            self.client.delete(key)
            return True
        except Exception as e:
            logger.error(f"Error deleting key {key}: {e}")
            return False
            
    def create_lease(self, ttl):
        """임대(lease) 생성"""
        try:
            return self.client.lease(ttl=ttl)
        except Exception as e:
            logger.error(f"Error creating lease with TTL {ttl}: {e}")
            return None
            
    def watch(self, key, callback):
        """키 변경 감시"""
        try:
            watch_id = self.client.add_watch_callback(key, callback)
            return watch_id
        except Exception as e:
            logger.error(f"Error watching key {key}: {e}")
            return None
            
    def cancel_watch(self, watch_id):
        """감시 취소"""
        try:
            self.client.cancel_watch(watch_id)
            return True
        except Exception as e:
            logger.error(f"Error canceling watch {watch_id}: {e}")
            return False
