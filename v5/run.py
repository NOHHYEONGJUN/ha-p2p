import argparse
import logging
import sys
import os
import time
import signal
import threading
from typing import List

from config import (
    ETCD_CLUSTER_ENDPOINTS,
    CLUSTER_BACKUP_MAP
)
from p2p_node import ManagementNode, ParticipantNode

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger('p2p_runner')

def signal_handler(sig, frame):
    logger.info(f"Received signal {sig}")
    logger.info("Shutting down node...")
    if 'node' in globals():
        global node
        node.stop()
    logger.info("Node shutdown complete")
    logger.info("Node runner shutdown complete")
    sys.exit(0)

def get_backup_etcd_endpoints(cluster_id: str) -> List[str]:
    backup_cluster_id = CLUSTER_BACKUP_MAP.get(cluster_id)
    if backup_cluster_id:
        return ETCD_CLUSTER_ENDPOINTS.get(backup_cluster_id, [])
    return []

def run_management_node(node_id: str, cluster_id: str, is_primary: bool=False, 
                       backup_for: str=None, etcd_endpoints: List[str]=None,
                       backup_etcd_endpoints: List[str]=None):
    global node
    
    if not backup_etcd_endpoints:
        backup_etcd_endpoints = get_backup_etcd_endpoints(cluster_id)
    
    node = ManagementNode(
        node_id=node_id,
        cluster_id=cluster_id,
        is_primary=is_primary,
        backup_for=backup_for,
        etcd_endpoints=etcd_endpoints,
        backup_etcd_endpoints=backup_etcd_endpoints
    )
    
    node.start()
    logger.info(f"Node {node_id} is running...")
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            break
    
    node.stop()
    logger.info(f"Node {node_id} stopped")

def run_participant_node(node_id: str, cluster_id: str, etcd_endpoints: List[str]=None):
    global node
    
    backup_etcd_endpoints = get_backup_etcd_endpoints(cluster_id)
    
    node = ParticipantNode(
        node_id=node_id,
        cluster_id=cluster_id,
        etcd_endpoints=etcd_endpoints,
        backup_etcd_endpoints=backup_etcd_endpoints
    )
    
    node.start()
    logger.info(f"Node {node_id} is running...")
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            break
    
    node.stop()
    logger.info(f"Node {node_id} stopped")

def parse_etcd_endpoints(endpoints_str: str) -> List[str]:
    if not endpoints_str:
        return None
    return endpoints_str.split(',')

def main():
    parser = argparse.ArgumentParser(description='P2P 노드 실행')
    parser.add_argument('node_id', type=str, help='노드 ID')
    parser.add_argument('--cluster', type=str, required=True, help='클러스터 ID')
    parser.add_argument('--type', choices=['manager', 'participant'], required=True, help='노드 유형')
    parser.add_argument('--primary', action='store_true', help='Primary 관리자 여부')
    parser.add_argument('--backup-for', type=str, help='백업 관리자 역할을 할 클러스터 ID')
    parser.add_argument('--etcd-endpoints', type=str, help='ETCD 엔드포인트 목록 (콤마로 구분)')
    parser.add_argument('--backup-etcd-endpoints', type=str, help='백업 ETCD 엔드포인트 목록 (콤마로 구분)')
    
    args = parser.parse_args()
    
    etcd_endpoints = parse_etcd_endpoints(args.etcd_endpoints)
    backup_etcd_endpoints = parse_etcd_endpoints(args.backup_etcd_endpoints)
    
    if args.type == 'manager':
        run_management_node(
            args.node_id, 
            args.cluster, 
            args.primary, 
            args.backup_for, 
            etcd_endpoints,
            backup_etcd_endpoints
        )
    else:
        run_participant_node(
            args.node_id, 
            args.cluster, 
            etcd_endpoints
        )

if __name__ == "__main__":
    main()