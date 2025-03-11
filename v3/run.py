import argparse
import logging
import sys
import os
import json
import etcd3
import time
import signal
import threading

from config import *
from p2p_node import ManagementNode, ParticipantNode

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger('p2p_runner')

def signal_handler(sig, frame):
    """시그널 핸들러 (종료 처리)"""
    logger.info(f"Received signal {sig}")
    logger.info("Shutting down node...")
    
    # 전역 노드 객체가 있으면 중지
    if 'node' in globals():
        global node
        node.stop()
        
    logger.info("Node shutdown complete")
    logger.info("Node runner shutdown complete")
    sys.exit(0)

def run_management_node(node_id, cluster_id, is_primary=False, backup_for=None, desired_secondary=None):
    """관리 노드 실행"""
    global node
    
    # ETCD 클라이언트 초기화
    etcd_client = etcd3.client(host=ETCD_HOST, port=ETCD_PORT)
    
    # 관리 노드 생성 및 초기화
    node = ManagementNode(node_id, cluster_id, is_primary, backup_for, etcd_client=etcd_client)
    node.initialize(node_id, desired_secondary, backup_for)
    
    # 노드 시작
    node.start()
    
    # 메인 스레드는 대기
    logger.info(f"Node {node_id} is running...")
    
    # 시그널 핸들러 등록 (SIGINT, SIGTERM)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # 무한 대기
    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            break
    
    # 종료 처리
    node.stop()
    logger.info(f"Node {node_id} stopped")

def run_participant_node(node_id, cluster_id):
    """참여 노드 실행"""
    global node
    
    # ETCD 클라이언트 초기화
    etcd_client = etcd3.client(host=ETCD_HOST, port=ETCD_PORT)
    
    # 참여 노드 생성
    node = ParticipantNode(node_id, cluster_id, etcd_client=etcd_client)
    
    # 노드 시작
    node.start()
    
    # 메인 스레드는 대기
    logger.info(f"Node {node_id} is running...")
    
    # 시그널 핸들러 등록 (SIGINT, SIGTERM)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # 무한 대기
    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            break
    
    # 종료 처리
    node.stop()
    logger.info(f"Node {node_id} stopped")

def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='P2P 노드 실행')
    parser.add_argument('node_id', type=str, help='노드 ID')
    parser.add_argument('--cluster', type=int, required=True, help='클러스터 ID')
    parser.add_argument('--type', choices=['manager', 'participant'], required=True, help='노드 유형')
    parser.add_argument('--primary', action='store_true', help='Primary 관리자 여부')
    parser.add_argument('--backup-for', type=int, help='백업 관리자 역할을 할 클러스터 ID')
    parser.add_argument('--secondary', type=str, help='Secondary 관리자 노드 ID')
    
    args = parser.parse_args()
    
    # 타입에 따라 적절한 노드 실행
    if args.type == 'manager':
        run_management_node(args.node_id, args.cluster, args.primary, args.backup_for, args.secondary)
    else:
        run_participant_node(args.node_id, args.cluster)

if __name__ == "__main__":
    main()
