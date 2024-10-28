# -*- coding: utf-8 -*-
import redis

def get_connected_clients(redis_host, redis_port,redis_password):
    r = redis.Redis(host=redis_host, port=redis_port,password=redis_password, decode_responses=True)
    clients = r.client_list()
    unique_ips = set()
    print(clients)
    for client in clients:
        ip = client['addr'].replace('[::1]','127.0.0.1').split(':')[0]
        unique_ips.add(ip)

    return unique_ips

if __name__ == "__main__":
    redis_host = '10.21.90.130'
    redis_port = 6379
    redis_password = 'nint'
    unique_ips = get_connected_clients(redis_host, redis_port,redis_password)
    print(f"Number of unique connected clients: {len(unique_ips)}")
    print(unique_ips)
    for ip in unique_ips:
        print(ip)
