#!/usr/bin/env python3

import json
import subprocess
import threading
import time
from queue import Queue, Empty

def test_multi_node():
    print("Multi-node read test...")
    
    # Start the counter process
    counter_proc = subprocess.Popen(
        ['cargo', 'run', '--bin', 'counter'],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1
    )
    
    def send_and_read():
        try:
            # Send init
            init_msg = {
                "src": "c1",
                "dest": "n1", 
                "body": {
                    "type": "init",
                    "node_id": "n1",
                    "node_ids": ["n1", "n2", "n3"],
                    "msg_id": 1
                }
            }
            print(f"SEND: {json.dumps(init_msg)}")
            counter_proc.stdin.write(json.dumps(init_msg) + '\n')
            counter_proc.stdin.flush()
            
            # Read responses and handle them
            kv_requests = []
            
            for _ in range(10):  # Read up to 10 messages
                try:
                    line = counter_proc.stdout.readline().strip()
                    if not line:
                        break
                    print(f"RECV: {line}")
                    
                    msg = json.loads(line)
                    
                    # Handle different message types
                    if msg.get('dest') == 'seq-kv':
                        kv_requests.append(msg)
                        
                        # Send appropriate responses
                        if msg['body']['type'] == 'write':
                            response = {
                                "src": "seq-kv",
                                "dest": "n1",
                                "body": {
                                    "type": "write_ok",
                                    "in_reply_to": msg['body']['msg_id']
                                }
                            }
                        elif msg['body']['type'] == 'read':
                            # Different values for different nodes
                            key = msg['body']['key']
                            value = {'n1': 10, 'n2': 20, 'n3': 30}.get(key, 0)
                            
                            response = {
                                "src": "seq-kv",
                                "dest": "n1",
                                "body": {
                                    "type": "read_ok",
                                    "value": value,
                                    "in_reply_to": msg['body']['msg_id']
                                }
                            }
                        
                        print(f"SEND: {json.dumps(response)}")
                        counter_proc.stdin.write(json.dumps(response) + '\n')
                        counter_proc.stdin.flush()
                    
                    # After init, send read request
                    elif msg['body'].get('type') == 'init_ok':
                        read_msg = {
                            "src": "c1",
                            "dest": "n1",
                            "body": {
                                "type": "read",
                                "msg_id": 100
                            }
                        }
                        print(f"SEND: {json.dumps(read_msg)}")
                        counter_proc.stdin.write(json.dumps(read_msg) + '\n')
                        counter_proc.stdin.flush()
                    
                    # Check for final read response
                    elif msg['body'].get('type') == 'read_ok':
                        print(f"✅ Final result: {msg['body']['value']} (should be 60 = 10+20+30)")
                        break
                        
                except json.JSONDecodeError:
                    print(f"Could not parse: {line}")
                except Exception as e:
                    print(f"Error: {e}")
                    break
                    
        except Exception as e:
            print(f"Error: {e}")
        finally:
            counter_proc.terminate()
    
    send_and_read()

if __name__ == "__main__":
    test_multi_node()
