#!/usr/bin/env python3

import json
import subprocess
import threading
import time
from queue import Queue, Empty

def test_simple():
    print("Complete read test with value verification...")
    
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
                    "node_ids": ["n1", "n2", "n3"],  # Test with multiple nodes
                    "msg_id": 1
                }
            }
            print(f"SEND: {json.dumps(init_msg)}")
            counter_proc.stdin.write(json.dumps(init_msg) + '\n')
            counter_proc.stdin.flush()
            
            # Read init response
            response = counter_proc.stdout.readline().strip()
            print(f"RECV: {response}")
            
            # Read the kv write request
            kv_write = counter_proc.stdout.readline().strip()
            print(f"RECV: {kv_write}")
            
            # Send write_ok response
            write_ok = {
                "src": "seq-kv",
                "dest": "n1",
                "body": {
                    "type": "write_ok",
                    "in_reply_to": 0
                }
            }
            print(f"SEND: {json.dumps(write_ok)}")
            counter_proc.stdin.write(json.dumps(write_ok) + '\n')
            counter_proc.stdin.flush()
            
            # Now send read request
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
            
            # Expected values for each node
            expected_values = {"n1": 10, "n2": 20, "n3": 30}
            expected_total = sum(expected_values.values())  # 60
            
            # Read and respond to each KV read request
            for i in range(3):  # We expect 3 read requests (for n1, n2, n3)
                kv_read = counter_proc.stdout.readline().strip()
                print(f"RECV: {kv_read}")
                
                # Parse the KV read message to get the key and msg_id
                kv_read_msg = json.loads(kv_read)
                kv_msg_id = kv_read_msg['body']['msg_id']
                key = kv_read_msg['body']['key']
                
                # Send read_ok response with appropriate value
                value = expected_values.get(key, 0)
                read_ok = {
                    "src": "seq-kv",
                    "dest": "n1",
                    "body": {
                        "type": "read_ok",
                        "value": value,
                        "in_reply_to": kv_msg_id
                    }
                }
                print(f"SEND: {json.dumps(read_ok)} (value for {key})")
                counter_proc.stdin.write(json.dumps(read_ok) + '\n')
                counter_proc.stdin.flush()
            
            # Read the final response to client (with timeout)
            print("Waiting for final response...")
            counter_proc.stdout.settimeout(5)  # 5 second timeout
            try:
                final_response = counter_proc.stdout.readline().strip()
                print(f"RECV: {final_response}")
                
                # Verify the response
                final_msg = json.loads(final_response)
                if (final_msg.get('src') == 'n1' and 
                    final_msg.get('dest') == 'c1' and
                    final_msg['body']['type'] == 'read_ok' and
                    final_msg['body']['in_reply_to'] == 100):
                    
                    actual_value = final_msg['body']['value']
                    if actual_value == expected_total:
                        print(f"✅ SUCCESS: Got correct total value {actual_value} (expected {expected_total})")
                    else:
                        print(f"❌ FAILURE: Got wrong total value {actual_value} (expected {expected_total})")
                else:
                    print(f"❌ FAILURE: Invalid response format: {final_response}")
            except Exception as e:
                print(f"❌ TIMEOUT or ERROR waiting for final response: {e}")
                print("This suggests the counter is still deadlocked")
            
        except Exception as e:
            print(f"Error: {e}")
        finally:
            counter_proc.terminate()
    
    send_and_read()

if __name__ == "__main__":
    test_simple()
