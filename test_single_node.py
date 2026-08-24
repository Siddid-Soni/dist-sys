#!/usr/bin/env python3

import json
import subprocess

def test_single_node():
    print("Testing with single node...")
    
    counter_proc = subprocess.Popen(
        ['cargo', 'run', '--bin', 'counter'],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1
    )
    
    try:
        # Send init with only one node
        init_msg = {
            "src": "c1",
            "dest": "n1", 
            "body": {
                "type": "init",
                "node_id": "n1",
                "node_ids": ["n1"],  # Only one node
                "msg_id": 1
            }
        }
        print(f"SEND: {json.dumps(init_msg)}")
        counter_proc.stdin.write(json.dumps(init_msg) + '\n')
        counter_proc.stdin.flush()
        
        # Process init
        response = counter_proc.stdout.readline().strip()
        print(f"RECV: {response}")
        
        kv_write = counter_proc.stdout.readline().strip()
        print(f"RECV: {kv_write}")
        
        write_ok = {"src": "seq-kv", "dest": "n1", "body": {"type": "write_ok", "in_reply_to": 0}}
        print(f"SEND: {json.dumps(write_ok)}")
        counter_proc.stdin.write(json.dumps(write_ok) + '\n')
        counter_proc.stdin.flush()
        
        # Send read request
        read_msg = {"src": "c1", "dest": "n1", "body": {"type": "read", "msg_id": 100}}
        print(f"SEND: {json.dumps(read_msg)}")
        counter_proc.stdin.write(json.dumps(read_msg) + '\n')
        counter_proc.stdin.flush()
        
        # Handle the single KV read
        kv_read = counter_proc.stdout.readline().strip()
        print(f"RECV: {kv_read}")
        
        kv_read_msg = json.loads(kv_read)
        kv_msg_id = kv_read_msg['body']['msg_id']
        
        read_ok = {"src": "seq-kv", "dest": "n1", "body": {"type": "read_ok", "value": 42, "in_reply_to": kv_msg_id}}
        print(f"SEND: {json.dumps(read_ok)}")
        counter_proc.stdin.write(json.dumps(read_ok) + '\n')
        counter_proc.stdin.flush()
        
        # Check if we get final response
        import select
        import sys
        
        print("Waiting for final response (5 seconds)...")
        ready, _, _ = select.select([counter_proc.stdout], [], [], 5)
        
        if ready:
            final_response = counter_proc.stdout.readline().strip()
            print(f"RECV: {final_response}")
            
            final_msg = json.loads(final_response)
            if final_msg['body']['type'] == 'read_ok' and final_msg['body']['value'] == 42:
                print("✅ SUCCESS: Single node read works!")
            else:
                print(f"❌ FAILURE: Wrong response: {final_response}")
        else:
            print("❌ TIMEOUT: No response received - still deadlocked")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        counter_proc.terminate()

if __name__ == "__main__":
    test_single_node()
