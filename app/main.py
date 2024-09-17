import socket  # noqa: F401

def create_message(correlation_id, api_key, error_code = 0):
     message = Message(
        request.correlation_id.to_bytes(4, byteorder="big"),
        int(35).to_bytes(2, byteorder="big"),
        # ApiVersion V3 Response | Ref.: https://kafka.apache.org/protocol.html#The_Messages_ApiVersions
        # error_code [api_keys] throttle_time_ms TAG_BUFFER
        # error_code INT16
        error_code.to_bytes(2, byteorder="big") +
        # num_api_keys => empirically VARINT of N + 1 for COMPACT_ARRAY
        # 255 / 11111111 => 126
        # 127 / 01111111 => 126
        #  63 / 00111111 =>  62
        int(1 + 1).to_bytes(1, byteorder="big") +
        # api_key INT16
        int(18).to_bytes(2, byteorder="big") +
        # min_version INT16
        int(4).to_bytes(2, byteorder="big") +
        # max_version INT16
        int(4).to_bytes(2, byteorder="big") +
        # TAG_BUFFER -> empirically INT16
        int(0).to_bytes(2, byteorder="big") +
        # throttle_time_ms INT32
        int(0).to_bytes(4, byteorder="big"),
    )

    return len(message).to_bytes(4, byteorder='big') + message

def handle_client(conn):
    with conn:
        print("Handling client")
        
        req = conn.recv(1024)
        correlation_id = int.from_bytes(req[8:12], byteorder='big')
        api_key = int.from_bytes(req[3:5], byteorder='big')
        api_version = int.from_bytes(req[5:7], byteorder='big')
        print("API Key:", api_key)
        print("API Version:", api_version)

        if 0 <= api_version <= 4:
            conn.sendall(create_message(correlation_id, api_key))
        else:
            conn.sendall(create_message(correlation_id, api_key, 35))
 

def main():
    print("Logs from your program will appear here!")
    data = 7
    data = data.to_bytes(4, byteorder='big')

    server = socket.create_server(("localhost", 9092), reuse_port=True)
    conn, addr = server.accept() # wait for client
    
    print('Connected by', addr)

    with conn:
        # Handle the incoming request
        handle_client(conn)
                

if __name__ == "__main__":
    main()
