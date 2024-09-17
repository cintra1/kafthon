import socket  # noqa: F401

def validate_version(api_version):
    if api_version != {0, 1, 2, 3, 4}:
        return false

def create_message(data, error_code = None):
    message = data.to_bytes(4, byteorder='big')
    if error_code is not None:
        message += error_code.to_bytes(2, byteorder='big')

    return len(message).to_bytes(4, byteorder='big') + message

def handle_client(conn):
    with conn:
        print("Handling client")
        
        req = conn.recv(1024)
        correlation_id = int.from_bytes(req[8:12], byteorder='big')
        api_version = int.from_bytes(req[5:7], byteorder='big')
        print("API Version: ", api_version)
        conn.sendall(create_message(correlation_id, 35))
 

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
