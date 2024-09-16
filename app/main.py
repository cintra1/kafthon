import socket  # noqa: F401

def create_message(data):
    return data.to_bytes(4, byteorder='big')

def handle_client(conn):
    with conn:
        print("Handling client")
        data = 7 # conn.recv(1024)
        conn.sendall(create_message(data))


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
