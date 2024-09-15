import socket  # noqa: F401


def main():
    print("Logs from your program will appear here!")
    data = 7
    data = data.to_bytes(4, byteorder='big')

    server = socket.create_server(("localhost", 9092), reuse_port=True)
    server.accept() # wait for client



if __name__ == "__main__":
    main()
