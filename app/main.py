import socket  # noqa: F401


def main():
    print("Logs from your program will appear here!")

    server = socket.create_server(("localhost", 9092), reuse_port=True)
    server.accept() # wait for client
    server.send(b"Hello, World!")	# send data to client


if __name__ == "__main__":
    main()
