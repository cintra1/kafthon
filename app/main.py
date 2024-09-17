import socket


def create_message(correlation_id, api_key, error_code=0):
    # Cria uma mensagem com base no correlation_id, api_key e error_code
    message = correlation_id.to_bytes(4, byteorder='big')
    message += error_code.to_bytes(2, byteorder='big')
    message += int(2).to_bytes(1, byteorder='big')  # Simplificado 1+1 para 2
    message += api_key.to_bytes(2, byteorder='big')
    message += int(4).to_bytes(2, byteorder='big')
    message += int(4).to_bytes(2, byteorder='big')
    message += int(0).to_bytes(2, byteorder='big')
    message += int(0).to_bytes(4, byteorder='big')

    # Retorna a mensagem precedida pelo seu tamanho em bytes
    return len(message).to_bytes(4, byteorder='big') + message


def handle_client(conn):
    with conn:
        print("Handling client...")

        # Recebe e processa a requisição do cliente
        req = conn.recv(1024)
        correlation_id = int.from_bytes(req[8:12], byteorder='big')
        api_key = int.from_bytes(req[3:5], byteorder='big')
        api_version = int.from_bytes(req[5:7], byteorder='big')

        print(f"Received API Key: {api_key}, API Version: {api_version}")

        # Envia uma resposta com base na mensagem recebida
        response = create_message(correlation_id, api_key)
        conn.sendall(response)
        print("Response sent.")


def main():
    print("Starting server...")

    server = socket.create_server(("localhost", 9092), reuse_port=True)
    print("Server listening on localhost:9092")

    conn, addr = server.accept()  # Aguarda conexão do cliente
    print(f"Connected by {addr}")

    # Lida com a conexão do cliente
    handle_client(conn)


if __name__ == "__main__":
    main()
