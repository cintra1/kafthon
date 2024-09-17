import socket

def create_api_versions_response(correlation_id, error_code=0):
    # Correlation ID (4 bytes) + Error Code (2 bytes)
    message = correlation_id.to_bytes(4, byteorder='big')
    message += error_code.to_bytes(2, byteorder='big')

    # Number of API version entries (at least 1 for API key 18)
    message += int(1+1).to_bytes(1, byteorder='big')

    # API_VERSIONS (API key 18, MinVersion 0, MaxVersion 4)
    api_key = 18
    min_version = 0
    max_version = 4
    message += api_key.to_bytes(2, byteorder='big')   # API Key
    message += min_version.to_bytes(2, byteorder='big')  # Min Version
    message += max_version.to_bytes(2, byteorder='big')  # Max Version
    message += int(0).to_bytes(2, byteorder='big')
    message += int(0).to_bytes(4, byteorder='big')

    # Calcular e adicionar o tamanho da mensagem no início (4 bytes)
    message_length = len(message).to_bytes(4, byteorder='big')
    
    # Retornar a mensagem completa
    return message_length + message

def handle_client(conn):
    with conn:
        print("Handling client...")

        # Recebe a requisição do cliente
        req = conn.recv(1024)
        correlation_id = int.from_bytes(req[8:12], byteorder='big')
        api_key = int.from_bytes(req[3:5], byteorder='big')
        api_version = int.from_bytes(req[5:7], byteorder='big')

        print(f"Received API Key: {api_key}, API Version: {api_version}")

        # Verifica se o pedido é para API_VERSIONS (API key 18)
        if 0 <= api_version <= 4:
            response = create_api_versions_response(correlation_id)
            conn.sendall(response)
            print("APIVersions response sent.")
        else:
            # Se o pedido não for o esperado, retorna um erro ou outra lógica.
            print("Unsupported API key or version.")

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
