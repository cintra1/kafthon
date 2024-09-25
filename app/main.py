import socket

def from_client(client: socket.socket):
    try:
        while True:
            data = client.recv(2048)
            api_key = int.from_bytes(data[4:6], byteorder='big')
            api_version = int.from_bytes(data[6:8], byteorder='big')
            correlation_id = int.from_bytes(data[8:12], byteorder='big')

            if not data:
                return None
            else:
                return api_key, api_version, correlation_id
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()  # Close the connection when done
        print("Connection closed.")


def make_response(api_key, api_version, correlation_id):
    # Cabeçalho da resposta
    response_header = correlation_id.to_bytes(4, byteorder='big')

    valid_api_versions = [0, 1, 2, 3, 4]
    # Verifica se a versão da API é suportada
    error_code = 0 if api_version in valid_api_versions else 35  # 35 para versão não suportada
    min_version, max_version = 0, 4
    throttle_time_ms = 0
    tag_buffer = b"\x00"  # Buffer para tags adicionais

    # Corpo da resposta
    response_body = (
        error_code.to_bytes(2, byteorder='big') +
        int(2).to_bytes(1, byteorder='big') +  # Número de entradas de versão
        api_key.to_bytes(2, byteorder='big') +
        min_version.to_bytes(2, byteorder='big') +
        max_version.to_bytes(2, byteorder='big') +
        tag_buffer +
        throttle_time_ms.to_bytes(4, byteorder='big') +
        tag_buffer
    )

    # Calcula o tamanho total da resposta
    response_length = len(response_header) + len(response_body)
    return response_length.to_bytes(4, byteorder='big') + response_header + response_body


def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    print("Server listening on localhost:9092")
    
    client, _ = server.accept()
    print("Client connected")
    
    # Extrai os valores da requisição recebida do cliente
    api_key, api_version, correlation_id = from_client(client)
    print(f"API Key: {api_key}, API Version: {api_version}, Correlation ID: {correlation_id}")

    # Envia a resposta de volta para o cliente
    response = make_response(api_key, api_version, correlation_id)
    client.sendall(response)
    print("Response sent.")


if __name__ == "__main__":
    main()
