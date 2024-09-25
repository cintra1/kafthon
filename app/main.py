import socket
import threading

def from_client(client: socket.socket):
    data = client.recv(2048)
    api_key = int.from_bytes(data[4:6], byteorder='big')
    api_version = int.from_bytes(data[6:8], byteorder='big')
    correlation_id = int.from_bytes(data[8:12], byteorder='big')
    return api_key, api_version, correlation_id

def make_api_version_response(api_key, api_version, correlation_id):
    response_header = correlation_id.to_bytes(4, byteorder='big')

    valid_api_versions = list(range(0, 5))
    error_code = 0 if api_version in valid_api_versions else 35  # 35 para versão não suportada
    num_of_api_versions = 3 if error_code == 0 else 0
    fetch = 1
    min_api_version, max_api_version = 0, 4
    min_fetch_version, max_fetch_version = 0, 16
    throttle_time_ms = 0
    session_id = 0
    response_body = 0  # Corpo da resposta
    tag_buffer = b"\x00"  # Buffer para tags adicionais

    response_body = (
        error_code.to_bytes(2, byteorder='big') +
        num_of_api_versions.to_bytes(1, byteorder='big') +  # Número de entradas de versão
        api_key.to_bytes(2, byteorder='big') +
        min_api_version.to_bytes(2, byteorder='big') +
        max_api_version.to_bytes(2, byteorder='big') +
        tag_buffer +
        fetch.to_bytes(2, byteorder='big') +
        min_fetch_version.to_bytes(2, byteorder='big') +
        max_fetch_version.to_bytes(2, byteorder='big') +
        tag_buffer +
        throttle_time_ms.to_bytes(4, byteorder='big') +
        tag_buffer
    )

    response_length = len(response_header) + len(response_body)
    return response_length.to_bytes(4, byteorder='big') + response_header + response_body

def make_fetch_response(api_key, api_version, correlation_id):
    response_header = correlation_id.to_bytes(4, byteorder='big')
    
    valid_api_versions = list(range(0, 17))
    error_code = 0 if api_version in valid_api_versions else 35  # 35 para versão não suportada
    fetch = 1
    min_fetch_version, max_fetch_version = 0, 16
    throttle_time_ms = 0
    session_id = 0
    tag_buffer = b"\x00"

    response_body = (
        throttle_time_ms.to_bytes(4, byteorder='big') +
        error_code.to_bytes(2, byteorder='big') +
        session_id.to_bytes(4, byteorder='big') +
        fetch.to_bytes(2, byteorder='big') +
        tag_buffer
    )

    response_length = len(response_header) + len(response_body)
    return response_length.to_bytes(4, byteorder='big') + response_header + response_body

def handle_client(client):
    print("Client connected")
    
    try:
        while True:
            api_key, api_version, correlation_id = from_client(client)
            if api_key is None:  # Verifique se o cliente enviou algum dado
                break

            print(f"API Key: {api_key}, API Version: {api_version}, Correlation ID: {correlation_id}")

            match (api_key, api_version):
                case (18, 3):  # TODO: Alterar para v4
                    response = make_api_version_response(api_key, api_version, correlation_id)
                case (1, 16):
                    response = make_fetch_response(api_key, api_version, correlation_id)
                case _:
                    # Resposta para versões de API desconhecidas
                    response = UnknownAPIVersionHandler().handle((api_key, api_version))

            client.sendall(response)
            print("Response sent.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        client.close()  # Feche a conexão quando terminar
        print("Connection closed.")

def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    print("Server listening on localhost:9092")
    
    while True:
        client, _ = server.accept()
        client_thread = threading.Thread(target=handle_client, args=(client,))
        client_thread.start()  # Start a new thread for each client connection

if __name__ == "__main__":
    main()
