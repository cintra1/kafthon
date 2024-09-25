import socket
import threading

def from_client(client: socket.socket):
    data = client.recv(2048)
    api_key = int.from_bytes(data[4:6], byteorder='big')
    api_version = int.from_bytes(data[6:8], byteorder='big')
    correlation_id = int.from_bytes(data[8:12], byteorder='big')
    return api_key, api_version, correlation_id

def encode_varint(value):
    """Encode an integer as a varint."""
    result = bytearray()
    while value > 0x7F:
        result.append((value & 0x7F) | 0x80)  # Set the highest bit
        value >>= 7  # Shift right by 7 bits
    result.append(value & 0x7F)  # Append the last byte
    return bytes(result)

def make_api_version_response(api_key, api_version, correlation_id):
    response_header = correlation_id.to_bytes(4, byteorder='big')

    valid_api_versions = [0, 1, 2, 3, 4]
    error_code = 0 if api_version in valid_api_versions else 35  # 35 para versão não suportada
    num_of_api_versions = 3 if error_code == 0 else 0
    min_api_version, max_api_version = 0, 4
    throttle_time_ms = 0
    tag_buffer = b"\x00"

    response_body = (
        error_code.to_bytes(2, byteorder='big') +
        num_of_api_versions.to_bytes(1, byteorder='big') +  # Número de entradas de versão
        api_key.to_bytes(2, byteorder='big') +
        min_api_version.to_bytes(2, byteorder='big') +
        max_api_version.to_bytes(2, byteorder='big') +
        tag_buffer + 
        throttle_time_ms.to_bytes(4, byteorder='big') +
        tag_buffer
    )

    response_length = len(response_header) + len(response_body)
    return response_length.to_bytes(4, byteorder='big') + response_header + response_body

def make_fetch_response(api_key, correlation_id):
    response_header = correlation_id.to_bytes(4, byteorder='big')

    fetch = 1
    error_code = 0
    min_fetch_version, max_fetch_version = 0, 16
    throttle_time_ms = 0
    session_id = 0

    # Defina o tag_buffer como um exemplo de array de campos tagueados
    tag_buffer = b"\x00\x01\x02\x03"  # Exemplo de valores, ajuste conforme necessário

    response_body = (
        throttle_time_ms.to_bytes(4, byteorder='big') +
        error_code.to_bytes(2, byteorder='big') +
        session_id.to_bytes(4, byteorder='big') +
        encode_varint(fetch) +  # Use o varint para fetch
        tag_buffer  # Inclui o buffer de tags no corpo da resposta
    )

    response_length = len(response_header) + len(response_body)
    return response_length.to_bytes(4, byteorder='big') + response_header + response_body


def handle_client(client):
    print("Client connected")
    
    try:
        while True:
            api_key, api_version, correlation_id = from_client(client)
            if api_key is None:  # Check if the client sent any data
                break

            print(f"API Key: {api_key}, API Version: {api_version}, Correlation ID: {correlation_id}")

            if api_version in [0, 1, 2, 3, 4]:
                response = make_api_version_response(api_key, api_version, correlation_id)
            else:
                response = make_fetch_response(api_key, correlation_id)

            client.sendall(response)
            print("Response sent.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        client.close()  # Close the connection when done
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
