import socket
import threading

# Estrutura para armazenar mensagens e assinaturas dos tópicos
topics = {}
topic_subscribers = {}

lock = threading.Lock()  # Para garantir que o acesso ao dicionário seja thread-safe

def from_client(client: socket.socket):
    data = client.recv(2048)
    if not data:
        return None, None, None

    api_key = int.from_bytes(data[4:6], byteorder='big')
    api_version = int.from_bytes(data[6:8], byteorder='big')
    correlation_id = int.from_bytes(data[8:12], byteorder='big')
    
    # Topic ID (simulando o identificador do tópico, extraído da requisição)
    topic_id = int.from_bytes(data[12:16], byteorder='big')  # Exemplo de 4 bytes para topic_id
    
    return api_key, api_version, correlation_id, topic_id

def make_fetch_response(api_key, api_version, correlation_id, topic_id):
    response_header = correlation_id.to_bytes(4, byteorder='big')
    
    # Verificar se o topic_id existe
    if topic_id not in topics:
        error_code = 100  # UNKNOWN_TOPIC
    else:
        error_code = 0  # No Error

    throttle_time_ms = 0  # Pode ser qualquer valor
    session_id = 0
    partition_index = 0
    partition_error_code = 100  # UNKNOWN_TOPIC para o teste

    # Resposta do corpo para o fetch (v16)
    response_body = (
        throttle_time_ms.to_bytes(4, byteorder='big') +   # Throttle time
        error_code.to_bytes(2, byteorder='big') +         # Error code
        session_id.to_bytes(4, byteorder='big') +         # Session ID
        (1).to_bytes(4, byteorder='big') +               # Respostas (1 tópico)
        topic_id.to_bytes(4, byteorder='big') +           # Topic ID
        (1).to_bytes(4, byteorder='big') +               # Número de partições (1 partição)
        partition_index.to_bytes(4, byteorder='big') +    # Partition index (sempre 0)
        partition_error_code.to_bytes(2, byteorder='big') +  # Error code (100 - UNKNOWN_TOPIC)
        (0).to_bytes(8, byteorder='big')  # Placeholder para offsets (Exemplo de 8 bytes)
    )

    response_length = len(response_header) + len(response_body)
    return response_length.to_bytes(4, byteorder='big') + response_header + response_body

def handle_client(client):
    print("Client connected")
    
    try:
        while True:
            result = from_client(client)
            if result is None:
                break
            api_key, api_version, correlation_id, topic_id = result

            print(f"API Key: {api_key}, API Version: {api_version}, Correlation ID: {correlation_id}")
            print(f"Topic ID: {topic_id}")
            
            if api_key == 1 and api_version == 16:  # Fetch (v16)
                response = make_fetch_response(api_key, api_version, correlation_id, topic_id)
                client.sendall(response)
                print("Fetch response sent.")
            else:
                response = make_error(api_key, api_version, correlation_id)
                client.sendall(response)
                print("Unknown request, sent error response.")
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
