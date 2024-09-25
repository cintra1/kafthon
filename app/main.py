import socket

# Helper function to receive full data
def receive_full_data(client: socket.socket, expected_length: int):
    received_data = b""
    while len(received_data) < expected_length:
        chunk = client.recv(2048)
        if not chunk:  # Client closed the connection
            return None
        received_data += chunk
    return received_data

def from_client(client: socket.socket):
    try:
        # First, read the length of the incoming message (4 bytes)
        length_data = client.recv(4)
        if not length_data:  # Client closed connection
            return None
        
        # Get the full length of the message
        message_length = int.from_bytes(length_data, byteorder='big')

        # Now read the full message based on the provided length
        data = receive_full_data(client, message_length)
        if not data:
            return None
        
        # Extract fields (API key, version, correlation id) from the message
        api_key = int.from_bytes(data[4:6], byteorder='big')
        api_version = int.from_bytes(data[6:8], byteorder='big')
        correlation_id = int.from_bytes(data[8:12], byteorder='big')

        return api_key, api_version, correlation_id
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        client.close()  # Close the connection when done
        print("Connection closed.")

def make_response(api_key, api_version, correlation_id):
    # Header of the response
    response_header = correlation_id.to_bytes(4, byteorder='big')

    valid_api_versions = [0, 1, 2, 3, 4]
    # Check if the API version is supported
    error_code = 0 if api_version in valid_api_versions else 35  # 35 for unsupported version
    min_version, max_version = 0, 4
    throttle_time_ms = 0
    tag_buffer = b"\x00"  # Buffer for additional tags

    # Response body
    response_body = (
        error_code.to_bytes(2, byteorder='big') +
        int(1).to_bytes(1, byteorder='big') +  # Number of version entries
        api_key.to_bytes(2, byteorder='big') +
        min_version.to_bytes(2, byteorder='big') +
        max_version.to_bytes(2, byteorder='big') +
        tag_buffer +
        throttle_time_ms.to_bytes(4, byteorder='big') +
        tag_buffer
    )

    # Total response length
    response_length = len(response_header) + len(response_body)
    return response_length.to_bytes(4, byteorder='big') + response_header + response_body

def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    print("Server listening on localhost:9092")
    
    while True:
        client, _ = server.accept()
        print("Client connected")
        
        # Extract values from the client request
        api_data = from_client(client)
        if api_data is None:
            print("No valid data received, closing connection.")
            continue
        
        api_key, api_version, correlation_id = api_data
        print(f"API Key: {api_key}, API Version: {api_version}, Correlation ID: {correlation_id}")

        # Send the response back to the client
        response = make_response(api_key, api_version, correlation_id)
        client.sendall(response)
        print("Response sent.")

if __name__ == "__main__":
    main()
