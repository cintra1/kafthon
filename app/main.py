import socket

def create_api_versions_response(correlation_id, error_code=0):
    # Correlation ID (4 bytes) + Error Code (2 bytes)
    message = correlation_id.to_bytes(4, byteorder='big')
    message += error_code.to_bytes(2, byteorder='big')

    # API_VERSIONS (API key 18, MinVersion 0, MaxVersion 4)
    api_key = 18
    min_version = 0
    max_version = 4
    message += api_key.to_bytes(2, byteorder='big')  # API Key
    message += min_version.to_bytes(2, byteorder='big')  # Min Version
    message += max_version.to_bytes(2, byteorder='big')  # Max Version

    # Calculate and prepend the message length (4 bytes)
    message_length = len(message).to_bytes(4, byteorder='big')
    
    # Return the complete message
    return message_length + message

def handle_client(conn):
    with conn:
        print("Handling client...")

        # Receive request from client
        req = conn.recv(1024)
        correlation_id = int.from_bytes(req[8:12], byteorder='big')
        api_key = int.from_bytes(req[3:5], byteorder='big')
        api_version = int.from_bytes(req[5:7], byteorder='big')

        print(f"Received API Key: {api_key}, API Version: {api_version}")

        # Check if the request is for API_VERSIONS (API key 18)
        if api_key == 18:
            if 0 <= api_version <= 4:
                response = create_api_versions_response(correlation_id)
            else:
                response = create_api_versions_response(correlation_id, 35)  # Error code for unsupported version
                print("Unsupported API version.")
        else:
            response = create_api_versions_response(correlation_id)  # Error code for unsupported API key
            print("Unsupported API key.")

        # Send response to the client
        conn.sendall(response)
        print("API Versions response sent.")

def main():
    print("Starting server...")

    server = socket.create_server(("localhost", 9092), reuse_port=True)
    print("Server listening on localhost:9092")

    conn, addr = server.accept()  # Wait for client connection
    print(f"Connected by {addr}")

    # Handle client connection
    handle_client(conn)

if __name__ == "__main__":
    main()
