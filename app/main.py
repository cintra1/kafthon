import socket
from dataclasses import dataclass
from enum import Enum, unique

@unique
class ErrorCode(Enum):
    NONE = 0
    UNSUPPORTED_VERSION = 35

@dataclass
class KafkaRequest:
    api_key: int
    api_version: int
    correlation_id: int

    @staticmethod
    def from_client(client: socket.socket):
        data = client.recv(2048)
        return KafkaRequest(
            api_key=int.from_bytes(data[4:6], byteorder='big'),
            api_version=int.from_bytes(data[6:8], byteorder='big'),
            correlation_id=int.from_bytes(data[8:12], byteorder='big'),
        )

def make_response(request: KafkaRequest):
    # Valid API versions
    valid_api_versions = [0, 1, 2, 3, 4]
    
    # Determine error code based on API version
    error_code = (
        ErrorCode.NONE
        if request.api_version in valid_api_versions
        else ErrorCode.UNSUPPORTED_VERSION
    )

    # Create the response header (correlation ID + error code)
    response_header = request.correlation_id.to_bytes(4, byteorder='big')
    
    # API key versions
    min_version, max_version = 0, 4
    throttle_time_ms = 0
    tag_buffer = b"\x00"

    # Create the response body
    response_body = (
        error_code.value.to_bytes(2, byteorder='big')  # Error code (2 bytes)
        + int(1).to_bytes(1, byteorder='big')  # Number of APIs supported (1 byte)
        + request.api_key.to_bytes(2, byteorder='big')  # API key (2 bytes)
        + min_version.to_bytes(2, byteorder='big')  # Min version (2 bytes)
        + max_version.to_bytes(2, byteorder='big')  # Max version (2 bytes)
        + throttle_time_ms.to_bytes(4, byteorder='big')  # Throttle time (4 bytes)
        + tag_buffer  # Empty tag buffer
    )

    # Calculate response length
    response_length = len(response_header) + len(response_body)
    
    # Return the complete response (length + header + body)
    return int(response_length).to_bytes(4, byteorder='big') + response_header + response_body

def main():
    # Create server
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    
    # Accept a client connection
    client, _ = server.accept()
    
    # Handle the client request
    request = KafkaRequest.from_client(client)
    print(f"Received request: {request}")
    
    # Send the response
    client.sendall(make_response(request))
    print("Response sent.")

if __name__ == "__main__":
    main()
