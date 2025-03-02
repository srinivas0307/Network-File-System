# Network File System 

## Overview
Network File System (NFS) is a distributed file system that allows clients to access and manage files on a remote server over a network. This project implements a simplified NFS with functionalities such as file storage, retrieval, and management using a client-server architecture.

## Features
- Naming server to manage file locations
- Storage server to handle file operations
- Client interface for accessing files remotely
- Error handling and logging system

## Installation

### Prerequisites
Ensure you have the following installed on your system:
- GCC (GNU Compiler Collection)
- Make
- Linux environment (recommended for testing)

### Setup
1. Clone this repository:
   ```sh
   git clone <repository-url>
   cd Naming File System
   ```
2. Compile the project:
   ```sh
   make
   ```
3. Start the Naming Server:
   ```sh
   ./Naming_Server
   ```
4. Start the Storage Server:
   ```sh
   ./Storage_Server
   ```
5. Run the client program:
   ```sh
   ./client
   ```

## File Structure
```
Naming File System/
│── Naming_Server.c          # Naming server implementation
│── Storage_Server.c         # Storage server implementation
│── client.c                 # Client-side implementation
│── error_codes.h            # Error codes and definitions
│── README.md                # Project documentation
│── Makefile                 # Compilation script
```

## Dependencies
This project requires:
- Standard C libraries
- Networking libraries (`<sys/socket.h>`, `<netinet/in.h>`)

