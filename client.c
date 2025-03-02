#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <sys/select.h>
#include <sys/wait.h>
#include <pthread.h>
#include <sys/time.h>
#include "error_codes.h"

#define MAX_PATH_LEN 128
#define BUFFER 2048
#define NS_IP "192.168.191.160"
#define NS_PORT 8080

typedef struct clientreqeuset
{
    int ss;
    int ds;
    char request[30];
    char path[MAX_PATH_LEN];
    char dest[MAX_PATH_LEN];
    char data[BUFFER];
} clientreqeuset;

typedef struct ns_reply
{
    char ip[50];
    int port;
} ns_reply;

typedef struct storageinfo
{
    char ip[50];
    int client_port;
    int status;
    int number_of_paths;
    char paths[30][MAX_PATH_LEN];
    int backup1;
    int backup2;
} storageinfo;

void *wait_timeout(void *arg)
{
    int fd = *((int *)arg);
    struct timeval start, end;
    long elapsed_time = 0;
    gettimeofday(&start, NULL);

    while (elapsed_time < 1000000)
    {
        gettimeofday(&end, NULL);
        elapsed_time = (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);
    }
    close(fd);
}

void clear_socket_buffers(int socket_fd)
{
    int bytes_available;
    char discard_buffer[BUFFER];
    ioctl(socket_fd, FIONREAD, &bytes_available);
    while (bytes_available > 0)
    {
        int read_size = (bytes_available > BUFFER) ? BUFFER : bytes_available;
        recv(socket_fd, discard_buffer, read_size, MSG_DONTWAIT);
        ioctl(socket_fd, FIONREAD, &bytes_available);
    }
}

void play_audio(const char *filename)
{
    char command[BUFFER];
    snprintf(command, sizeof(command), "mpv --no-video %s", filename);
    system(command);
}

void *write_async(void *arg)
{
    clientreqeuset dump = *((clientreqeuset *)arg);

    int server_socket = dump.ss;
    int new_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (new_socket < 0)
    {
        printf("Error : %d\n", ERR_SOCKET_FAILED);
        return NULL;
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(NS_PORT);

    if (inet_pton(AF_INET, NS_IP, &server_addr.sin_addr) <= 0)
    {
        printf("Error : %d\n", ERR_INVALID_ADDRESS);
        close(new_socket);
        return NULL;
    }

    if (connect(new_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
    {
        printf("Error : %d\n", ERR_CONNECTION_FAILED);
        close(new_socket);
        return NULL;
    }

    int flag = 2;

    while (send(new_socket, (void *)&flag, sizeof(int), 0) < 0)
    {
        // wait here
    }
    usleep(10000);
    send(new_socket, &dump, sizeof(dump), 0);
    char buffer[BUFFER];
    recv(new_socket, buffer, BUFFER, 0);
    printf("%s\n", buffer);
    close(new_socket);
}

int connect_ss(ns_reply ss_cred)
{
    int server_socket;
    struct sockaddr_in server_addr;
    server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket < 0)
    {
        printf("Error : %d\n", ERR_SOCKET_FAILED);
        return -1;
    }

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(ss_cred.port);
    if (inet_pton(AF_INET, ss_cred.ip, &server_addr.sin_addr) <= 0)
    {
        printf("Error : %d\n", ERR_INVALID_ADDRESS);
        close(server_socket);
        return -1;
    }

    if (connect(server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
    {
        printf("Error : %d\n", ERR_CONNECTION_FAILED);
        close(server_socket);
        return -1;
    }
    int f = 0;
    send(server_socket, &f, sizeof(int), 0);
    printf("Connected to storage server at port %d and ip %s\n", ss_cred.port, ss_cred.ip);
    return server_socket;
}

void handle_read(clientreqeuset c, int server_socket)
{
    printf("Enter the file path: ");
    memset(c.path, 0, MAX_PATH_LEN);
    scanf("%s", c.path);
    strcat(c.path, "\0");

    send(server_socket, &c, sizeof(c), 0);
    ns_reply ss_cred;
    recv(server_socket, &ss_cred, sizeof(ss_cred), 0);
    if (strstr(ss_cred.ip, "Path not found") || strstr(ss_cred.ip, "try again"))
    {
        printf("Error : %d\n", ERR_PATH_NOT_FOUND);
        return;
    }
    else if (strstr(ss_cred.ip, "Storage server is down"))
    {
        printf("Error : %d\n", ERR_STORAGE_SERVER_DOWN);
        return;
    }
    else if (strstr(ss_cred.ip, "try again"))
    {
        printf("Error : %d\n", ERR_MAX_WRITE_LIMIT);
    }
    int ss_socket = connect_ss(ss_cred);
    char ack[BUFFER];
    if (ss_socket == -1)
    {
        strcpy(ack, "Error connecting to storage server");
        send(server_socket, ack, BUFFER, 0);
        printf("Error : %d\n", ERR_CONNECTION_TO_SS);
        return;
    }
    send(ss_socket, &c, sizeof(c), 0);
    ssize_t received;
    char buff[BUFFER];
    char line[BUFFER];
    int line_pos = 0;
    while (1)
    {
        memset(buff, 0, sizeof(buff));
        received = recv(ss_socket, buff, BUFFER, 0);
        if (received <= 0)
            break;

        buff[received] = '\0';
        for (int i = 0; i < received; i++)
        {
            if (buff[i] == '\n' || buff[i] == '\0')
            {
                line[line_pos] = '\0';
                printf("%s\n", line);
                line_pos = 0;
            }
            else
                line[line_pos++] = buff[i];
        }
    }
    wait_timeout((void *)&ss_socket);
    fflush(stdout);
    strcpy(ack, "Read successful");
    send(server_socket, ack, BUFFER, 0);
    pthread_t t;
    pthread_create(&t, NULL, wait_timeout, &ss_socket);
}

void handle_write(clientreqeuset c, int server_socket)
{
    printf("Enter the file path: ");
    memset(c.path, 0, MAX_PATH_LEN);
    scanf("%s", c.path);
    getchar();
    strcat(c.path, "\0");
    printf("Enter your new content (To end your data enter EOF): ");
    memset(c.data, 0, BUFFER);
    char temp[BUFFER];
    bool stay = true;
    while (stay)
    {
        while (fgets(temp, BUFFER, stdin))
        {
            if (strncmp(temp, "EOF  ", 3) == 0)
            {
                stay = false;
                break;
            }
            strcat(c.data, temp);
        }
    }
    strcat(c.data, "\0");
sync:
    printf("Enter 0 if you want to force synchronous writing, else 1: ");
    scanf("%d", &c.ds);

    if (c.ds != 0 && c.ds != 1)
    {
        printf("Enter valid input!\n");
        goto sync;
    }

    send(server_socket, &c, sizeof(c), 0);
    ns_reply ss_cred;
    recv(server_socket, &ss_cred, sizeof(ss_cred), 0);
    if (strstr(ss_cred.ip, "Path not found") || strstr(ss_cred.ip, "try again"))
    {
        printf("Error : %d\n", ERR_PATH_NOT_FOUND);
        return;
    }
    else if (strstr(ss_cred.ip, "Storage server is down"))
    {
        printf("Error : %d\n", ERR_STORAGE_SERVER_DOWN);
        return;
    }
    else if (strstr(ss_cred.ip, "try again"))
    {
        printf("Error : %d\n", ERR_MAX_WRITE_LIMIT);
    }

    printf("%d %s\n", ss_cred.port, ss_cred.ip);
    int ss_socket = connect_ss(ss_cred);
    char ack[BUFFER];
    if (ss_socket == -1)
    {
        strcpy(ack, "Error connecting to storage server");
        send(server_socket, ack, BUFFER, 0);
        printf("Error : %d\n", ERR_CONNECTION_TO_SS);
        return;
    }
    send(ss_socket, &c, sizeof(c), 0);
    char buff[BUFFER];
    memset(buff, 0, BUFFER);
    recv(ss_socket, buff, BUFFER, 0);
    if (strstr(buff, "Asynchronous"))
    {
        send(server_socket, buff, BUFFER, 0);
        pthread_t async;
        c.ss = server_socket;
        printf("ack %s\n", buff);
        pthread_create(&async, NULL, write_async, (void *)&c);
    }
    else
    {
        printf("%s\n", buff);
        strcpy(ack, "Write successful");
        send(server_socket, ack, BUFFER, 0);
    }
    close(ss_socket);
    printf("Disconnected from storage server\n");
}

void handle_copy(clientreqeuset c, int server_socket)
{
    printf("Enter the file path: ");
    memset(c.path, 0, MAX_PATH_LEN);
    scanf("%s", c.path);
    strcat(c.path, "\0");
    printf("Enter your destination file path: ");
    memset(c.dest, 0, MAX_PATH_LEN);
    scanf("%s", c.dest);
    strcat(c.dest, "\0");
    send(server_socket, &c, sizeof(c), 0);
    char buff[BUFFER];
    memset(buff, 0, BUFFER);
    ns_reply ss;
    recv(server_socket, &ss, sizeof(ss), 0);
    printf("%s\n", ss.ip);
}

void handle_delete(clientreqeuset c, int server_socket)
{
    printf("Enter the file path: ");
    memset(c.path, 0, MAX_PATH_LEN);
    scanf("%s", c.path);
    strcat(c.path, "\0");

    send(server_socket, &c, sizeof(c), 0);
    char output[BUFFER];
    memset(output, 0, BUFFER);
    ns_reply ss;
    recv(server_socket, &ss, sizeof(ss), 0);
    printf("%s\n", ss.ip);
}

void handle_create(clientreqeuset c, int server_socket)
{
    printf("Enter the file path: ");
    memset(c.path, 0, MAX_PATH_LEN);
    scanf("%s", c.path);
    strcat(c.path, "\0");

    send(server_socket, &c, sizeof(c), 0);
    char output[BUFFER];
    ns_reply ss;
    recv(server_socket, &ss, sizeof(ss), 0);
    printf("%s\n", ss.ip);
}

void handle_stream(clientreqeuset c, int server_socket)
{
    printf("Enter the audio file path: ");
    memset(c.path, 0, MAX_PATH_LEN);
    scanf("%s", c.path);
    strcat(c.path, "\0");

    send(server_socket, &c, sizeof(c), 0);
    ns_reply ss_cred;
    recv(server_socket, &ss_cred, sizeof(ss_cred), 0);
    if (strstr(ss_cred.ip, "Path not found") || strstr(ss_cred.ip, "try again"))
    {
        printf("Error : %d\n", ERR_PATH_NOT_FOUND);
        return;
    }
    else if (strstr(ss_cred.ip, "Storage server is down"))
    {
        printf("Error : %d\n", ERR_STORAGE_SERVER_DOWN);
        return;
    }
    else if (strstr(ss_cred.ip, "try again"))
    {
        printf("Error : %d\n", ERR_MAX_WRITE_LIMIT);
    }

    printf("%d %s\n", ss_cred.port, ss_cred.ip);
    int ss_socket = connect_ss(ss_cred);
    char ack[BUFFER];
    if (ss_socket == -1)
    {
        strcpy(ack, "Error connecting to storage server");
        send(server_socket, ack, BUFFER, 0);
        printf("Error : %d\n", ERR_CONNECTION_TO_SS);
        return;
    }
    if (send(ss_socket, &c, sizeof(c), 0) < 0)
    {
        printf("Error : %d\n", ERR_SEND_FAILED);
        return;
    }

    char buffer[BUFFER];
    ssize_t received = recv(ss_socket, buffer, BUFFER + 4, 0);
    if (received <= 0)
    {
        printf("Error : %d\n", ERR_NO_RESPONSE);
        close(ss_socket);
        return;
    }
    buffer[received] = '\0';

    if (strncmp(buffer, "SUCCESS", 7) != 0)
    {
        printf("Server response: %s\n", buffer);
        close(ss_socket);
        return;
    }

    const char *temp_file = "received_audio.mp3";
    int temp_fd = open(temp_file, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (temp_fd < 0)
    {
        printf("Error : %d\n", ERR_FILE_CREATION_FAILED);
        close(ss_socket);
        return;
    }
    FILE *temp_fp = fdopen(temp_fd, "w");
    int count = 0;
    while (1)
    {
        memset(buffer, 0, sizeof(buffer));
        received = recv(ss_socket, buffer, BUFFER + 4, 0);
        if (received <= 0)
            break;
        count++;
        if (received == 7 && strncmp(buffer, "FILEEND", 7) == 0)
            break;

        if (write(temp_fd, buffer, received) != received)
        {
            printf("Error : %d\n", ERR_WRITE_FAILED);
            break;
        }
        fflush(temp_fp);
    }
    close(ss_socket);
    close(temp_fd);
    strcpy(ack, "Stream successful");
    send(server_socket, ack, BUFFER, 0);
    printf("Audio file received and saved as %s\n", temp_file);

    play_audio(temp_file);
}

void handle_getlist(clientreqeuset c, int server_socket)
{
    if (send(server_socket, &c, sizeof(c), 0) < 0)
    {
        printf("Error : %d\n", ERR_SEND_FAILED);
        return;
    }
    int ss_count;
    while (1)
    {
        if (recv(server_socket, &ss_count, sizeof(int), 0) > 0)
            break;
    }
    char path[MAX_PATH_LEN];
    for (int i = 0; i < ss_count; i++)
    {
        int num_paths;
        recv(server_socket, &num_paths, sizeof(int), 0);
        for (int j = 0; j < num_paths; j++)
        {
            memset(&path, 0, sizeof(path));
            recv(server_socket, &path, MAX_PATH_LEN, 0);
            if (strlen(path))
                printf("%s\n", path);
        }
    }
}

void handle_getinfo(clientreqeuset c, int server_socket)
{
    printf("Enter the file path: ");
    memset(c.path, 0, MAX_PATH_LEN);
    scanf("%s", c.path);
    strcat(c.path, "\0");
    send(server_socket, &c, sizeof(c), 0);
    ns_reply ss_cred;
    recv(server_socket, &ss_cred, sizeof(ss_cred), 0);
    if (strstr(ss_cred.ip, "Path not found") || strstr(ss_cred.ip, "try again"))
    {
        printf("Error : %d\n", ERR_PATH_NOT_FOUND);
        return;
    }
    else if (strstr(ss_cred.ip, "Storage server is down"))
    {
        printf("Error : %d\n", ERR_STORAGE_SERVER_DOWN);
        return;
    }
    else if (strstr(ss_cred.ip, "try again"))
    {
        printf("Error : %d\n", ERR_MAX_WRITE_LIMIT);
    }
    printf("%d %s\n", ss_cred.port, ss_cred.ip);
    int ss_socket = connect_ss(ss_cred);
    char ack[BUFFER];
    if (ss_socket == -1)
    {
        strcpy(ack, "Error connecting to storage server");
        send(server_socket, ack, BUFFER, 0);
        printf("Error : %d\n", ERR_CONNECTION_TO_SS);
        return;
    }
    send(ss_socket, &c, sizeof(c), 0);
    char buff[BUFFER];
    memset(buff, 0, BUFFER);
    recv(ss_socket, buff, BUFFER, 0);
    printf("%s", buff);
    strcpy(ack, "Get info successful");
    send(server_socket, ack, BUFFER, 0);
    wait_timeout((void *)&ss_socket);
    fflush(stdout);
}

void handle_invalid(clientreqeuset c, int server_socket)
{
    int bytes = send(server_socket, &c, sizeof(c), 0);
    if (bytes < 0)
    {
        printf("Error : %d\n", ERR_SEND_FAILED);
        return;
    }
    ns_reply reply;
    bytes = recv(server_socket, &reply, sizeof(reply), 0);
    if (bytes < 0)
    {
        printf("Error : %d\n", ERR_RECV_FAILED);
        return;
    }
    printf("Error : %d\n", ERR_INVALID_CMD);
    return;
}

int main()
{
    int server_socket;
    struct sockaddr_in server_addr;
    int flag = 0;
    server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket < 0)
    {
        printf("Error : %d\n", ERR_SOCKET_FAILED);
        exit(EXIT_FAILURE);
    }

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(NS_PORT);
    if (inet_pton(AF_INET, NS_IP, &server_addr.sin_addr) <= 0)
    {
        printf("Error : %d\n", ERR_INVALID_ADDRESS);
        close(server_socket);
        exit(EXIT_FAILURE);
    }

    if (connect(server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
    {
        printf("Error : %d\n", ERR_CONNECTION_FAILED);
        close(server_socket);
        exit(EXIT_FAILURE);
    }
    send(server_socket, (void *)&flag, sizeof(int), 0);
    printf("Connected to the server at %s: %d\n", NS_IP, NS_PORT);
    clientreqeuset c;
    while (1)
    {
        printf("Give your command: ");
        scanf("%s", c.request);
        strcat(c.request, "\0");
        if (strcmp(c.request, "STOP\0") == 0)
            break;
        else if (strcmp(c.request, "READ\0") == 0)
            handle_read(c, server_socket);
        else if (strcmp(c.request, "WRITE\0") == 0)
            handle_write(c, server_socket);
        else if (strcmp(c.request, "COPY\0") == 0)
            handle_copy(c, server_socket);
        else if (strcmp(c.request, "DELETE\0") == 0)
            handle_delete(c, server_socket);
        else if (strcmp(c.request, "CREATE\0") == 0)
            handle_create(c, server_socket);
        else if (strcmp(c.request, "STREAM\0") == 0)
            handle_stream(c, server_socket);
        else if (strcmp(c.request, "GETINFO\0") == 0)
            handle_getinfo(c, server_socket);
        else if (strcmp(c.request, "GETLIST\0") == 0)
            handle_getlist(c, server_socket);
        else
            handle_invalid(c, server_socket);
        clear_socket_buffers(server_socket);
    }
    return 0;
}