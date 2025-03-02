#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <signal.h>
#include <stdlib.h>
#include <limits.h>
#include <errno.h>
#include <stdarg.h>
#include <stdbool.h>
#include <pwd.h>
#include <time.h>
#include <grp.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <pthread.h>
#include "error_codes.h"

#define SERVER_IP "192.168.25.230"
#define SERVER_PORT 8080
#define LOCAL_PORT 8003
#define BUFFER_SIZE 2048
#define MAX_PATH_LEN 128
#define ACK 100
#define CHUNK 100

struct credentials
{
    char ip[30];
    int client_port;
    int status;
    int num_paths;
    char paths[30][128];
};

typedef struct storageserverinfo
{
    char ip[50];
    int client_port;
    int status;
    int num_paths;
    char paths[30][128];
    int backup1;
    int backup2;
} storageserverinfo;

typedef struct clientrequest
{
    int ss;
    int ds;
    char request[30];
    char path[MAX_PATH_LEN];
    char dest[MAX_PATH_LEN];
    char data[BUFFER_SIZE];
} clientrequest;

pthread_mutex_t filelock = PTHREAD_MUTEX_INITIALIZER;

int check_execute(char *path)
{
    if (strchr(path, '.') != NULL)
    {
        return 0;
    }
    else
    {
        return 1;
    }
}

int connect_to_nm(char *ip, int port)
{
    int sock;
    struct sockaddr_in server_addr;
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        printf("Error : %d\n",ERR_SOCKET_FAILED);
        return -1;
    }
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(SERVER_PORT);
    if (inet_pton(AF_INET, SERVER_IP, &server_addr.sin_addr) <= 0)
    {
        printf("Error : %d\n",ERR_INVALID_ADDRESS);
        close(sock);
        return -1;
    }
    if (connect(sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
    {
        printf("Error : %d\n",ERR_CONNECTION_FAILED);
        close(sock);
        return -1;
    }
    printf("Connected to server %s on port %d\n", SERVER_IP, SERVER_PORT);
    return sock;
}
int create_local(char *name)
{
    int len = strlen(name);
    if (len > 0 && check_execute(name))
    {
        if (mkdir(name, 0755) == -1)
        {
            printf("Error : %d\n",ERR_DIR_CREATION_FAILED);
            return -1;
        }
        printf("Directory created: %s\n", name);
        return 1;
    }
    else
    {
        FILE *file = fopen(name, "w");
        if (file == NULL)
        {
            printf("Error : %d\n",ERR_FILE_CREATION_FAILED);
            return -1;
        }
        fclose(file);
        printf("File created: %s\n", name);
        return 0;
    }
}
int create_file_directory(char *name, int socket_ns)
{
    int len = strlen(name);
    if (len > 0 && check_execute(name))
    {
        if (mkdir(name, 0755) == -1)
        {
            if (send(socket_ns, "Failed to  create Directory", sizeof("Failed to create Directory"), 0) < 0)
            {
                printf("Error : %d\n",ERR_SEND_FAILED);
            }
            printf("Error : %d\n",ERR_DIR_CREATION_FAILED);
            return -1;
        }
        printf("Directory created: %s\n", name);
        char array[100] = "directory created";
        if (send(socket_ns, array, sizeof(array), 0) < 0)
        {
            printf("Error : %d\n",ERR_SEND_FAILED);
        }
        else
        {
            printf("sends successfully\n");
        }
        return 1;
    }
    else
    {
        FILE *file = fopen(name, "w");
        if (file == NULL)
        {
            if (send(socket_ns, "Failed to  create FILE", sizeof("Failed to create FILE"), 0) < 0)
            {
                printf("Error : %d\n",ERR_SEND_FAILED);
            }
            printf("Error : %d\n",ERR_FILE_CREATION_FAILED);
            return -1;
        }
        fclose(file);
        printf("File created: %s\n", name);
        if (send(socket_ns, "file created", sizeof("file created"), 0) < 0)
        {
            printf("Error : %d\n",ERR_SEND_FAILED);
        }
        else
        {
            printf("sends successfully \n");
        }
        return 0;
    }
    return 0;
}

int delete_directory(const char *path)
{
    DIR *dir = opendir(path);
    if (dir == NULL)
    {
        printf("Error : %d\n" , ERR_OPENDIR);
        return 0;
    }
    struct dirent *entry;
    char full_path[1024];
    while ((entry = readdir(dir)) != NULL)
    {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
        {
            continue;
        }
        snprintf(full_path, sizeof(full_path), "%s/%s", path, entry->d_name);
        struct stat entry_stat;
        if (stat(full_path, &entry_stat) == -1)
        {
            printf("Error : %d\n",ERR_FILESTAT_FAILED);
            closedir(dir);
            return 0;
        }
        if (S_ISDIR(entry_stat.st_mode))
        {
            if (delete_directory(full_path) == -1)
            {
                closedir(dir);
                return 0;
            }
        }
        else
        {
            if (remove(full_path) == -1)
            {
                printf("Error : %d\n",ERR_FILE_DELETION_FAILED);
                closedir(dir);
                return 0;
            }
        }
    }
    closedir(dir);
    if (rmdir(path) == -1)
    {
        printf("Error : %d\n",ERR_FILE_DELETION_FAILED);
        return 0;
    }
    return 1;
}
void delete_file_directory(const char *path, int socket)
{
    struct stat path_stat;

    // Get file/directory information
    if (stat(path, &path_stat) == -1)
    {
        printf("Error : %d\n",ERR_FILESTAT_FAILED);
        send(socket, "Failed to get file or directory status", sizeof("Failed to get file or directory status"), 0);
        return;
    }
    if (S_ISDIR(path_stat.st_mode))
    {
        int check = delete_directory(path);
        if (check)
        {
            printf("directory is deleted \n");
            char ack[100];
            strncpy(ack, "directory deleted", 99);
            ack[99] = '\0';
            send(socket, ack, strlen(ack) + 1, MSG_NOSIGNAL);
        }
        else
        {
            printf("directory is deleted gives error \n");
            send(socket, "error on delete", sizeof("error on delete"), 0);
        }
    }
    else if (S_ISREG(path_stat.st_mode))
    {
        if (remove(path) == -1)
        {
            printf("Error : %d\n",ERR_FILE_DELETION_FAILED);
            send(socket, "Failed to delete file", sizeof("Failed to delete file"), 0);
            return;
        }
        printf("file is deleted \n");
        send(socket, "file deleted", sizeof("file deleted"), 0);
    }
    else
    {
        printf("file or directory is not defined \n");
        fprintf(stderr, "Unsupported file type.\n");
        send(socket, "error on delete", sizeof("error on delete"), 0);
        return;
    }
    return;
}
void printing(const char *filename, int socket)
{
    FILE *file = fopen(filename, "r");
    if (file == NULL)
    {
        send(socket , "Error opening file" , sizeof("Error opening file") , 0);
        printf("Error : %d\n",ERR_OPEN_FILE);
        return;
    }
    char buffer[BUFFER_SIZE];
    while (fgets(buffer, sizeof(buffer) - 1, file) != NULL)
    {
        send(socket, buffer, strlen(buffer), 0);
        printf("%s", buffer);
    }
    send(socket, "\0", 1, 0);
    printf("\nfile sent\n");
    fclose(file);
}
void writing(char *path, char *string, int socket, int flag, int local_flag)
{
    pthread_mutex_lock(&filelock);
    FILE *file = fopen(path, "w");
    if (file == NULL)
    {
        send(socket , "Error opening file" , sizeof("Error opening file") , 0);
        printf("Error : %d\n",ERR_OPEN_FILE);
        return;
    }
    int length = strlen(string);
    int out = 0;
    int chunks = 0;
    if (CHUNK < length && flag != 0)
    {
        int remaining = length - chunks;
        int counter = 1;
        while (counter <= 2)
        {
            int sizeto;
            if (remaining < CHUNK)
            {
                sizeto = remaining;
            }
            else
            {
                sizeto = CHUNK;
            }
            fprintf(file, "%.*s", (int)sizeto, string + chunks);
            chunks += CHUNK;
            if (flag && out == 0 && local_flag)
            {
                send(socket, "Asynchronous processing has been completed", sizeof("Asynchronous processing has been completed"), 0);
                printf("asynchronous1 the flag is %d \n", flag);
            }
            out++;
            remaining = length - chunks;
            if (remaining <= CHUNK)
            {
                counter++;
            }
        }
    }
    else
    {
        fprintf(file, "%s", string);
    }
    if (!flag && local_flag || length < CHUNK)
    {
        send(socket, "Synchronous processing has been completed", sizeof("Synchronous processing has been completed"), 0);
        printf("Synchronous2 the flag is %d \n", flag);
    }
    fclose(file);
    pthread_mutex_unlock(&filelock);
    if (flag && local_flag && out != 0)
    {
        int socket_ack = connect_to_nm(SERVER_IP, SERVER_PORT);
        clientrequest c;
        int temp = 2;
        send(socket_ack, &temp, sizeof(temp), 0);
        strcpy(c.request, "ACK");
        strcpy(c.path, path);
        send(socket_ack, &c, sizeof(c), 0);
        printf("command sent successfully \n");
        close(socket);
    }
    // 0 means synchronous
    // 1 means asynchronous
    // local flag 0 means local
    return;
}

void copy_files_and_folders(char *source, char *destination, storageserverinfo c)
{
    printf("Entered at function\n");
    char relative_path[256], dest_path[256];
    char command[BUFFER_SIZE], response[BUFFER_SIZE];
    int sockfd;
    int flag_dir = 0;
    int count = 0;
    printf("the number of paths %d \n", c.num_paths);
    for (int i = 0; i < c.num_paths; i++)
    {
        printf("initial path \n");
        char *result = strstr(c.paths[i], source);
        if (result == NULL)
        {
            continue;
        }
        char arr[BUFFER_SIZE];
        snprintf(arr, sizeof(arr), "%s%s", destination, result);
        printf("the intial path is %s \n", c.paths[i]);
        printf("the created path is %s \n", arr);
        int f;
        f = create_local(arr);
        if (f == 0)
        {
            int nm_socket = connect_to_nm(c.ip, c.client_port);
            clientrequest d;
            strcpy(d.request, "READ");
            strcpy(d.path, c.paths[i]);
            char buff[BUFFER_SIZE];
            int line_pos = 0;
            while (1)
            {
                memset(buff, 0, sizeof(buff));
                int received = recv(nm_socket, buff, BUFFER_SIZE, 0);
                if (received <= 0)
                    break;
                buff[received] = '\0';
                char line[BUFFER_SIZE];
                for (int i = 0; i < received; i++)
                {
                    if (buff[i] == '\n' || buff[i] == '\0')
                    {
                        line[line_pos] = '\0';
                        writing(arr, line, 0, 0, 0);
                        line_pos = 0;
                    }
                    else
                        line[line_pos++] = buff[i];
                }
            }
            close(nm_socket);
        }
        else if (f < 0)
        {
            printf("error on creating a file \n");
        }
    }
    return;
}

int copy_all(char *source, char *destination, int socket)
{
    printf("the source is %s \n the destination is %s \n", source, destination);
    struct stat source_stat;
    storageserverinfo c;
    int bytes_read = recv(socket, &c, sizeof(c), 0);
    printf("the nu %d \n", bytes_read);
    printf("the num paths %d the status %d the ip %s \n", c.num_paths, c.status, c.ip);
    if ((check_execute(source) && check_execute(destination)) || (!check_execute(source) && !check_execute(destination)))
    {
        copy_files_and_folders(source, destination, c);
        printf("copy files and folders function called \n");
        if (check_execute(source))
        {
            send(socket, "Directory copied\0", sizeof("Directory copied\0"), 0);
        }
        else
        {
            send(socket, "File copied\0", sizeof("File copied\0"), 0);
        }
        return 1;
    }
    else
    {
        send(socket, "Unsupported source type/0", sizeof("Unsupported source type/0"), 0);
        fprintf(stderr, "Unsupported source type\n");
        return -1;
    }
}

void retrieve(const char *path, int socket)
{
    char info_buffer[BUFFER_SIZE];
    struct stat file_stat;
    if (stat(path, &file_stat) == -1)
    {
        printf("Error : %d\n",ERR_FILESTAT_FAILED);
        strcat(info_buffer, "Error: Unable to retrieve information.\n");
        send(socket, info_buffer, sizeof(info_buffer), 0);
        return;
    }
    char temp[256];
    snprintf(temp, sizeof(temp), "%s  ", path);
    strcat(info_buffer, temp);
    snprintf(temp, sizeof(temp), "Size: %lld ", (long long)file_stat.st_size);
    strcat(info_buffer, temp);
    if (S_ISREG(file_stat.st_mode))
    {
        strcat(info_buffer, " Regular File ");
    }
    else if (S_ISDIR(file_stat.st_mode))
    {
        strcat(info_buffer, " Directory ");
    }
    else
    {
        strcat(info_buffer, " Unknown Type ");
    }
    char pm[11];
    pm[0] = (S_ISDIR(file_stat.st_mode)) ? 'd' : '-';
    pm[1] = (file_stat.st_mode & S_IRUSR) ? 'r' : '-';
    pm[2] = (file_stat.st_mode & S_IWUSR) ? 'w' : '-';
    pm[3] = (file_stat.st_mode & S_IXUSR) ? 'x' : '-';
    pm[4] = (file_stat.st_mode & S_IRGRP) ? 'r' : '-';
    pm[5] = (file_stat.st_mode & S_IWGRP) ? 'w' : '-';
    pm[6] = (file_stat.st_mode & S_IXGRP) ? 'x' : '-';
    pm[7] = (file_stat.st_mode & S_IROTH) ? 'r' : '-';
    pm[8] = (file_stat.st_mode & S_IWOTH) ? 'w' : '-';
    pm[9] = (file_stat.st_mode & S_IXOTH) ? 'x' : '-';
    pm[10] = '\0';
    snprintf(temp, sizeof(temp), " %s ", pm);
    strcat(info_buffer, temp);
    struct passwd *pw = getpwuid(file_stat.st_uid);
    struct group *gr = getgrgid(file_stat.st_gid);
    snprintf(temp, sizeof(temp), " %s (UID: %d)\n", pw ? pw->pw_name : "Unknown", file_stat.st_uid);
    strcat(info_buffer, temp);
    snprintf(temp, sizeof(temp), " %s (GID: %d)\n", gr ? gr->gr_name : "Unknown", file_stat.st_gid);
    strcat(info_buffer, temp);
    snprintf(temp, sizeof(temp), "Last Access : %s", ctime(&file_stat.st_atime));
    strcat(info_buffer, temp);
    snprintf(temp, sizeof(temp), " Modification : %s", ctime(&file_stat.st_mtime));
    strcat(info_buffer, temp);
    snprintf(temp, sizeof(temp), " Status Change : %s", ctime(&file_stat.st_ctime));
    strcat(info_buffer, temp);
    printf("\nFile Information:\n%s", info_buffer);
    send(socket, info_buffer, BUFFER_SIZE, 0);
    memset(info_buffer, 0, sizeof(info_buffer));
    return;
}

void streaming(char *file_path, int client_socket)
{
    char buffer[BUFFER_SIZE];
    if (access(file_path, F_OK) != 0)
    {
        const char *error_msg = "ERROR: File not found or inaccessible\n";
        send(client_socket, error_msg, strlen(error_msg), 0);
        close(client_socket);
        return;
    }
    int file_fd = open(file_path, O_RDONLY);
    if (file_fd < 0)
    {
        const char *error_msg = "ERROR: Unable to open the file\n";
        send(client_socket, error_msg, strlen(error_msg), 0);
        close(client_socket);
        return;
    }
    const char *success_msg = "SUCCESS\n";
    send(client_socket, success_msg, strlen(success_msg), 0);
    ssize_t bytes_read;
    int count = 0;
    while ((bytes_read = read(file_fd, buffer, sizeof(buffer))) > 0)
    {
        ssize_t sent = 0;
        while (sent < bytes_read)
        {
            ssize_t current_sent = send(client_socket, buffer + sent, bytes_read - sent, 0);
            usleep(1000);
            if (current_sent <= 0)
            {
                printf("Error : %d\n",ERR_SEND_FAILED);
                return;
            }
            count++;
            sent += current_sent;
        }
        // send(client_socket, buffer, bytes_read, 0);
    }
    printf("the buffer is %s \n", buffer);
    printf("the count is %d\n", count);
    send(client_socket, "FILEEND\0", 8, 0);
    // printf("%zu", total);
    close(file_fd);
    printf("File sent successfully\n");
    return;
}

ssize_t readn(int fd, void *buf, size_t n)
{
    size_t nleft = n;
    ssize_t nread;
    char *ptr = buf;

    while (nleft > 0)
    {
        if ((nread = recv(fd, ptr, nleft, 0)) < 0)
        {
            if (errno == EINTR)
                continue;
            return -1;
        }
        else if (nread == 0)
        {
            break; // EOF
        }
        nleft -= nread;
        ptr += nread;
    }
    return n - nleft;
}

void appending(char *path, char *data)
{
    // Lock the mutex to ensure thread safety
    pthread_mutex_lock(&filelock);
    // Open the file in append mode
    FILE *file = fopen(path, "a");
    if (file == NULL)
    {
        printf("Error : %d\n",ERR_OPEN_FILE);
        pthread_mutex_unlock(&filelock);
        return;
    }

    // Write data to the file
    if (fprintf(file, "%s", data) < 0)
    {
        printf("Error : %d\n",ERR_WRITE_FAILED);
    }
    else
    {
        printf("Data appended successfully to %s\n", path);
        printf("the data is %s\n", data);
    }
    fclose(file);
    pthread_mutex_unlock(&filelock);
    return;
}

void *handle_naming(void *arg)
{
    int test = 0;
    int server_sock = *(int *)arg;
    test++;
    clientrequest req;
    memset(&req, 0, sizeof(clientrequest));
    int read = recv(server_sock, &req, sizeof(clientrequest), 0);
    if (read < 0)
    {
        printf("Error : %d\n",ERR_RECV_FAILED);
        close(server_sock);
        exit(EXIT_FAILURE);
    }
    if (read == 0)
    {
        printf("Server closed connection\n");
        close(server_sock);
        exit(EXIT_FAILURE);
    }
    if (strcmp(req.request, "PING") == 0)
    {
        int byte = send(server_sock, "Inlevalable", strlen("Inlevalable") + 1, 0);
        if (byte <= 0)
            exit(0);
        return NULL;
    }
    printf("Naming server connected \n");
    printf("Received request: %s for path: %s\n", req.request, req.path);
    if (strcmp(req.request, "CREATE") == 0)
    {
        create_file_directory(req.path, server_sock);
        printf("the create is done\n");
    }
    else if (strcmp(req.request, "DELETE") == 0)
    {
        delete_file_directory(req.path, server_sock);
        printf("the deletion is done \n");
    }
    else if (strcmp(req.request, "COPY") == 0)
    {
        // copy_all(req.path, req.dest, server_sock);
        copy_all(req.path, req.dest, server_sock);
        printf("copy is done \n");
    }
    else if (strcmp(req.request, "APPEND") == 0)
    {
        printf("the data is %s\n", req.data);
        appending(req.path, req.data);
        printf("Appending is done \n");
    }
    else
    {
        printf("the request is %s\n", req.request);
        printf("Invalid command \n");
        send(server_sock, "Invalid command", sizeof("Invalid command"), 0);
    }
    close(server_sock);
    return NULL;
}
void *handle_client(void *arg)
{
    clientrequest buffer;
    int socket = *(int *)arg;
    int read = recv(socket, &buffer, sizeof(buffer), 0);
    printf("read %d\n", read);
    if (strcmp(buffer.request, "READ") == 0)
    {
        printf("the read\n");
        printing(buffer.path, socket);
    }
    else if (strcmp(buffer.request, "WRITE") == 0)
    {
        printf("the string is %s the flag is %d\n", buffer.data, buffer.ds);
        writing(buffer.path, buffer.data, socket, buffer.ds, 1);
        printf("the write\n");
    }
    else if (strcmp(buffer.request, "GETINFO") == 0)
    {
        retrieve(buffer.path, socket);
        printf("the get information \n");
    }
    else if (strcmp(buffer.request, "STREAM") == 0)
    {
        streaming(buffer.path, socket);
        printf("the stream \n");
    }
    else
    {
        printf("Invalid command \n");
    }
    close(socket);
    return NULL;
}

int main(int argc, char *argv[])
{
    storageserverinfo name;
    pthread_mutex_init(&filelock, NULL);
    strcpy(name.ip, "192.168.25.22");
    name.client_port = LOCAL_PORT;
    name.num_paths = argc - 1;
    // char some[BUFFER_SIZE][BUFFER_SIZE];
    printf("%d\n", argc);
    for (int i = 1; i < argc; i++)
    {
        int n = strlen(argv[i]);
        strcpy(name.paths[i - 1], argv[i]);
        name.paths[i - 1][n] = '\0';
    }

    name.status = 1;
    struct sockaddr_in server_addr;
    int server_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (server_sock < 0)
    {
        printf("Error : %d\n",ERR_SOCKET_FAILED);
        exit(EXIT_FAILURE);
    }
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(SERVER_PORT);
    if (inet_pton(AF_INET, SERVER_IP, &server_addr.sin_addr) <= 0)
    {
        printf("Error : %d\n",ERR_INVALID_ADDRESS);
        close(server_sock);
        exit(EXIT_FAILURE);
    }
    if (connect(server_sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
    {
        printf("Error : %d\n",ERR_CONNECTION_FAILED);
        close(server_sock);
        exit(EXIT_FAILURE);
    }
    printf("Connected to the primary server at %s:%d\n", SERVER_IP, SERVER_PORT);
    int temp = 1;
    if (send(server_sock, &temp, sizeof(temp), 0) < 0)
    {
        printf("Error : %d\n",ERR_SEND_FAILED);
        close(server_sock);
        exit(EXIT_FAILURE);
    }
    usleep(1000);
    printf("%ld\n", sizeof(name));
    for (int i = 0; i < name.num_paths; i++)
    {
        printf("%s\n", name.paths[i]);
    }
    if (send(server_sock, &name, sizeof(name), 0) < 0)
    {
        printf("Error : %d\n",ERR_SEND_FAILED);
        close(server_sock);
        exit(EXIT_FAILURE);
    }
    close(server_sock);
    int local_server_sock, client_sock;
    struct sockaddr_in local_addr, client_addr;
    socklen_t client_addr_len = sizeof(client_addr);
    char buffer[BUFFER_SIZE];
    local_server_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (local_server_sock < 0)
    {
        printf("Error : %d\n",ERR_SOCKET_FAILED);
        exit(EXIT_FAILURE);
    }
    local_addr.sin_family = AF_INET;
    local_addr.sin_addr.s_addr = INADDR_ANY;
    local_addr.sin_port = htons(LOCAL_PORT);
    if (bind(local_server_sock, (struct sockaddr *)&local_addr, sizeof(local_addr)) < 0)
    {
        printf("Error : %d\n",ERR_BIND_FAILED);
        close(local_server_sock);
        exit(EXIT_FAILURE);
    }
    if (listen(local_server_sock, 20) < 0)
    {
        printf("Error : %d\n",ERR_LISTEN_FAILED);
        close(local_server_sock);
        exit(EXIT_FAILURE);
    }
    while (1)
    {

        int socket_i;
        if ((socket_i = accept(local_server_sock, (struct sockaddr *)&(local_addr), (socklen_t *)&(client_addr_len))) < 0)
        {
            printf("Error : %d\n",ERR_ACCEPT_FAILED);
            close(local_server_sock);
            exit(-1);
        }
        int *sock = (int *)malloc(sizeof(int));
        int flag;
        recv(socket_i, &flag, sizeof(int), 0);
        *sock = socket_i;
        pthread_t thread;
        if (flag == 1)
        {
            pthread_create(&thread, NULL, handle_naming, sock);
        }
        else if (flag == 0)
        {
            printf("Client connected\n");
            pthread_create(&thread, NULL, handle_client, sock);
        }
        pthread_detach(thread);
    }
    return 0;
}