#ifndef SS_CONNECTION_C
#define SS_CONNECTION_C

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <pthread.h>
#include <errno.h>
#include "error_codes.h"

#define PORT 8080
#define IP "192.168.191.160"
#define MAX_SS 5
#define MAX_Client 100
#define BUFFER 2048
#define ACK 100
#define MAX_PATHS 30
#define MAX_PATH_LEN 128
#define SIZE 65
#define RED "\033[1;31m"
#define GREEN "\033[1;32m"
#define YELLOW "\033[1;33m"
#define PINK "\033[1;35m"
#define ESC "\033[0m"

typedef struct storageserverinfo
{
    char ip[50];
    int client_port;
    int status;
    int num_paths;
    char paths[MAX_PATHS][MAX_PATH_LEN];
    int backup1;
    int backup2;
} storageserverinfo;

typedef struct nsreply{
    char ip[50];
    int port;
}nsreply;

typedef struct clientreqeuset{
    int ss;
    int ds;
    char request[30];
    char path[MAX_PATH_LEN];
    char dest[MAX_PATH_LEN];
    char data[BUFFER];
}clientreqeuset;

typedef struct trienode{
    struct trienode *c[SIZE];
    int present;
    int writeflag;
}trienode;

typedef struct LRUdt{
    char path[MAX_PATH_LEN];
    int ss;
    struct LRUdt *next;
    struct LRUdt *pre;
}LRUdt;

typedef struct LRU{
    LRUdt *head;
    LRUdt *tail;
    int size;
    int capacity;
}LRU;

#endif

int ss_count = 0;
int client_count = 0;
int client_socket[MAX_Client] = {-1};
storageserverinfo storageinfo[MAX_SS];
trienode *root;
LRU *lru;
pthread_mutex_t ss_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t lru_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t trie_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t log_message_mutex = PTHREAD_MUTEX_INITIALIZER;

LRU *createLRU(int capacity){
    LRU *lru=(LRU *)malloc(sizeof(LRU));
    lru->head=NULL;
    lru->tail=NULL;
    lru->size=0;
    lru->capacity=capacity;
    return lru;
}

void addlru(LRU *lru,char *path,int s){
    LRUdt *node=(LRUdt *)malloc(sizeof(LRUdt));
    strcpy(node->path,path);
    node->ss=s;
    node->next=NULL;
    node->pre=NULL;
    if(lru->size==0){
        lru->head=node;
        lru->tail=node;
        lru->size++;
    }
    else{
        if(lru->size==lru->capacity){
            LRUdt *temp=lru->head;
            lru->head=lru->head->next;
            free(temp);
            lru->size--;
        }
        lru->tail->next=node;
        node->pre=lru->tail;
        lru->tail=node;
        lru->size++;
    }
}


void printlru(LRU *lru){
    LRUdt *temp=lru->head;
    while(temp!=NULL){
        printf("%s %d\n",temp->path,temp->ss);
        temp=temp->next;
    }
}

void updatelru(LRU *lru,char *path){
    LRUdt *temp=lru->head;
    while(temp!=NULL){
        if(strcmp(temp->path,path)==0){
            if(temp==lru->head){
                lru->head=lru->head->next;
                if(lru->head!=NULL)
                lru->head->pre=NULL;
            }
            else if(temp==lru->tail){
                lru->tail=lru->tail->pre;
                lru->tail->next=NULL;
            }
            else{
                temp->pre->next=temp->next;
                temp->next->pre=temp->pre;
            }
            lru->size--;
            printf("lru size %d\n",lru->size);
            addlru(lru,path,temp->ss);
            return;
        }
        temp=temp->next;
    }
}

int checklru(LRU *lru,char *path){
    LRUdt *temp=lru->head;
    while(temp!=NULL){
        printf("path %s %s\n",temp->path,path);
        if(strcmp(temp->path,path)==0){
            updatelru(lru,path);
            int ss=temp->ss;
            free(temp);
            return ss;
        }
        temp=temp->next;
    }
    return -1;
}


int cindex(char c){
    if(c>='a'&&c<='z'){
        return c-'a';
    }
    if(c>='A'&&c<='Z'){
        return c-'A'+26;
    }
    if(c>='0'&&c<='9'){
        return c-'0'+52;
    }
    if(c=='_'){
        return 62;
    }
    if(c=='.'){
        return 63;
    }
    if(c=='/'){
        return 64;
    }
    else return -1;
}

int checktrie(trienode *root,char *str){
    trienode *temp=root;
    int j=0;
    for(int i=0;i<strlen(str);i++){
        int ind=cindex(str[i]);
        if(ind==-1) return -1;
        if(temp->c[ind]==NULL){
            return -1;
        }
        temp=temp->c[ind];
    }
    return temp->present;
}

trienode* check_write(trienode *root,char *str){
    trienode *temp=root;
    int j=0;
    for(int i=0;i<strlen(str);i++){
        int ind=cindex(str[i]);
        if(ind==-1) return NULL;
        if(temp->c[ind]==NULL){
            return NULL;
        }
        temp=temp->c[ind];
    }
    return temp;
}

void inserttrie(trienode *root,char *str,int ss){
    trienode *temp=root;
    for(int i=0;i<strlen(str);i++){
        int ind=cindex(str[i]);
        if(ind==-1) return ;
        if(temp->c[ind]==NULL){
            temp->c[ind]=(trienode *)malloc(sizeof(trienode));
            for(int j=0;j<SIZE;j++){
                temp->c[ind]->c[j]=NULL;
            }
            temp->c[ind]->present=ss;
        }
        temp=temp->c[ind];
    }
    temp->present=ss;
    temp->writeflag=0;
}

void deletesub(trienode *root){
    if(root==NULL) return;
    for(int i=0;i<SIZE;i++){
        deletesub(root->c[i]);
    }
    free(root);
}

void deletetrie(trienode *root,char *str){
    trienode *temp=root;
    trienode *parent;
    int ind;
    for(int i=0;i<strlen(str);i++){
        ind=cindex(str[i]);
        if(ind==-1) return ;
        if(temp->c[ind]==NULL){
            return;
        }
        parent=temp;
        temp=temp->c[ind];
    }
    parent->c[ind]=NULL;
    temp->present=0;
    deletesub(temp);
    return;
}


int copytrie(trienode *root,char *str,char *dest,int s,int d,int l){
    trienode *src=root;
    trienode *des=root;
    for(int i=0;i<l;i++){
        int ind=cindex(str[i]);
        if(ind==-1) return -1;
        if(src->c[ind]==NULL){
            return -1;
        }
        src=src->c[ind];
    }
    for(int i=0;i<strlen(dest);i++){
        int ind=cindex(dest[i]);
        if(ind==-1) return -1;
        if(des->c[ind]==NULL){
            return -1;
        }
        des=des->c[ind];
    }
    des->c[cindex(str[l])]=src->c[cindex(str[l])];
    return 0;
}

void safe_close(int* fd){
    if(*fd>0){
        close(*fd);
        *fd=-1;
    }
}

void logmessage(char *messages){
    pthread_mutex_lock(&log_message_mutex);
    FILE *fp=fopen("log.txt","a");
    if(fp==NULL){
        printf("Error : %d\n", ERR_OPEN_FILE);
        pthread_mutex_unlock(&log_message_mutex);
        return;
    }
    fprintf(fp,"%s\n",messages);
    fclose(fp);
    pthread_mutex_unlock(&log_message_mutex);
}

int connect_to_ss(char *ip, int port){
    int server_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (server_sock<0){
        printf("Error : %d\n", ERR_SOCKET_FAILED);
        return -1;
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    inet_pton(AF_INET, ip, &server_addr.sin_addr);

    if (connect(server_sock,(struct sockaddr*)&server_addr,sizeof(server_addr))<0){
        printf("Error : %d\n", ERR_CONNECTION_FAILED);
        close(server_sock);
        return -1;
    }
    int flag=1;
    if(send(server_sock,&flag,sizeof(flag),0)<0){
        printf("Error : %d\n", ERR_SEND_FAILED);
        close(server_sock);
        return -1;
    }
    return server_sock;
}

int connect_to_ss_read(char *ip, int port){
    int server_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (server_sock<0){
        printf("Error : %d\n", ERR_SOCKET_FAILED);
        return -1;
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    inet_pton(AF_INET, ip, &server_addr.sin_addr);

    if (connect(server_sock,(struct sockaddr*)&server_addr,sizeof(server_addr))<0){
        printf("Error : %d\n", ERR_CONNECTION_FAILED);
        close(server_sock);
        return -1;
    }
    int flag=0;
    if(send(server_sock,&flag,sizeof(flag),0)<0){
        printf("Error : %d\n", ERR_SEND_FAILED);
        close(server_sock);
        return -1;
    }
    return server_sock;
}

void backup(int s,int r1,int r2){
    char *back="backup";
    for(int i=0;i<storageinfo[s-1].num_paths;i++){
        char arr[BUFFER];
        snprintf(arr , sizeof(arr) , "%s/%s" , storageinfo[s-1].paths[i] , back);
        int sock1 = connect_to_ss(storageinfo[r1-1].ip , storageinfo[r1-1].client_port);
        if(sock1 < 0){
            printf("Error : %d\n", ERR_CONNECTION_FAILED);
            return ;
        }
        clientreqeuset c;
        strcpy(c.request , "CREATE");
        strcpy(c.path , arr);
        if(send(sock1 , &c , sizeof(c) , 0) < 0){
            printf("Error : %d\n", ERR_SEND_FAILED);
            close(sock1);
            return ;
        }
        char str[100];
        if(recv(sock1 , str , sizeof(str) , 0) < 0){
            printf("Error : %d\n", ERR_RECV_FAILED);
            close(sock1);
            return ;
        }
        close(sock1);
        int sock2 = connect_to_ss(storageinfo[r2-1].ip , storageinfo[r2-1].client_port);
        if(sock2 < 0){
            printf("Error : %d\n", ERR_CONNECTION_FAILED);
            return ;
        }
        clientreqeuset d;
        strcpy(d.request , "CREATE");
        strcpy(d.path , arr);
        if(send(sock2 , &d , sizeof(d) , 0) < 0){
            printf("Error : %d\n", ERR_SEND_FAILED);
            close(sock2);
            return ;
        }
        char str1[100];
        if(recv(sock2 , str1 , sizeof(str1) , 0) < 0){
            printf("Error : %d\n", ERR_RECV_FAILED);
            close(sock2);
            return ;
        }
        close(sock2);
        if(strchr(arr , '.') != NULL){
            printf("path %s\n",arr);
            int sock2 = connect_to_ss_read(storageinfo[s-1].ip , storageinfo[s-1].client_port);
            if(sock2<0){
                printf("Error : %d\n", ERR_CONNECTION_FAILED);
                return ;
            }
            clientreqeuset d;
            strcpy(c.request , "READ");
            strcpy(c.path , storageinfo[s-1].paths[i]);
            if(send(sock2,&c,sizeof(c),0)<0){
                printf("Error : %d\n", ERR_SEND_FAILED);
                return ;
            }
            printf("send successfully \n");
            ssize_t received;
            char buff[BUFFER];
            char line[BUFFER];
            int line_pos = 0;
            while (1)
            {
                printf("enter in loop\n");
                memset(buff, 0, sizeof(buff));
                received = recv(sock2, buff, BUFFER, 0);
                if (received <= 0){
                    printf("error\n");
                    break;
                }
                buff[received] = '\0';
                for (int i = 0; i < received; i++)
                {
                    if (buff[i] == '\n' || buff[i] == '\0')
                    {
                        line[line_pos] = '\0';
                        clientreqeuset some;
                        strcpy(some.request , "APPEND");
                        strcpy(some.path , arr);
                        strcpy(some.data,line);
                        printf("%s\n",line);
                        // some.ds = 0;
                        int sock3=connect_to_ss(storageinfo[r1-1].ip , storageinfo[r1-1].client_port);
                        if(send(sock3 , &some , sizeof(some) , 0)<0){
                            printf("Error : %d\n", ERR_SEND_FAILED);
                            close(sock2);
                            close(sock3);
                            return ;
                        }
                        line_pos = 0;
                        close(sock3);

                        int sock4=connect_to_ss(storageinfo[r2-1].ip , storageinfo[r2-1].client_port);
                        if(send(sock4 , &some , sizeof(some) , 0)<0){
                            printf("Error : %d\n", ERR_SEND_FAILED);
                            close(sock2);
                            close(sock4);
                            return ;
                        }
                        line_pos = 0;
                        close(sock4);
                    }
                    else
                        line[line_pos++] = buff[i];
                }
            }
            close(sock2);
        }
    }
}

void copypaths(int s,int d,char *src,char *dest){
    char *sr=strrchr(src,'/');
    if(sr==NULL) return;
    sr++;
    int temp=storageinfo[s-1].num_paths;
    for(int i=0;i<temp;i++){
        char *ptr=strstr(storageinfo[s-1].paths[i],sr);
        if(ptr!=NULL){
            char temp[100];
            snprintf(temp,sizeof(temp),"%s/%s",dest,ptr);
            clientreqeuset req;
            strcpy(storageinfo[d-1].paths[storageinfo[d-1].num_paths],temp);
            storageinfo[d-1].num_paths++;
            inserttrie(root,temp,d);
            if(strstr(temp,".")!=NULL){
                addlru(lru,temp,d);
            }
        }
    }
}

int copying(int source , int destination , char *src , char *d){
    char *s=strrchr(src,'/');
    s++;
    for(int i=0;i<storageinfo[source - 1].num_paths;i++){
        char *result = strstr(storageinfo[source - 1].paths[i] , s);
        if(result != NULL){
            char arr[BUFFER];
            snprintf(arr, sizeof(arr), "%s/%s", d, result);
            int sock1 = connect_to_ss(storageinfo[destination-1].ip , storageinfo[destination-1].client_port);
            if(sock1<0){
                printf("Error : %d\n", ERR_CONNECTION_FAILED);
                return -1;
            }
            clientreqeuset c;
            strcpy(c.request , "CREATE");
            strcpy(c.path , arr);
            if(strchr(d , '.') == NULL){
                if(send(sock1 , &c , sizeof(c) , 0)<0){
                    printf("Error : %d\n", ERR_SEND_FAILED);
                    close(sock1);
                    return -1;
                }
                char str[100];
                if(recv(sock1,str,sizeof(str),0)<0){
                    printf("Error : %d\n", ERR_RECV_FAILED);
                    close(sock1);
                    return -1;
                }
                printf("%s\n",str);
            }
            close(sock1);
            if(strchr(arr , '.') != NULL){
                int sock2 = connect_to_ss_read(storageinfo[source-1].ip , storageinfo[source-1].client_port);
                if(sock2<0){
                    printf("Error : %d\n", ERR_CONNECTION_FAILED);
                    return -1;
                }
                clientreqeuset d;
                strcpy(c.request , "READ");
                strcpy(c.path , storageinfo[source-1].paths[i]);
                if(send(sock2,&c,sizeof(c),0)<0){
                    printf("Error : %d\n", ERR_SEND_FAILED);
                    return -1;
                }
                ssize_t received;
                char buff[BUFFER];
                char line[BUFFER];
                int line_pos = 0;
                while (1)
                {
                    memset(buff, 0, sizeof(buff));
                    received = recv(sock2, buff, BUFFER, 0);
                    if (received <= 0){
                        printf("error\n");
                        break;
                    }
                    buff[received] = '\0';
                    for (int i = 0; i < received; i++)
                    {
                        if (buff[i] == '\n' || buff[i] == '\0')
                        {
                            line[line_pos] = '\0';
                            clientreqeuset some;
                            strcpy(some.request , "APPEND");
                            strcpy(some.path , arr);
                            strcpy(some.data,line);
                            printf("%s\n",line);
                            int sock3=connect_to_ss(storageinfo[source-1].ip , storageinfo[source-1].client_port);
                            if(send(sock3 , &some , sizeof(some) , 0)<0){
                                printf("Error : %d\n", ERR_SEND_FAILED);
                                close(sock2);
                                close(sock3);
                                return -1;
                            }
                            line_pos = 0;
                            close(sock3);
                        }
                        else
                            line[line_pos++] = buff[i];
                    }
                }
                close(sock2);
            }
        } 
    }
    return 0;
}

void *handle_client(void *socket){
    int *client_socket = (int *)socket;
    char dump[BUFFER];
    snprintf(dump,sizeof(dump),"Client connected with socket %d\n",*client_socket);
    logmessage(dump);
    printf("client %d connected\n",*client_socket);
    clientreqeuset req;
    while(1){
        memset(&req,0,sizeof(req));
        printf("waiting for the request from the client %d\n",*client_socket);
        int bytes=recv((*client_socket), &req, sizeof(req)+8, 0);
        printf("bytes %d\n",bytes);
        if(bytes<=0){
            printf("Error : %d\n", ERR_CLIENT_DISCONNECTED);
            close((*client_socket));
            pthread_exit(NULL);
        }
        
        char buffer[BUFFER];
        if(strcmp("COPY",req.request)==0){
            snprintf(buffer,sizeof(buffer),"Request from client-%d: %s from %s to %s\n",*client_socket,req.request,req.path,req.dest);
        }
        else if(strcmp("GETLIST",req.request)==0){
            snprintf(buffer,sizeof(buffer),"Request from client-%d: %s\n",*client_socket,req.request);
        }
        else{
            snprintf(buffer,sizeof(buffer),"Request from client-%d: %s for %s\n", *client_socket,req.request,req.path);
        }
        printf("%s",buffer);
        logmessage(buffer);
        if(strcmp(req.request,"CREATE")&&strcmp(req.request,"DELETE")&&strcmp(req.request,"COPY")&&strcmp(req.request,"WRITE")&&strcmp(req.request,"READ")&&strcmp(req.request,"STREAM")&&strcmp(req.request,"GETINFO")&&strcmp(req.request,"GETLIST")){
            printf("Invalid request\n");
            nsreply reply;
            strcpy(reply.ip,"Invalid request\0");
            if(send(*client_socket,&reply,sizeof(reply),0)<0){
                printf("Error : %d\n", ERR_SEND_FAILED);
                close(*client_socket);
                pthread_exit(NULL);   
            }
            snprintf(buffer,sizeof(buffer),"Invalid request from the client-%d\n",*client_socket);
            logmessage(buffer);
            continue;
        }
        if(strcmp("GETLIST",req.request)==0){
            printf("sending the list of accessible paths\n");
            char temp[BUFFER];
            printf("ss_count %d\n",ss_count);
            int bytes=send(*client_socket,&ss_count,sizeof(ss_count),0);
            if(bytes<=0){
                printf("Error : %d\n", ERR_SEND_FAILED);
                close(*client_socket);
                pthread_exit(NULL);   
            }
            for(int i=0;i<ss_count;i++){
                if(send(*client_socket,&storageinfo[i].num_paths,sizeof(storageinfo[i].num_paths),0)<0){
                    printf("Error : %d\n", ERR_SEND_FAILED);
                    close(*client_socket);
                    pthread_exit(NULL);   
                }
                for(int j=0;j<storageinfo[i].num_paths;j++){
                    usleep(1000);
                    if(send(*client_socket,&storageinfo[i].paths[j],sizeof(storageinfo[i].paths[j]),0)<0){
                        printf("Error : %d\n", ERR_SEND_FAILED);
                        close(*client_socket);
                        pthread_exit(NULL);   
                    }
                }
            }
            snprintf(temp,sizeof(temp),"List of accessible paths sent to the client-%d\n",*client_socket);
            logmessage(temp);
            continue;
        }
        //find the path
        char reqpath[MAX_PATH_LEN];
        strcpy(reqpath,req.path);
        if(strcmp("CREATE",req.request)==0){
            char *ptr=strrchr(reqpath,'/');
            if(ptr==NULL){
                strcpy(reqpath,".");
            }
            else{
                *ptr='\0';
            }
        }
        printf("reqpath %s\n",reqpath);

        int ss=1;

        pthread_mutex_lock(&lru_mutex);
        ss=checklru(lru,reqpath);
        pthread_mutex_unlock(&lru_mutex);
        printf("lru fount ss %d\n",ss);
        if(ss==-1){
            pthread_mutex_lock(&trie_mutex);
            ss=checktrie(root,reqpath);
            pthread_mutex_unlock(&trie_mutex);
            printf("trie found ss %d\n",ss);
            if(ss==-1){
                printf(RED"Path not found\n"ESC);
                nsreply reply;
                strcpy(reply.ip,"Path not found\0");
                if(send(*client_socket,&reply,sizeof(reply),0)<0){
                    printf("Error : %d\n", ERR_SEND_FAILED);
                    close(*client_socket);
                    pthread_exit(NULL);   
                }
                snprintf(buffer,sizeof(buffer),"Path not found for the request of the client-%d\n",*client_socket);
                logmessage(buffer);
                continue;
            }
            else{
                pthread_mutex_lock(&lru_mutex);
                addlru(lru,reqpath,ss);
                pthread_mutex_unlock(&lru_mutex);
            }
        }
        else{
            printf(GREEN"Path found in LRU\n"ESC);
            pthread_mutex_lock(&lru_mutex);
            updatelru(lru,reqpath);
            pthread_mutex_unlock(&lru_mutex);
        }
        int ssd=ss;
        if(strcmp("COPY",req.request)==0){

            pthread_mutex_lock(&lru_mutex);
            ssd=checklru(lru,req.dest);
            pthread_mutex_unlock(&lru_mutex);
            printf("lru fount ss %d\n",ssd);
            if(ssd==-1){
                pthread_mutex_lock(&trie_mutex);
                ssd=checktrie(root,req.dest);
                pthread_mutex_unlock(&trie_mutex);
                printf("trie found ss %d\n",ssd);
                if(ssd==-1){
                    printf(RED"Path not found\n"ESC); 
                    nsreply reply;
                    strcpy(reply.ip,"Path not found\0");
                    if(send(*client_socket,&reply,sizeof(reply),0)<0){
                        printf("Error : %d\n", ERR_SEND_FAILED);
                        close(*client_socket);
                        pthread_exit(NULL);   
                    }
                    snprintf(buffer,sizeof(buffer),"Path not found for the request of the client-%d\n",*client_socket);
                    logmessage(buffer);
                    continue;
                }
                else{
                    pthread_mutex_lock(&lru_mutex);
                    addlru(lru,req.dest,ssd);
                    pthread_mutex_unlock(&lru_mutex);
                }
            }
            else{
                printf(GREEN"Path found in LRU\n"ESC);
                pthread_mutex_lock(&lru_mutex);
                updatelru(lru,req.dest);
                pthread_mutex_unlock(&lru_mutex);
            }
        }
        printlru(lru);
        req.ss=ss;
        req.ds=ssd;

        snprintf(buffer,sizeof(buffer),"found the storage server for the client path %s is %d\n",reqpath,ss);
        printf("%s",buffer);
        logmessage(buffer);

        if(storageinfo[ssd-1].status==0||storageinfo[ss-1].status==0){
            char ack1[BUFFER];
            nsreply reply;
            strcpy(reply.ip,"Storage server is down\0");
            if(send(*client_socket,&reply,sizeof(reply),0)<0){
                printf("Error : %d\n", ERR_SEND_FAILED);
                close(*client_socket);
                pthread_exit(NULL);   
            }
            snprintf(ack1,sizeof(ack1),"Storage server is down for the request of the client-%d\n",*client_socket);
            logmessage(ack1);
            continue;
        }
        if(strcmp("CREATE",req.request)==0){
            int ss_sock=connect_to_ss(storageinfo[ssd-1].ip,storageinfo[ssd-1].client_port);
            printf("connection %d\n",ss);
            if(send(ss_sock, &req, sizeof(req), 0)<0){
                printf("Error : %d\n", ERR_SEND_FAILED);
                send(*client_socket,"Failed to send the request to the storage server",sizeof("Failed to send the request to the storage server"),0);
                printf("Failed to send the request to the server\n");
                continue;
            }
            char ack[ACK];
            printf("working\n");
            if(recv(ss_sock,ack,sizeof(ack),0)<0){
                printf(" acknowlege is failed received\n");
                send(*client_socket,"ack failed",sizeof("ack failed"),0);
                continue;
            }
            printf("ack for the given request is %s\n",ack);
            if(strcmp(ack,"directory created")==0){
                pthread_mutex_lock(&trie_mutex);
                inserttrie(root,req.path,ss);
                pthread_mutex_unlock(&trie_mutex);
                pthread_mutex_lock(&ss_lock);
                storageinfo[ssd-1].num_paths++;
                strcpy(storageinfo[ssd-1].paths[storageinfo[ssd-1].num_paths-1],req.path);
                pthread_mutex_unlock(&ss_lock);
            }
            close(ss_sock);
            printf("working\n");
            nsreply ackno;
            strcpy(ackno.ip,ack);
            if(send(*client_socket,&ackno,sizeof(ackno),0)<0){
                printf("Error : %d\n", ERR_SEND_FAILED);
                close(*client_socket);
                return NULL;
            }
            char ack2[BUFFER];
            snprintf(ack2,sizeof(ack2),"acknowlege received for the request of the client-%d is %s\n",*client_socket,ack);
            logmessage(ack2);
            continue;
        }
        else if(strcmp("DELETE",req.request)==0){
            int ss_sock=connect_to_ss(storageinfo[ssd-1].ip,storageinfo[ssd-1].client_port);
            if(send(ss_sock, &req, sizeof(req), 0)<0){
                send(*client_socket,"Failed to send the request to the storage server",sizeof("Failed to send the request to the storage server"),0);
                printf("Failed to send the request to the server\n");
                continue;
            }
            char ack[ACK];
            printf("working\n");
            if(recv(ss_sock,ack,sizeof(ack),0)<0){
                printf(" acknowlege is failed received\n");
                send(*client_socket,"ack failed",sizeof("ack failed"),0);
                continue;
            }
            close(ss_sock);
            printf("ack for the given request is %s\n",ack);
            if(strcmp(ack,"directory deleted")==0){
                pthread_mutex_lock(&trie_mutex);
                deletetrie(root,req.path);
                pthread_mutex_unlock(&trie_mutex);
                pthread_mutex_lock(&ss_lock);
                for(int i=0;i<storageinfo[ssd-1].num_paths;i++){
                    if(strcmp(storageinfo[ssd-1].paths[i],req.path)==0){
                        for(int j=i;j<storageinfo[ssd-1].num_paths-1;j++){
                            strcpy(storageinfo[ssd-1].paths[j],storageinfo[ssd-1].paths[j+1]);
                        }
                        storageinfo[ssd-1].num_paths--;
                        break;
                    }
                }
                pthread_mutex_unlock(&ss_lock);
            }
            nsreply ackno;
            strcpy(ackno.ip,ack);
            if(send(*client_socket,&ackno,sizeof(ackno),0)<0){
                printf("Error : %d\n", ERR_SEND_FAILED);
                close(*client_socket);
                return NULL;
            }
            char ack3[BUFFER];
            snprintf(ack3,sizeof(ack3),"acknowlege received for the request of the client-%d is %s\n",*client_socket,ack);
            logmessage(ack3);
            continue;
        }
        else if(strcmp("COPY",req.request)==0){
            char ack[ACK];
            int temp=copying(ss,ssd,req.path,req.dest);
            if(temp<0){
                strcpy(ack,"Failed to copy the directory");
            }
            else{
                if(strstr(req.path,".")!=NULL){
                    strcpy(ack,"file copied");
                }
                else
                 strcpy(ack,"directory copied");
            }
            printf("ack %s\n",ack);
            if(strstr(ack,"copied")!=NULL){
                char *temp=strstr(req.path,req.path);
                char *temp1=strrchr(req.path,'/');
                int l=temp1-temp+1;
                pthread_mutex_lock(&ss_lock);
                copypaths(ss,ssd,req.path,req.dest);
                pthread_mutex_unlock(&ss_lock);
            }
            nsreply ackno;
            strcpy(ackno.ip,ack);
            if(send(*client_socket,&ackno,sizeof(ackno),0)<0){
                printf("Error : %d\n", ERR_SEND_FAILED);
                close(*client_socket);
                return NULL;
            }
            char ack4[BUFFER];
            snprintf(ack4,sizeof(ack4),"acknowlege received for the request of the client-%d is %s\n",*client_socket,ack);
            logmessage(ack4);
            continue;
        }
        else if(strcmp("WRITE",req.request)==0){
            trienode *temp=check_write(root,req.path);
            if(temp->writeflag==1){
                nsreply reply;
                strcpy(reply.ip,"File is being written by other client , try again\0");
                if(send(*client_socket,&reply,sizeof(reply),0)<0){
                    printf("Error : %d\n", ERR_SEND_FAILED);
                    close(*client_socket);
                    pthread_exit(NULL);   
                }
                char ack5[BUFFER];
                snprintf(ack5,sizeof(ack5),"File is already written for the request of the client-%d\n",*client_socket);
                logmessage(ack5);
                continue;
            }
            nsreply storage;
            storage.port=storageinfo[ss-1].client_port;
            strcpy(storage.ip,storageinfo[ss-1].ip);
            temp->writeflag=1;
            printf("storage server ip %s port %d\n",storage.ip,storage.port);
            if(send(*client_socket,&storage,sizeof(storage),0)<0){
                send(*client_socket,"Failed to send the data to the client",sizeof("Failed to send the data to the client"),0);
                printf("Failed to send the data to the client\n");
                continue;
            }
            char ack6[BUFFER];
            snprintf(ack6,sizeof(ack6),"Storage server ip %s and port %d sent to the client-%d\n",storage.ip,storage.port,*client_socket);
            logmessage(ack6);
            char ack[BUFFER];
            if(recv(*client_socket,ack,sizeof(ack),0)<0){
                printf("Error : %d\n", ERR_RECV_FAILED);
                close(*client_socket);
                pthread_exit(NULL);
            }
            char ack8[BUFFER+100];
            snprintf(ack8,sizeof(ack8),"acknowlege received for the request of the client-%d is %s\n",*client_socket,ack);
            logmessage(ack8);
            if(strstr(ack,"asyncronous")==NULL){
                temp->writeflag=0;
            }
        }
        else if(strcmp("READ",req.request)==0||strcmp("STREAM",req.request)==0||strcmp("GETINFO",req.request)==0){
            trienode *temp=check_write(root,req.path);
            if(temp->writeflag==1){
                nsreply reply;
                strcpy(reply.ip,"File is being written by other client , try again\0");
                if(send(*client_socket,&reply,sizeof(reply),0)<0){
                    printf("Error : %d\n", ERR_SEND_FAILED);
                    close(*client_socket);
                    pthread_exit(NULL);   
                }
                char ack9[BUFFER];
                snprintf(ack9,sizeof(ack9),"File is already written for the request of the client-%d\n",*client_socket);
                logmessage(ack9);
                continue;
            }
            nsreply storage;
            storage.port=storageinfo[ss-1].client_port;
            strcpy(storage.ip,storageinfo[ss-1].ip);
            printf("storage server ip %s port %d\n",storage.ip,storage.port);
            if(send(*client_socket,&storage,sizeof(storage),0)<0){
                send(*client_socket,"Failed to send the data to the client",sizeof("Failed to send the data to the client"),0);
                printf("Failed to send the data to the client\n");
                continue;
            }
            char ack10[BUFFER];
            snprintf(ack10,sizeof(ack10),"Storage server ip %s and port %d sent to the client-%d\n",storage.ip,storage.port,*client_socket);
            logmessage(ack10);
            char ack[BUFFER];
            if(recv(*client_socket,ack,sizeof(ack),0)<0){
                printf("Error : %d\n", ERR_RECV_FAILED);
                close(*client_socket);
                pthread_exit(NULL);
            }
            char ack11[BUFFER+100];
            snprintf(ack11,sizeof(ack11),"acknowlege received for the request of the client-%d is %s\n",*client_socket,ack);
            logmessage(ack11);
            // if(strstr())
        }
    }
    free(client_socket);
}


void *handle_ss(void *arg){
    storageserverinfo c;
    int socket_i = *((int*)arg);
    size_t temp=sizeof(c.client_port)+sizeof(c.ip)+sizeof(c.status)+sizeof(c.num_paths)+sizeof(c.paths);
    printf("size of the structure %ld -%ld\n",temp,sizeof(c));
    if(recv(socket_i, &c, sizeof(storageserverinfo), 0)<0){
        printf("Error : %d\n", ERR_RECV_FAILED);
        close(socket_i);
        free(arg);
        pthread_exit(NULL);
    }
    int flag=0;
    printf("number of clients %d\n",ss_count);
    for(int i=0;i<ss_count;i++){
        if(storageinfo[i].client_port==c.client_port){
            printf("Storage server %d again connected\n",i+1);
            storageinfo[i].status=1;
            flag=1;
            break;
        }
    }
    int ssid;
    if(flag==0){
        pthread_mutex_lock(&ss_lock);
        ss_count++;
        ssid=ss_count;
        pthread_mutex_unlock(&ss_lock);
        storageinfo[ssid-1].client_port = c.client_port;
        strcpy(storageinfo[ssid-1].ip, c.ip);
        storageinfo[ssid-1].status = c.status;
        storageinfo[ssid-1].num_paths = c.num_paths;
        printf("number of paths %d\n",storageinfo[ssid-1].num_paths);
        for(int i=0;i<storageinfo[ssid-1].num_paths;i++){
            strcpy(storageinfo[ssid-1].paths[i],c.paths[i]);
        }
        
        pthread_mutex_lock(&trie_mutex);
        for(int i=0;i<storageinfo[ssid-1].num_paths;i++){
            inserttrie(root,storageinfo[ssid-1].paths[i],ssid);
        }
        pthread_mutex_unlock(&trie_mutex);
    }
    if(flag==1){
        if(c.num_paths!=storageinfo[ssid-1].num_paths){
            printf("number of paths are different\n");
            close(socket_i);
        }
        for(int i=0;i<storageinfo[ssid-1].num_paths;i++){
            if(strcmp(storageinfo[ssid-1].paths[i],c.paths[i])!=0){
                printf("paths are different\n");
                close(socket_i);
            }
        }
    }
    printf("paths present %d\n",storageinfo[ssid-1].num_paths);
    // for(int i=0;i<storageinfo[ssid-1].num_paths;i++){
    //     printf("path%d %s\n",i,storageinfo[ssid-1].paths[i]);
    // }
    // if(ssid==3){
    //     backup(3,1,2);
    //     backup(1,3,2);
    //     backup(2,1,3);
    // }
    // if(ssid>3){
    //     backup(ssid,ssid/2,ssid/2+1);
    // }

    char buffer[BUFFER];
    snprintf(buffer,sizeof(buffer),"Storage server connected with ip %s and port %d\n",c.ip,c.client_port);
    printf("%s",buffer);
    logmessage(buffer);
    printf("Total storage server connected %d\n",ss_count);
    close(socket_i);
    int soc;
    //  heartbeat for the failure detection
    clientreqeuset req;
    strcpy(req.request,"PING");
    while(1){
        soc=connect_to_ss(c.ip,c.client_port);
        if(send(soc,&req,sizeof(req),0)<0){
            snprintf(buffer,sizeof(buffer),"storage server %d is diconnected\n",ssid);
            printf("storage server %d is Diconnected\n",ssid);
            logmessage(buffer);
            pthread_mutex_lock(&ss_lock);
            storageinfo[ssid-1].status=0;
            pthread_mutex_unlock(&ss_lock);
            close(soc);
            free(arg);
            pthread_exit(NULL);
        }
        else{
            char ack[ACK];
            if(recv(soc,&ack,sizeof(ack),0)<0){
                snprintf(buffer,sizeof(buffer),"Storage server %d is diconnected\n",ssid);
                printf("storage server %d is diconnected\n",ssid);
                logmessage(buffer);
                pthread_mutex_lock(&ss_lock);
                storageinfo[ssid-1].status=0;
                pthread_mutex_unlock(&ss_lock);
                close(soc);
                free(arg);
                pthread_exit(NULL);
            }
            close(soc);
        }
    }
}

void *handle_wait(void *arg){
    printf("unknown request\n");
    int *sock=(int *)arg;
    int socket_i=*sock;
    clientreqeuset req;
    if(recv(*sock,&req,sizeof(req),0)<0){
        printf("Error : %d\n", ERR_RECV_FAILED);
        close(socket_i);
        free(sock);
        pthread_exit(NULL);
    }
    char path[MAX_PATH_LEN];
    strcpy(path,req.path);

    if(strcmp(req.request,"ACK")==0){
        trienode *temp=check_write(root,path);
        if(temp==NULL){
            printf("null returned\n");
            close(socket_i);
            free(sock);
            pthread_exit(NULL);
        }
        pthread_mutex_lock(&trie_mutex);
        temp->writeflag=0;
        pthread_mutex_unlock(&trie_mutex);
    }
    else{
        trienode *temp=check_write(root,path);
        if(temp==NULL){
            printf("null returned\n");
            close(socket_i);
            free(sock);
            pthread_exit(NULL);
        }
        while(temp->writeflag==1){
            usleep(1000);
        }
        nsreply reply;
        strcpy(reply.ip,"Asyncronous file writting is successfull\0");
        if(send(socket_i,&reply,sizeof(reply),0)<0){
            printf("Error : %d\n", ERR_SEND_FAILED);
            close(socket_i);
            free(sock);
            pthread_exit(NULL);
        }
    }
    close(socket_i);
    free(sock);
    pthread_exit(NULL);
}

int main()
{
    int server_fd;
    int opt = 1;
    client_count=0;
    ss_count=0;
    root=(trienode *)malloc(sizeof(trienode));
    lru=createLRU(5);
    pthread_mutex_init(&ss_lock, NULL);
    pthread_mutex_init(&lru_mutex, NULL);
    pthread_mutex_init(&trie_mutex, NULL);
    pthread_mutex_init(&log_message_mutex, NULL);
    for(int i=0;i<SIZE;i++){
        root->c[i]=NULL;
    }
    struct sockaddr_in address;
    int addrlen;
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0)
    {
        printf("Error : %d\n", ERR_SOCKET_FAILED);
        exit(-1);
    }
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        printf("Error : %d\n", ERR_SETSOCKOPT_FAILED);
        exit(EXIT_FAILURE);
    }
    addrlen = sizeof(address);
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);
    if (bind(server_fd, (struct sockaddr *)&(address), sizeof(address)) < 0)
    {
        printf("Error : %d\n", ERR_BIND_FAILED);
        close(server_fd);
        exit(-1);
    }
    if (listen(server_fd, MAX_SS) < 0)
    {
        printf("Error : %d\n", ERR_LISTEN_FAILED);
        close(server_fd);
        exit(-1);
    }
    printf("Server is listening on port %d and ip = %s...\n", PORT, IP);
    while (1)
    {
        int socket_i;
        if ((socket_i = accept(server_fd, (struct sockaddr *)&(address), (socklen_t *)&(addrlen))) < 0)
        {
            printf("Error : %d\n", ERR_ACCEPT_FAILED);
            close(server_fd);
            exit(-1);
        }
        int *sock=(int *)malloc(sizeof(int));
        int flag;
        recv(socket_i, &flag, sizeof(int), 0);
        *sock = socket_i;
        pthread_t thread;
        if(flag == 1){
            if(ss_count>=MAX_SS){
                printf(RED"Storage server limit reached\n"ESC);
                close(socket_i);
                continue;
            }
            printf(GREEN"Storage server connected\n"ESC);
            pthread_create(&thread, NULL, handle_ss, sock);
        }
        else if(flag == 0){
            if(client_count>=MAX_Client){
                printf(RED"Client limit reached\n"ESC);
                close(socket_i);
                continue;
            }
            printf(GREEN"Client connected\n"ESC);
            client_socket[client_count]=socket_i;
            client_count++;
            pthread_create(&thread, NULL, handle_client, sock);
        }
        else if(flag==2){
            pthread_create(&thread,NULL,handle_wait,sock);
        }
        pthread_detach(thread);
    }
    return 0;
}