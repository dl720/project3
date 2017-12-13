//sorter_server.c
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/sendfile.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>
#include <errno.h>
#include "sorter_thread.h"
#include <fcntl.h>
#include <string.h>
#include <netinet/in.h>

int movInd;
//char* movies;
struct Movie** movies;
int* threadCount;//counter to join all threads in while loop
pthread_t* TIDs;// stores all TIDS
pthread_mutex_t arrayLock;

static void* threadService(void* arg){
	FILE* data = fdopen((int)arg, "r");
	char garbage[1024];
	fgets(garbage,1024,data);	//gets rid of first line
	pthread_mutex_lock(&arrayLock);
	while(fgets(movies[movInd],1024,data) != NULL){
		movInd++;
	} 
	pthread_mutex_unlock(&arrayLock);
 	fclose(data);
	pthread_exit(NULL);
}

int main(int argc, char* argv[]){

	if(argc != 3){
		printf("incorrect input format, correct format is ./sorter_server -p [port_num]\n");
		return -1;
	}
	char *trash;
	int port;
	if(strcmp(argv[1],"-p")==0){
		port=strtol(argv[2],&trash,10);//strtol returns a long int
	}
	else{
		printf("incorrect input format, correct format is ./sorter_server -p [port_num]\n");
		return -1;
	}
	TIDs=malloc(sizeof(pthread_t)*2000);
	int i;
	for(i=0;i<1024;i++){
		movies[i]=malloc(sizeof(struct Movie)*10000);
	}
	threadCount=malloc(sizeof(int));
	*threadCount=0;
	movInd = 0;
	setbuf(stdout, NULL);
	movies = malloc(500000*sizeof(struct Movie));
	//int port = argv[2];
	struct sockaddr_in address;
	int sockfd;
	int clientfd;
	pthread_mutex_init(&arrayLock, NULL);

	if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) <= 0)
	{
		printf("error: socket creation failed\n");
		return -1;
	}

	address.sin_family = AF_INET;
	address.sin_addr.s_addr = INADDR_ANY;
	address.sin_port = htons(port);

	if (bind(sockfd, (struct sockaddr*)&address, sizeof(address)) < 0){
		printf("error: binding socket failed\n");
		close(sockfd);
		return -1;
	}

	if(listen(sockfd,5)<0){
		printf("error: listening failed\n");
		close(sockfd);
		return -1;
	}

	while(true){

		clientfd = accept(sockfd,NULL,NULL);
		
		if(clientfd < 1){
			printf("error: accept failed\n");
			close(sockfd);
			return -1;
		}

		//print the client IP

		int arg = clientfd;
		//pthread_t thrd[1];
		char* mode;
		read(clientfd,&mode,1);
		if(*mode == 'f'){
			pthread_create(&TIDs[*threadCount],NULL,&threadService,(void*)arg);
			(*threadCount)++;
		}


	}
	close(sockfd);
	return 0;


}
