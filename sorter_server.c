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

char* buffer;
struct Movie* movies;
int ARRINDEX; 
int* threadCount;//counter to join all threads in while loop
pthread_t* TIDs;// stores all TIDs
pthread_mutex_t arrayLock;
pthread_mutex_t argLock;


static void* threadService(void* arg){
	int FD = *(int*)arg;
	FILE* data = fdopen(FD, "r");
	char garbage[1024];
	fgets(garbage,1024,data);	//gets rid of first line
	char* strbuf[1024];  //setup for getline
	memset(strbuf,0,1024);
	char* delim = ",\0";
	char* delim2 = "\"\0";
	size_t linesize = 1024;
	pthread_mutex_lock(&arrayLock);
	while(getline(strbuf,&linesize,data) >= 28){
		int fieldcounter = 0;
		char* token;
		do{
			char token[150] = {0};
			strcat(token, strsep(strbuf, delim));
			//printf("%s",token);
			if (token[0] == 34){		//handles the titles with "," in them, all movies that have that have quotes around name
	 			int i = 0;
	 	 		while(1){
		 			if (token[i] == '\0'){
		 				token[i] = ',';
		 				strcat(token, strsep(strbuf, delim2));
		 				strsep(strbuf, delim);
		 				movies[ARRINDEX].movie_title = (char*)malloc(sizeof(char)*(strlen(token)+1));
		 				strcpy(movies[ARRINDEX].movie_title,token);
		 				break;
		 			}
		 			i++;
		 		}
			}
			switch (fieldcounter) {
				case 0:
					movies[ARRINDEX].color = (char*)malloc(sizeof(char)*(strlen(token)+1));
					strcpy(movies[ARRINDEX].color,token);
					break;
				case 1:
					movies[ARRINDEX].director_name = (char*)malloc(sizeof(char)*(strlen(token)+1));
					strcpy(movies[ARRINDEX].director_name,token);
					break;
				case 2:
					movies[ARRINDEX].num_critic_for_reviews = atoi(token);
					break;
				case 3:
					movies[ARRINDEX].duration = atoi(token);
					break;
				case 4:
					movies[ARRINDEX].director_facebook_likes = atoi(token);
					break;
				case 5:
					movies[ARRINDEX].actor_3_facebook_likes = atoi(token);
					break;
				case 6:
					movies[ARRINDEX].actor_2_name = (char*)malloc(sizeof(char)*(strlen(token)+1));
					strcpy(movies[ARRINDEX].actor_2_name,token);
					break;
				case 7:
					movies[ARRINDEX].actor_1_facebook_likes = atoi(token);
					break;
				case 8:
					movies[ARRINDEX].gross = atoi(token);
					break;				
				case 9:
					movies[ARRINDEX].genres = (char*)malloc(sizeof(char)*(strlen(token)+1));
					strcpy(movies[ARRINDEX].genres,token);
					break;
				case 10:
					movies[ARRINDEX].actor_1_name = (char*)malloc(sizeof(char)*(strlen(token)+1));
					strcpy(movies[ARRINDEX].actor_1_name,token);
					break;
				case 11:
					if(movies[ARRINDEX].movie_title == NULL){
						movies[ARRINDEX].movie_title = (char*)malloc(sizeof(char)*(strlen(token)+1));
						strcpy(movies[ARRINDEX].movie_title,token);
					}
					break;
				case 12:
					movies[ARRINDEX].num_voted_users = atoi(token);
					break;
				case 13:
					movies[ARRINDEX].cast_total_facebook_likes = atoi(token);
					break;
				case 14:
					movies[ARRINDEX].actor_3_name = (char*)malloc(sizeof(char)*(strlen(token)+1));
					strcpy(movies[ARRINDEX].actor_3_name,token);
					break;
				case 15:
					movies[ARRINDEX].facenumber_in_poster = atoi(token);
					break;
				case 16:
					movies[ARRINDEX].plot_keywords = (char*)malloc(sizeof(char)*(strlen(token)+1));
					strcpy(movies[ARRINDEX].plot_keywords,token);
					break;
				case 17:
					movies[ARRINDEX].movie_imdb_link = (char*)malloc(sizeof(char)*(strlen(token)+1));
					strcpy(movies[ARRINDEX].movie_imdb_link,token);
					break;
				case 18:
					movies[ARRINDEX].num_user_for_reviews = atoi(token);
					break;
				case 19:
					movies[ARRINDEX].language = (char*)malloc(sizeof(char)*(strlen(token)+1));
					strcpy(movies[ARRINDEX].language,token);
					break;
				case 20:
					movies[ARRINDEX].country = (char*)malloc(sizeof(char)*(strlen(token)+1));
					strcpy(movies[ARRINDEX].country,token);
					break;
				case 21:
					movies[ARRINDEX].content_rating = (char*)malloc(sizeof(char)*(strlen(token)+1));
					strcpy(movies[ARRINDEX].content_rating,token);
					break;
				case 22:
					movies[ARRINDEX].budget = atoi(token);
					break;
				case 23:
					movies[ARRINDEX].title_year = atoi(token);
					break;
				case 24:
					movies[ARRINDEX].actor_2_facebook_likes = atoi(token);
					break;
				case 25:
					movies[ARRINDEX].imdb_score = atof(token);
					break;
				case 26:
					movies[ARRINDEX].aspect_ratio = atof(token);
					break;
				case 27:
					movies[ARRINDEX].movie_facebook_likes = atoi(token);
					break;
			}
			fieldcounter++;
		}
		while(fieldcounter<=27);
		printf("%d",ARRINDEX);
		ARRINDEX++;
	} 
	pthread_mutex_unlock(&arrayLock);
	char* ret = malloc(sizeof(char*));
	*ret = 'y';
	printf("sending close...");
	send(FD,(void*)&ret,1,0);
	printf("close sent");
 	fclose(data);
 	close(FD);
 	free(ret);
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
	TIDs = malloc(sizeof(pthread_t)*10000);
	int** argList = malloc(sizeof(int*)*10000);
	int i;
	movies = malloc(500000*sizeof(struct Movie));
	threadCount=malloc(sizeof(int));
	*threadCount=0;
	setbuf(stdout, NULL);
	
	pthread_t children[1024];		//max files open allowed by ilabs
	int childCount = 0;
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

	if(listen(sockfd,10)<0){
		printf("error: listening failed\n");
		close(sockfd);
		return -1;
	}

	printf("Recieved connections from: ");
	struct sockaddr* incoming = malloc(sizeof(struct sockaddr*));

	while(1){

		int* len = malloc(sizeof(int*));
		*len = sizeof(struct sockaddr);
		clientfd = accept(sockfd,incoming,len);
		
		if(clientfd < 1){
			printf("error: accept failed\n");
			close(sockfd);
			return -1;
		}

		int addr = getpeername(clientfd, incoming, len);
		printf("%s",incoming->sa_data);
		argList[*threadCount] = malloc(sizeof(int*));
		*(argList[*threadCount]) = clientfd;
		int* fsize = malloc(sizeof(int*));
		char* mode = malloc(sizeof(char*));
		recv(clientfd,(void*)mode,1,0);
		recv(clientfd,(void*)fsize,sizeof(int),0);
		printf("%c",*mode);
		if(*mode == 'f'){
			pthread_create(&TIDs[*threadCount],NULL,&threadService,(void*)argList[*threadCount]);
			(*threadCount)++;
		}
		if(*mode == 'd'){
			for(;*threadCount>0;(*threadCount)--){
				pthread_join(TIDs[*threadCount-1],NULL);
			}
			int* field = malloc(sizeof(int*));
			recv(clientfd,(void*)&field,sizeof(int),0);
			printf("%d", *field);
			mergesort(movies,0,ARRINDEX,*field);
			free(field);
		}
		free(mode);
		free(fsize);


	}
	//still need to join
	return 0;


}