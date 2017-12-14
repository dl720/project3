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

//int movInd;
//char* movies;
char* buffer;
struct Movie** movies;
int* numrows;
int* arrIndex; 
int* threadCount;
int* threadCount;//counter to join all threads in while loop
pthread_t* TIDs;// stores all TIDS
pthread_mutex_t arrayLock;
pthread_mutex_t arrIndexLock;


static void* threadService(void* arg){
	FILE* data = fdopen((int)arg, "r");
	pthread_mutex_lock(&arrIndexLock);
	int ARRINDEX=*arrIndex;
	(*arrIndex)++;
	pthread_mutex_unlock(&arrIndexLock);
	char garbage[1024];
	fgets(garbage,1024,data);	//gets rid of first line
	pthread_mutex_lock(&arrayLock);
	char* strbuf[1024];  //setup for fgets
	int rownum=0;
	memset(strbuf,0,1024);
	char* delim = ",\0";
	char* delim2 = "\"\0";
	while(fgets(movies[ARRINDEX],1024,data) != NULL){
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
		 				movies[ARRINDEX][rownum].movie_title = (char*)malloc(sizeof(char)*(strlen(token)+1));
		 				strcpy(movies[ARRINDEX][rownum].movie_title,token);
		 				break;
		 			}
		 			i++;
		 		}
			}
			switch (fieldcounter) {
				case 0:
					movies[ARRINDEX][rownum].color = (char*)malloc(sizeof(char)*(strlen(token)+1));
					strcpy(movies[ARRINDEX][rownum].color,token);
					break;
				case 1:
					movies[ARRINDEX][rownum].director_name = (char*)malloc(sizeof(char)*(strlen(token)+1));
					strcpy(movies[ARRINDEX][rownum].director_name,token);
					break;
				case 2:
					movies[ARRINDEX][rownum].num_critic_for_reviews = atoi(token);
					break;
				case 3:
					movies[ARRINDEX][rownum].duration = atoi(token);
					break;
				case 4:
					movies[ARRINDEX][rownum].director_facebook_likes = atoi(token);
					break;
				case 5:
					movies[ARRINDEX][rownum].actor_3_facebook_likes = atoi(token);
					break;
				case 6:
					movies[ARRINDEX][rownum].actor_2_name = (char*)malloc(sizeof(char)*(strlen(token)+1));
					strcpy(movies[ARRINDEX][rownum].actor_2_name,token);
					break;
				case 7:
					movies[ARRINDEX][rownum].actor_1_facebook_likes = atoi(token);
					break;
				case 8:
					movies[ARRINDEX][rownum].gross = atoi(token);
					break;				
				case 9:
					movies[ARRINDEX][rownum].genres = (char*)malloc(sizeof(char)*(strlen(token)+1));
					strcpy(movies[ARRINDEX][rownum].genres,token);
					break;
				case 10:
					movies[ARRINDEX][rownum].actor_1_name = (char*)malloc(sizeof(char)*(strlen(token)+1));
					strcpy(movies[ARRINDEX][rownum].actor_1_name,token);
					break;
				case 11:
					if(movies[ARRINDEX][rownum].movie_title == NULL){
						movies[ARRINDEX][rownum].movie_title = (char*)malloc(sizeof(char)*(strlen(token)+1));
						strcpy(movies[ARRINDEX][rownum].movie_title,token);
					}
					break;
				case 12:
					movies[ARRINDEX][rownum].num_voted_users = atoi(token);
					break;
				case 13:
					movies[ARRINDEX][rownum].cast_total_facebook_likes = atoi(token);
					break;
				case 14:
					movies[ARRINDEX][rownum].actor_3_name = (char*)malloc(sizeof(char)*(strlen(token)+1));
					strcpy(movies[ARRINDEX][rownum].actor_3_name,token);
					break;
				case 15:
					movies[ARRINDEX][rownum].facenumber_in_poster = atoi(token);
					break;
				case 16:
					movies[ARRINDEX][rownum].plot_keywords = (char*)malloc(sizeof(char)*(strlen(token)+1));
					strcpy(movies[ARRINDEX][rownum].plot_keywords,token);
					break;
				case 17:
					movies[ARRINDEX][rownum].movie_imdb_link = (char*)malloc(sizeof(char)*(strlen(token)+1));
					strcpy(movies[ARRINDEX][rownum].movie_imdb_link,token);
					break;
				case 18:
					movies[ARRINDEX][rownum].num_user_for_reviews = atoi(token);
					break;
				case 19:
					movies[ARRINDEX][rownum].language = (char*)malloc(sizeof(char)*(strlen(token)+1));
					strcpy(movies[ARRINDEX][rownum].language,token);
					break;
				case 20:
					movies[ARRINDEX][rownum].country = (char*)malloc(sizeof(char)*(strlen(token)+1));
					strcpy(movies[ARRINDEX][rownum].country,token);
					break;
				case 21:
					movies[ARRINDEX][rownum].content_rating = (char*)malloc(sizeof(char)*(strlen(token)+1));
					strcpy(movies[ARRINDEX][rownum].content_rating,token);
					break;
				case 22:
					movies[ARRINDEX][rownum].budget = atoi(token);
					break;
				case 23:
					movies[ARRINDEX][rownum].title_year = atoi(token);
					break;
				case 24:
					movies[ARRINDEX][rownum].actor_2_facebook_likes = atoi(token);
					break;
				case 25:
					movies[ARRINDEX][rownum].imdb_score = atof(token);
					break;
				case 26:
					movies[ARRINDEX][rownum].aspect_ratio = atof(token);
					break;
				case 27:
					movies[ARRINDEX][rownum].movie_facebook_likes = atoi(token);
					break;
			}
			fieldcounter++;
		}
		while(fieldcounter<=27);
		ARRINDEX++;
		rownum++;
	} 
	pthread_mutex_unlock(&arrayLock);
 	fclose(data);
 	numrows[ARRINDEX]=rownum;
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
	movies = malloc(500000*sizeof(struct Movie));
	for(i=0;i<1024;i++){
		movies[i]=malloc(sizeof(struct Movie)*10000);
	}
	threadCount=malloc(sizeof(int));
	*threadCount=0;
	arrIndex=malloc(sizeof(int));
	*arrIndex=0;
	numrows=calloc(1024,sizeof(int));
	*threadCount=0;
	//movInd = 0;
	setbuf(stdout, NULL);
	
	pthread_t children[1024];		//max files open allowed by ilabs
	int childCount = 0;
	//int port = argv[2];
	struct sockaddr_in address;
	int sockfd;
	int clientfd;
	pthread_mutex_init(&arrayLock, NULL);
	pthread_mutex_init(&arrIndexLock,NULL);
	int totalLength = 0;
	int j=0;
	for (j;j<1024;j++){
		totalLength = totalLength + numrows[j];
	}
	struct Movie* movieSort = malloc(sizeof(struct Movie)*totalLength); //dump
	int k = 0;
	int n = 0;
	int m = 0;
	for(k; k < *arrIndex; k++){
		for(n = 0; n < numrows[k]; n++){
			movieSort[m] = movies[k][n];
			m++;
		}
	}
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
	//still need to join
	close(sockfd);
	return 0;


}