#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
#include <sys/sendfile.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <time.h>
#include <netdb.h>
#include <fcntl.h>
#include <pthread.h>
#include <errno.h>

//gcc -std=gnu99 -pthread sorter_thread1.c -o sorter_thread

pthread_mutex_t arrIndexLock;
pthread_mutex_t threadParamLock;
pthread_mutex_t createThreadLock;

char * hostName;//name of host
int port = -1;//port number

char* buffer;
char* in;
char* out;

int* threadCount; 
pthread_t* TIDs;

DIR* input;

struct dThreadInfo{
	char path[1000];
};

struct fThreadInfo{
	char path[1000];
};

static void* fileThread(void* arg){		//read in the lines from the given file if its a valid csv
	pthread_mutex_lock(&threadParamLock);
	struct fThreadInfo* data = (struct fThreadInfo*)arg;	//NEEDS MUTEX
	pthread_mutex_unlock(&threadParamLock);
	FILE * add = fopen(data->path, "a");
	fprintf(add, "@\n");
	fclose(add);
	FILE* file = fopen(data->path, "r");
	char line[1024] = {0};
	int commas=0;
	int i = 0;
	if(fgets(line, 1024, file)){
		char *temp = strdup(line);
		for(i=0;temp[i]!=0 ;i++){
			if(temp[i]==','){
				commas++;
			}
		}
		if(commas<27){
			printf("Incorrect csv format\n");
			pthread_exit(NULL);
		}
	}
	fseek(file, 0, SEEK_SET);	//resets the file
	struct sockaddr_in serverAddressInfo;
	struct hostent *serverIPAddress;
	struct stat stat_buf;
	int sockfd = -1;

	int fileD = fileno(file); 
	fstat(fileD, &stat_buf);
	serverIPAddress = gethostbyname(hostName);
	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	bzero((char*) &serverAddressInfo, sizeof(serverAddressInfo));
	serverAddressInfo.sin_family = AF_INET;
	serverAddressInfo.sin_port = htons(port);
	bcopy((char *)serverIPAddress->h_addr, (char*)&serverAddressInfo.sin_addr.s_addr, serverIPAddress->h_length);
	connect(sockfd,(struct sockaddr *)&serverAddressInfo,sizeof(serverAddressInfo));
	
	char fileAccept = 'f';

	send(sockfd, (void*) &fileAccept, sizeof(char), 0);
	send(sockfd, (void*) &stat_buf.st_size, sizeof(int), 0);

	sendfile(sockfd, fileD, NULL, stat_buf.st_size);
	char* closer = malloc(sizeof(char*));
	recv(sockfd,(void*)closer,1, 0);
	if (*closer == 'y') {
		close(sockfd);
		fclose(file);
	}
	free(closer);
	pthread_exit(NULL);
}

static void* dirThread(void* arg){		//continue traversing directory tree
	struct dThreadInfo* data = (struct dThreadInfo*)arg;	//NEEDS MUTEX
	pthread_t children[1024];
	int childCount = 0;
	DIR* inp = opendir(data->path);
	struct dirent* item;	
	
	while ((item = readdir(inp)) != NULL){
		//printf("%s\n",item->d_name);
		if (!strcmp(item->d_name, ".") || !strcmp(item->d_name, ".."))
			continue;
		
		if (item->d_type!=DT_DIR){
			//printf("not a dir\n");
			if (strstr(item->d_name, ".csv") != NULL){
				pthread_mutex_lock(&createThreadLock);
				struct fThreadInfo* param = malloc(sizeof(struct fThreadInfo));
				memset(param->path,'\0',1000);
				strcpy(param->path, data->path);
				strcat(param->path, "/\0");
				strcat(param->path, item->d_name);
				
				pthread_create(&TIDs[*threadCount],NULL,&fileThread,param);
				children[childCount] = TIDs[*threadCount];
				childCount++;
				(*threadCount)++;
				pthread_mutex_unlock(&createThreadLock);
				continue;
			}	
		}
		else{
			//printf("dir\n");
			opendir(item->d_name);
			pthread_mutex_lock(&createThreadLock);
			struct dThreadInfo* param = malloc(sizeof(struct dThreadInfo));
			memset(param->path,'\0',1000);
			strcpy(param->path, data->path);
			strcat(param->path, "/\0");
			strcat(param->path, item->d_name);
			
			pthread_create(&TIDs[*threadCount],NULL,&dirThread,param);
			children[childCount] = TIDs[*threadCount];
			childCount++;
			(*threadCount)++;
			pthread_mutex_unlock(&createThreadLock);
		}
		
	}
	closedir(inp);
	int i = 0;
	for(i;i<childCount;i++){
		pthread_join(children[i],NULL);
	}


	pthread_exit(NULL);
}


int main(int argc, char **argv){
	
	if(argc<=6||argc==8||argc==10||argc>=12){
		printf("Incorrect input\n");
		return 0;
	}
	buffer=(char*)malloc(sizeof(char)*25);
	in=(char*)malloc(sizeof(char)*1024);
	out=(char*)malloc(sizeof(char)*1024);
	getcwd(in,1024);
	getcwd(out,1024);
	if (argc == 7) {
		int i = 1;
		for (; i < 7; i += 2) {
			if (strcmp(argv[i], "-c") == 0) {
				strcpy(buffer, argv[i+1]);
			}
			else if (strcmp(argv[i], "-h") == 0) {
				hostName = argv[i+1];
			}
			else if (strcmp(argv[i], "-p") == 0) {
				port = atoi(argv[i+1]);
			}
			else {
				printf("Incorrect input\n");
				return -1;
			}
		}
	}
	else if (argc == 9) {
		int i = 1;
		for (; i < 9; i += 2) {
			if (strcmp(argv[i], "-c") == 0) {
				strcpy(buffer, argv[i+1]);
			}
			else if (strcmp(argv[i], "-h") == 0) {
				hostName = argv[i+1];
			}
			else if (strcmp(argv[i], "-p") == 0) {
				port = atoi(argv[i+1]);
			}
			else if (strcmp(argv[i], "-d") == 0) {
				strcpy(in, argv[i+1]);
			}
			else if (strcmp(argv[i], "-o") == 0) {
				strcpy(out, argv[i+1]);
			}
			else {
				printf("Incorrect input\n");
				return -1;
			}
		}
	}
	else if (argc == 11) {
		int i = 1;
		for (; i < 9; i += 2) {
			if (strcmp(argv[i], "-c") == 0) {
				strcpy(buffer, argv[i+1]);
			}
			else if (strcmp(argv[i], "-h") == 0) {
				hostName = argv[i+1];
			}
			else if (strcmp(argv[i], "-p") == 0) {
				port = atoi(argv[i+1]);
			}
			else if (strcmp(argv[i], "-d") == 0) {
				strcpy(in, argv[i+1]);
			}
			else if (strcmp(argv[i], "-o") == 0) {
				strcpy(out, argv[i+1]);
			}
			else {
				printf("Incorrect input\n");
				return -1;
			}
		}
	}
	else {
		return -1;
	}

	//account for ./ argument
	if(strcmp(in, "./")==0){
		getcwd(in,1024);
	}
	if(strcmp(out,"./")==0){
		getcwd(out,1024);
	}

	pthread_mutex_init(&arrIndexLock, NULL);
	pthread_mutex_init(&createThreadLock, NULL);
	pthread_mutex_init(&threadParamLock, NULL);


	TIDs = malloc(sizeof(pthread_t)*2000);	//users are capped at 2000 threads on ilabs
	int i;
	
	threadCount = malloc(sizeof(int));
	*threadCount = 0;

	pthread_t children[1024];		//max files open allowed by ilabs
	int childCount = 0;

	struct dThreadInfo* param = malloc(sizeof(struct dThreadInfo));
	memset(param->path,'\0',1000);
	strcpy(param->path, in);
	pthread_create(&TIDs[*threadCount],NULL,&dirThread,param);
	(*threadCount)++;
	pthread_join(TIDs[0],NULL);

	int length = strlen(buffer);
	char newFile[1024];
	if (strcmp(out, "") == 0) {
		newFile[0] = 'A';
		newFile[1] = 'l';
		newFile[2] = 'l';
		newFile[3] = 'F';
		newFile[4] = 'i';
		newFile[5] = 'l';
		newFile[6] = 'e';
		newFile[7] = 's';
		newFile[8] = '-';
		newFile[9] = 's';
		newFile[10] = 'o';
		newFile[11] = 'r';
		newFile[12] = 't';
		newFile[13] = 'e';
		newFile[14] = 'd';
		newFile[15] = '-';
		int i = 0;
		for (; i < length; i++) {
			newFile[16 + i] = buffer[i];
		}
		newFile[16 + length] = '.';
		newFile[17 + length] = 'c';
		newFile[18 + length] = 's';
		newFile[19 + length] = 'v';
		newFile[20 + length] = '\0';
	}
	else {
		int outLen = strlen(out);
		int z = 0;
		for (; z < outLen; z++) {
			newFile[z] = out[z];
		}
		newFile[0+outLen] = 'A';
		newFile[1+outLen] = 'l';
		newFile[2+outLen] = 'l';
		newFile[3+outLen] = 'F';
		newFile[4+outLen] = 'i';
		newFile[5+outLen] = 'l';
		newFile[6+outLen] = 'e';
		newFile[7+outLen] = 's';
		newFile[8+outLen] = '-';
		newFile[9+outLen] = 's';
		newFile[10+outLen] = 'o';
		newFile[11+outLen] = 'r';
		newFile[12+outLen] = 't';
		newFile[13+outLen] = 'e';
		newFile[14+outLen] = 'd';
		newFile[15+outLen] = '-';
		int i = 0;
		for (; i < length; i++) {
			newFile[16 + i+outLen] = buffer[i];
		}
		newFile[16 + length+outLen] = '.';
		newFile[17 + length+outLen] = 'c';
		newFile[18 + length+outLen] = 's';
		newFile[19 + length+outLen] = 'v';
		newFile[20 + length+outLen] = '\0';
	}

	FILE* outFile=fopen(newFile,"w");	
	fprintf(outFile,"color,director_name,num_critic_for_reviews,duration,director_facebook_likes,actor_3_facebook_likes,actor_2_name,actor_1_facebook_likes,gross,genres,actor_1_name,movie_title,num_voted_users,cast_total_facebook_likes,actor_3_name,facenumber_in_poster,plot_keywords,movie_imdb_link,num_user_for_reviews,language,country,content_rating,budget,title_year,actor_2_facebook_likes,imdb_score,aspect_ratio,movie_facebook_likes\n");

	struct sockaddr_in serverAddressInfo;
	struct hostent *serverIPAddress;
	int sockfd = -1;

	serverIPAddress = gethostbyname(hostName);
	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	bzero((char*) &serverAddressInfo, sizeof(serverAddressInfo));
	serverAddressInfo.sin_family = AF_INET;
	serverAddressInfo.sin_port = htons(port);
	bcopy((char *)serverIPAddress->h_addr, (char*)&serverAddressInfo.sin_addr.s_addr, serverIPAddress->h_length);
	connect(sockfd,(struct sockaddr *)&serverAddressInfo,sizeof(serverAddressInfo));

	int field = 0;
	

	if(strcmp(buffer, "color") == 0){
		field = 0;
	}
	if(strcmp(buffer, "director_name") == 0){
		field = 1;
	}
	if(strcmp(buffer, "num_critic_for_reviews") == 0) {
		field = 2;
	}
	if(strcmp(buffer, "duration") == 0){
		field = 3;
	}
	if(strcmp(buffer, "director_facebook_likes") == 0){
		field = 4;
	}
	if(strcmp(buffer, "actor_3_facebook_likes") == 0){
		field = 5;
	}
	if(strcmp(buffer, "actor_2_name") == 0){
		field = 6;
	}
	if(strcmp(buffer, "actor_1_facebook_likes") == 0){
		field = 7;
	}
	if(strcmp(buffer, "gross") == 0){
		field = 8;
	}
	if(strcmp(buffer, "genres") == 0){
		field = 9;
	}
	if(strcmp(buffer, "actor_1_name") == 0){
		field = 10;
	}
	if(strcmp(buffer, "movie_title") == 0){
		field = 11;
	}
	if(strcmp(buffer, "num_voted_users") == 0){
		field = 12;
	}
	if(strcmp(buffer, "cast_total_facebook_likes") == 0){
		field = 13;
	}
	if(strcmp(buffer, "actor_3_name") == 0){
		field = 14;
	}
	if(strcmp(buffer, "facenumber_in_poster") == 0){
		field = 15;
	}
	if(strcmp(buffer, "plot_keywords") == 0){
		field = 16;
	}
	if(strcmp(buffer, "movie_imdb_link") == 0){
		field = 17;
	}
	if(strcmp(buffer, "num_user_for_reviews") == 0){
		field = 18;
	}
	if(strcmp(buffer, "language") == 0){
		field = 19;
	}
	if(strcmp(buffer, "country") == 0){
		field = 20;
	}
	if(strcmp(buffer, "content_rating") == 0){
		field = 21;
	}
	if(strcmp(buffer, "budget") == 0){
		field = 22;
	}
	if(strcmp(buffer, "title_year") == 0){
		field = 23;
	}
	if(strcmp(buffer, "actor_2_facebook_likes") == 0){
		field = 24;
	}
	if(strcmp(buffer, "imdb_score") == 0){
		field = 25;
	}
	if(strcmp(buffer, "aspect_ratio") == 0){
		field = 26;
	}
	if(strcmp(buffer, "movie_facebook_likes") == 0){
		field = 27;
	}

	//request dump and get dump
	char dumpReq;
	dumpReq = 'd';

	send(sockfd, (void*) &dumpReq, sizeof(char), 0);
	send(sockfd, (void*) &field, sizeof(int), 0);
	//getting dump
	int * dump = malloc(sizeof(int*));
	*dump = sockfd;

	FILE * data = fdopen(*dump, "r");
	char c[1024];
	fgets(c, 1024, data);
	while (c[0] != EOF) {
		fprintf(outFile, c);
		fgets(c, 1024, data);
	}

	close(sockfd);
	fclose(outFile);
	return 0;
}