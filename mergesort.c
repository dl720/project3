#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "sorter_client.h"
#include <strings.h>
//#include <pthread.h>

int merge(struct Movie arr[], int col, int start, int end){
	int mid = (start+(end-1))/2;
	int n1=mid-start+1;
	int n2=end-mid;
	struct Movie left[n1];
	struct Movie right[n2];
	int i;
	int j;
	int k;
	for(i=0;i<n1;i++)
		left[i]=arr[start+i];
	for(j=0;j<n2;j++)
		right[j]=arr[mid+1+j];
	i=0;
	j=0;
	k=start;



	if(col==0){//color
		while(i<n1 && j<n2){
			if(strcmp(left[i].color,right[j].color)<0){
				arr[k]=left[i];
				i++;
			}
			else{
				arr[k]=right[j];
				j++;
			}
			k++;
		}
		
	
	}
	
	if(col==1){//director_name
		while(i<n1 && j<n2){
			if(strcmp(left[i].director_name,right[j].director_name)<0){
				arr[k]=left[i];
				i++;
			}
			else{
				arr[k]=right[j];
				j++;
			}
			k++;
		}
	}
	if(col==2){//num_critic_for_reviews
		while(i<n1 && j<n2){
			if(left[i].num_critic_for_reviews<=right[j].num_critic_for_reviews){
				arr[k]=left[i];
				i++;
			}
			else{
				arr[k]=right[j];
				j++;
			}
			k++;
		}
	}
	if(col==3){//duration

		while(i<n1 && j<n2){
			if(left[i].duration<=right[j].duration){
				arr[k]=left[i];
				i++;
			}
			else{
				arr[k]=right[j];
				j++;
			}
			k++;
		}
		
		
		
	}
	if(col==4){//director_facebook_likes

		while(i<n1 && j<n2){
			if(left[i].director_facebook_likes<=right[j].director_facebook_likes){
				arr[k]=left[i];
				i++;
			}
			else{
				arr[k]=right[j];
				j++;
			}
			k++;
		}
		
		
		
	}
	if(col==5){//actor_3_facebook_likes

		while(i<n1 && j<n2){
			if(left[i].actor_3_facebook_likes<=right[j].actor_3_facebook_likes){
				arr[k]=left[i];
				i++;
			}
			else{
				arr[k]=right[j];
				j++;
			}
			k++;
		}
		
		
		
	}
	if(col==6){//actor 2 name
		while(i<n1 && j<n2){
			if(strcmp(left[i].actor_2_name,right[j].actor_2_name)<0){
				arr[k]=left[i];
				i++;
			}
			else{
				arr[k]=right[j];
				j++;
			}
			k++;
		}
	}
	if(col==7){//actor_1_facebook_likes

		while(i<n1 && j<n2){
			if(left[i].actor_1_facebook_likes<=right[j].actor_1_facebook_likes){
				arr[k]=left[i];
				i++;
			}
			else{
				arr[k]=right[j];
				j++;
			}
			k++;
		}
	}
	if(col==8){//gross

		while(i<n1 && j<n2){
			if(left[i].gross<=right[j].gross){
				arr[k]=left[i];
				i++;
			}
			else{
				arr[k]=right[j];
				j++;
			}
			k++;
		}
		
		
		
	}
	
	if(col==9){//genres
		while(i<n1 && j<n2){
			if(strcmp(left[i].genres,right[j].genres)<0){
				arr[k]=left[i];
				i++;
			}
			else{
				arr[k]=right[j];
				j++;
			}
			k++;
		}
	}
	if(col==10){//actor 1 name
		while(i<n1 && j<n2){
			if(strcmp(left[i].actor_1_name,right[j].actor_1_name)<0){
				arr[k]=left[i];
				i++;
			}
			else{
				arr[k]=right[j];
				j++;
			}
			k++;
		}
	}
	if(col==11){//movie title
		while(i<n1 && j<n2){
			if(strcmp(left[i].movie_title,right[j].movie_title)<0){
				arr[k]=left[i];
				i++;
			}
			else{
				arr[k]=right[j];
				j++;
			}
			k++;
		}
	}
	if(col==12){//num_voted_users

		while(i<n1 && j<n2){
			if(left[i].num_voted_users<=right[j].num_voted_users){
				arr[k]=left[i];
				i++;
			}	
			else{
				arr[k]=right[j];
				j++;
			}
			k++;
		}
		
		
		
	}
	if(col==13){//cast_total_facebook_likes

		while(i<n1 && j<n2){
			if(left[i].cast_total_facebook_likes<=right[j].cast_total_facebook_likes){
				arr[k]=left[i];
				i++;
			}
			else{
				arr[k]=right[j];
				j++;
			}
			k++;
		}
		
	}
	if(col==14){//actor 3 name
		while(i<n1&&j<n2){
			if(strcmp(left[i].actor_3_name,right[j].actor_3_name)<0){
				arr[k]=left[i];
				i++;
			}
			else{
				arr[k]=right[j];
				j++;
			}
			k++;	
		}
	}
	if(col==15){//facenumber_in_poster

		while(i<n1 && j<n2){
			if(left[i].facenumber_in_poster<=right[j].facenumber_in_poster){
				arr[k]=left[i];
				i++;
			}
			else{
				arr[k]=right[j];
				j++;
			}
			k++;
		}
		
	}
	if(col==16){//plot keywords
		while(i<n1 && j<n2){
			if(strcmp(left[i].plot_keywords,right[j].plot_keywords)<0){
				arr[k]=left[i];
				i++;
			}
			else{
				arr[k]=right[j];
				j++;
			}
			k++;
		}
	}
	if(col==17){//movie imdb link
		while(i<n1 && j<n2){
			if(strcmp(left[i].movie_imdb_link,right[j].movie_imdb_link)<0){
				arr[k]=left[i];
				i++;
			}
			else{
				arr[k]=right[j];
				j++;
			}
			k++;
		}
	}
	if(col==18){//num_user_for_reviews

		while(i<n1 && j<n2){
			if(left[i].num_user_for_reviews<=right[j].num_user_for_reviews){
				arr[k]=left[i];
				i++;
			}
			else{
				arr[k]=right[j];
				j++;
			}
			k++;
		}
		
	}
	if(col==19){//language
		while(i<n1 && j<n2){
			if(strcmp(left[i].language,right[j].language)<0){
				arr[k]=left[i];
				i++;
			}
			else{
				arr[k]=right[j];
				j++;
			}
			k++;
		}
	}
	if(col==20){//country
		while(i<n1 && j<n2){
			if(strcmp(left[i].country,right[j].country)<0){
				arr[k]=left[i];
				i++;
			}
			else{
				arr[k]=right[j];
				j++;
			}
			k++;
		}
	}
	if(col==21){//content rating
		while(i<n1 && j<n2){
			if(strcmp(left[i].content_rating,right[j].content_rating)<0){
				arr[k]=left[i];
				i++;
			}
			else{
				arr[k]=right[j];
				j++;
			}
			k++;
		}
	}
	if(col==22){//budget

		while(i<n1 && j<n2){
			if(left[i].budget<=right[j].budget){
				arr[k]=left[i];
				i++;
			}
			else{
				arr[k]=right[j];
				j++;
			}
			k++;
		}
		
	}
	if(col==23){//title_year

		while(i<n1 && j<n2){
			if(left[i].title_year<=right[j].title_year){
				arr[k]=left[i];
				i++;
			}
			else{
				arr[k]=right[j];
				j++;
			}
			k++;
		}
		
	}
	if(col==24){//actor_2_facebook_likes

		while(i<n1 && j<n2){
			if(left[i].actor_2_facebook_likes<=right[j].actor_2_facebook_likes){
				arr[k]=left[i];
				i++;
			}
			else{
				arr[k]=right[j];
				j++;
			}
			k++;
		}
		
	}
	if(col==25){//imdb_score

		while(i<n1 && j<n2){
			if(left[i].imdb_score<=right[j].imdb_score){
				arr[k]=left[i];
				i++;
			}
			else{
				arr[k]=right[j];
				j++;
			}
			k++;
		}
		
	}
	if(col==26){//aspect_ratio

		while(i<n1 && j<n2){
			if(left[i].aspect_ratio<=right[j].aspect_ratio){
				arr[k]=left[i];
				i++;
			}
			else{
				arr[k]=right[j];
				j++;
			}
			k++;
		}
		
	}		
	if(col==27){//movie_facebook_likes

		while(i<n1 && j<n2){
			if(left[i].movie_facebook_likes<=right[j].movie_facebook_likes){
				arr[k]=left[i];
				i++;
			}
			else{
				arr[k]=right[j];
				j++;
			}
			k++;
		}
	}
	while(i<n1){
		arr[k]=left[i];
		i++;
		k++;
	}
	while(j<n2){
		arr[k]=right[j];
		j++;
		k++;
	}
}	

int mergesort(struct Movie arr[],int start, int end, int col){
	if(start<end){
		int mid=(start+(end-1))/2;
		//pthread_t t1,t2;
      	//pthread_create(&t1,NULL,mergesort,arr,start,mid,col);
     	//pthread_create(&t2,NULL,mergesort,arr,mid+1,end,col);
     	//pthread_join(t1,NULL);
      	//pthread_join(t2,NULL);
		mergesort(arr,start,mid,col);
		mergesort(arr,mid+1,end,col);
		merge(arr,col,start,end);
	}
	
	return 0;
	
}