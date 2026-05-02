#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#define BUFFER_SIZE 5

char buffer[BUFFER_SIZE];
int in = 0;    // write index
int out = 0;   // read index
int count = 0; // number of items in buffer

pthread_mutex_t lock;
pthread_cond_t not_full;
pthread_cond_t not_empty;

void* producer(void* arg) {
    // open file
    FILE* fp = fopen("message.txt", "r");
    char c;

    // loop until end of file
    while ((c = fgetc(fp)) != EOF) {

        pthread_mutex_lock(&lock);

        while (count == BUFFER_SIZE)
            pthread_cond_wait(&not_full, &lock);

        buffer[in] = c;
	// print here to see upload
        //printf("(%c)", c);
        in = (in + 1) % BUFFER_SIZE;
        count++;

        pthread_cond_signal(&not_empty);
        pthread_mutex_unlock(&lock);
    }

    // send termination signal
    pthread_mutex_lock(&lock);
    // because end of file, send null character
    buffer[in] = '\0';
    count++;
    // wake up consumer when buffer is full
    pthread_cond_signal(&not_empty);
    pthread_mutex_unlock(&lock);

    fclose(fp);
    pthread_exit(NULL);
}


void* consumer(void* arg) {
    char c;

    while (1) {
        pthread_mutex_lock(&lock);

        while (count == 0)
            pthread_cond_wait(&not_empty, &lock);

        c = buffer[out];
        out = (out + 1) % BUFFER_SIZE;
        count--;

	// wake up producer when buffer isnt full
        pthread_cond_signal(&not_full);
        pthread_mutex_unlock(&lock);

	// stop at null character
        if (c == '\0')
            break;

	// print is here
        printf("%c", c);
    }

    pthread_exit(NULL);
}

int main() {
    pthread_t prod, cons;

    pthread_mutex_init(&lock, NULL);
    pthread_cond_init(&not_full, NULL);
    pthread_cond_init(&not_empty, NULL);

    pthread_create(&prod, NULL, producer, NULL);
    pthread_create(&cons, NULL, consumer, NULL);

    pthread_join(prod, NULL);
    pthread_join(cons, NULL);

    return 0;
}
