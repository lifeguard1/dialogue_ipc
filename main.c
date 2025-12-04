#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <semaphore.h>
#include <unistd.h>
#define MAX_PARTICIPANTS 10
#define MAX_DIALOGUES 10
#define MSG_BUFFER_SIZE 256
#define SHM_NAME "/dialogue_shm"
#define SEM_NAME "/dialogue_sem"

typedef struct{
    unsigned int sender_id;
    sem_t sync_sem;
    char message[MSG_BUFFER_SIZE];
    pthread_mutex_t mutex;
    int readers_remaining;
} MessageBlock;

typedef struct{
    unsigned int dialogue_id;
    pid_t participants[MAX_PARTICIPANTS];
    MessageBlock msg_slot;
} Dialogue;

typedef struct{
    pthread_mutex_t system_mutex;
    Dialogue dialogue_shm[MAX_DIALOGUES];
} SharedMemory;

SharedMemory *shm_ptr = NULL;

// Function to enable sychronization of mutex between processes
void init_pshared_mutex_attr(pthread_mutexattr_t *attr){
    pthread_mutexattr_init(attr);

    // must set attr to be shared between processes
    pthread_mutexattr_setpshared(attr, PTHREAD_PROCESS_SHARED);
}

int main(argc, *argv[]){

  pthread_mutexattr_t mutex_attr;
  init_pshared_mutex_attr(&mutex_attr);

  my_pid = getpid();

  int fd = shm_open(SHM_NAME, O_CREAT | O_EXCL | O_RDWR , 0666);
  int is_initializer = 0;

  // if shm_open failed fd value is kept as -1 
  if(fd != -1){

    // First process
    is_initializer = 1;
    ftruncate(fd, sizeof(SharedMemory));
    shm_ptr = mmap(0, sizeof(SharedMemory), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if(shm_ptr == MAP_FAILED){ perror("mmap of first process failed"); return -1;}
    
    pthread_mutex_init(&shm_ptr->system_mutex, &mutex_attr);
    memset(shm_ptr->dialogue_shm, 0, sizeof(shm_ptr->dialogue_shm));

  }else{
    if(errno == EEXIST){
      // shared memory object already exists so just open it
      fd = shm_open(SHM_NAME, O_RDWR, 0666);
      if(fd == -1){
        perror("shm_open fail");
        return 1;
      }
    }
  }

  shm_ptr = mmap(0, sizeof(SharedMemory), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);


  if(pthread_mutex_init(&mutex, NULL) != 0){
     printf("Mutex init failed\n");
    return 1;
  }

  // Create the threads
  pthread_t writer, reader;

}
