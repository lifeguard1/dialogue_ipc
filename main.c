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
    pthread_mutex_t msg_mutex;
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

  int fd = shm_open(SHM_NAME, O_CREAT | O_EXCL | O_RDWR , 0666);
  int is_initializer = 0;

  // if shm_open failed fd value is kept as -1 
  if(fd != -1){

    is_initializer = 1;
    ftruncate(fd, sizeof(SharedMemory));

    // map the virtual address space of the process to SharedMemory
    shm_ptr = mmap(0, sizeof(SharedMemory), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if(shm_ptr == MAP_FAILED){ perror("mmap of first process failed"); return -1; }
    
    pthread_mutex_init(&shm_ptr->system_mutex, &mutex_attr);
    memset(shm_ptr->dialogue_shm, 0, sizeof(shm_ptr->dialogue_shm)); // initialize(zero-out) the entire dialogue struct

    // Initialize mutexes and semaphores for all available dialogue slots
    for(int i = 0; i < MAX_DIALOGUES; i++){
        pthread_mutex_init(&shm_ptr->dialogue_shm[i].msg_slot.msg_mutex, &mutex_attr);
        if(sem_init(&shm_ptr->dialogue_shm[i].msg_slot.sync_sem, 1, 0) == -1){
           perror("sem_init failed"); return -1;
      }
    }

  }else if{
    if(errno == EEXIST){
      // shared memory object already exists so just open it
      fd = shm_open(SHM_NAME, O_RDWR, 0666);
      if(fd == -1){
        perror("shm_open fail");
        return 1;
      }
    
      // map the virtual address space of the process to SharedMemory
      shm_ptr = mmap(0, sizeof(SharedMemory), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
      if(shm_ptr == MAP_FAILED){ perror("mmap of client process failed"); return -1; }
    }

  }else{
    perror("shm_open of initializer failed");
    return -1;
  }

  // Create the threads
  pthread_t writer, reader;

}
