#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <semaphore.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdbool.h>
#include <sys/select.h>

#define MAX_PARTICIPANTS 10
#define MAX_DIALOGUES 10
#define MSG_BUFFER_SIZE 256
#define SHM_NAME "/dialogue_shm"
#define SEM_NAME "/dialogue_sem"

#define DIALOGUE_FREE 0
#define DIALOGUE_ACTIVE 1

#define MESSAGE_FREE 0
#define MESSAGE_FULL 1

typedef struct{
    pid_t sender_id;
    sem_t sync_sem;
    int status;
    char payload[MSG_BUFFER_SIZE];
    pthread_mutex_t msg_mutex;
    int readers_remaining;
} MessageBlock;

typedef struct{
    int dialogue_id;
    int status;
    int participant_count;
    pid_t participants[MAX_PARTICIPANTS];
    MessageBlock msg_struct;
} Dialogue;

typedef struct{
    pthread_mutex_t system_mutex;
    int last_dialogue_id;
    int current_dialogues_count;
    Dialogue dialogues_shm[MAX_DIALOGUES];
} SharedMemory;

struct thread_args{
  int dialogue_index;
  pid_t pid;
  atomic_bool terminate_flag;
};

pid_t global_pid = 0;
int global_dialogue_index = -1;
SharedMemory *shm_ptr = NULL;
int fd = -1;
struct thread_args *global_args = NULL;  // Need reference to set terminate_flag

void cleanup();

// Function to enable sychronization of mutex between processes
void init_pshared_mutex_attr(pthread_mutexattr_t *attr){
    pthread_mutexattr_init(attr);

    // must set attr to be shared between processes
    pthread_mutexattr_setpshared(attr, PTHREAD_PROCESS_SHARED);
}

// Initialize memory
int startup(){

  pthread_mutexattr_t mutex_attr;
  init_pshared_mutex_attr(&mutex_attr);

  // if shm exists then fd's value will remain -1
  // this can help us determine if its creator process or not
  fd = shm_open(SHM_NAME, O_CREAT | O_EXCL | O_RDWR , 0666);

  // if shm_open failed fd value is kept as -1 
  if(fd != -1){

    ftruncate(fd, sizeof(SharedMemory));

    // map the virtual address space of the process to SharedMemory
    shm_ptr = mmap(0, sizeof(SharedMemory), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if(shm_ptr == MAP_FAILED){ perror("mmap of first process failed"); return -1; }

    pthread_mutex_init(&shm_ptr->system_mutex, &mutex_attr);
    shm_ptr->current_dialogues_count = 0;
    shm_ptr->last_dialogue_id = 0;
    memset(shm_ptr->dialogues_shm, 0, sizeof(shm_ptr->dialogues_shm)); // initialize(zero-out) the entire dialogue struct

    // Initialize mutexes and semaphores for all available dialogue slots
    for(int i = 0; i < MAX_DIALOGUES; i++){
        pthread_mutex_init(&shm_ptr->dialogues_shm[i].msg_struct.msg_mutex, &mutex_attr);
        shm_ptr->dialogues_shm[i].status = DIALOGUE_FREE;
        if(sem_init(&shm_ptr->dialogues_shm[i].msg_struct.sync_sem, 1, 0) == -1){        // initialize the semaphore once in first calling prcoess
           perror("sem_init failed"); return -1;
      }
    }

  }else if(errno == EEXIST){
      // shared memory object already exists so just open it
      fd = shm_open(SHM_NAME, O_RDWR, 0666);
      if(fd == -1){
        perror("shm_open fail");
        return 1;
      }

      // map the virtual address space of the process to SharedMemory
      shm_ptr = mmap(0, sizeof(SharedMemory), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
      if(shm_ptr == MAP_FAILED){ perror("mmap of client process failed"); return -1; }

  }else{
    perror("shm_open of initializer failed");
    return -1;
  }
  return 0;
}

int join_dialogue(pid_t pid, int dialogue_id){

  pthread_mutex_lock(&shm_ptr->system_mutex);
  int dialogue_index = -1;

  // case for attempting to create new dialogue
  if(dialogue_id == 0){
    int new_dialogue_id = shm_ptr->last_dialogue_id + 1;

    for(int i=0; i<MAX_DIALOGUES; i++){
      if(shm_ptr->dialogues_shm[i].status == DIALOGUE_FREE){
        // empty slot for new dialogue
        dialogue_index = i;
        break;
      }
    }

    if(dialogue_index != -1){
        // found empty slot for a new dialogue
        shm_ptr->dialogues_shm[dialogue_index].dialogue_id = new_dialogue_id;
        shm_ptr->dialogues_shm[dialogue_index].participant_count = 1;
        shm_ptr->dialogues_shm[dialogue_index].participants[0] = pid;
        shm_ptr->dialogues_shm[dialogue_index].status = DIALOGUE_ACTIVE;
        shm_ptr->last_dialogue_id++;         // updated last dialogue create
        shm_ptr->current_dialogues_count++;
        printf("Process ID: %d. Created Dialogue ID: %d\n", pid, new_dialogue_id);
        pthread_mutex_unlock(&shm_ptr->system_mutex);
        return dialogue_index;
    }else{
        // no empty slot for new dialogue
        printf("No empty slot for new dialogue creation. Exiting...\n");
        pthread_mutex_unlock(&shm_ptr->system_mutex);
        return -1;
    }
  }else{
      // case for joining a existing dialogue
      for(int i=0; i<MAX_DIALOGUES; i++){
        // search for the desirable dialogue to join
        if(shm_ptr->dialogues_shm[i].dialogue_id == dialogue_id && shm_ptr->dialogues_shm[i].participant_count < MAX_PARTICIPANTS){
          dialogue_index = i;
          break;
        }
      }

      if(dialogue_index != -1){
        int pos = -1;

        // search for insertion slot for new process
        for(int j=0; j<MAX_PARTICIPANTS; j++){
          if(shm_ptr->dialogues_shm[dialogue_index].participants[j] == 0){
             pos = j;
             break;
          }
        }

        if(pos != -1){
           shm_ptr->dialogues_shm[dialogue_index].participant_count++;
           shm_ptr->dialogues_shm[dialogue_index].participants[pos] = pid;
           pthread_mutex_unlock(&shm_ptr->system_mutex);
           printf("Joined dialogue ID: %d\n", dialogue_id);
           return dialogue_index;
        }else{
           // this should never happen but rare case for faulty previous update
           printf("Error available dialogue but no participant empty slot.\n");
           pthread_mutex_unlock(&shm_ptr->system_mutex);
           return -1;
        }
      }else{
         printf("Dialogue ID: %d either does not exist or is full. Try again later.\n", dialogue_id);
         pthread_mutex_unlock(&shm_ptr->system_mutex);
         return -1;
      }
  }

  pthread_mutex_unlock(&shm_ptr->system_mutex);
  return -1;
}

// function to remove a process from a dialogue
int remove_process_from_dialogue(pid_t pid, int dialogue_index){

  if(dialogue_index < 0 || dialogue_index >= MAX_DIALOGUES) return 0;
  // if its already free, return
  if(shm_ptr->dialogues_shm[dialogue_index].status == DIALOGUE_FREE)  return 0; 

  pthread_mutex_lock(&shm_ptr->dialogues_shm[dialogue_index].msg_struct.msg_mutex);

  for(int i=0; i<MAX_PARTICIPANTS; i++){
    if(shm_ptr->dialogues_shm[dialogue_index].participants[i] == pid){
     shm_ptr->dialogues_shm[dialogue_index].participants[i] = 0;
     shm_ptr->dialogues_shm[dialogue_index].participant_count--;
     break; 
    }
  }

  int current_participants = shm_ptr->dialogues_shm[dialogue_index].participant_count;
  pthread_mutex_unlock(&shm_ptr->dialogues_shm[dialogue_index].msg_struct.msg_mutex);

  // mark dialogue as free
  if(current_participants == 0){
    pthread_mutex_lock(&shm_ptr->system_mutex);
    printf("PID: %d was only participant. Deleting dialogue ID: %d\n", pid, shm_ptr->dialogues_shm[dialogue_index].dialogue_id);
    shm_ptr->dialogues_shm[dialogue_index].status = DIALOGUE_FREE;
    shm_ptr->dialogues_shm[dialogue_index].dialogue_id = 0;
    shm_ptr->current_dialogues_count--;
    pthread_mutex_unlock(&shm_ptr->system_mutex);
    return shm_ptr->current_dialogues_count;
  }

  return 1;
}

void *sender_thread(void *arg){

  struct thread_args *a = (struct thread_args *)arg;
  int dialogue_index = a->dialogue_index;
  int pid = a->pid;

  char input[MSG_BUFFER_SIZE];

  fd_set readfds;
  struct timeval tv;
  int retval;

  while(!atomic_load(&a->terminate_flag)){
    //print menu
    printf("\n====== AVAILABLE ACTIONS ======\n");
    printf("Send message, (CTRL-C) to kill process and remove it from dialogue or type TERMINATE to kill dialogue.\n");
    printf("> ");
    fflush(stdout);

    int data_available = 0;
    
    // inner while loop to not get stuck on fgets
    // if input from user then it breaks from loop
    while(!atomic_load(&a->terminate_flag) && !data_available){
        FD_ZERO(&readfds);
        FD_SET(STDIN_FILENO, &readfds);

        //set timeout to 1 second
        //must reset this every time because select updates it
        tv.tv_sec = 1;
        tv.tv_usec = 0;
        // monitor stdin(fd 0) for reading
        retval = select(STDIN_FILENO + 1, &readfds, NULL, NULL, &tv);

        if (retval == -1) {
            if (errno == EINTR) continue; // interrupted by signal, retry
            perror("select error");
            break; 
        } else if (retval > 0) {
            // data is available on stdin
            data_available = 1;
        }
        //if retval==0, it was a timeout so loop back to check terminate_flag
    }

    // If loop exited because of flag, break immediately
    if(atomic_load(&a->terminate_flag)){
      break;
    }

    fgets(input, MSG_BUFFER_SIZE, stdin);

    // remove new line character
    input[strcspn(input, "\n")] = 0;

    if(strlen(input) == 0){
      continue;
    }

    pthread_mutex_lock(&shm_ptr->dialogues_shm[dialogue_index].msg_struct.msg_mutex);

    // check if message slot is empty
    if(shm_ptr->dialogues_shm[dialogue_index].msg_struct.status == MESSAGE_FULL){
      printf("Error, slot is busy. Try again later.\n");
      pthread_mutex_unlock(&shm_ptr->dialogues_shm[dialogue_index].msg_struct.msg_mutex);
      continue; // retry the loop
    }

    int isTerminate = (strcmp(input, "TERMINATE") == 0); // if input is "TERMINATE" strcmp will return 0 so isTerminate will be true

    strncpy(shm_ptr->dialogues_shm[dialogue_index].msg_struct.payload, input, MSG_BUFFER_SIZE);

    shm_ptr->dialogues_shm[dialogue_index].msg_struct.status = MESSAGE_FULL;
    shm_ptr->dialogues_shm[dialogue_index].msg_struct.sender_id = pid;
    shm_ptr->dialogues_shm[dialogue_index].msg_struct.readers_remaining = shm_ptr->dialogues_shm[dialogue_index].participant_count;

    // increase the semaphore value for each participant
    for(int i=0; i<shm_ptr->dialogues_shm[dialogue_index].participant_count; i++){
      sem_post(&shm_ptr->dialogues_shm[dialogue_index].msg_struct.sync_sem);
    }

    pthread_mutex_unlock(&shm_ptr->dialogues_shm[dialogue_index].msg_struct.msg_mutex);

    if(isTerminate){
      printf("TERMINATE sent, writer exiting...\n");
      atomic_store(&a->terminate_flag, true);
      break;
    }

    if(atomic_load(&a->terminate_flag)){
      break;
    }
  }
  
  return NULL;
}

void *receiver_thread(void *arg){

  struct thread_args *a = (struct thread_args *)arg;
  int dialogue_index = a->dialogue_index;
  pid_t pid = a->pid;

  // make sure terminate_flag is not raised
  while(!atomic_load(&a->terminate_flag)){

    sem_wait(&shm_ptr->dialogues_shm[dialogue_index].msg_struct.sync_sem);

    // check for flag again
    if(atomic_load(&a->terminate_flag)){
      break;
    }

    pthread_mutex_lock(&shm_ptr->dialogues_shm[dialogue_index].msg_struct.msg_mutex);

    if(shm_ptr->dialogues_shm[dialogue_index].msg_struct.sender_id == pid){
      // if its writer's process do nothing (only decrement), checking later if its terminate payload
    }else{
      printf("\n\nDialogue ID: %d | Sent by: %d\n", shm_ptr->dialogues_shm[dialogue_index].dialogue_id, shm_ptr->dialogues_shm[dialogue_index].msg_struct.sender_id);
      printf("Message: %s\n", shm_ptr->dialogues_shm[dialogue_index].msg_struct.payload);
    }

    int isTerminate = (strcmp(shm_ptr->dialogues_shm[dialogue_index].msg_struct.payload, "TERMINATE") == 0);
    shm_ptr->dialogues_shm[dialogue_index].msg_struct.readers_remaining--;

    // if this was last reader mark message slot as empty
    if(shm_ptr->dialogues_shm[dialogue_index].msg_struct.readers_remaining <= 0){
       shm_ptr->dialogues_shm[dialogue_index].msg_struct.status = MESSAGE_FREE;
    }

    pthread_mutex_unlock(&shm_ptr->dialogues_shm[dialogue_index].msg_struct.msg_mutex);

    if(isTerminate){
      printf("TERMINATE received, receiver exiting...\n");
      atomic_store(&a->terminate_flag, true);
      break;
    }

    if(atomic_load(&a->terminate_flag)){
      break;
    }
  }

  return NULL;
}

void cleanup(){
  if(shm_ptr == NULL){
    exit(0);
  }

  printf("\nCleaning up...\n");

  // signal threads to stop
  if(global_args != NULL){
    atomic_store(&global_args->terminate_flag, true);
  }

  if(global_dialogue_index >= 0){
    int rem = remove_process_from_dialogue(global_pid, global_dialogue_index);

    // if return value was 0 it means no more dialogues remaining
    if(rem <= 0){
      printf("Last dialogue closed. removing shared memory...\n");
      munmap(shm_ptr, sizeof(SharedMemory));
      shm_unlink(SHM_NAME);
      close(fd);
      exit(0);
    }else{
      printf("Process removed. Other dialogues still active.\n");
      munmap(shm_ptr, sizeof(SharedMemory));
      close(fd);
      exit(0);
    }
  }
    munmap(shm_ptr, sizeof(SharedMemory));
    close(fd);
    exit(0);
}

int main(int argc, char *argv[]){

  // dialogue target for process to enter
  int dialogue_id;
  pid_t mypid = getpid();

  global_pid = mypid;

  // signal handler helps us do a safe cleanup on ctrl-c
  signal(SIGINT, cleanup);

  if(argc > 1){
    dialogue_id = atoi(argv[1]);
  } else{
    printf("Incorrect command line arguments. Exiting...\n");
    return 1;
  }

  // initializing memory failed
  int v = startup();
  if(v == -1){
    return -1;
  }

  int dialogue_index = join_dialogue(mypid, dialogue_id);
  if(dialogue_index == -1){
    munmap(shm_ptr, sizeof(SharedMemory));
    close(fd);
    return -1;
  }

  global_dialogue_index = dialogue_index;

  struct thread_args *args = malloc(sizeof(struct thread_args));
  args->dialogue_index = dialogue_index;
  args->pid = mypid;
  atomic_init(&args->terminate_flag, 0);

  global_args = args;

  pthread_t sender, receiver;

  pthread_create(&sender, NULL, sender_thread, args);
  pthread_create(&receiver, NULL, receiver_thread, args);

  pthread_join(sender, NULL);
  pthread_join(receiver, NULL);

  int rem = remove_process_from_dialogue(mypid, dialogue_index);

  if(rem <= 0){
      printf("Last dialogue closed. removing shared memory...\n");
      munmap(shm_ptr, sizeof(SharedMemory));
      shm_unlink(SHM_NAME);
  }else{
    munmap(shm_ptr, sizeof(SharedMemory));
  }

  close(fd);
  free(args);

  printf("\n PROCESS TERMINATED\n");

  return 0;
}
