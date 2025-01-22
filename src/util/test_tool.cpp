#include "test_tool.h"
#include "string.h"
#include <functional>
#include <iostream>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

void multipleProcessorTester(int const numProcesses, void (*func)(std::string, int), std::string prefix) {
  int pipe_fd[2];
  if (pipe(pipe_fd) == -1) {
    std::cerr << "Pipe creation failed" << std::endl;
    return;
  }

  for (int i = 0; i < numProcesses; ++i) {
    pid_t pid = fork();
    if (pid < 0) {
      std::cerr << "Fork failed for process " << i << std::endl;
      continue;  // Skip this iteration and try to fork the next process
    } else if (pid == 0) {
      // Child process
      close(pipe_fd[1]);  // Close the write end of the pipe in the child

      // Wait for the signal to start
      char buf;
      read(pipe_fd[0], &buf, 1);
      func(prefix, i);
      exit(0);
    } else {
      // Parent process continues to next iteration to fork another child
    }
  }

  // Parent process closes the read end of the pipe and signals all children to start
  close(pipe_fd[0]);          // Close the read end of the pipe in the parent
  sleep(1);                   // Optional: give all children time to set up
  write(pipe_fd[1], "s", 1);  // Signal all children to start
  close(pipe_fd[1]);          // Close the write end of the pipe in the parent

  // Parent process waits for all child processes to complete

  for (int i = 0; i < numProcesses; ++i) {
    int status;
    pid_t terminated_pid = wait(&status);  // Wait for each child process to complete
    if (WIFEXITED(status)) {
      if (WEXITSTATUS(status) != 0) {
        std::cerr << "Child process with pid " << terminated_pid << " exited with status " << WEXITSTATUS(status) << std::endl;
      }
    } else {
      int signal_number = WTERMSIG(status);
      std::cerr << "Child process with pid " << terminated_pid << " terminated by signal " << signal_number << std::endl;
      // Optionally, you can also print the name of the signal
      std::cerr << "Signal name: " << strsignal(signal_number) << std::endl;
    }
  }
}
