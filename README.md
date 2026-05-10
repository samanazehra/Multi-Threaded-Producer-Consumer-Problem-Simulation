# CS-2006 OS Project: Multi-Threaded Producer-Consumer

Authors: Syeda Samana Zehra Rizvi, Fizza Batool, Isbah Rani Abbasi

For this project, we are going to simulate the classic producer-consumer problem using multiple threads. Our goal is to show how to properly synchronize threads so they can share a fixed-size buffer without crashing or corrupting the data. We will use standard operating system tools like mutexes and semaphores to manage the data flow safely.

### --- PREREQUISITES ---

This simulation requires the ncurses library to render the terminal UI. 
If you are running this on a fresh Ubuntu/WSL environment, please install the header files first by running:
sudo apt update && sudo apt install libncurses-dev -y

### --- COMPILATION ---

To compile the single-file simulation, open your Linux terminal in this directory and run the following command. 
(Note: The -lpthread and -lncurses flags are strictly required).

g++ -Wall -Wextra -std=c++17 -o output main.cpp -lpthread -lncurses

### --- EXECUTION ---

Run the compiled executable with:
./output

### --- CONTROLS ---
- The simulation runs automatically.
- Resize your terminal window if the UI elements appear cramped.
- Press 'q' at any time to trigger a graceful shutdown and view the final statistics. 

### --- LOG OUTPUT ---
Upon completion, the simulation will automatically generate a 'simulation.log' file in this same directory containing the exact timestamps and thread activity.
