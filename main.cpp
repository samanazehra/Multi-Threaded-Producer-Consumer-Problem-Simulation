#include <iostream>
#include <vector>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <random>
#include <fstream>
#include <atomic>
#include <cstdarg>
#include <cstring>
#include <csignal>
#include <ncurses.h>
using namespace std;

constexpr int BUFFER_SIZE     = 10;
constexpr int NUM_PRODUCERS   = 3;
constexpr int NUM_CONSUMERS   = 3;
constexpr int PRODUCE_MIN_MS  = 300;
constexpr int PRODUCE_MAX_MS  = 900;  
constexpr int CONSUME_MIN_MS  = 400;
constexpr int CONSUME_MAX_MS  = 1100;
constexpr int UI_REFRESH_MS   = 120;
constexpr int LOG_LINES       = 14;
constexpr int LOG_LINE_LEN    = 90;
const string LOG_FILE    = "simulation.log";

class Semaphore {
private:
    mutex mtx;
    condition_variable cv;
    int count;

public:
    Semaphore(int initial_count = 0) : count(initial_count) {}

    void release() {
        lock_guard<mutex> lock(mtx);
        count++;
        cv.notify_one();
    }

    void acquire() {
        unique_lock<mutex> lock(mtx);
        cv.wait(lock, [this]() { return count > 0; });
        count--;
    }

    bool try_acquire_for(int ms) {
        unique_lock<mutex> lock(mtx);
        if (cv.wait_for(lock, chrono::milliseconds(ms), [this]() { return count > 0; })) {
            count--;
            return true;
        }
        return false;
    }
};

vector<int> g_buf(BUFFER_SIZE, 0);
int g_in    = 0;
int g_out   = 0;
int g_count = 0;

mutex g_mutex;
Semaphore g_empty_sem(BUFFER_SIZE);
Semaphore g_full_sem(0);

atomic<bool> g_shutdown{false};

long g_total_produced = 0;
long g_total_consumed = 0;
chrono::time_point<chrono::system_clock> g_start_time;

struct AgentStat {
    int  id;
    long count = 0;
    int  last_val = 0;
    string status = "starting";
};

vector<AgentStat> g_pstat(NUM_PRODUCERS);
vector<AgentStat> g_cstat(NUM_CONSUMERS);

vector<string> g_log(LOG_LINES, "");
int g_log_next = 0;
mutex g_log_lock;
ofstream g_log_file;

WINDOW *w_buf, *w_stat, *w_agents, *w_log;

void ms_sleep(int ms) {
    this_thread::sleep_for(chrono::milliseconds(ms));
}

int rand_range(int lo, int hi) {
    static thread_local mt19937 generator(random_device{}());
    uniform_int_distribution<int> distribution(lo, hi);
    return distribution(generator);
}

void producer(int id) {
    while (!g_shutdown) {
        {
            lock_guard<mutex> lock(g_mutex);
            g_pstat[id].status = "sleeping";
        }

        ms_sleep(rand_range(PRODUCE_MIN_MS, PRODUCE_MAX_MS));
        if (g_shutdown) break;

        int item = rand_range(1, 99);

        {
            lock_guard<mutex> lock(g_mutex);
            g_pstat[id].status = "waiting ";
        }

        while (!g_shutdown) {
            if (g_empty_sem.try_acquire_for(1000)) break;
        }
        if (g_shutdown) break;

        int slot, snap_count;
        {
            lock_guard<mutex> lock(g_mutex);
            slot        = g_in;
            g_buf[g_in] = item;
            g_in        = (g_in + 1) % BUFFER_SIZE;
            g_count++;
            
            g_total_produced++;
            g_pstat[id].count++;
            g_pstat[id].last_val = item;
            g_pstat[id].status = "produced";
            snap_count = g_count;
        }

        g_full_sem.release();

        log_event("P%d produced %2d -> slot[%d]  (buf %d/%d)",
                  id + 1, item, slot, snap_count, BUFFER_SIZE);
    }

    lock_guard<mutex> lock(g_mutex);
    g_pstat[id].status = "done    ";
}
