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
void log_event(const char *fmt, ...) {
    char body[LOG_LINE_LEN];
    char line[LOG_LINE_LEN + 30];
    
    auto now_c = chrono::system_clock::to_time_t(chrono::system_clock::now());
    struct tm *t = localtime(&now_c);

    va_list ap;
    va_start(ap, fmt);
    vsnprintf(body, sizeof(body), fmt, ap);
    va_end(ap);

    snprintf(line, sizeof(line), "[%02d:%02d:%02d] %s",
             t->tm_hour, t->tm_min, t->tm_sec, body);

    lock_guard<mutex> lock(g_log_lock);
    g_log[g_log_next] = string(line);
    g_log_next = (g_log_next + 1) % LOG_LINES;
    
    if (g_log_file.is_open()) {
        g_log_file << line << endl;
    }
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

void consumer(int id) {
    while (!g_shutdown) {
        {
            lock_guard<mutex> lock(g_mutex);
            g_cstat[id].status = "sleeping";
        }

        ms_sleep(rand_range(CONSUME_MIN_MS, CONSUME_MAX_MS));
        if (g_shutdown) break;
        {
            lock_guard<mutex> lock(g_mutex);
            g_cstat[id].status = "waiting ";
        }

        while (!g_shutdown) {
            if (g_full_sem.try_acquire_for(1000)) break;
        }
        if (g_shutdown) break;

        int slot, item, snap_count;
        {
            lock_guard<mutex> lock(g_mutex);
            slot         = g_out;
            item         = g_buf[g_out];
            g_buf[g_out] = 0;             
            g_out        = (g_out + 1) % BUFFER_SIZE;
            g_count--;
            
            g_total_consumed++;
            g_cstat[id].count++;
            g_cstat[id].last_val = item;
            g_cstat[id].status = "consumed";
            snap_count = g_count;
        }

        g_empty_sem.release();

        log_event("C%d consumed %2d <- slot[%d]  (buf %d/%d)",
                  id + 1, item, slot, snap_count, BUFFER_SIZE);
    }

    lock_guard<mutex> lock(g_mutex);
    g_cstat[id].status = "done    ";
}

void init_ui() {
    initscr();
    cbreak();
    noecho();
    curs_set(0);
    keypad(stdscr, TRUE);
    nodelay(stdscr, TRUE);

    if (has_colors()) {
        start_color();
        use_default_colors();
        init_pair(1, COLOR_GREEN,   -1);
        init_pair(2, COLOR_RED,     -1);
        init_pair(3, COLOR_CYAN,    -1);
        init_pair(4, COLOR_YELLOW,  -1);
        init_pair(5, COLOR_MAGENTA, -1);   
        init_pair(6, COLOR_WHITE,   -1);   
    }
}

void create_windows() {
    int rows, cols;
    getmaxyx(stdscr, rows, cols);

    int buf_h   = 9;
    int agent_h = NUM_PRODUCERS + NUM_CONSUMERS + 4;
    int log_h   = rows - 1 - buf_h - agent_h;
    if (log_h < 5) log_h = 5;

    int buf_w  = cols / 2;
    int stat_w = cols - buf_w;

    if (w_buf)    delwin(w_buf);
    if (w_stat)   delwin(w_stat);
    if (w_agents) delwin(w_agents);
    if (w_log)    delwin(w_log);

    w_buf    = newwin(buf_h,   buf_w,  1,                    0);
    w_stat   = newwin(buf_h,   stat_w, 1,                    buf_w);
    w_agents = newwin(agent_h, cols,   1 + buf_h,            0);
    w_log    = newwin(log_h,   cols,   1 + buf_h + agent_h,  0);
}

void draw_ui() {
    int rows, cols;
    getmaxyx(stdscr, rows, cols);
    (void)rows;
    vector<int> snap_buf;
    int  snap_count, snap_in, snap_out;
    long snap_tp, snap_tc;
    vector<AgentStat> sp, sc;

    {
        lock_guard<mutex> lock(g_mutex);
        snap_buf   = g_buf;
        snap_count = g_count;
        snap_in    = g_in;
        snap_out   = g_out;
        snap_tp    = g_total_produced;
        snap_tc    = g_total_consumed;
        sp         = g_pstat;
        sc         = g_cstat;
    }

    auto now = chrono::system_clock::now();
    auto elapsed = chrono::duration_cast<chrono::seconds>(now - g_start_time).count();
    int  emins   = (int)(elapsed / 60);
    int  esecs   = (int)(elapsed % 60);

    attron(COLOR_PAIR(3) | A_BOLD | A_REVERSE);
    mvhline(0, 0, ' ', cols);
    const char *title = " MULTI-THREADED PRODUCER-CONSUMER  |  CS-2006 OS ";
    mvprintw(0, (cols - (int)strlen(title)) / 2, "%s", title);
    mvprintw(0, cols - 15, " 'q' to quit ");
    attroff(COLOR_PAIR(3) | A_BOLD | A_REVERSE);

    werase(w_buf);
    wattron(w_buf, COLOR_PAIR(3) | A_BOLD);
    box(w_buf, 0, 0);
    mvwprintw(w_buf, 0, 2, " CIRCULAR BUFFER  [size=%d] ", BUFFER_SIZE);
    wattroff(w_buf, COLOR_PAIR(3) | A_BOLD);

    /* slot index row */
    mvwprintw(w_buf, 1, 2, "Slot: ");
    for (int i = 0; i < BUFFER_SIZE; i++)
        mvwprintw(w_buf, 1, 8 + i * 4, "[%d]", i);

    mvwprintw(w_buf, 2, 2, "Val:  ");
    for (int i = 0; i < BUFFER_SIZE; i++) {
        int occupied = 0;
        if (snap_count == BUFFER_SIZE) {
            occupied = 1;
        } else if (snap_count > 0) {
            if (snap_in > snap_out)
                occupied = (i >= snap_out && i < snap_in);
            else
                occupied = (i >= snap_out || i < snap_in);
        }

        if (occupied) {
            wattron(w_buf, COLOR_PAIR(1) | A_BOLD);
            mvwprintw(w_buf, 2, 8 + i * 4, "%2d ", snap_buf[i]);
            wattroff(w_buf, COLOR_PAIR(1) | A_BOLD);
        } else {
            wattron(w_buf, COLOR_PAIR(2) | A_DIM);
            mvwprintw(w_buf, 2, 8 + i * 4, "-- ");
            wattroff(w_buf, COLOR_PAIR(2) | A_DIM);
        }
    }

    mvwprintw(w_buf, 3, 2, "      ");
    for (int i = 0; i < BUFFER_SIZE; i++) {
        string ptr = "   ";
        if (i == snap_in && i == snap_out && snap_count == 0)
            ptr = " ^ ";
        else if (i == snap_in)
            ptr = " W ";
        else if (i == snap_out)
            ptr = " R ";
            
        wattron(w_buf, A_BOLD);
        mvwprintw(w_buf, 3, 8 + i * 4, "%s", ptr.c_str());
        wattroff(w_buf, A_BOLD);
    }
    mvwprintw(w_buf, 3, getmaxx(w_buf) - 14, "(W=in R=out)");

    int bar_w  = getmaxx(w_buf) - 14;
    int filled = (bar_w * snap_count) / BUFFER_SIZE;
    mvwprintw(w_buf, 5, 2, "Fill [");
    wattron(w_buf, COLOR_PAIR(1) | A_BOLD);
    for (int i = 0; i < filled; i++)       waddch(w_buf, ACS_BLOCK);
    wattroff(w_buf, COLOR_PAIR(1) | A_BOLD);
    wattron(w_buf, COLOR_PAIR(2) | A_DIM);
    for (int i = filled; i < bar_w; i++)   waddch(w_buf, '-');
    wattroff(w_buf, COLOR_PAIR(2) | A_DIM);
    wprintw(w_buf, "] %d/%d", snap_count, BUFFER_SIZE);

    mvwprintw(w_buf, 6, 2, "in=%-2d  out=%-2d", snap_in, snap_out);
    wnoutrefresh(w_buf);

    werase(w_stat);
    wattron(w_stat, COLOR_PAIR(3) | A_BOLD);
    box(w_stat, 0, 0);
    mvwprintw(w_stat, 0, 2, " STATISTICS ");
    wattroff(w_stat, COLOR_PAIR(3) | A_BOLD);

    wattron(w_stat, COLOR_PAIR(6));
    mvwprintw(w_stat, 1, 2, "Runtime    :  %02d:%02d",  emins, esecs);
    mvwprintw(w_stat, 2, 2, "Produced   :  %ld",        snap_tp);
    mvwprintw(w_stat, 3, 2, "Consumed   :  %ld",        snap_tc);
    mvwprintw(w_stat, 4, 2, "In buffer  :  %d",         snap_count);
    double tput = (elapsed > 0) ? (double)snap_tc / (double)elapsed * 60.0 : 0.0;
    mvwprintw(w_stat, 5, 2, "Throughput :  %.1f /min",  tput);
    mvwprintw(w_stat, 6, 2, "Producers  :  %d threads", NUM_PRODUCERS);
    mvwprintw(w_stat, 7, 2, "Consumers  :  %d threads", NUM_CONSUMERS);
    wattroff(w_stat, COLOR_PAIR(6));
    wnoutrefresh(w_stat);

    werase(w_agents);
    wattron(w_agents, COLOR_PAIR(3) | A_BOLD);
    box(w_agents, 0, 0);
    mvwprintw(w_agents, 0, 2, " THREAD ACTIVITY ");
    wattroff(w_agents, COLOR_PAIR(3) | A_BOLD);

    wattron(w_agents, A_UNDERLINE | A_BOLD);
    mvwprintw(w_agents, 1, 2, "%-14s %-12s %8s %10s", "Thread", "Status", "Count", "Last val");
    wattroff(w_agents, A_UNDERLINE | A_BOLD);

    for (int i = 0; i < NUM_PRODUCERS; i++) {
        wattron(w_agents, COLOR_PAIR(4));
        mvwprintw(w_agents, 2 + i, 2,
                  "Producer  %-2d  %-12s  %6ld      %4d",
                  sp[i].id + 1, sp[i].status.c_str(), sp[i].count, sp[i].last_val);
        wattroff(w_agents, COLOR_PAIR(4));
    }
    for (int i = 0; i < NUM_CONSUMERS; i++) {
        wattron(w_agents, COLOR_PAIR(5));
        mvwprintw(w_agents, 2 + NUM_PRODUCERS + i, 2,
                  "Consumer  %-2d  %-12s  %6ld      %4d",
                  sc[i].id + 1, sc[i].status.c_str(), sc[i].count, sc[i].last_val);
        wattroff(w_agents, COLOR_PAIR(5));
    }
    wnoutrefresh(w_agents);

    werase(w_log);
    wattron(w_log, COLOR_PAIR(3) | A_BOLD);
    box(w_log, 0, 0);
    mvwprintw(w_log, 0, 2, " EVENT LOG  ->  %s ", LOG_FILE.c_str());
    wattroff(w_log, COLOR_PAIR(3) | A_BOLD);

    int log_content_h = getmaxy(w_log) - 2;
    int n = (log_content_h < LOG_LINES) ? log_content_h : LOG_LINES;
    int log_w = getmaxx(w_log) - 4;

    lock_guard<mutex> lock(g_log_lock);
    for (int i = 0; i < n; i++) {
        int idx = (g_log_next - n + i + LOG_LINES * 100) % LOG_LINES;
        if (g_log[idx].empty()) continue;

        /* colour by producer vs consumer */
        if (g_log[idx].find(" P") != string::npos)
            wattron(w_log, COLOR_PAIR(4));
        else
            wattron(w_log, COLOR_PAIR(5));

        mvwprintw(w_log, 1 + i, 2, "%-*.*s", log_w, log_w, g_log[idx].c_str());
        wattroff(w_log, COLOR_PAIR(4));
        wattroff(w_log, COLOR_PAIR(5));
    }

    wnoutrefresh(w_log);
    doupdate();
}

void ui_thread() {
    init_ui();
    create_windows();

    while (!g_shutdown) {
        int ch = getch();
        if (ch == 'q' || ch == 'Q') {
            g_shutdown = true;
            break;
        }
        if (ch == KEY_RESIZE)
            create_windows();

        draw_ui();
        ms_sleep(UI_REFRESH_MS);
    }

    draw_ui();

    int rows, cols;
    getmaxyx(stdscr, rows, cols);
    const char *msg = "  Shutting down — waiting for threads...  ";
    attron(COLOR_PAIR(3) | A_BOLD | A_REVERSE);
    mvprintw(rows / 2, (cols - (int)strlen(msg)) / 2, "%s", msg);
    attroff(COLOR_PAIR(3) | A_BOLD | A_REVERSE);
    refresh();
    ms_sleep(1200);

    endwin();
}

void signal_handler(int) {
    g_shutdown = true;
}


void print_final_stats() {
    cout << "\n";
    cout << "╔══════════════════════════════════════════╗\n";
    cout << "║      FINAL  SIMULATION  STATISTICS       ║\n";
    cout << "╠══════════════════════════════════════════╣\n";
    printf("║  Total produced  : %-21ld ║\n", g_total_produced);
    printf("║  Total consumed  : %-21ld ║\n", g_total_consumed);
    printf("║  Items in buffer : %-21d ║\n", g_count);
    cout << "╠══════════════════════════════════════════╣\n";
    for (int i = 0; i < NUM_PRODUCERS; i++)
        printf("║  Producer %d      : %-5ld items produced  ║\n",
               i + 1, g_pstat[i].count);
    cout << "║                                          ║\n";
    for (int i = 0; i < NUM_CONSUMERS; i++)
        printf("║  Consumer %d      : %-5ld items consumed  ║\n",
               i + 1, g_cstat[i].count);
    cout << "╚══════════════════════════════════════════╝\n";
    cout << "Full log saved to: " << LOG_FILE << "\n\n";
}

int main() {
    g_start_time = chrono::system_clock::now();

    signal(SIGINT, signal_handler);

    g_log_file.open(LOG_FILE);
    if (!g_log_file.is_open()) {
        cerr << "Failed to open log file" << endl;
        return 1;
    }
    g_log_file << "=== Producer-Consumer Simulation Log ===\n"
               << "Buffer: " << BUFFER_SIZE << " slots | Producers: " 
               << NUM_PRODUCERS << " | Consumers: " << NUM_CONSUMERS << "\n\n";

    for (int i = 0; i < NUM_PRODUCERS; i++) {
        g_pstat[i].id = i;
    }
    for (int i = 0; i < NUM_CONSUMERS; i++) {
        g_cstat[i].id = i;
    }

    vector<thread> prod_threads;
    vector<thread> cons_threads;

    for (int i = 0; i < NUM_PRODUCERS; i++) {
        prod_threads.emplace_back(producer, i);
    }
    for (int i = 0; i < NUM_CONSUMERS; i++) {
        cons_threads.emplace_back(consumer, i);
    }
    
    thread ui_t(ui_thread);

    ui_t.join();

    g_shutdown = true;
    for (int i = 0; i < NUM_PRODUCERS + NUM_CONSUMERS; i++) {
        g_full_sem.release();
        g_empty_sem.release();
    }

    for (auto& t : prod_threads) {
        if (t.joinable()) t.join();
    }
    for (auto& t : cons_threads) {
        if (t.joinable()) t.join();
    }
    if (g_log_file.is_open()) {
        g_log_file << "\n=== Simulation ended ===\n";
        g_log_file.close();
    }

    print_final_stats();
    return 0;
}
