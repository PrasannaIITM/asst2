#include "tasksys.h"
#include <thread>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads), num_threads(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}


void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    if (num_total_tasks == 0) return;

    // ---- 1. shared state ---------------------------------------------------
    std::atomic<int> next_task{0};

    // ---- 2. worker lambda ---------------------------------------------------
    auto worker = [&]() {
        while (true) {
            int i = next_task.fetch_add(1, std::memory_order_relaxed);
            if (i >= num_total_tasks) break;
            runnable->runTask(i, num_total_tasks);
        }
    };

    // ---- 3. launch threads --------------------------------------------------
    int n = std::min(num_threads, num_total_tasks);
    std::thread* threads = new std::thread[num_threads];

    for (int t = 0; t < n; ++t) threads[t] = std::thread(worker);

    // ---- 4. join ------------------------------------------------------------
    for (int t = 0; t < n; ++t) threads[t].join();
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

/* -------------------- worker thread body --------------------- */
void TaskSystemParallelThreadPoolSpinning::workerLoop() {
    while (true) {
        /* graceful shutdown */
        if (shutdown.load(std::memory_order_acquire)) break;

        /* fast-path: no work */
        if (!has_work.load(std::memory_order_acquire)) {
            std::this_thread::yield();
            continue;
        }

        /* steal a task index */
        int i = next_task.fetch_add(1, std::memory_order_relaxed);
        if (i >= total_tasks.load(std::memory_order_relaxed)) {
            /* queue empty – yield and re-check */
            std::this_thread::yield();
            continue;
        }

        /* run it (outside any critical section) */
        current_runnable->runTask(i, total_tasks.load(std::memory_order_relaxed));

        /* book-keeping */
        if (tasks_left.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            /* last task finished – clear the work flag */
            /* need to do this for synchronosity as has work needs to be set false only after last worker completes its last task */
            has_work.store(false, std::memory_order_release);
        }
    }
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads), num_threads(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    workers = new std::thread[num_threads];   // default-construct N empty threads
    for (int t = 0; t < num_threads; ++t) {
        // create a temporary std::thread that runs workerLoop(this)
        workers[t] = std::thread(&TaskSystemParallelThreadPoolSpinning::workerLoop,
                                 this);        // <-- pass the 'this' pointer
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    shutdown.store(true, std::memory_order_release);
    for (int t = 0; t < num_threads; ++t) workers[t].join();
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    /* initialise shared launch state */
    current_runnable = runnable;
    total_tasks.store(num_total_tasks, std::memory_order_relaxed);
    next_task.store(0, std::memory_order_relaxed);
    tasks_left.store(num_total_tasks, std::memory_order_relaxed);
    has_work.store(true, std::memory_order_release);

    /* wait until every worker observes tasks_left_ == 0 */
    while (tasks_left.load(std::memory_order_acquire) != 0) {
        std::this_thread::yield();
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
