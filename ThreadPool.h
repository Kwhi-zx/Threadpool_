#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <vector>
#include <queue>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>
#include <functional>
#include <stdexcept>
#include <type_traits>


class Threadpool {
public:
    Threadpool(size_t);
    template<class F, class... Args>
    auto enqueue(F&& f, Args&&... args) 
        -> std::future<std::invoke_result_t<F, Args...>>;
    ~Threadpool();
private:
    /* need to keep track of threads so we can join them */
    std::vector< std::thread > workers;
    /* the task queue */
    std::queue< std::function<void()> > tasks; 
    /* synchronization */
    std::mutex queue_mutex;
    std::condition_variable condition;
    /* 标记线程池是否停止 */
    bool stop;  
};


/* 构造函数: 往workers中添加工作线程 */
Threadpool::Threadpool(size_t threads_num): stop(false) {
    for(size_t i = 0; i < threads_num; i++) {
        /* emplace_back(args) */
        this->workers.emplace_back(
            /* 每个线程都会执行这个 lambda */
            [this] { /* 捕获当前 Threadpool 对象指针 */
                for(;;) { 
                    std::function<void()> task;
                    {   /* {} 内上锁 */
                        std::unique_lock<std::mutex> lock(this->queue_mutex);
                        /* 等待条件成立 */
                        this->condition.wait(
                            lock, 
                            [this] {
                                return this->stop || !this->tasks.empty();
                            });
                        /* 线程池停止并且没有任务时 */
                        if(this->stop && this->tasks.empty()) {
                            return;
                        }
                        /* 线程池运行或者仍有任务时 */
                        task = std::move(this->tasks.front());
                        this->tasks.pop();
                        /* {} 外自动释放锁 */
                    }
                    /* 运行任务 */
                    task();
                }
            }
        );
    }
}

/**
 *  函数模板：add new work item to the pool
 *  # && 表示万能引用/转发引用
 *  · F&& f：接收任务函数
 *  · Args&& ... args：接收任务参数
 *  · 返回值：返回一个std::future
 *  · -> std::future<xxx> : 尾置返回类型
 * 
 * 
 *  std::queue<std::function<void()>> tasks;
 *  --> void()： 无参数、无返回值 --> task() 直接执行
 *  --> 用户提交的任务可能有参数、有返回值
 *  --> layer 1: 用 std::bind 把函数和参数绑起来，变成无参数调用
 *  --> layer 2: 用 std::packaged_task 保存返回值，让调用者之后能拿
 */
template<class F, class... Args>
auto Threadpool::enqueue(F&& f, Args&&... args)
    -> std::future<std::invoke_result_t<F, Args...>>
{
    using return_type = std::invoke_result_t<F, Args...>;

    /* 绑定函数与参数 */
    /* std::forward<T>(),完美转发 */
    auto bound_function = std::bind(std::forward<F>(f), std::forward<Args>(args)...);

    /* 定义 packaged_task类型 */
    using task_type = std::packaged_task<return_type()>;

    /* 用 shared_ptr 管理 */
    auto task = std::make_shared<task_type>(std::move(bound_function));

    /* 从 packaged_task 里取出一个 future，用来以后获取这个任务的执行结果 */
    std::future<return_type> res = task->get_future();

    {
        std::unique_lock<std::mutex> lock(this->queue_mutex);

        if(stop) {
            throw std::runtime_error("enqueue on stopped ThreadPool");
        }

        /* 将一个可以执行 packaged_task 的 lambda 放进任务队列 tasks 中 */
        tasks.emplace(
            [task]() {
                (*task)();
            }
        );
    }

    condition.notify_one();
    return res;
}

/* 析构函数 */
Threadpool::~Threadpool() {
    {
        std::unique_lock<std::mutex> lock(this->queue_mutex);
        this->stop = true;
    }
    /* 通知所有等待条件的task，stop现在为true */
    this->condition.notify_all();
    for(std::thread &worker:workers) {
        /* 等待线程结束 */
        worker.join();
    }
    /* 析构函数结束时，销毁成员变量 */
}


#endif

