use std::{
    sync::{Arc, Mutex, mpsc},
    thread,
};

pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Option<mpsc::Sender<Job>>,
}
type Job = Box<dyn FnOnce() + Send + 'static>;

impl ThreadPool {
    pub fn new(size: usize) -> ThreadPool {
        assert!(size > 0);
        let (sender, reciever) = mpsc::channel();
        let reciever = Arc::new(Mutex::new(reciever));
        let mut workers = Vec::with_capacity(size);

        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&reciever)));
        }

        ThreadPool {
            workers,
            sender: Some(sender),
        }
    }
    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);
        self.sender.as_ref().unwrap().send(job).unwrap();
    }
}

struct Worker {
    id: usize,
    thread: thread::JoinHandle<()>,
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Job>>>) -> Worker {
        let thread = thread::spawn(move || {
            loop {
                let message = receiver.lock().unwrap().recv();

                match message {
                    Ok(job) => {
                        println!("Worker {id} got a job; executing.");
                        job();
                    }
                    Err(_) => {
                        println!("Worker {id} disconnected; shutting down");
                        break;
                    }
                }
            }
        });
        Worker { id, thread }
    }
}
// IMPORTANT: Be careful with MutexGuard lifetimes.
//
// `receiver.lock()` returns a MutexGuard, which keeps the mutex locked
// for as long as the guard is alive.
//
// This is good:
// let job = receiver.lock().unwrap().recv().unwrap();
// // MutexGuard is dropped here, so the lock is released.
// job();
//
// But this is problematic:
// while let Ok(job) = receiver.lock().unwrap().recv() {
//     job();
// }
// `while let` keeps the temporary MutexGuard alive for the entire loop body,
// so the mutex remains locked while `job()` runs.
//
// If `job()` is slow, other workers cannot lock the receiver to get jobs.
// Therefore, acquire the lock only long enough to receive the job,
// then release it before executing the job.

impl Drop for ThreadPool {
    fn drop(&mut self) {
        drop(self.sender.take());

        for worker in self.workers.drain(..) {
            println!("Shutting down worker {}", worker.id);
            worker.thread.join().unwrap();
        }
    }
}
