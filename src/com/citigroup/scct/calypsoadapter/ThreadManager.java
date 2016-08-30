package com.citigroup.scct.util;

import java.util.*;
import org.apache.log4j.Logger;


public class ThreadManager {
	
  public transient static int DEFAULT_PRIORITY = Thread.NORM_PRIORITY;
  protected transient static final int DEFAULT_THREAD_COUNT = 15;
  private transient static final int V_TIMEOUT = 3000;
  //
  private String requestType; //the type of requests handled by this thread manager
  private int priority;

  //The first queue keeps the requests that can be processed by any thread.
  //Each other queue keeps requests for an allocated thread.
  protected transient ArrayList queues;
                                        
  protected transient WorkerThread[] workers;
  private transient volatile int runningCount = 0; //count of running worker threads
  private transient Object completed;
  private transient volatile boolean shutdown = false;
  private transient volatile boolean stopping = false;
  //
  private boolean allocationOn = false;
  private boolean renamingOn = false;
  private HashMap allocatedThreads, allocatedUsers;
  //
  private Object stats = new Object();
  private transient long STALL_THRESHOLD = 30000;
  private boolean possibleStall = false;
  protected transient ArrayList activeRequests; //requests which are being processed
  private transient volatile int activeCount = 0; //count of requests currently being processed
  private long maxQueueTime = 0;
  private long maxProcessTime = 0;
  private long lastEnqueueTime = 0;
  private long totalQueueTime = 0;
  private long totalProcessTime = 0;
  private long lastCollectionTime = System.currentTimeMillis();
  private int maxActiveCount = 0;
  private int maxQueueSize = 0;
  private boolean isFailover;
  private int requestStartCount = 0; //count of requests which have started since last stat collection
  private int requestFinishCount = 0; //count of requests which have finished
  private int enqueueCount = 0; //count of requests which have been placed on the queue
  private Logger logger;

  public ThreadManager() {
    this(DEFAULT_THREAD_COUNT);
  }

  public ThreadManager(int threadCount) {
    this(threadCount, DEFAULT_PRIORITY);
  }

  public ThreadManager(int threadCount, String requestType) {
    this(threadCount, DEFAULT_PRIORITY, requestType);
  }

  public ThreadManager(int threadCount, String requestType, boolean allocationOn_) {
    this(threadCount, DEFAULT_PRIORITY, requestType, allocationOn_);
  }

  public ThreadManager(int threadCount, String requestType, boolean allocationOn_, boolean renamingOn) {
    this(threadCount, DEFAULT_PRIORITY, requestType, allocationOn_);
    this.renamingOn = renamingOn;
  }

  public ThreadManager(int threadCount, int priority) {
    this(threadCount, priority, "Request");
  }

  public ThreadManager(int threadCount, int priority, String requestType) {
    this(threadCount, priority, requestType, false);
  }

  public ThreadManager(int threadCount, int priority_, String requestType_, boolean allocationOn_) {
	logger = Logger.getLogger(this.getClass().getName());//log4j logger
    requestType = requestType_;
    priority = priority_;
    queues = new ArrayList();
    queues.add(new WorkQueue());
    activeRequests = new ArrayList();
    workers = new WorkerThread[threadCount];
    completed = new Object();

    if (allocationOn_){
      allocationOn = true;
      allocatedThreads = new HashMap();
      allocatedUsers = new HashMap();
    }

    for (int k = 0; k < threadCount; k++){
      workers[k] = new WorkerThread(k+1);
      workers[k].setPriority(priority);
      if (allocationOn){
        queues.add(new WorkQueue());
        //set up an empty list of allocated users
        allocatedUsers.put(workers[k], new Vector());
      }
    }
    //start workers after complete initialization
    for (int k = 0; k < threadCount; k++)
      workers[k].start();
  }

  public String getName() {
    return requestType+"ThreadManager";
  }


  public int getActiveCount() {
    return activeCount;
  }
  
  public void setFailover(boolean failover){
	  isFailover = failover;
  }

  //Only for threads which will not be preassigned
  public void increaseQueueSize(int num) {
    if (allocationOn)
      throw new RuntimeException("This threadManager cannot increase its size!");

    //build new workers
    WorkerThread[] newWorkers = new WorkerThread[num];
    for (int k = 0; k < num; k++){
      newWorkers[k] = new WorkerThread(workers.length+k+1);
      newWorkers[k].setPriority(priority);
      newWorkers[k].start();
    }
    WorkerThread[] tmpWorkers = workers;
    workers = new WorkerThread[num + tmpWorkers.length];
    System.arraycopy(tmpWorkers, 0, workers, 0, tmpWorkers.length);
    System.arraycopy(newWorkers, 0, workers, tmpWorkers.length, num);
  }
  
  private void processValues(RunnableRequest value){
	  if(value != null && value.isValid && isFailover){
		  while(value.finishedTime == 0){
			  synchronized(value){
				  try{
					  value.wait();
				  }
				  catch(Exception e){
				  }
			  }
		  }
	  }
  }


  //allocate a thread with the least workload to a given user
  public void allocateThread(String user){
    if (user == null) return;
    synchronized(allocatedThreads){
      WorkerThread thread = (WorkerThread)allocatedThreads.get(user);
      if (thread != null) return; //avoid duplicates

      //iterate thru all threads
      Iterator threads = allocatedUsers.keySet().iterator();
      int minLoad = Integer.MAX_VALUE;
      WorkerThread minThread = null;
      Vector minUsers = null;

      while(threads.hasNext()){
        thread = (WorkerThread)threads.next();
        Vector users = (Vector)allocatedUsers.get(thread);
        int usize = users.size();
        if (usize < minLoad){
          minLoad = usize;
          minThread = thread;
          minUsers = users;
        }
      }
      allocatedThreads.put(user, minThread);
      minUsers.add(user);
      //this only make sense if there is one user per thread
      if(renamingOn)
        minThread.setName(minThread.getName() + "(" + user + ")");
      logger.info( "allocated " + user + " to ");
    }
  }

  public void deallocateThread(String user){
    if (user == null) return;
    WorkerThread thread = null;
    synchronized(allocatedThreads){
      thread = (WorkerThread)allocatedThreads.remove(user); //remove the thread
    }
    if (thread == null) {
      logger.warn("Warning: could not deallocate " + user + " - never allocated to a thread");
      return;
    }

    //remove the user
    Vector users = (Vector)allocatedUsers.get(thread);
    if (!users.remove(user)){
      logger.warn("Warning: could not deallocate " + user + " from " + thread);
      return;
    }

    //remove the user's requests - user context may not exist!
    WorkQueue queue = (WorkQueue)queues.get((int)thread.getId());
    synchronized(queue){
      for(int k=queue.size()-1; 0 <= k; k--){
        RunnableRequest request = (RunnableRequest)queue.get(k);
        if (user.equals(request.getUserId()))
          queue.remove(k);
      }
    }
    logger.info("deallocated " + user + " from " + thread);
  }

  public void process(String userId, RunnableRequest request) {
    request.setUserId(userId);
    process(request);
  }

  public void process(RunnableRequest request) {
    long qid = 0;
    if (allocationOn){
      WorkerThread thread = null;
      synchronized(allocatedThreads){
        thread = (WorkerThread)allocatedThreads.get(request.getUserId());
      }
      if (thread == null)
        throw new RuntimeException(
          "Not thread has been assigned to process requests from user " + request.getUserId());
      qid = thread.getId();
    }
    ((WorkQueue)queues.get((int)qid)).enqueue(request);
    processValues(request);
  }
  
  /* WorkThread
   */
  protected class WorkerThread extends Thread {
    long id;

    public WorkerThread(long id_) {
      super(requestType+"Thread-"+id_);
      id = id_;
    }

  
    public long getId(){
      return id;
    }

    public String toString(){
      return super.getName();
    }

    public void run() {
      runningCount++;
      try {
        while (!shutdown && !Thread.interrupted()) {
          WorkQueue queue = (WorkQueue) (allocationOn ? queues.get((int)id) : queues.get(0));
          RunnableRequest request = queue.dequeue();

          if (request != null) {
            try {
              request.run();
            } finally {
              queue.endRequest(request);
            }
          }
          synchronized (completed) {
            completed.notify();
          }
        }
      } catch (InterruptedException ie) {
      } catch (Throwable exc) {
      } finally {
        runningCount--;
        synchronized (completed) {
          completed.notify();
        }
      }
    }
  } //WorkThread

  //test if all queues are empty
  protected boolean isQueueEmpty() {
    boolean status = true;
    for(int q = 0; q < queues.size(); q++){
      WorkQueue queue = (WorkQueue)queues.get(q);
      status &= queue.isEmpty();
    }
    return status;
  }

  public List getQueueSize() {
    List sizes = new ArrayList();
    for(int q = 0; q < queues.size(); q++){
      WorkQueue queue = (WorkQueue)queues.get(q);
      sizes.add(new Integer(queue.size()));
    }
    return sizes;
  }

  /* WorkQueue
   */
  protected class WorkQueue extends ArrayList {

    public synchronized int size() {
      return super.size();
    }

    public synchronized boolean isEmpty(){
      return super.isEmpty();
    }

    // used to track stats
    protected synchronized void enqueue(RunnableRequest request) {
      if (stopping)
        throw new IllegalThreadStateException("ThreadManager is shutting down");
      add(0,request);
      notify();

      synchronized (stats) {
        request.notifyEnqueued();
        request.setType(requestType);
        enqueueCount++;
        lastEnqueueTime = System.currentTimeMillis();
        possibleStall = true;
        maxQueueSize = Math.max(size(), maxQueueSize);
        request.notifyEnqueued();
      }
    }

    protected synchronized RunnableRequest dequeue() throws InterruptedException {
      RunnableRequest request = null;
      while (isEmpty() && !shutdown) {
        wait(V_TIMEOUT);
      }
      if (shutdown) return null;
      request = (RunnableRequest)remove(size()-1);
      beginRequest(request);
      return request;
    }

    // used to track stats
    private void beginRequest(RunnableRequest request) {
      synchronized(stats){
        request.notifyDequeued();
        activeCount++;
        requestStartCount++;
        possibleStall = false;
        maxActiveCount = Math.max(activeCount, maxActiveCount);
        long qtime = request.getQueueTime();
        maxQueueTime = Math.max(qtime, maxQueueTime);
        totalQueueTime += qtime;
        activeRequests.add(request);
      }
    }

    // used to track stats
    protected void endRequest(RunnableRequest request) {
      synchronized (stats) {
        request.notifyFinished();
        activeRequests.remove(activeRequests.indexOf(request));
        activeCount--;
        requestFinishCount++;
        long ptime = request.getProcessTime();
        maxProcessTime = Math.max(ptime, maxProcessTime);
        totalProcessTime += ptime;
      }
    }
  }

  /** Wait for the queue to empty, and prevent new requests from being posted.
   * <CR>
   * Ignoring interruptions since predicate dictates stopping condition
   */
  protected void waitForQueueToEmpty() {
    stopping = true;
    int tries = 0;
    synchronized (completed) {
      while (tries < 3 && !isQueueEmpty() && !Thread.interrupted()) 
        try {
          // assert (queue is not empty && completed will be notified)
          completed.wait(2000);
          tries++;
        } catch (InterruptedException ie) {
          return;
        }
    }
  }

  /** Direct a shutdown of running threads and wait for them to exit. <CR>
   * Ignoring interruptions since predicate dictates stopping condition
   */
  protected void waitForThreadsToFinish() {
    shutdown = true;
    for(int q=0; q < queues.size(); q++){
      WorkQueue queue = (WorkQueue)queues.get(q);
      synchronized (queue) {
        queue.notifyAll();
      }
    }
    synchronized (completed) {
      int tries = 0;
      while (runningCount > 0 && tries < 10 && !Thread.interrupted()) {
        tries++;
        try {
          completed.wait(2000);
          logger.info("Waiting for thread shutdown: "+runningCount);
        } catch (InterruptedException ie) {
          return;
        }
      }
    }
  }

  public void shutdown() {
    waitForQueueToEmpty();
    for (int index = 0; workers != null && index < workers.length; index++) {
      if (workers[index] != Thread.currentThread())
        workers[index].interrupt();
    }
    waitForThreadsToFinish();
    for (int index = 0; workers != null && index < workers.length; index++) {
      if (workers[index] != Thread.currentThread())
        workers[index] = null;
    }
  }

  private String formatTime(long time)
  {
    int seconds = (int)(time / 1000);
    int minutes = seconds / 60;
    int hours = minutes / 60;

    int millis = (int)(time % 1000);
    seconds = seconds % 60;
    minutes = minutes % 60;

    return hours + ":" + minutes + ":" + seconds + "." + millis;
  }
}
