package com.citigroup.scct.util;



public abstract class RunnableRequest implements Runnable {

  long enqueueTime = 0;
  long dequeueTime = 0;
  long finishedTime = 0;
  boolean isValid;
  String userId = null; //For thread allocation
  String threadName = null; //For stats
  String type = null; //For stats

  public abstract void execute() throws Exception;

  public abstract void handleThreadException(Throwable exception);

  //For thread allocation - called by ThreadManager.process(userId, request)
  public void setUserId(String id){
    userId = id;
  }

  //For ThreadManager to track requests
  public String getUserId(){
    return userId;
  }

  //For stats collection
  public String getWorkerName(){
    return threadName; //set in notifyDequeued
  }

  //For stats collection - set by worker threads
  //Used for building default name
  public void setType(String type_){
    type = type_;
  }
  
  public void setIdValue(String value){
	  if(value != null && value.toUpperCase().
			  indexOf("LOGIN") == -1){
		  isValid = true;
	  }
  }
  
  //For stat collector - overridden by subclasses to give more details
  public String toString() {
    return (userId != null ? userId+"."+type: type);
  }

  void notifyEnqueued() {
    enqueueTime = System.currentTimeMillis();
  }

  void notifyDequeued() {
    threadName = Thread.currentThread().getName();
    dequeueTime = System.currentTimeMillis();
  }

  void notifyFinished() {
	  synchronized(this){
		  finishedTime = System.currentTimeMillis();
		  notify();
	  }  
  }

  long getQueueTime() {
    return dequeueTime - enqueueTime;
  }

  long getProcessTime() {
    return finishedTime - dequeueTime;
  }

  public void run() {
    try { 
      execute();
    } catch (Throwable throwable) {
      handleThreadException(throwable);
    }
  }
}
