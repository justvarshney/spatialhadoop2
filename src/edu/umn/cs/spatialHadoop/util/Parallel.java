/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota.   *
 *                                                                 *
 * This Software is released under the Apache License, Version 2.0 *
 * http://www.apache.org/licenses/LICENSE-2.0                      *
 *******************************************************************/
package edu.umn.cs.spatialHadoop.util;

import java.util.Vector;

/**
 * SOme primitives to provide parallel processing over arrays and lists
 * @author Ahmed Eldawy
 *
 */
public class Parallel {

  private Parallel() { /* Static use only */ }
  
  public static interface RunnableRange<T> {
    public T run(int id, int i1, int i2);
  }
  
  /**
   * An interface that is implemented by users to loop over a partial array.
   * @author Ahmed Eldawy
   *
   */
  public static class RunnableRangeThread<T> extends Thread {
    protected int id;
    private int i1;
    private int i2;
    private RunnableRange<T> runnableRange;
    private T result;
    
    protected RunnableRangeThread(RunnableRange<T> runnableRange, int id, int i1, int i2) {
      super("Worker #"+id);
      this.id = id;
      this.i1 = i1;
      this.i2 = i2;
      this.runnableRange = runnableRange;
    }
    
    @Override
    public void run() {
      this.result = this.runnableRange.run(id, i1, i2);
    }
    
    public T getResult() {
      return result;
    }
  }
  
  public static <T> Vector<T> forEach(int size, RunnableRange<T> r) throws InterruptedException {
    int parallelism = Runtime.getRuntime().availableProcessors() * 2;
    final int[] partitions = new int[parallelism + 1];
    for (int i_thread = 0; i_thread <= parallelism; i_thread++)
      partitions[i_thread] = i_thread * size / parallelism;
    final Vector<RunnableRangeThread<T>> threads = new Vector<RunnableRangeThread<T>>();
    Vector<T> results = new Vector<T>();
    for (int i_thread = 0; i_thread < parallelism; i_thread++) {
      threads.add(new RunnableRangeThread<T>(r, i_thread, partitions[i_thread], partitions[i_thread+1]));
      threads.lastElement().start();
    }
    for (int i_thread = 0; i_thread < parallelism; i_thread++) {
      threads.get(i_thread).join();
      results.add(threads.get(i_thread).getResult());
    }
    return results;
  }

  /**
   * @param args
   * @throws InterruptedException 
   */
  public static void main(String[] args) throws InterruptedException {
    final int[] values = new int[1000000];
    for (int i = 0; i < values.length; i++)
      values[i] = i;
    Vector<Long> results = Parallel.forEach(values.length, new RunnableRange<Long>() {
      @Override
      public Long run(int id, int i1, int i2) {
        long total = 0;
        for (int i = i1; i < i2; i++)
          total += values[i];
        return total;
      }
    });
    long finalResult = 0;
    for (Long result : results) {
      finalResult += result;
    }
    System.out.println(finalResult);
  }

}