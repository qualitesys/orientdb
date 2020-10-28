package com.orientechnologies.orient.core.db;

import java.util.TimerTask;

public interface OSchedulerInternal {

  /**
   * @param task
   * @param delay milliseconds
   * @param period
   */
  void schedule(TimerTask task, long delay, long period);

  /**
   * @param task
   * @param delay milliseconds
   */
  void scheduleOnce(TimerTask task, long delay);
}
