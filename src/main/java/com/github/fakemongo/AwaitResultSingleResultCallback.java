package com.github.fakemongo;

import com.mongodb.async.SingleResultCallback;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class AwaitResultSingleResultCallback<T> implements SingleResultCallback<T> {

  private T result;
  private Throwable throwable;
  private CountDownLatch countDownLatch = new CountDownLatch(1);

  @Override
  public void onResult(T result, Throwable t) {
    this.result = result;
    throwable = t;
    countDownLatch.countDown();
  }

  public T awaitResult() throws Throwable {
    return awaitResult(1, TimeUnit.MINUTES);
  }

  public T awaitResult(long time, TimeUnit timeUnit) throws Throwable {
    if (!countDownLatch.await(time, timeUnit)) {
      throw new RuntimeException("take too much time...");
    }
    if (throwable != null) {
      throw throwable;
    }
    return result;
  }
}
