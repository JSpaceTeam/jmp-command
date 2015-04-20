package net.juniper.jmp.execution;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.functions.Func1;
import rx.functions.Func2;

class JmpCommandPrevalidationHandler<R> implements JmpCommandRetryHandler {
  
  private final JmpAbstractCommand<R> command;
  
  
  protected JmpCommandPrevalidationHandler(JmpAbstractCommand<R> command) {
    this.command = command;
  }
  
  private final Func1<Observable<? extends Throwable>, Observable<?>> retryHandler = new Func1<Observable<? extends Throwable>, Observable<?>>() {
    @Override
    public Observable<?> call(Observable<? extends Throwable> sourceObservable) {
      return sourceObservable.zipWith(Observable.range(1, command.commandSettings.getMaxCommandRetry()), new Func2<Throwable, Integer, IntermediateResultPassthrough>() {
        @Override
        public IntermediateResultPassthrough call(final Throwable throwable, Integer retryCount) {
          IntermediateResultPassthrough pass = new IntermediateResultPassthrough(retryCount, throwable);
          return pass;
        }
      }).flatMap(new Func1<IntermediateResultPassthrough, Observable<?>>() {

        @Override
        public Observable<?> call(IntermediateResultPassthrough passThroughResult) {
          
          if (!isExceptionRetriable(passThroughResult.t)) {
            return Observable.error(passThroughResult.t);
          }
          JmpAbstractCommand.logger().warn("Retry due to prevalidation failure command: " + command.getCommandName() + " Error: " + passThroughResult.t.getMessage());
          if (passThroughResult.retryCount == (command.commandSettings.getMaxCommandRetry())) {
            return Observable.error(new JmpCommandRetryTimedOutException("Max retry exceeded  ", passThroughResult.t));
          }
          return Observable.timer(getRetryInMillis(passThroughResult), TimeUnit.MILLISECONDS);
        }
      });
    }
  };
  
  
  private boolean isExceptionRetriable(Throwable t) {
    if (t instanceof JmpCommandPrevalidationException || t instanceof JmpCommandResourceUnavailableException) {
      return true;
    }
    return false;
  }
  
  private Long getRetryInMillis(IntermediateResultPassthrough passThroughResult) {
    if (passThroughResult.t instanceof JmpCommandPrevalidationException) {
      JmpCommandPrevalidationException prevalidationException = (JmpCommandPrevalidationException) passThroughResult.t;
      return prevalidationException.getResult().getRetryTimeInMillis();
    } else {
      JmpAbstractCommand.logger().info(
        "No resource avaiable for " + command.groupKey.name() + ":"
            + command.commandKey.name() + " reschedule later " + command.getRetryTime());
      return command.getRetryTime();
    }
  }
  
  public  Func1<Observable<? extends Throwable>, Observable<?>>  retryHandler() {
    return retryHandler;
  }
 
  
  
  protected static class IntermediateResultPassthrough implements Serializable {

    /**
    * 
    */
    private static final long serialVersionUID = 1L;

    int retryCount;

    Throwable t;
    

    public IntermediateResultPassthrough(int retryCount,Throwable t) {
      this.retryCount = retryCount;
      this.t = t;
    }
  }
}
