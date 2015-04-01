package net.juniper.jmp.execution;

import rx.Observable;
import rx.functions.Func1;

public interface JmpCommandRetryHandler {

  Func1<Observable<? extends Throwable>, Observable<?>> retryHandler();
  
}
