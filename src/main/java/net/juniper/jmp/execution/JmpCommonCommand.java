package net.juniper.jmp.execution;


import java.util.Random;

import net.juniper.jmp.execution.JmpCommandSettings.JmpCommandBuilder;
import net.juniper.jmp.execution.JmpCommandSettings.JmpCommandDuplicateHandler;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Func0;

import com.google.common.base.Preconditions;

/**
 * Extends and create any command
 *   
 *
 * @param <R>
 */
public abstract class JmpCommonCommand<R> extends JmpAbstractCommand<R> {
 

  public JmpCommonCommand(JmpCommonCommandBuilder bldr) {
    super(bldr.commandGroupKey, bldr.commandKey);
    this.commandSettings = new JmpCommonCommandSettings(bldr);
  }
  
  /**
   * The actual command being wrapped
   * @return
   */
  public abstract R run();
  
  @Override
  protected Observable<ObservableResult<R>> getExecutionObservable() {
    return Observable.create(new OnSubscribe<ObservableResult<R>>() {
      @Override
      public void call(Subscriber<? super ObservableResult<R>> subs) {
        if (subs.isUnsubscribed()) return;
        try {
          R result = run();
          subs.onNext(new ObservableResult<R>(result));
          
        } catch (Throwable ex) {
          subs.onNext(new ObservableResult<R>(ex));
        }
        subs.onCompleted();
      }});
  }
  
  @Override
  protected String getDefaltCommandKey() {
    return this.getClass().getName();
  }
  
  
  public static class JmpCommonCommandSettings extends JmpCommandSettings {

    public JmpCommonCommandSettings(JmpCommonCommandBuilder builder) {
      super(builder);
    }
    
  }
  
  public static class JmpCommonCommandBuilder extends JmpCommandBuilder {

    public static JmpCommonCommandBuilder withCommandGroupKey(JmpCommandGroupKey commandGroupKey) {
      Preconditions.checkArgument(commandGroupKey != null);
      return new JmpCommonCommandBuilder(commandGroupKey);
    }

    private JmpCommonCommandBuilder(JmpCommandGroupKey commandGroupKey) {
      this.commandGroupKey = commandGroupKey;
    }

    public JmpCommonCommandBuilder andDefaultKey() {
      Random random = new Random();
      this.commandKey = JmpCommandKey.Factory.asKey(commandGroupKey.name() + ":" + random.nextDouble());
      return this;
    }
    
    public JmpCommonCommandBuilder andCommandKey(JmpCommandKey key) {
      this.commandKey = key;
      return this;
    }

    public JmpCommonCommandBuilder andPrevalidate(JmpCommandPrevalidator validator) {
      this.commandPrevalidator = validator;
      return this;
    }

    public JmpCommonCommandBuilder andMemoryEstimator(Func0<Long> memoryEstimator, Long retryInMilli) {
      this.memoryEstimator = memoryEstimator;
      this.retryInMillsAfterEstimation = retryInMilli;
      return this;
    }

    public JmpCommonCommandBuilder andMaxRetry(Integer maxCommandRetry) {
      this.maxNumValidationRetry = maxCommandRetry;
      return this;
    }
    
    public JmpCommonCommandBuilder andDuplicateCommandHandler(JmpCommandDuplicateHandler duplicateHandler) {
      this.duplicateCommandHandler = duplicateHandler;
      return this;
    }

  }

}
