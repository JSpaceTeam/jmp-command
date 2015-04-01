package net.juniper.jmp.execution;

import java.util.concurrent.ConcurrentSkipListMap;

import net.juniper.jmp.cmp.jobManager.JobPrevalidationResult.ResultEnum;
import net.juniper.jmp.cmp.systemService.load.NodeLoadSummary;
import net.juniper.jmp.exception.JMPException;
import net.juniper.jmp.execution.JmpCommandSettings.JmpCommandBuilder;

import org.apache.log4j.Logger;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.schedulers.Schedulers;

import com.google.common.base.Optional;

public abstract class JmpAbstractCommand<R> implements JmpCommandInfo<R> {

  private static final Logger logger = Logger.getLogger(JmpAsyncEjbCommand.class);

  
  protected final JmpCommandGroupKey groupKey;
  
  protected final JmpCommandKey commandKey;
  
  protected JmpCommandSettings commandSettings;
  
  private JmpCommandResourceUtlizationHandler<R> resourceUtlizationHandler;
  
  private static final ConcurrentSkipListMap<String, JmpAbstractCommand<?>> currentActiveCommands =  new ConcurrentSkipListMap<String, JmpAbstractCommand<?>>();

  
  @testOnly public Boolean mockStats = new Boolean(false);
  
  
  protected JmpAbstractCommand(JmpCommandGroupKey groupKey, JmpCommandKey commandKey) {
    if (commandKey == null) {
      this.commandKey = JmpCommandKey.Factory.asKey(getDefaltCommandKey());
    } else {
      this.commandKey = commandKey;
    }
    this.groupKey = groupKey;
  }
  
  protected JmpAbstractCommand(JmpCommandBuilder builder) {
    this(builder.commandGroupKey, builder.commandKey);
  }
  
  protected void setSettings(JmpCommandSettings settings) {
    this.commandSettings = settings;
  }
  
  @Override
  public JmpCommandGroupKey getCommandGroup() {
    return groupKey;
  }

  @Override
  public JmpCommandKey getCommandKey() {
    return commandKey;
  }
  
  public final static Logger logger() {
    return logger;
  }
  
  /**
   * If resource is reserved on the IP then return the ip or else null
   * @return
   */
  protected String getReservedNodeIP() {
    if (resourceUtlizationHandler != null) {
       return resourceUtlizationHandler.getReservedNode().isPresent() ? resourceUtlizationHandler.getReservedNode().get().getIpAddress() : null;
    }
    return null;
  }
  
  /**
   * The default command key builder 
   * 
   * @return 
   */
  protected abstract String getDefaltCommandKey();
  
  
  protected abstract Observable<ObservableResult<R>> getExecutionObservable();
  
  
  /**
   * A cold observable that executes the command when a subscriber subscribes to it. The observable does not throw error if the future fails rather it wraps it around an Observable result.
   * This is because once an observable reports onError no further events are sent to the subscriber but this is not what we want. Hence if a splitted future fails we just call onNext and let the 
   * client handle it any way the want it to.
   * This API block and waits on the future to complete. There is not timeout specified at this point since Async EJB calls already have a timeout configuration on the EJB end.
   *  However clients can easily add a timeout using the returned observable ex. observable.timeout(...).subscribe(_
   * 
   * {{{
   *     
   *  Observable<ObservableResult<Integer>> asObservable = new JmpAsyncEjbCommand<Integer>(JmpAsyncCommandBuilder
   *      .withEJBCommandAndGroupKey(JmpEJBCommandConfig.Factory.withEJb(DomainManager.BEAN_NAME, "domainAdditionTest", 1, sumAll, sctx),
   *        JmpCommandGroupKey.Factory.asKey("domain-group")
   *       )
   *      .andClustered(true)
   *      .andSplitStrategy(new FixedSizeListSplitStrategy(), 1)).asObservable();
   *  //Now command is executed when someone subscribes. 
   *   asObservable.subscribe(new Subscriber<ObservableResult<Integer>>() {
   *     AtomicInteger answer = new AtomicInteger();
   *     @Override
   *     public void onCompleted() {
   *       logger.warn("Final Answer : " + answer.get() + " from thread : " + Thread.currentThread().getName());
   *     }
   *     @Override
   *     public void onError(Throwable e) {
   *       logger.error(e);
   *     }
   *     @Override
   *     public void onNext(ObservableResult<Integer> t) {
   *       int result;
   *       try {
   *         result = t.getResult();
   *         logger.warn("Get Answer " + result);
   *         answer.getAndAdd(result);
   *       } catch (Throwable e) {
   *         logger.warn("Result is not successfull " + e);
   *       }
   *    }});
   * 
   * }}}
   * @return
   * @throws Exception 
   */
  public Observable<ObservableResult<R>> toObservable() {

    Observable<ObservableResult<R>> obs = null;

    if (resourceUtlizationHandler == null) {
      resourceUtlizationHandler = new JmpCommandResourceUtlizationHandler<R>(this);
    }
    final JmpAbstractCommand<R> _command = this;
    obs = Observable
        .create(new OnSubscribe<ObservableResult<R>>() {
          @Override
          public void call(Subscriber<? super ObservableResult<R>> child) {
            if (child.isUnsubscribed()) {
              // Nothing to do here
              return;
            }
            if (commandSettings.failOnDuplicateCommand) {
              logger().warn(" Check command exists " + getCommandName());
              JmpAbstractCommand<?> _current =
                  currentActiveCommands.putIfAbsent(getCommandName(), _command);
              if (_current != null && _current != _command) {
                logger.error(" Command Exists reject this");
                child.onError(new JmpCommandDuplicateExecutionRejected(String.format(
                    " command %s : %s is already active this command is rejected", groupKey.name(),
                    commandKey.name())));
                return;
              }
            }

            Long memoryRequired = 0L;

            Optional<NodeLoadSummary> selectedNode = Optional.absent();
            memoryRequired = commandSettings.getMemoryEstimator().get().call();
            // Memory first
            if (memoryRequired > 0) {
              selectedNode =
                  resourceUtlizationHandler.calcuateAndGetAvailableNodeList(memoryRequired);
            }

            if (memoryRequired > 0 && !selectedNode.isPresent()) {

              child.onError(new JmpCommandResourceUnavailableException());

            } else {

              resourceUtlizationHandler.reserveResource(memoryRequired, selectedNode);

              JmpCommandPrevalidationResult jmpCommandPrevalidationResult =
                  commandSettings.getPrevalidator().get().prevalidate();
              if (jmpCommandPrevalidationResult.getResult() == ResultEnum.SUCCESS) {
                resourceUtlizationHandler.setCommandRunning();
                getDecoratedObservable().unsafeSubscribe(child);

              } else if (jmpCommandPrevalidationResult.getResult() == ResultEnum.QUEUED) {
                child.onError(new JmpCommandPrevalidationException(jmpCommandPrevalidationResult));
                releaseResource();
              } else {
                child.onError(new JMPException(jmpCommandPrevalidationResult.getSummary()));
              }
            }
          }
        }).retryWhen(new JmpCommandPrevalidationHandler<R>(this).retryHandler())
        .subscribeOn(Schedulers.io());


    return obs.doOnError(onError()).doOnCompleted(cleanupAfterCommandTerimation());
  }
  
  
 
  private Action1<Throwable> onError() {
    final JmpAbstractCommand<R> _command = this;
    return new Action1<Throwable>() {
      @Override
      public void call(Throwable t1) {
        logger.error(String.format("Command %s failed to execute ", _command.getCommandKey().name()), t1);
        cleanupAfterCommandTerimation().call();
      }
    };
    
  }
  /**
   * Command Cleanup
   * @return
   */
  private Action0 cleanupAfterCommandTerimation() {
    final JmpAbstractCommand<R> _command = this;
    return new Action0() {
      @Override
      public void call() {
        _command.releaseResource();
        currentActiveCommands.remove(getCommandName(), _command);
      }
    };
  }
  
  /**
   * decorated observable 
   * @return
   */
  private Observable<ObservableResult<R>> getDecoratedObservable() {
    Observable<ObservableResult<R>> obs = null;
    if (commandSettings != null && commandSettings.getPrevalidator().isPresent()) {
       obs = Observable.create(new OnSubscribe<ObservableResult<R>>() {
        @Override
        public void call(Subscriber<? super ObservableResult<R>> child) {
          if (child.isUnsubscribed()) {
            //Nothing to do here
            return;
          }
          getExecutionObservable().unsafeSubscribe(child);
          
        }});
       
    } else {
      obs = getExecutionObservable();
    }
    return obs;
  }
  
 
  protected void releaseResource() {
    if (resourceUtlizationHandler != null) {
        resourceUtlizationHandler.freeResource();
    }
  }
  
  
  protected String getCommandName() {
    return groupKey.name() + ":" + commandKey.name();
  }

  protected Long getRetryTime() {
    return resourceUtlizationHandler.getRetrytime();
  }
}
