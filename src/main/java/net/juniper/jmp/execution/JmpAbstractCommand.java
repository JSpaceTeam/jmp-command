package net.juniper.jmp.execution;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import net.juniper.jmp.cmp.jobManager.JobPrevalidationResult.ResultEnum;
import net.juniper.jmp.cmp.systemService.load.NodeLoadSummary;
import net.juniper.jmp.common.configuration.ServerConfiguration;
import net.juniper.jmp.exception.JMPException;
import net.juniper.jmp.execution.JmpCommandSettings.JmpCommandBuilder;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.schedulers.Schedulers;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public abstract class JmpAbstractCommand<R> implements JmpCommandInfo {

  private static final Logger logger = Logger.getLogger(JmpAsyncEjbCommand.class);

  
  public enum JmpCommandState {
    UNSUBSCRIBED, SCHEDULED, PENDING, VALIDATING, RUNNING, COMPLETED;
  }
  
  public enum JmpDuplicateCommandAction {
    REJECT, QUEUE
  }
  
  
  private AtomicReference<JmpCommandState> commandState = new AtomicReference<>(JmpCommandState.UNSUBSCRIBED);
  
  private AtomicReference<CountDownLatch> waitCompleteLatch = new AtomicReference<>();  
  
  protected final JmpCommandGroupKey groupKey;
  
  protected final JmpCommandKey commandKey;
  
  protected JmpCommandSettings commandSettings;
  
  private JmpCommandResourceUtlizationHandler<R> resourceUtlizationHandler;
  
  private AtomicReference<Observable<ObservableResult<R>>> actualCommand = new AtomicReference<Observable<ObservableResult<R>>>();
  
  final JmpCommandMetrics metrics;
  
  private static final ExecutorService commandExecutor = new ThreadPoolExecutor(Runtime
    .getRuntime().availableProcessors(), ServerConfiguration.getUntypedAsInt("jmp-command-max-threads", 32, new Predicate<Integer>() {
      @Override
      public boolean apply(Integer value) {
        //Lets no allow uses to put too many threads here
        return value > 0 && value < 100;
      }}), 60, TimeUnit.SECONDS,
    new LinkedBlockingQueue<Runnable>(), new ThreadFactoryBuilder().setNameFormat("Jmp-Command-Pool-%d").build());
  
  
  
  private static final ConcurrentSkipListMap<String,JmpAbstractCommand<?>> currentActiveCommands =  new ConcurrentSkipListMap<>();

  @testOnly public Boolean mockStats = new Boolean(false);
  
  
  
  protected JmpAbstractCommand(JmpCommandGroupKey groupKey, JmpCommandKey commandKey) {
    if (commandKey == null) {
      this.commandKey = JmpCommandKey.Factory.asKey(getDefaltCommandKey());
    } else {
      this.commandKey = commandKey;
    }
    this.groupKey = groupKey;
    this.metrics = JmpCommandMetrics.getInstance(groupKey, commandKey);
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
  
  @Override
  public boolean isCommandExecuted() {
    return commandState.get().ordinal() >= JmpCommandState.COMPLETED.ordinal();
  }
  
  @Override
  public JmpCommandState getCommandState() {
    return commandState.get();
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
  
  public String getCommandMetrics() {
    if (isCommandExecuted()) {
      return metrics.getStatsOutput();
    }
    return "";
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
            commandState.set(JmpCommandState.VALIDATING);
            metrics.markResouceWaitEnd();
            metrics.markValidationQueueEnd();
            JmpAbstractCommand<?> _current =
                currentActiveCommands.putIfAbsent(getCommandName(), _command);
            logger().debug("current  Command = "+ _command + " existing command = " + _current);
            boolean duplicateCommand = (_current != null && _current != _command);
            if (!commandSettings.duplicateCommandHandler.isPresent() && duplicateCommand) {
              logger().warn(" Reject duplicate command exists " + getCommandName());
              rejectDuplicateCommand(child);
              return;
            }

            if (commandSettings.duplicateCommandHandler.isPresent() && duplicateCommand) {
              boolean proceed = proceedAfterDuplicateHandler(_current, child);
              if (!proceed) {
                rejectDuplicateCommand(child);
                return;
              }
            }
            Long memoryRequired = 0L;

            Optional<NodeLoadSummary> selectedNode = Optional.absent();
            memoryRequired = commandSettings.getMemoryEstimator().get().call();
            // Memory first
            if (memoryRequired > 0) {
              metrics.markResourceCalculationStart();
              selectedNode =
                  resourceUtlizationHandler.calcuateAndGetAvailableNodeList(memoryRequired);
              metrics.markResourceCalculationEnd();
            }

            if (memoryRequired > 0 && !selectedNode.isPresent()) {
              metrics.markResouceWaitStart();
              child.onError(new JmpCommandResourceUnavailableException(
                String.format("Command %s cannot run, system does not have enough resources. resources: { memoryRequired: %d Bytes} ",(groupKey.name()+":"+commandKey.name()), memoryRequired)));
            } else {

              resourceUtlizationHandler.reserveResource(memoryRequired, selectedNode);

              metrics.markValidationStart();
              JmpCommandPrevalidationResult jmpCommandPrevalidationResult =
                  commandSettings.getPrevalidator().get().prevalidate();
              metrics.markValidationEnd();
              if (jmpCommandPrevalidationResult.getResult() == ResultEnum.SUCCESS) {
                resourceUtlizationHandler.setCommandRunning();
                commandState.set(JmpCommandState.RUNNING);
                metrics.markExecutionStart();
                getDecoratedObservable().unsafeSubscribe(child);
               
              } else if (jmpCommandPrevalidationResult.getResult() == ResultEnum.QUEUED) {
                commandState.set(JmpCommandState.PENDING);
                metrics.markValidationQueueStart();
                child.onError(new JmpCommandPrevalidationException(jmpCommandPrevalidationResult));
                releaseResource();
              } else {
                child.onError(new JMPException(jmpCommandPrevalidationResult.getSummary()));
              }
            }
          }
        }).retryWhen(new JmpCommandPrevalidationHandler<R>(this).retryHandler())
        .subscribeOn(Schedulers.from(commandExecutor));
    
    obs = obs.doOnError(onError()).doOnCompleted(cleanupAfterCommandTerimation());
    
    actualCommand.compareAndSet(null, obs);
    
    return  Observable
        .create(new OnSubscribe<ObservableResult<R>>() {
          @Override
          public void call(Subscriber<? super ObservableResult<R>> child) {
            if (child.isUnsubscribed()) {
              // Nothing to do here
              return;
            }
            if (!commandState.compareAndSet(JmpCommandState.UNSUBSCRIBED, JmpCommandState.SCHEDULED)){
              child.onError(new JmpCommandRejected(String.format("Command %s is already in state %s cannot be subscribed again, try using replay() "
                  + " if multiple clients needs to subscribe on same command without executing multiple times ", commandKey.name(), commandState.get())));
            }
            metrics.markCommandStarted();
            actualCommand.get().subscribe(child);
          }
        });
  }

  /**
   * Handle duplicate command
   * @param _current
   * @param child
   */
  private boolean proceedAfterDuplicateHandler(JmpAbstractCommand<?> _current, Subscriber<? super ObservableResult<R>> child) {
    AtomicReference<JmpCommandState> activeCommandState = new AtomicReference<>(_current.getCommandState());
    JmpDuplicateCommandAction nextAction = commandSettings.duplicateCommandHandler.get().call(activeCommandState.get());
    while (!activeCommandState.compareAndSet(activeCommandState.get(), _current.getCommandState())) {
      activeCommandState.set(_current.getCommandState());
      nextAction = commandSettings.duplicateCommandHandler.get().call(activeCommandState.get());
    }
    boolean proceed = false;
    switch(nextAction){
      case QUEUE:
        if (!_current.waitCompleteLatch.compareAndSet(null, new CountDownLatch(1))) {
          rejectDuplicateCommand(child);
          proceed = false;
          break;
        }
        logger().warn(String.format("Duplicate command %s Queued", commandKey.name()));
        commandState.set(JmpCommandState.PENDING);
        metrics.markDuplicateStart();
        while(_current.commandState.get() != JmpCommandState.COMPLETED) {
          try {
            _current.waitCompleteLatch.get().await(10, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            //Woken up 
          }
        }
        metrics.markDuplicateEnd();
        currentActiveCommands.put(getCommandName(), this);
        proceed = true;
        break;
      case REJECT:
        rejectDuplicateCommand(child);
        proceed = false;
      default:
        throw new NotImplementedException("Action not implemented");
    }
    return proceed;
  }
 
  private void rejectDuplicateCommand(Subscriber<? super ObservableResult<R>> child) {
    commandState.set(JmpCommandState.UNSUBSCRIBED);
    child.onError(new JmpCommandDuplicateExecutionRejected(String.format(
      " command %s : %s is already active this command is rejected", groupKey.name(),
      commandKey.name())));
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
        metrics.markExecutionEnd();
        _command.releaseResource();
        if (_command.waitCompleteLatch.get()!=null) {
          _command.waitCompleteLatch.get().countDown();
        } else {
          logger().debug("Remove from active command after termination " + _command + " count : " +currentActiveCommands.size());
          currentActiveCommands.remove(getCommandName());
        }
        commandState.set(JmpCommandState.COMPLETED);
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
