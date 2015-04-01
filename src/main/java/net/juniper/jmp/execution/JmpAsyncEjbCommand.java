package net.juniper.jmp.execution;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import net.juniper.jmp.cmp.distribution.LoadSplitStrategy;
import net.juniper.jmp.cmp.system.JMPScopedContext;
import net.juniper.jmp.cmp.system.JxServiceLocator;
import net.juniper.jmp.exception.JMPException;
import net.juniper.jmp.execution.JmpCommandSettings.JmpCommandBuilder;
import net.juniper.jmp.execution.JmpCommandSettings.JmpNodeFilter;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Func0;

import com.google.common.base.Preconditions;

/**
 * Simple Async EJB Command wrapper class that can be used to make asynchronous EJB calls. This call automatically splits the EJB methods 
 * and can make multiple EJB calls when {@link #execute()} is called. Hence execute always returns a list of {@link #futures}.
 * 
 * The command can also return an observable
 * 
 * Usage Example
 *  {{{
 *  List<Integer> arrayList = new ArrayList<Integer>();
 *   JmpAsyncEjbCommand<Integer> command = new JmpAsyncEjbCommand<Integer>(JmpAsyncCommandBuilder
  *      .withEJBCommandAndGroupKey(JmpEJBCommandConfig.Factory.withEJb(DomainManager.BEAN_NAME, "domainAdditionTest", 1, sumAll, sctx),
  *        JmpCommandGroupKey.Factory.asKey("domain-group")
  *      )
  *      .andClustered(true)
  *      .andSplitStrategy(new FixedSizeListSplitStrategy(), 1));
 *   try {
 *     List<Future<String>> results = command.execute();
 *     for (Future<String> result : results) {
 *       if (result.isDone()) {
 *        Integer answer = result.get(10, TimeUnit.SECONDS);
 *       }
 *     }
 *   } catch (Exception e) {
 *   }
 *   
 *   }}}
 *   Note: For EJBs to be clustered and distributed annotate the EJB with @JMPClustered annotation
 *   
 * @author jalandip
 *
 * @param <R>
 */
public  class JmpAsyncEjbCommand<R> extends JmpAbstractCommand<R> {
  
  private JmpEjbCommandSettings commandSettings;
  
  protected final JmpEJBCommandConfig ejbCommandConfig;
  
  private AtomicBoolean commandExecuted = new AtomicBoolean(false);

  
 
  @SuppressWarnings({"unchecked", "rawtypes"})
  protected final List<Future<R>> run() throws Exception {
    logger().warn("Executing " + getCommandGroup() + " key " + getCommandKey().name());
    
    if (commandExecuted.get()) {
      throw new Exception("Command has already been executed, create a new command");
    }
    
    commandExecuted.set(true);
    
    List<Future<R>>  results = new ArrayList<Future<R>>();

    
    Object ejb = lookupEJB();
    if (ejb == null) throw new JMPException("Failed to lookup " + ejbCommandConfig.ejbName());
    
    Method[] methods = ejb.getClass().getMethods();
    Method methodToInvoke = null;
    for (Method m : methods) {
      if (m.getName().equals(ejbCommandConfig.method())) {
        if( m.getReturnType().isAssignableFrom(Future.class)) {
          methodToInvoke = m;
        } else {
          throw new JMPException("Method needs to be asynchronous to use this class");
        }
        break;
      }
    }
    if (methodToInvoke == null) throw new JMPException("No method found in EJB " + ejbCommandConfig.ejbName() + " method " + ejbCommandConfig.ejbName());
    if (needsSplit()) {
      ArrayList argList = new ArrayList(1);
      argList.add(ejbCommandConfig.args()[commandSettings.splitIndex]);
      ArrayList<ArrayList> splittedList = commandSettings.splitStrategy.split(argList);
      Object[][] argsArray = new Object[splittedList.size()][ejbCommandConfig.args().length];
      int i = 0;
      for (Iterator iter = splittedList.iterator(); iter.hasNext(); i++) {
        argsArray[i] = constructArgs(iter.next());
      }
      for (Object[] args : argsArray) {
        Future<R> future = (Future<R>) methodToInvoke.invoke(ejb, args);
        results.add(future);
      }
    } else {
      Future<R> future = (Future<R>) methodToInvoke.invoke(ejb, ejbCommandConfig.args());
      results.add(future);
    }
    return results;
  }
  

  /**
   * Consider using {{@link JmpAbstractCommand.toObservable}}}
   */
  @Deprecated 
  public Observable<? extends ObservableResult<R>> asObservable() {
    return getExecutionObservable();
  }
  
  
  @Override
  protected Observable<ObservableResult<R>> getExecutionObservable() {
    Observable<ObservableResult<R>> ob = Observable.create(new OnSubscribe<ObservableResult<R>>() {
      @Override
      public void call(Subscriber<? super ObservableResult<R>> sub) {
        if (!sub.isUnsubscribed()) {
          List<Future<R>> results = null;
          try {
            results = run();
          } catch (Exception e) {
            sub.onError(e);
            return;
          }
          for (Future<R> result : results) {
            ObservableResult<R> obResult = new ObservableResult<R>();
            try {
              R r = null;
              if (ejbCommandConfig.timeout() > 0) {
                r = result.get(ejbCommandConfig.timeout(), TimeUnit.MILLISECONDS);
              } else {
                r = result.get();
              }
              obResult.setResult(r);
            } catch (InterruptedException | ExecutionException e) {
              obResult.e = e;
            } catch (TimeoutException e) {
              result.cancel(true);
              obResult.e = e;
            }
            sub.onNext(obResult);
          }
          sub.onCompleted();
        }
      }});
    
   
    
    return ob;
  }
  /**
   * return "async-ejb-call"
   */
  @Override
  protected String getDefaltCommandKey() {
    StringBuilder nameBuilder = new StringBuilder();
    return nameBuilder.append("async-ejb-call").toString();
  }
  
 

  public JmpAsyncEjbCommand(JmpAsyncCommandBuilder commandBuilder) {
    super(commandBuilder);
    this.ejbCommandConfig = commandBuilder.ejbCommandConfig;
    this.commandSettings  =  new JmpEjbCommandSettings(commandBuilder);
    if (commandSettings.needsSplit)
      Preconditions.checkArgument(canSplit(), "Invalid split index specified");
    super.setSettings(commandSettings);
  }
  
  /**
   * Look UP the EJB in case node is reserved use the reserved node
   * @return
   */
  private Object lookupEJB() {
    if (true) {
      return JxServiceLocator.lookup(ejbCommandConfig.ejbName());
    }
    if (this.getReservedNodeIP() != null) {
      return JxServiceLocator.lookupByIP(ejbCommandConfig.ejbName(), this.getReservedNodeIP(),
          new JMPScopedContext());
    } else {
      return commandSettings.useClustered ? JxServiceLocator.lookupClusteredEJB(
          ejbCommandConfig.ejbName(), new JMPScopedContext()) : JxServiceLocator
          .lookup(ejbCommandConfig.ejbName());
    }
  }
  
  @SuppressWarnings("rawtypes")
  private Object[] constructArgs(Object object) {
    ArrayList splitList = (ArrayList)object;
    Object modifiedParams[] = new Object[ejbCommandConfig.args().length];
    for (int newIndex = 0; newIndex < ejbCommandConfig.args().length; newIndex++) {
      if (commandSettings.splitIndex == newIndex) {
        modifiedParams[newIndex] = splitList.get(0);
      } else {
        modifiedParams[newIndex] = ejbCommandConfig.args()[newIndex];
      }
    }
    return modifiedParams;
  }
  
  private boolean canSplit() {
    return commandSettings.splitIndex >= 0 && ejbCommandConfig.args() != null && ejbCommandConfig.args().length > commandSettings.splitIndex;
  }
  
  protected final Boolean needsSplit() {
    return commandSettings.needsSplit;
  }
  /**
   * This is the command builder class that is used to build {@link JmpAsyncEjbCommand}
   * The with methods are mandatory where as the and methods are supplemntary
   *  Example
   * {{{
   *    
   *    JmpAsyncCommand<String> command = new JmpAsyncCommand<String>(JmpAsyncCommandBuilder
   *     .withEJBCommandAndGroupKey(JmpEJBCommandConfig.Factory.withEJb("Test", "call", 1,Arrays.asList(1,2,4)),
   *       JmpCommandGroupKey.Factory.asKey("device-commands"))
   *       .andClustered(true)
   *       .andSplitStrategy(new FixedSizeListSplitStrategy(), 1));
   * 
   * 
   * 
   * }}}
   * 
   * @author jalandip
   *
   * @param <T>
   */
  final public static class JmpAsyncCommandBuilder extends JmpCommandBuilder {
    
    LoadSplitStrategy splitStrategy = null;
    
    int splitIndex = -1;
    
    private final JmpEJBCommandConfig ejbCommandConfig;
    
    
    
    protected boolean clustered = false;
    
    
    protected static JmpAsyncCommandBuilder withCommandGroupKey(JmpCommandGroupKey commandGroupKey) {
      Preconditions.checkArgument(commandGroupKey != null);

      return new JmpAsyncCommandBuilder(commandGroupKey);
    }
    
    
    
    private JmpAsyncCommandBuilder(JmpEJBCommandConfig ejbCommandConfig, JmpCommandGroupKey commandGroupKey) {
      this.commandGroupKey = commandGroupKey;
      this.ejbCommandConfig = ejbCommandConfig;
      //Default command key
      this.commandKey = JmpCommandKey.Factory.asKey(ejbCommandConfig.ejbName() + ejbCommandConfig.method());
    }
    
    private JmpAsyncCommandBuilder(JmpCommandGroupKey commandGroupKey) {
      this.commandGroupKey = commandGroupKey;
      ejbCommandConfig = null;
    }
    
    
    public static JmpAsyncCommandBuilder withEJBCommandAndGroupKey(JmpEJBCommandConfig ejbCommandConfig,JmpCommandGroupKey commandGroup) {
      Preconditions.checkArgument(commandGroup != null && ejbCommandConfig != null);
      
      return new JmpAsyncCommandBuilder(ejbCommandConfig, commandGroup);
    }
    
  
    
    public JmpAsyncCommandBuilder andSplitStrategy(LoadSplitStrategy splitStrategy, int splitIndex) {
      this.splitStrategy = splitStrategy;
      this.splitIndex = splitIndex;
      return this;
    }
    
    public JmpAsyncCommandBuilder andClustered(boolean clustered) {
      this.clustered = clustered;
      return this;
    }
    
    public JmpAsyncCommandBuilder andCommandKey(JmpCommandKey key) {
      this.commandKey = key;
      return this;
    }
    
    public JmpAsyncCommandBuilder andPrevalidate(JmpCommandPrevalidator validator) {
      this.commandPrevalidator = validator;
      return this;
    }
    
    /**
     * Estimate memory required to run this command
     * @param memoryEstimator
     * @param retryInMilli
     * @return
     */
    public JmpAsyncCommandBuilder andMemoryEstimator(Func0<Long> memoryEstimator, Long retryInMilli) {
      this.memoryEstimator = memoryEstimator;
      this.retryInMillsAfterEstimation = retryInMilli;
      return this;
    }
    
    /**
     * Filter nodes that cannot run this command.
     * 
     * @param nodeFilter
     * @param retryInMilli time to retry if no nodes available from running this command
     * @return
     */
    public JmpAsyncCommandBuilder andNodeFilter(JmpNodeFilter nodeFilter, Long retryInMilli) {
      this.nodeFilter = nodeFilter;
      this.retryInMillsAfterNodeFilter = retryInMilli;
      return this;
    }
   
    public JmpAsyncCommandBuilder andMaxRetry(Integer maxCommandRetry) {
      this.maxNumValidationRetry = maxCommandRetry;
      return this;
    }
    
    public JmpAsyncCommandBuilder andFailOnDuplicateCommand() {
      this.failOnDuplicateCommand = true;
      return this;
    }
   
  }
}
