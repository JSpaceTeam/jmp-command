package net.juniper.jmp.execution;

import java.util.List;

import net.juniper.jmp.cmp.jobManager.JobPrevalidationResult.ResultEnum;
import net.juniper.jmp.cmp.systemService.load.NodeLoadSummary;
import net.juniper.jmp.execution.JmpAbstractCommand.JmpCommandState;
import net.juniper.jmp.execution.JmpAbstractCommand.JmpDuplicateCommandAction;
import rx.functions.Func0;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

public abstract class JmpCommandSettings {
  
  private  Optional<? extends JmpCommandPrevalidator> commandPrevalidator;
  private int  maxRetryOnPrevalidationFailure;

  
  
  private  Optional<? extends Func0<Long>> memoryEstimator;
  protected Long retryInMillsAfterEstimation;
  
  private Optional<? extends JmpNodeFilter> nodeFilter; 
  protected Long retryInMillsAfterNodeFilter;
  

  protected Optional<JmpCommandDuplicateHandler> duplicateCommandHandler = Optional.absent();
  
  public interface JmpNodeFilter {
    
     public List<NodeLoadSummary> filter(List<NodeLoadSummary> nodeList);
  }
  
 
  public interface JmpCommandDuplicateHandler {
    public JmpDuplicateCommandAction call(JmpCommandState existingCommandState);
  }
  
  public JmpCommandSettings(JmpCommandBuilder builder) {
     
    Preconditions.checkArgument(builder.commandGroupKey != null, "Jmp Command Group Key cannot be null");
    Preconditions.checkArgument(builder.commandKey != null, "JmpCommandKey cannot be null");
    
    if (builder.commandPrevalidator != null) {
       this.commandPrevalidator = Optional.of(builder.commandPrevalidator);
     } else {
       this.commandPrevalidator = Optional.of(new JmpCommandSuccessPrevalidator());
     }

    this.maxRetryOnPrevalidationFailure = builder.maxNumValidationRetry;

     if (builder.memoryEstimator != null) {
       this.memoryEstimator = Optional.of(builder.memoryEstimator);
     } else {
       this.memoryEstimator  = Optional.of(new JmpCommandNoResourceMemoryEstimator());
     }
     if (builder.nodeFilter != null) {
       this.nodeFilter = Optional.of(builder.nodeFilter);
     } else {
       this.nodeFilter = Optional.of(new JmpNoNodeFilter());
     }
     this.retryInMillsAfterEstimation = builder.retryInMillsAfterEstimation;
     if (builder.duplicateCommandHandler != null) {
       this.duplicateCommandHandler =  Optional.of(builder.duplicateCommandHandler);
     }
     this.retryInMillsAfterNodeFilter = builder.retryInMillsAfterNodeFilter;
  }
  
  public int getMaxCommandRetry() {
    return maxRetryOnPrevalidationFailure;
  }

  public Optional<? extends JmpCommandPrevalidator> getPrevalidator() {
    return commandPrevalidator;
  }
  
  public Optional<? extends Func0<Long>> getMemoryEstimator() {
    return memoryEstimator;
  }
  
  public Optional<? extends JmpNodeFilter> getNodeFilter() {
    return nodeFilter;
  }
  
  /**
   * Builder for JmpCommandSettings
   *
   */
  public abstract static class JmpCommandBuilder {
    
    protected JmpCommandGroupKey commandGroupKey = null;
    
    protected JmpCommandKey commandKey = null;
    
    
    protected JmpCommandPrevalidator commandPrevalidator;
    protected int maxNumValidationRetry = 50;

    
    protected Func0<Long> memoryEstimator;
    
    protected JmpNodeFilter nodeFilter;
    
    protected Long retryInMillsAfterEstimation = 1000L;
    
    protected Long retryInMillsAfterNodeFilter = 10000L;
    
    protected JmpCommandDuplicateHandler duplicateCommandHandler = null;
   
  }
  
  /**
   * Always success prevaidation 
   *
   */
  protected static class JmpCommandSuccessPrevalidator implements JmpCommandPrevalidator {
    @Override
    public JmpCommandPrevalidationResult prevalidate() {
      return JmpCommandPrevalidationResult.create(ResultEnum.SUCCESS, "Always success");
    }
  }
  
  /**
   * A no memory usage estimator 
   *
   */
  protected static class JmpCommandNoResourceMemoryEstimator implements Func0<Long> {
    @Override
    public Long call() {
      return 0L;
    }
    
  }
  
  protected static class JmpNoNodeFilter implements JmpNodeFilter {
    @Override
    public List<NodeLoadSummary> filter(List<NodeLoadSummary> nodeList) {
      //no - op
      return nodeList;
    }
  }
  
}
