package net.juniper.jmp.execution;

import java.util.List;

import net.juniper.jmp.cmp.jobManager.JobPrevalidationResult.ResultEnum;
import net.juniper.jmp.cmp.systemService.load.NodeLoadSummary;
import rx.functions.Func0;

import com.google.common.base.Optional;

public abstract class JmpCommandSettings {
  
  private  Optional<? extends JmpCommandPrevalidator> commandPrevalidator;
  private int  maxRetryOnPrevalidationFailure;

  
  
  private  Optional<? extends Func0<Long>> memoryEstimator;
  protected Long retryInMillsAfterEstimation;
  
  private Optional<? extends JmpNodeFilter> nodeFilter; 
  protected Long retryInMillsAfterNodeFilter;
  

 

  protected boolean failOnDuplicateCommand;
  
  public interface JmpNodeFilter {
    
     public List<NodeLoadSummary> filter(List<NodeLoadSummary> nodeList);
  }
  
  
  public JmpCommandSettings(JmpCommandBuilder builder) {
     
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
     this.failOnDuplicateCommand = builder.failOnDuplicateCommand;
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
    
    protected JmpCommandGroupKey commandGroupKey;
    
    protected JmpCommandKey commandKey;
    
    
    protected JmpCommandPrevalidator commandPrevalidator;
    protected int maxNumValidationRetry = 50;

    
    protected Func0<Long> memoryEstimator;
    
    protected JmpNodeFilter nodeFilter;
    
    protected Long retryInMillsAfterEstimation = 1000L;
    
    protected Long retryInMillsAfterNodeFilter = 10000L;
    
    protected Boolean failOnDuplicateCommand = false;
    
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
