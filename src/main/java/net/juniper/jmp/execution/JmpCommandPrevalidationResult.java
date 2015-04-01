package net.juniper.jmp.execution;

import net.juniper.jmp.cmp.jobManager.JobPrevalidationResult;

public class JmpCommandPrevalidationResult extends JobPrevalidationResult {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  /**
   * Factory Method to create the prevalidation result
   * 
   * @param result : ResultEnumeration
   * @param summary : summary for the validation
   * @return : A newly instantiated JmpCommandPrevalidationResult object
   */
  public static JmpCommandPrevalidationResult create(ResultEnum result, String summary) {
    return new JmpCommandPrevalidationResult(result, summary);
  }
  
  
  public JmpCommandPrevalidationResult(JobPrevalidationResult jobPrevalidationResult) {
    super(jobPrevalidationResult.getResult(), jobPrevalidationResult.getSummary(), jobPrevalidationResult.getRetryTimeInMillis());
    this.setOpaqueData(jobPrevalidationResult.getOpaqueData());
  }

  public JmpCommandPrevalidationResult(ResultEnum result, String summary) {
    super(result, summary);
  }

  public JmpCommandPrevalidationResult(ResultEnum result, String summary, long retryTimeInMillis) {
    super(result, summary);
    setRetryTimeInMillis(retryTimeInMillis);
  }
}
