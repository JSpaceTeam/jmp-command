package net.juniper.jmp.execution;

class JmpCommandPrevalidationException extends Exception {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  
  
  private final JmpCommandPrevalidationResult result;
  
  public JmpCommandPrevalidationException(JmpCommandPrevalidationResult result) {
    super(result.getSummary());
    this.result = result;
  }
  
  protected JmpCommandPrevalidationResult getResult() {
    return result;
  }
}
