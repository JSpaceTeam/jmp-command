package net.juniper.jmp.execution;

import net.juniper.jmp.exception.JMPException;

public class JmpCommandDuplicateExecutionRejected extends JMPException {
  
  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public JmpCommandDuplicateExecutionRejected(String message) {
    
    super(message);
    
  }
  
}
