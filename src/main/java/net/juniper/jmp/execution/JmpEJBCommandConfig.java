package net.juniper.jmp.execution;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
/**
 * This is the EJB Command Configuration
 * @author jalandip
 *
 */

public interface JmpEJBCommandConfig {

  public String ejbName();
  
  public String method();
  
  public Object[] args();
  
  /**
   * Time out in milliseconds for future. In case the Asynchronous EJB call is split to more than one task 
   * then each Future task will be at most waited for timeout milliseconds in an iteration. This means that the timeout does not 
   * apply to each split future hence the overall timeout of the command could be longer if the task is split.
   * 
   * @return
   */
  public Long timeout();
  
  public JmpEJBCommandConfig andTimeout(Long timeout);
  
  public static class Factory {
    
    private Factory() {}
    
    
    public static JmpEJBCommandConfig withEJb(String ejbName, String method, Object... args) {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(ejbName) && !Strings.isNullOrEmpty(method), "Method name and EJB Name needs to be set");
      return new JmpCommandConfigDefault(ejbName, method, -1L, args);
    }
    
    private static class JmpCommandConfigDefault implements JmpEJBCommandConfig {
      private String ejbName;
      
      private String method;
      
      private Object[] args;
      
      private Long timeout = -1L;

      
      private JmpCommandConfigDefault(String ejbName, String method, Long timeout, Object... args) {
        this.ejbName = ejbName;
        this.method = method;
        this.args = args;
        this.timeout = timeout;
      }
      
      public Long timeout() {
        return timeout;
      }
      
      @Override
      public String ejbName() {
        return ejbName;
      }

      @Override
      public String method() {
         return method;
      }

      @Override
      public Object[] args() {
        return args;
      }

      @Override
      public JmpCommandConfigDefault andTimeout(Long timeout) {
        this.timeout = timeout;
        return this;
      }
      
     
    }
  }
}
