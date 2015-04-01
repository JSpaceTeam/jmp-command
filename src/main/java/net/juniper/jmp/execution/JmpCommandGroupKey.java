package net.juniper.jmp.execution;

import java.util.concurrent.ConcurrentHashMap;
/**
 * This is the command group each command {@link JmpAsyncEjbCommand} belongs to used for grouping similar commands
 * @author jalandip
 *
 */
public interface JmpCommandGroupKey {
  
  public String name();
  
  
  public static class Factory {
    
    private Factory() {}
    
    
    private static ConcurrentHashMap<String, JmpCommandGroupKey> intern = new ConcurrentHashMap<String, JmpCommandGroupKey>();
    
    public static JmpCommandGroupKey asKey(String name) {
      JmpCommandGroupKey k = intern.get(name);
      if (k == null) {
        intern.putIfAbsent(name, new JmpCommandGroupDefault(name));
      }
      return intern.get(name);
    }
    
    private static class JmpCommandGroupDefault implements JmpCommandGroupKey {

      private String name;
      
      private JmpCommandGroupDefault(String name) {
        this.name = name;
      }
      
      
      @Override
      public String name() { 
        return name;
      }  
      
      @Override
      public String toString() {
        return "JmpCommandGroupKey: " + name + " ";
      }
    }
  } 
  
 

}
