package net.juniper.jmp.execution;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A key to represent a {@link JmpAsyncEjbCommand} used for things like command collapsing etc
 * @author jalandip
 *
 */

public interface JmpCommandKey {

  public String name();

  public static class Factory {

    private Factory() {}


    private static ConcurrentHashMap<String, JmpCommandKey> intern =
        new ConcurrentHashMap<String, JmpCommandKey>();

    public static JmpCommandKey asKey(String name) {
      JmpCommandKey k = intern.get(name);
      if (k == null) {
        intern.putIfAbsent(name, new JmpCommandKeyDefault(name));
      }
      return intern.get(name);
    }

    private static class JmpCommandKeyDefault implements JmpCommandKey {

      private String name;

      private JmpCommandKeyDefault(String name) {
        this.name = name;
      }


      @Override
      public String name() {
        return name;
      }
      
      @Override
      public String toString() {
        return "JmpCommandKey: " + name;
      }
    }
  }

}
