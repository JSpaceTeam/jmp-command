package net.juniper.jmp.execution;

public interface JmpCommandInfo<R> {

  public JmpCommandGroupKey getCommandGroup();
  
  public JmpCommandKey getCommandKey();
  
}
