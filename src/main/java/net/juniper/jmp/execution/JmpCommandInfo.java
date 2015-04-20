package net.juniper.jmp.execution;

import net.juniper.jmp.execution.JmpAbstractCommand.JmpCommandState;

public interface JmpCommandInfo {

  public JmpCommandGroupKey getCommandGroup();
  
  public JmpCommandKey getCommandKey();
  
  public boolean isCommandExecuted();
  
  public JmpCommandState getCommandState();
}
