package net.juniper.jmp.execution;

import java.util.List;

public interface JmpCommand {
  
  /**
   * Command can be in either of these states.
   *  Complete state is transient and will be only available for a short window of time before the cleanup is done.
   * @author jalandip
   *
   */
  public enum JmpCommandStatus {
    QUEUED, RUNNING, COMPLETED
  }
  
  
  /**
   * get list of commands for a group
   * @param groupKey
   * @return
   */
  List<JmpCommand> getJmpCommands(JmpCommandGroupKey groupKey);

}
