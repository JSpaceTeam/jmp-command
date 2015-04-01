package net.juniper.jmp.execution;

import net.juniper.jmp.cmp.distribution.LoadSplitStrategy;
import net.juniper.jmp.execution.JmpAsyncEjbCommand.JmpAsyncCommandBuilder;
/**
 * Settings for each {@link JmpAsyncEjbCommand}
 * @author jalandip
 *
 */
public class JmpEjbCommandSettings extends JmpCommandSettings {

  protected LoadSplitStrategy splitStrategy = null;

  protected int splitIndex = -1;

  protected boolean useClustered = false;

  protected boolean needsSplit = false;
  
  
  public JmpEjbCommandSettings(JmpAsyncCommandBuilder commandBuilder) {
    super(commandBuilder);
    this.splitIndex = commandBuilder.splitIndex;
    this.splitStrategy = commandBuilder.splitStrategy;
    this.useClustered = commandBuilder.clustered;
    this.needsSplit = splitIndex != -1 && splitStrategy != null;
  }
}
