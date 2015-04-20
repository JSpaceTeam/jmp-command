package net.juniper.jmp.execution;

import net.juniper.jmp.exception.JMPException;

public class JmpCommandRetryTimedOutException extends JMPException {

  public JmpCommandRetryTimedOutException(String message, Throwable e) {
    super(message, e);
  }
}
