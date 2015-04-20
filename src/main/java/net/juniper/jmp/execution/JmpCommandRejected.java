package net.juniper.jmp.execution;

import net.juniper.jmp.exception.JMPException;

public class JmpCommandRejected extends JMPException {

  public JmpCommandRejected(String msg) {
    super(msg);
  }
}
