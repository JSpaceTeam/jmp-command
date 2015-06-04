package net.juniper.jmp.execution;

import java.io.IOException;
import java.io.StringWriter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLongArray;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import com.google.common.base.Stopwatch;



class JmpCommandMetrics {

  private final JmpCommandKey key;
  private final JmpCommandGroupKey groupKey;
  private JmpCommandStat commandStats = new  JmpCommandStat();
  private final ConcurrentHashMap<JmpCommandEvents, Stopwatch> commandEventWatch = new ConcurrentHashMap<>(JmpCommandEvents.values().length);
  private boolean failed = false;
  private long start;
  private static final JsonFactory factory = new JsonFactory();


  protected static JmpCommandMetrics getInstance(JmpCommandGroupKey groupKey, JmpCommandKey key) {
    JmpCommandMetrics commandMetrics = new JmpCommandMetrics(key, groupKey);    
    return commandMetrics;
  }
  
  JmpCommandMetrics(JmpCommandKey key, JmpCommandGroupKey groupKey) {
    this.key = key;
    this.groupKey = groupKey;
    for (JmpCommandEvents event : JmpCommandEvents.values()) {
      commandEventWatch.put(event, new Stopwatch());
    }
  }
  
  void reset() {
    commandStats.reset();
    for (Stopwatch watch : commandEventWatch.values()) {
      watch.reset();
    }
  }
  
  void markCommandStarted() {
    start = System.currentTimeMillis();
  }
  
  long startTime() {
    return start;
  }
  
  void markFailed() {
    failed = true;
  }
  
  void markDuplicateStart() {
    markStart(JmpCommandEvents.DUPLICATE_QUEUE);
  }
  void markDuplicateEnd() {
    markEnd(JmpCommandEvents.DUPLICATE_QUEUE);
  }

  
  void markExecutionStart() {
    markStart(JmpCommandEvents.COMMAND_EXECUTION);
  }
  
  void markExecutionEnd() {
    markEnd(JmpCommandEvents.COMMAND_EXECUTION);
  }
  
  void markValidationQueueStart() {
    markStart(JmpCommandEvents.VAIDATION_QUEUE);
  }
  
  void markValidationQueueEnd() {
    markEnd(JmpCommandEvents.VAIDATION_QUEUE);
  }
  
  void markValidationStart() {
    markStart(JmpCommandEvents.VALIDATING);
  }
  void markValidationEnd() {
    markEnd(JmpCommandEvents.VALIDATING);
  }
  
  void markResourceCalculationStart() {
    markStart(JmpCommandEvents.RESOURCE_CALCULATION);
  }
  void markResourceCalculationEnd() {
    markEnd(JmpCommandEvents.RESOURCE_CALCULATION);
  }

  
  void markResouceWaitStart() {
    markStart(JmpCommandEvents.RESOURCE_AVAILABILITY_QUEUE);
  }
  void markResouceWaitEnd() {
    markEnd(JmpCommandEvents.RESOURCE_AVAILABILITY_QUEUE);
  }
  
  void markTimeout(long duration) {
    commandStats.markDuration(JmpCommandEvents.TIMEOUT, duration);
  }
  
  void markTimedOut() {
    commandStats.markTimeout();
  }

  private void markStart(JmpCommandEvents event) {
    commandEventWatch.get(event).start();
  }
  
  private void markEnd(JmpCommandEvents event) {
    if (commandEventWatch.get(event).isRunning()) {
      long elapsedMillis = commandEventWatch.get(event).elapsedMillis();
      commandEventWatch.get(event).reset();
      this.commandStats.markDuration(event, elapsedMillis);
    }
  }
  
  

  
  public JmpCommandKey getKey() {
    return key;
  }

  public JmpCommandGroupKey getGroupKey() {
    return groupKey;
  }


  enum JmpCommandEvents {
    START,VALIDATING, RESOURCE_CALCULATION, COMMAND_EXECUTION, DUPLICATE_QUEUE, VAIDATION_QUEUE, RESOURCE_AVAILABILITY_QUEUE, TIMEOUT
  }
  

  String getStatsOutput() {
    StringWriter writer = new StringWriter();
    JsonGenerator jsonGenerator = null;
    try {
      jsonGenerator = factory.createJsonGenerator(writer);
      jsonGenerator.writeStartObject();
      jsonGenerator.writeStringField("group", groupKey.name());
      jsonGenerator.writeStringField("name", key.name());
      if (commandStats.timedOut) {
         jsonGenerator.writeBooleanField("command-timed-out", true);
      }
      if (failed) {
        jsonGenerator.writeString("command-failed");

      }
      jsonGenerator.writeNumberField("command-start-timestamp", start);
      jsonGenerator.writeNumberField("command-execution-time", commandStats.getStat(JmpCommandEvents.COMMAND_EXECUTION));
      jsonGenerator.writeNumberField("command-validation-time", commandStats.getStat(JmpCommandEvents.VALIDATING));
      jsonGenerator.writeNumberField("command-resource-calculation-time", commandStats.getStat(JmpCommandEvents.RESOURCE_CALCULATION));
      jsonGenerator.writeNumberField("command-duplicate-queue-time", commandStats.getStat(JmpCommandEvents.DUPLICATE_QUEUE));
      jsonGenerator.writeNumberField("command-validation-wait-time", commandStats.getStat(JmpCommandEvents.VAIDATION_QUEUE));
      jsonGenerator.writeNumberField("command-resoure-wait-time", commandStats.getStat(JmpCommandEvents.RESOURCE_AVAILABILITY_QUEUE));
      jsonGenerator.writeNumberField("command-time-out", commandStats.getStat(JmpCommandEvents.TIMEOUT));
      jsonGenerator.writeNumberField("command-total-time", commandStats.getTotal());
      jsonGenerator.writeEndObject();
      jsonGenerator.close();
    } catch (IOException e) {
    }
    return writer.toString();
  }

  class JmpCommandStat {
    
    AtomicLongArray stats = new AtomicLongArray(JmpCommandEvents.values().length);
     
    volatile boolean timedOut = false;
    
    void markDuration(JmpCommandEvents event, long duration) {
      stats.getAndAdd(event.ordinal(), duration);
    }
    
    void reset() {
      for (int i=0;i < JmpCommandEvents.values().length ; i++) {
        stats.getAndSet(i, 0);
      }
    }
    
    long getStat(JmpCommandEvents event) {
       return stats.get(event.ordinal());
    }
    
    long getTotal() {
      long total = 0;
      for (int i=0; i < JmpCommandEvents.values().length ; i++) {
        total = total +  stats.get(i);
      }
      return total;
    }
    void markTimeout() {
      timedOut = true;
    }
  }
  
}
