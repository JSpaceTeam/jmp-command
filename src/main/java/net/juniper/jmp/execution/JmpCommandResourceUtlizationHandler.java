package net.juniper.jmp.execution;

import static net.juniper.jmp.execution.JmpAbstractCommand.logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import net.juniper.jmp.cmp.jobManager.InternalScheduleContext.ContextType;
import net.juniper.jmp.cmp.jobManager.JobResource;
import net.juniper.jmp.cmp.system.JxServiceLocator;
import net.juniper.jmp.cmp.system.utils.ServerInfo;
import net.juniper.jmp.cmp.systemService.load.LoadCalculatorInterface;
import net.juniper.jmp.cmp.systemService.load.LoadStatFilter;
import net.juniper.jmp.cmp.systemService.load.LoadStatisticsInterface;
import net.juniper.jmp.cmp.systemService.load.LoadStatisticsMO;
import net.juniper.jmp.cmp.systemService.load.LoadStatisticsMO.ResourceState;
import net.juniper.jmp.cmp.systemService.load.LoadStatisticsResourceUpdater;
import net.juniper.jmp.cmp.systemService.load.MaxMemSizeMO;
import net.juniper.jmp.cmp.systemService.load.NodeLoadSummary;
import net.juniper.jmp.tracer.uid.UniqueIdGenerator;

import com.google.common.base.Optional;

/**
 * Class to handle resource utilization of a command. 
 *
 * @param <R>
 */
final class JmpCommandResourceUtlizationHandler<R> {

  public static final String LOAD_STAT_INTF = "cmp.LoadStatisticsEJB";

  public static final String LOAD_CALC_INTF = "cmp.LoadCalculatorEJB";
  
  private Optional<NodeLoadSummary> reservedNode = Optional.absent(); 

  private AtomicBoolean isRetryOnFilter = new AtomicBoolean(false);
  
  
  private LoadCalculatorInterface getLoadCalculatorImpl() {
    if (command.mockStats) {
      return new MockLoadCalculatorInterface();
    } else {
      return JxServiceLocator.lookup(LOAD_CALC_INTF);
    }
  }
  
  
  private LoadStatisticsInterface getLoadStatImpl() {
    if (command.mockStats) {
      return new MockLoadStatistics();
    } else {
      return  JxServiceLocator.lookup(LOAD_STAT_INTF);
    }
  }
  
  private final JmpAbstractCommand<R> command;

  private static String hostName = ServerInfo.getHostName();

  
  String generatedResourceId = hostName + ":" + UniqueIdGenerator.getInstance().nextOid();
  
  
  private Boolean resourceReserved = false;

  protected JmpCommandResourceUtlizationHandler(JmpAbstractCommand<R> command) {
    this.command = command;
  }

  private NodeLoadSummary calcuateResoureAndUpdateAvailableNodes(Long mem) {
    // Load Calc. should get JVM size and reservedMemory, then calculate totals for each node and
    // subtract from that

    isRetryOnFilter.set(false);
    
    List<NodeLoadSummary> nodeList = getLoadCalculatorImpl().getNodeLoad(null);

    if (nodeList == null || nodeList.size() == 0) {
      return null;
    }

    List<NodeLoadSummary> tempNodeList = new ArrayList<NodeLoadSummary>();
    for (NodeLoadSummary availableNode : nodeList) {
      if (availableNode.getAvailableMemory() >= mem) {
        tempNodeList.add(availableNode);
      }
    }
    // sort the list in decreasing order of available memory
    Collections.sort(tempNodeList, new Comparator<NodeLoadSummary>() {
      public int compare(NodeLoadSummary o1, NodeLoadSummary o2) {
        long f1 = o1.getAvailableMemory();
        long f2 = o2.getAvailableMemory();
        if (f2 < f1) {
          return -1;
        }
        if (f2 > f1) {
          return 1;
        }
        return 0;
      }
    });

    nodeList = tempNodeList;
    if (command.commandSettings.getNodeFilter().isPresent()) {
      if (nodeList.size() > 0 ) {
         nodeList = command.commandSettings.getNodeFilter().get().filter(nodeList);
         if (nodeList.size() == 0) {
           isRetryOnFilter.compareAndSet(false, true); 
         }
      }
    }
    
    if (nodeList.size() == 0) {
      return null;
    }

    return nodeList.get(0);
  }

  public Optional<NodeLoadSummary> calcuateAndGetAvailableNodeList(Long mem) {

    NodeLoadSummary selectedNode = calcuateResoureAndUpdateAvailableNodes(mem);

    if (selectedNode != null) return Optional.of(selectedNode);

    return Optional.absent();

  }

  protected Long getRetrytime() {
    if (isRetryOnFilter.get()) {
      return command.commandSettings.retryInMillsAfterNodeFilter;
    } else {
      return command.commandSettings.retryInMillsAfterEstimation;
    }
  }
  /**
   * Reserve node with the resource
   * @param selectedNode 
   * 
   */
  protected void reserveResource(Long memoryEstimation, Optional<NodeLoadSummary> selectedNode) {
    if (memoryEstimation > 0 && selectedNode.isPresent()) {
      String resourceId = getResourceId();
      logger().debug("Reserve memory for command " + resourceId);
      LoadStatisticsInterface loadStatIntf = getLoadStatImpl();
      reservedNode = selectedNode;
      if (reservedNode.isPresent()) {
        LoadStatisticsMO stat = loadStatIntf.getResource(resourceId);
        if (stat == null) {
          stat = new LoadStatisticsMO();
          stat.setResourceId(resourceId);
          stat.setType(command.getClass().getCanonicalName());
          stat.setContextType(ContextType.FIRST_ROOT_JOB);
          stat.setSubtype(command.getCommandName());
          stat.setCreationTimestamp(Calendar.getInstance().getTime().getTime());
          stat.setLastModifiedTimestamp(stat.getCreationTimestamp());
          stat.setEstimatedMemory(memoryEstimation);
          stat.setIpAddress(reservedNode.get().getIpAddress());
          stat.setQueue("");
          stat.setState(ResourceState.QUEUED);
          loadStatIntf.createResource(stat);
        } else {
          stat.setLastModifiedTimestamp(stat.getCreationTimestamp());
          stat.setEstimatedMemory(memoryEstimation);
          stat.setIpAddress(reservedNode.get().getIpAddress());
          stat.setQueue("");
          stat.setState(ResourceState.QUEUED);
          loadStatIntf.replaceResouce(stat);
        }
      }
      resourceReserved = true;
    }
  }

  protected void setCommandRunning() {
    String resourceId = getResourceId();
    if (reservedNode.isPresent() && resourceReserved) {
        getLoadStatImpl().updateResource(resourceId, new LoadStatisticsResourceUpdater()
            .state(ResourceState.RUNNING).build());
    } 
  }
  
  /**
   * check to see if node is reserved
   * @return
   */
  public Boolean isResourceReserved() {
    return resourceReserved;
  }
  
  /**
   * Get the IP of the reserved node
   * @return
   */
  protected String getReservedNodeIP() {
    if (resourceReserved && reservedNode.isPresent()) {
      return reservedNode.get().getIpAddress();
    }
    return null;
  }
  
  protected void freeResource() {
    if (resourceReserved) {
      logger().debug("Free resource called for resource " + getResourceId());
      getLoadStatImpl().deleteResource(getResourceId());
      reservedNode = Optional.absent();
      resourceReserved = false;
    }
  }

  private String getResourceId() {
    return command.getCommandName() + ":" + generatedResourceId;
  }
  
  public Optional<NodeLoadSummary> getReservedNode() {
    return reservedNode;
  }
  
 
      
  /**
   * Just an extension of Job resource and nothing more 
   *
   */
  class JmpCommandResource extends JobResource {

    /**
     * 
     */
    private static final long serialVersionUID = -4586612831071948646L;


  }
  
  
  
  /**
   * Test mockups follows from here
   * @author jalandip
   *
   */
  
  @testOnly
  class MockLoadStatistics implements LoadStatisticsInterface {
    @Override
    public LoadStatisticsMO getResource(String resourceId) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public List<LoadStatisticsMO> getAllResources(LoadStatFilter filterBuilder) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void createResource(LoadStatisticsMO statistic) {
      System.out.println("Create resource " + statistic.getResourceId() + " : " + statistic.getIpAddress() + " ," + statistic.getEstimatedMemory());
      
    }

    @Override
    public void updateResource(String resourceId, LoadStatisticsResourceUpdater resourceUpdater) {
      System.out.println("Resouce updated: " + resourceId + " " + resourceUpdater.toString());
    }

    @Override
    public void replaceResouce(LoadStatisticsMO statistic) {
    }

    @Override
    public void deleteResource(String resourceId) {
      System.out.println("Delete resources " + resourceId);
    }

    @Override
    public void deleteResources(Collection<String> resourceIds) {
    }

    @Override
    public void deleteResources(LoadStatFilter fieldValueMap) {
    }

    @Override
    public MaxMemSizeMO getLimits(String resourceID) {
      return null;
    }

    @Override
    public void deleteLimits(String resourceID) {
    }
    @Override
    public long getReservedMemory() {
      return 10;
    }
  }
  
  @testOnly
  class MockLoadCalculatorInterface implements LoadCalculatorInterface {

    @Override
    public List<NodeLoadSummary> getNodeLoad(LoadStatFilter filter) {
      NodeLoadSummary a = new NodeLoadSummary();
      a.setAvailableMemory(1000);
      a.setIpAddress("1.1.1.1");
      
      NodeLoadSummary b = new NodeLoadSummary();
      b.setAvailableMemory(200);
      b.setIpAddress("1.1.1.2");
      
      return Arrays.asList(a, b);
    }
    
  }
}
