package net.juniper.jmp.execution;

/**
 *
 *
 * @param <T>
 */
public class ObservableResult<T> {
  
  private T result;
  
  protected Throwable e;
  
  private boolean isSuccess = false;
  
  public ObservableResult() {}
  
  public ObservableResult(T result) {
    setResult(result);
    this.isSuccess = true;
  }
  
  public ObservableResult(Throwable e) {
    this.e = e;
    this.isSuccess = false;
  }
  
  public T getResult() throws Throwable {
    if (isSuccess) return result;
    throw e;
  }
  
  public void setResult(T r) {
    result = r;
    isSuccess = true;
  }
  
  public static class NullObservableResult<ResultType> extends ObservableResult<ResultType> {
    
    public NullObservableResult(ResultType result) {
      super(result);
    }

    public ResultType getResult() throws Throwable {
      return null;
    }
  }
}



