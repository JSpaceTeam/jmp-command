package net.juniper.jmp.execution;

import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;
import junit.framework.TestCase;
import net.juniper.jmp.cmp.jobManager.JobPrevalidationResult.ResultEnum;
import net.juniper.jmp.execution.JmpCommonCommand.JmpCommonCommandBuilder;
import org.junit.Test;
import rx.Subscriber;
import rx.functions.Action1;

public class JmpCommonCommandTest  {


    @Test
   public void testOne() {
     JmpCommonCommand<Integer> cmd = new JmpCommonCommand<Integer>(JmpCommonCommandBuilder
         .withCommandGroupKey(JmpCommandGroupKey.Factory.asKey("Test"))) {
          @Override
          public Integer run() {
             return 1;
          }
       
     };
     
     try {
      Assert.assertEquals(1, cmd.toObservable().toBlocking().first().getResult().intValue());
    } catch (Throwable e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
     
   }

    @Test
   public void testCommonWithPrevalidationRetryExceeded() {
     JmpCommonCommandBuilder bldr = JmpCommonCommandBuilder.withCommandGroupKey(JmpCommandGroupKey.Factory.asKey("Test"))
         .andMaxRetry(5)
         .andPrevalidate(new JmpCommandPrevalidator() {
          @Override
          public JmpCommandPrevalidationResult prevalidate() {
            return JmpCommandPrevalidationResult.create(ResultEnum.QUEUED, "Not ready yet");
          }
        });
     JmpCommonCommand<Integer> cmd = new JmpCommonCommand<Integer>(bldr) {
      @Override
      public Integer run() {
        return 1;
      }
     };
     Throwable e = null;
     try {
       cmd.toObservable().toBlocking().first();
     } catch (Exception ex) {
       e = ex.getCause();
     }
     Assert.assertEquals("Max retry exceeded".trim(), e.getMessage().trim());
     
   }

    @Test
   public void testCommonWithPrevalidationSuccess() {
     JmpCommonCommandBuilder bldr = JmpCommonCommandBuilder.withCommandGroupKey(JmpCommandGroupKey.Factory.asKey("Test"))
         .andMaxRetry(5)
         .andPrevalidate(new JmpCommandPrevalidator() {
          @Override
          public JmpCommandPrevalidationResult prevalidate() {
            return JmpCommandPrevalidationResult.create(ResultEnum.SUCCESS, "Success");
          }
        });
     JmpCommonCommand<Integer> cmd = new JmpCommonCommand<Integer>(bldr) {
      @Override
      public Integer run() {
        return 1;
      }
     };
     try {
      Assert.assertEquals(1, cmd.toObservable().toBlocking().first().getResult().intValue());
    } catch (Throwable e) {
      Assert.fail(e.getMessage());
    }
   }

    @Test
   public void testCommonWithPrevalidationFail() {
     final String validationMessage = "Validation failed dont run command";
     JmpCommonCommandBuilder bldr = JmpCommonCommandBuilder.withCommandGroupKey(JmpCommandGroupKey.Factory.asKey("Test"))
         .andMaxRetry(5)
         .andPrevalidate(new JmpCommandPrevalidator() {
          @Override
          public JmpCommandPrevalidationResult prevalidate() {
            return JmpCommandPrevalidationResult.create(ResultEnum.FAILURE, validationMessage);
          }
        });
     JmpCommonCommand<Integer> cmd = new JmpCommonCommand<Integer>(bldr) {
      @Override
      public Integer run() {
        return 1;
      }
     };
     Throwable e = null;
     try {
       cmd.toObservable().toBlocking().first();
     } catch (Exception ex) {
       e = ex.getCause();
     }
     Assert.assertEquals(validationMessage, e.getMessage().trim());
   }   
   
   @Test
   public void testFailOnCommand() {
     JmpCommonCommandBuilder bldr = JmpCommonCommandBuilder.withCommandGroupKey(JmpCommandGroupKey.Factory.asKey("Test"))
         .andMaxRetry(5);
     JmpCommonCommand<Integer> cmd = new JmpCommonCommand<Integer>(bldr) {
      @Override
      public Integer run() {
        return 1/0;
      }
     };
     final AtomicBoolean arithmaticExceptionReceived = new AtomicBoolean(false);
     cmd.toObservable().subscribe(new Subscriber<ObservableResult<Integer>>() {
      @Override
      public void onCompleted() {
        
      }
      @Override
      public void onError(Throwable e) {
      }
      @Override
      public void onNext(ObservableResult<Integer> t) {
        try {
          t.getResult();
        } catch (Throwable e) {
          arithmaticExceptionReceived.set(true);
        }
      }});
     await(1000L);
     Assert.assertEquals(true, arithmaticExceptionReceived.get());
   }

   @Test
   public void testCommonWithPrevalidationFailOnSubscribe() {
     final String validationMessage = "Validation failed dont run command";
     JmpCommonCommandBuilder bldr = JmpCommonCommandBuilder.withCommandGroupKey(JmpCommandGroupKey.Factory.asKey("Test"))
         .andMaxRetry(5)
         .andPrevalidate(new JmpCommandPrevalidator() {
          @Override
          public JmpCommandPrevalidationResult prevalidate() {
            return JmpCommandPrevalidationResult.create(ResultEnum.FAILURE, validationMessage);
          }
        });
     JmpCommonCommand<Integer> cmd = new JmpCommonCommand<Integer>(bldr) {
      @Override
      public Integer run() {
        return 1;
      }
     };
     final AtomicBoolean nonExecutionException = new AtomicBoolean(false);
     cmd.toObservable().subscribe(new Action1<ObservableResult<Integer>>() {

      @Override
      public void call(ObservableResult<Integer> t1) {
      }}, new Action1<Throwable>() {
        @Override
        public void call(Throwable t1) {
           nonExecutionException.set(true);
        }});
     await(100L);
     Assert.assertEquals(true, nonExecutionException.get());
   }   
   
   
   
   private void await(Long ms) {
     try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
    }
   }
   

}
