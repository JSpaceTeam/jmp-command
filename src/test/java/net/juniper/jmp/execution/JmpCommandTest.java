package net.juniper.jmp.execution;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;
import net.juniper.jmp.cmp.jobManager.JobPrevalidationResult.ResultEnum;
import net.juniper.jmp.cmp.systemService.domain.DomainManager;
import net.juniper.jmp.cmp.systemService.load.NodeLoadSummary;
import net.juniper.jmp.exception.JMPException;
import net.juniper.jmp.execution.JmpAsyncEjbCommand.JmpAsyncCommandBuilder;
import net.juniper.jmp.execution.JmpCommandSettings.JmpCommandBuilder;
import net.juniper.jmp.execution.JmpCommandSettings.JmpNodeFilter;
import org.junit.Test;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Observer;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;

import com.google.common.base.Stopwatch;

public class JmpCommandTest  {

    /**
     * A simple command for testing.
     * @author jalandip
     *
     */
    public static class MyCommand extends JmpAbstractCommand<Integer> {

        boolean pass = false;

        public MyCommand(
                JmpCommandBuilder commandBuilder) {
            super(commandBuilder);
            this.commandSettings = new JmpCommandSettings(commandBuilder) {};
        }

        @Override
        protected String getDefaltCommandKey() {
            return "MyCommand";
        }

        @Override
        protected Observable<ObservableResult<Integer>> getExecutionObservable() {
            return Observable.create(new OnSubscribe<ObservableResult<Integer>>() {
                @Override
                public void call(
                        Subscriber<? super ObservableResult<Integer>> sub) {
                    System.out.println(Thread.currentThread().getName() + ": Operation Called");
                    ObservableResult<Integer> result = new ObservableResult<Integer>(1);
                    sub.onNext(result);
                    if (!pass)
                        sub.onError(new Exception("Command Failed"));
                    sub.onCompleted();
                }
            });

        }
    }
    @Test
    public void testValidationFailed() {
        final AtomicInteger rejectedCommandCount = new AtomicInteger(0);
        JmpCommandTest.MyCommand cmd =
                new MyCommand(JmpAsyncCommandBuilder
                        .withEJBCommandAndGroupKey(
                                JmpEJBCommandConfig.Factory.withEJb(DomainManager.BEAN_NAME, "domainAdditionTest",
                                        1, null, null), JmpCommandGroupKey.Factory.asKey("domain-group"))
                        .andPrevalidate(new JmpCommandPrevalidator() {
                            public JmpCommandPrevalidationResult prevalidate() {
                                JmpCommandPrevalidationResult create =
                                        JmpCommandPrevalidationResult.create(
                                                net.juniper.jmp.execution.JmpCommandPrevalidationResult.ResultEnum.QUEUED,
                                                "Failed dont run");
                                create.setRetryTimeInMillis(200);
                                return create;
                            }
                        }).andMaxRetry(3).andFailOnDuplicateCommand());
        cmd.pass = true;

        final AtomicBoolean cmdFailed = new AtomicBoolean(false);
        cmd.toObservable().subscribe(new Observer<ObservableResult<Integer>>() {
            @Override
            public void onCompleted() {}

            @Override
            public void onError(Throwable e) {
                cmdFailed.set(true);
                if (e instanceof JmpCommandDuplicateExecutionRejected) {
                    rejectedCommandCount.incrementAndGet();
                }
            }

            @Override
            public void onNext(ObservableResult<Integer> t) {}
        });

        JmpCommandTest.MyCommand cmd2 =
                new MyCommand(JmpAsyncCommandBuilder
                        .withEJBCommandAndGroupKey(
                                JmpEJBCommandConfig.Factory.withEJb(DomainManager.BEAN_NAME, "domainAdditionTest",
                                        1, null, null), JmpCommandGroupKey.Factory.asKey("domain-group"))
                        .andPrevalidate(new JmpCommandPrevalidator() {
                            public JmpCommandPrevalidationResult prevalidate() {
                                JmpCommandPrevalidationResult create =
                                        JmpCommandPrevalidationResult.create(
                                                net.juniper.jmp.execution.JmpCommandPrevalidationResult.ResultEnum.QUEUED,
                                                "Failed dont run why 2 ");
                                create.setRetryTimeInMillis(200);
                                return create;
                            }
                        }).andMaxRetry(2).andFailOnDuplicateCommand());

        cmd2.toObservable().subscribe(new Observer<ObservableResult<Integer>>() {
            @Override
            public void onCompleted() {}

            @Override
            public void onError(Throwable e) {
                cmdFailed.set(true);
                if (e instanceof JmpCommandDuplicateExecutionRejected) {
                    rejectedCommandCount.incrementAndGet();
                }
            }

            @Override
            public void onNext(ObservableResult<Integer> t) {}
        });

        await(2);

        Assert.assertEquals(1, rejectedCommandCount.get());
    }




    @Test
    public void testSuccessWithRetry() {
        JmpCommandTest.MyCommand cmd = new MyCommand(JmpAsyncCommandBuilder
                .withEJBCommandAndGroupKey(JmpEJBCommandConfig.Factory.withEJb(DomainManager.BEAN_NAME, "domainAdditionTest", 1, null, null),
                        JmpCommandGroupKey.Factory.asKey("domain-group")
                ).andPrevalidate(new JmpCommandPrevalidator() {
                    int count = 0;
                    public JmpCommandPrevalidationResult prevalidate() {
                        count++;
                        if (count < 5) {
                            JmpCommandPrevalidationResult create = JmpCommandPrevalidationResult.create(net.juniper.jmp.execution.JmpCommandPrevalidationResult.ResultEnum.QUEUED, "retry");
                            create.setRetryTimeInMillis(100);
                            return create;
                        }
                        return JmpCommandPrevalidationResult.create(ResultEnum.SUCCESS, "Done");
                    }
                }).andMaxRetry(10)
        );
        cmd.pass = true;

        try {
            Assert.assertEquals(new Integer(1), cmd.toObservable().toBlocking().last().getResult());
        } catch (Throwable e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void testResouceReservationWithValiation() {

        JmpCommandTest.MyCommand cmd =
                new MyCommand(JmpAsyncCommandBuilder.withEJBCommandAndGroupKey(
                        JmpEJBCommandConfig.Factory.withEJb(DomainManager.BEAN_NAME, "domainAdditionTest", 1,
                                null, null), JmpCommandGroupKey.Factory.asKey("domain-group")).andPrevalidate(
                        new JmpCommandPrevalidator() {
                            public JmpCommandPrevalidationResult prevalidate() {
                                JmpCommandPrevalidationResult create =
                                        JmpCommandPrevalidationResult.create(
                                                net.juniper.jmp.execution.JmpCommandPrevalidationResult.ResultEnum.SUCCESS,
                                                "complete");
                                create.setRetryTimeInMillis(50);
                                return create;
                            }
                        })
                        .andMemoryEstimator(new Func0<Long>() {
                            @Override
                            public Long call() {
                                return 420L;
                            }
                        }, 200L)
                        .andMaxRetry(4)
                );
        cmd.pass = true;
        cmd.mockStats = true;


        try {
            cmd.toObservable().toBlocking().last().getResult();
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
        await(2);
    }

    @Test
    public void testNodeFilterValiationSuccess() {

        final AtomicInteger retryCount = new AtomicInteger(0);

        final Integer numberOfRetry = 5;

        JmpCommandTest.MyCommand cmd =
                new MyCommand(JmpAsyncCommandBuilder.withEJBCommandAndGroupKey(
                        JmpEJBCommandConfig.Factory.withEJb(DomainManager.BEAN_NAME, "domainAdditionTest", 1,
                                null, null), JmpCommandGroupKey.Factory.asKey("domain-group"))
                        .andMemoryEstimator(new Func0<Long>() {
                            @Override
                            public Long call() {
                                return 20L;
                            }
                        }, 200L)
                        .andMaxRetry(numberOfRetry)
                        .andNodeFilter(new JmpNodeFilter() {
                            @Override
                            public List<NodeLoadSummary> filter(List<NodeLoadSummary> nodeList) {
                                retryCount.getAndAdd(1);
                                return nodeList;
                            }
                        }, 100L)
                )
                ;
        cmd.pass = true;
        cmd.mockStats = true;
        ObservableResult<Integer> answer = cmd.toObservable().toBlocking().first();
        //Verify if the validation is called as many time as the number of retry configured
        Assert.assertEquals(retryCount.get() , 1);

        try {
            //This command fails but the fallback should send 100 in case the exection is max retry
            Assert.assertEquals(new Integer(1), answer.getResult());
        } catch (Throwable e) {
            Assert.fail(e.getMessage());
        }





    }

    @Test
    public void testNodeFilterSuccess_Second() {

        final AtomicInteger retryCount = new AtomicInteger(0);

        final Integer numberOfRetry = 5;

        final Long time = System.currentTimeMillis();
        JmpCommandTest.MyCommand cmd =
                new MyCommand(JmpAsyncCommandBuilder.withEJBCommandAndGroupKey(
                        JmpEJBCommandConfig.Factory.withEJb(DomainManager.BEAN_NAME, "domainAdditionTest", 1,
                                null, null), JmpCommandGroupKey.Factory.asKey("domain-group"))
                        .andMemoryEstimator(new Func0<Long>() {
                            @Override
                            public Long call() {
                                return 20L;
                            }
                        }, 10L)
                        .andMaxRetry(numberOfRetry)
                        .andNodeFilter(new JmpNodeFilter() {
                            @Override
                            public List<NodeLoadSummary> filter(List<NodeLoadSummary> nodeList) {
                                int count = retryCount.get();
                                if (count > 1)
                                    return nodeList;
                                else {
                                    retryCount.incrementAndGet();
                                    return new ArrayList<>();
                                }
                            }
                        }, 10L)
                )
                ;
        cmd.pass = true;
        cmd.mockStats = true;
        ObservableResult<Integer> answer = cmd.toObservable().toBlocking().first();
        //Verify if the validation is called as many time as the number of retry configured
        Assert.assertEquals(retryCount.get() , 2);

        try {
            //This command fails but the fallback should send 100 in case the exection is max retry
            Assert.assertEquals(new Integer(1), answer.getResult());
        } catch (Throwable e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testNodeFilterAndMemoryTimeout() {
        final AtomicInteger retryCount = new AtomicInteger(0);

        final Integer numberOfRetry = 8;

        final AtomicBoolean filterTimeoutSuccess = new  AtomicBoolean(false);

        final AtomicBoolean memTimeoutSuccess = new  AtomicBoolean(false);


        final Stopwatch watch = new Stopwatch();
        watch.start();
        JmpCommandTest.MyCommand cmd =
                new MyCommand(JmpAsyncCommandBuilder
                        .withEJBCommandAndGroupKey(
                                JmpEJBCommandConfig.Factory.withEJb(DomainManager.BEAN_NAME, "domainAdditionTest",
                                        1, null, null), JmpCommandGroupKey.Factory.asKey("domain-group"))
                        .andMemoryEstimator(new Func0<Long>() {
                            @Override
                            public Long call() {
                                if (retryCount.get() <= 2) {
                                    return 20L;
                                } else if (retryCount.get() < 5){
                                    if (retryCount.get() == 3) {
                                        watch.reset().start();
                                    }
                                    if (retryCount.get() == 4) {
                                        if (watch.elapsedMillis() <= 30) {
                                            memTimeoutSuccess.set(true);
                                        }
                                    }
                                    retryCount.incrementAndGet();
                                    return 2000L;
                                } else {
                                    return 20L;
                                }
                            }
                        }, 10L).andMaxRetry(numberOfRetry).andNodeFilter(new JmpNodeFilter() {
                            @Override
                            public List<NodeLoadSummary> filter(List<NodeLoadSummary> nodeList) {
                                int count = retryCount.get();
                                if (count > 2){
                                    retryCount.incrementAndGet();
                                    return nodeList;

                                }
                                else {
                                    if (retryCount.get() == 0) {
                                        watch.stop().start();
                                    } if (retryCount.get() == 1) {
                                        if (watch.elapsedMillis() >= 2000) {
                                            filterTimeoutSuccess.set(true);
                                        }
                                    }
                                    retryCount.incrementAndGet();
                                    return new ArrayList<>();
                                }
                            }
                        }, 2000L));
        cmd.pass = true;
        cmd.mockStats = true;
        ObservableResult<Integer> answer = cmd.toObservable().toBlocking().first();
        // Verify if the validation is called as many time as the number of retry configured
        Assert.assertEquals(retryCount.get(), 6);

        Assert.assertEquals(true, filterTimeoutSuccess.get());
        Assert.assertEquals(true, memTimeoutSuccess.get());

        try {
            // This command fails but the fallback should send 100 in case the exection is max retry
            Assert.assertEquals(new Integer(1), answer.getResult());
        } catch (Throwable e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void testNodeFilterValiationFailure() {
        final AtomicInteger retryCount = new AtomicInteger(0);

        final Integer numberOfRetry = 5;

        JmpCommandTest.MyCommand cmd =
                new MyCommand(JmpAsyncCommandBuilder.withEJBCommandAndGroupKey(
                        JmpEJBCommandConfig.Factory.withEJb(DomainManager.BEAN_NAME, "domainAdditionTest", 1,
                                null, null), JmpCommandGroupKey.Factory.asKey("domain-group"))
                        .andMemoryEstimator(new Func0<Long>() {
                            @Override
                            public Long call() {
                                return 20L;
                            }
                        }, 200L)
                        .andMaxRetry(numberOfRetry)
                        .andNodeFilter(new JmpNodeFilter() {
                            @Override
                            public List<NodeLoadSummary> filter(List<NodeLoadSummary> nodeList) {
                                retryCount.getAndAdd(1);
                                return new ArrayList<>();
                            }
                        }, 100L)
                )
                ;
        cmd.pass = true;
        cmd.mockStats = true;
        ObservableResult<Integer> answer = cmd.toObservable().onErrorResumeNext(new Func1<Throwable, Observable<ObservableResult<Integer>>>() {
            @Override
            public Observable<ObservableResult<Integer>> call(final Throwable th) {
                if (th instanceof JMPException && th.getMessage().contains("Max retry exceeded")) {
                    return Observable.create(new OnSubscribe<ObservableResult<Integer>>() {
                        @Override
                        public void call(Subscriber<? super ObservableResult<Integer>> t1) {
                            t1.onNext(new ObservableResult<Integer>(100));
                            t1.onCompleted();
                        }});
                } else {
                    return Observable.create(new OnSubscribe<ObservableResult<Integer>>() {
                        @Override
                        public void call(Subscriber<? super ObservableResult<Integer>> t1) {
                            t1.onNext(new ObservableResult<Integer>(999));
                            t1.onCompleted();
                        }});
                }

            }}).toBlocking().first();
        //Verify if the validation is called as many time as the number of retry configured
        Assert.assertEquals(numberOfRetry.intValue() , retryCount.get());

        try {
            //This command fails but the fallback should send 100 in case the exection is max retry
            Assert.assertEquals(new Integer(100), answer.getResult());
        } catch (Throwable e) {
            Assert.fail(e.getMessage());
        }


    }


    @Test
    public void testValidationSuccess() {
        JmpCommandTest.MyCommand cmd =
                new MyCommand(JmpAsyncCommandBuilder.withEJBCommandAndGroupKey(
                        JmpEJBCommandConfig.Factory.withEJb(DomainManager.BEAN_NAME, "domainAdditionTest", 1,
                                null, null), JmpCommandGroupKey.Factory.asKey("domain-group")).andPrevalidate(
                        new JmpCommandPrevalidator() {
                            public JmpCommandPrevalidationResult prevalidate() {
                                JmpCommandPrevalidationResult create =
                                        JmpCommandPrevalidationResult.create(
                                                net.juniper.jmp.execution.JmpCommandPrevalidationResult.ResultEnum.SUCCESS,
                                                "complete");
                                create.setRetryTimeInMillis(100);
                                return create;
                            }
                        }).andMaxRetry(10));
        cmd.pass = true;

        try {
            Assert.assertEquals(new Integer(1), cmd.toObservable().toBlocking().last().getResult());
        } catch (Throwable e) {
            Assert.fail(e.getMessage());
        }

    }




    @Test
    public void testRetryAndFail() {

        JmpCommandTest.MyCommand cmd = new MyCommand(JmpAsyncCommandBuilder
                .withEJBCommandAndGroupKey(JmpEJBCommandConfig.Factory.withEJb(DomainManager.BEAN_NAME, "domainAdditionTest", 1, null, null),
                        JmpCommandGroupKey.Factory.asKey("domain-group")
                ).andPrevalidate(new JmpCommandPrevalidator() {
                    int count = 0;
                    public JmpCommandPrevalidationResult prevalidate() {
                        count++;
                        if (count < 5) {
                            JmpCommandPrevalidationResult create = JmpCommandPrevalidationResult.create(net.juniper.jmp.execution.JmpCommandPrevalidationResult.ResultEnum.QUEUED, "retry");
                            create.setRetryTimeInMillis(100);
                            return create;
                        }

                        return JmpCommandPrevalidationResult.create(ResultEnum.SUCCESS, "Done");
                    }
                }).andMaxRetry(10)
        );

        try {
            cmd.toObservable().delaySubscription(5, TimeUnit.SECONDS).toBlocking().forEach(new Action1<ObservableResult<Integer>>() {
                @Override
                public void call(ObservableResult<java.lang.Integer> t1) {
                    try {
                        System.out.println(Thread.currentThread().getName() + ": " + t1.getResult());
                    } catch (Throwable e) {
                        System.out.println(Thread.currentThread().getName() + ": Error Result: " + t1.e);
                    }
                }});
        } catch (RuntimeException ex) {
            if (!ex.getCause().getMessage().contains("Command Failed")) {
                Assert.fail();
            }
            return;
        }
        Assert.fail("test did not fail as expected");
    }

    @Test
    public void testRetry() {
        final AtomicInteger retryCount = new AtomicInteger(0);

        final Integer numberOfRetry = 5;

        JmpCommandTest.MyCommand cmd =
                new MyCommand(JmpAsyncCommandBuilder.withEJBCommandAndGroupKey(
                        JmpEJBCommandConfig.Factory.withEJb(DomainManager.BEAN_NAME, "domainAdditionTest", 1,
                                null, null), JmpCommandGroupKey.Factory.asKey("domain-group")).andPrevalidate(
                        new JmpCommandPrevalidator() {
                            public JmpCommandPrevalidationResult prevalidate() {
                                retryCount.getAndAdd(1);
                                JmpCommandPrevalidationResult create =
                                        JmpCommandPrevalidationResult.create(
                                                net.juniper.jmp.execution.JmpCommandPrevalidationResult.ResultEnum.QUEUED,
                                                "retry");
                                create.setRetryTimeInMillis(100);
                                return create;
                            }
                        }).andMaxRetry(numberOfRetry));
        cmd.pass = true;

        ObservableResult<Integer> answer = cmd.toObservable().onErrorResumeNext(new Func1<Throwable, Observable<ObservableResult<Integer>>>() {
            @Override
            public Observable<ObservableResult<Integer>> call(final Throwable th) {
                if (th instanceof JMPException && th.getMessage().contains("Max retry exceeded")) {
                    return Observable.create(new OnSubscribe<ObservableResult<Integer>>() {
                        @Override
                        public void call(Subscriber<? super ObservableResult<Integer>> t1) {
                            t1.onNext(new ObservableResult<Integer>(100));
                            t1.onCompleted();
                        }});
                } else {
                    return Observable.create(new OnSubscribe<ObservableResult<Integer>>() {
                        @Override
                        public void call(Subscriber<? super ObservableResult<Integer>> t1) {
                            t1.onNext(new ObservableResult<Integer>(999));
                            t1.onCompleted();
                        }});
                }

            }}).toBlocking().first();
        //Verify if the validation is called as many time as the number of retry configured
        Assert.assertEquals(numberOfRetry.intValue() , retryCount.get());

        try {
            //This command fails but the fallback should send 100 in case the exection is max retry
            Assert.assertEquals(new Integer(100), answer.getResult());
        } catch (Throwable e) {
            Assert.fail(e.getMessage());
        }
    }


    private void await(int timeinSec) {
        try {
            Thread.sleep(1000 * timeinSec);
        } catch (InterruptedException e1) {}
    }
}
