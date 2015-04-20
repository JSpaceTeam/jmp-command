package net.juniper.jmp.execution;

import net.juniper.jmp.cmp.distribution.FixedSizeListSplitStrategy;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import rx.functions.Action1;

import javax.ejb.AsyncResult;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
/**
 * Created by jalandip on 4/2/15.
 */
public class JmpAsyncCommandTest {

    interface Calculator {
        Future<Integer> add(int a, int b);
        Future<Integer> sub(int a, int b);
        Future<Integer> multiply(int a, int b);
        Future<Integer> addAll(List<Integer> list);
    }

    public class MyAsyncCommand extends JmpAsyncEjbCommand<Integer> {

        public MyAsyncCommand(JmpAsyncCommandBuilder commandBuilder) {
            super(commandBuilder);
        }

        protected Object lookupEJB() {
            Calculator mock = mock(Calculator.class);
            when(mock.add(1, 2)).thenReturn(new AsyncResult<>(3));
            when(mock.sub(2, 3)).thenReturn(new AsyncResult<>(-1));
            when(mock.multiply(2, 2)).thenReturn(new AsyncResult<>(4));
            when(mock.add(10,10)).then(new Answer<Future<Integer>>() {
                @Override
                public Future<Integer> answer(InvocationOnMock invocation) throws Throwable {
                    return Executors.newCachedThreadPool().submit(new Callable<Integer>() {
                        @Override
                        public Integer call() {
                            try {
                                Thread.sleep(10000);
                            } catch (InterruptedException e) {
                            }
                            return 20;
                        }
                    });
                }
            });

            return mock;
        }
    }

    @Test
    public void simpleTest() {
        JmpAsyncEjbCommand.JmpAsyncCommandBuilder commandBuilder = JmpAsyncEjbCommand.JmpAsyncCommandBuilder
                .withEJBCommandAndGroupKey(
                        JmpEJBCommandConfig.Factory.withEJb("my-mock-bean-2", "add",
                                1, 2), JmpCommandGroupKey.Factory.asKey("domain-group"))
                .andPrevalidate(new JmpCommandPrevalidator() {
                    public JmpCommandPrevalidationResult prevalidate() {
                        JmpCommandPrevalidationResult create =
                                JmpCommandPrevalidationResult.create(
                                        JmpCommandPrevalidationResult.ResultEnum.SUCCESS,
                                        "Start Test");
                        create.setRetryTimeInMillis(200);
                        return create;
                    }
                }).andMaxRetry(3);

        MyAsyncCommand myAsyncCommand = new MyAsyncCommand(commandBuilder);
        ObservableResult<Integer> first = myAsyncCommand.toObservable().toBlocking().first();
        try {
            int value = first.getResult();
            Assert.assertEquals(3, value);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            org.junit.Assert.fail();
        }

    }

    @Test
    public void timeoutTest() {
        JmpAsyncEjbCommand.JmpAsyncCommandBuilder commandBuilder = JmpAsyncEjbCommand.JmpAsyncCommandBuilder
                .withEJBCommandAndGroupKey(
                        JmpEJBCommandConfig.Factory.withEJb("my-mock-bean", "add",
                                10, 10).andTimeout(100L), JmpCommandGroupKey.Factory.asKey("domain-group"))
                .andPrevalidate(new JmpCommandPrevalidator() {
                    public JmpCommandPrevalidationResult prevalidate() {
                        JmpCommandPrevalidationResult create =
                                JmpCommandPrevalidationResult.create(
                                        JmpCommandPrevalidationResult.ResultEnum.SUCCESS,
                                        "Start Test");
                        create.setRetryTimeInMillis(200);
                        return create;
                    }
                }).andMaxRetry(3);

        MyAsyncCommand myAsyncCommand = new MyAsyncCommand(commandBuilder);
        Throwable exception = null;
        ObservableResult<Integer> first = myAsyncCommand.toObservable().toBlocking().first();
        try {
            first.getResult();
        } catch (Throwable ex) {
            exception = ex;
        }

        Assert.assertNotNull(exception);
        Assert.assertEquals(true, exception instanceof TimeoutException);
    }

    @Test
    public void testCommandExecuted() {
        JmpAsyncEjbCommand.JmpAsyncCommandBuilder commandBuilder = JmpAsyncEjbCommand.JmpAsyncCommandBuilder
                .withEJBCommandAndGroupKey(
                        JmpEJBCommandConfig.Factory.withEJb("my-mock-bean-"+ UUID.randomUUID(), "add",
                                1, 2), JmpCommandGroupKey.Factory.asKey("domain-group"))
                .andPrevalidate(new JmpCommandPrevalidator() {
                    public JmpCommandPrevalidationResult prevalidate() {
                        JmpCommandPrevalidationResult create =
                                JmpCommandPrevalidationResult.create(
                                        JmpCommandPrevalidationResult.ResultEnum.SUCCESS,
                                        "Start Test");
                        create.setRetryTimeInMillis(200);
                        return create;
                    }
                }).andMaxRetry(3);

        MyAsyncCommand myAsyncCommand = new MyAsyncCommand(commandBuilder);
        ObservableResult<Integer> first = myAsyncCommand.toObservable().toBlocking().first();
        try {
            int value = first.getResult();
            Assert.assertEquals(3, value);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            org.junit.Assert.fail();
        }
    }

    @Test
    public void testCommanRerunFail() {
        JmpAsyncEjbCommand.JmpAsyncCommandBuilder commandBuilder = JmpAsyncEjbCommand.JmpAsyncCommandBuilder
                .withEJBCommandAndGroupKey(
                        JmpEJBCommandConfig.Factory.withEJb("my-mock-bean-"+ UUID.randomUUID(), "add",
                                1, 2), JmpCommandGroupKey.Factory.asKey("domain-group"))
                .andPrevalidate(new JmpCommandPrevalidator() {
                    public JmpCommandPrevalidationResult prevalidate() {
                        JmpCommandPrevalidationResult create =
                                JmpCommandPrevalidationResult.create(
                                        JmpCommandPrevalidationResult.ResultEnum.SUCCESS,
                                        "Start Test");
                        create.setRetryTimeInMillis(200);
                        return create;
                    }
                }).andMaxRetry(3);
        MyAsyncCommand myAsyncCommand = new MyAsyncCommand(commandBuilder);
        myAsyncCommand.toObservable().subscribe();
        await(1);
        final AtomicBoolean disallow = new AtomicBoolean(false);
        myAsyncCommand.toObservable().subscribe(new Action1<ObservableResult<Integer>>() {
            @Override
            public void call(ObservableResult<Integer> integerObservableResult) {
                try {
                    integerObservableResult.getResult();
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                }
            }
        }, new Action1<Throwable>() {
            @Override
            public void call(Throwable throwable) {
                if (throwable.getMessage().contains("Command has already been executed")) {
                    disallow.set(true);
                }
            }
        });
        await(1);
        Assert.assertEquals(true, disallow.get());
    }

    @Test
    public void testSplit() {

        List<Integer> sumList = new ArrayList<>();
        int expectedAnswer = 0;
        for (int i = 1; i<41;i++) {
            sumList.add(i);
            expectedAnswer += i;
        }
        final AtomicInteger timesCalled = new AtomicInteger();
        class SplitAsyncClass extends JmpAsyncEjbCommand<Integer> {

            public SplitAsyncClass(JmpAsyncCommandBuilder commandBuilder) {
                super(commandBuilder);
            }

            protected Object lookupEJB() {
                Calculator mock = new Calculator() {
                    @Override
                    public Future<Integer> add(int a, int b){return null;}

                    @Override
                    public Future<Integer> sub(int a, int b) { return null;}

                    @Override
                    public Future<Integer> multiply(int a, int b) {return null;}

                    @Override
                    public Future<Integer> addAll(List<Integer> list) {
                        int count = 0;
                        for (Integer i : list) {
                            count += i;
                        }
                        timesCalled.incrementAndGet();
                        return new AsyncResult<>(count);
                    }
                };

                return mock;
            }
        }
        JmpAsyncEjbCommand.JmpAsyncCommandBuilder commandBuilder = JmpAsyncEjbCommand.JmpAsyncCommandBuilder
                .withEJBCommandAndGroupKey(
                        JmpEJBCommandConfig.Factory.withEJb("my-mock-bean-3", "addAll",
                                sumList).andTimeout(100L), JmpCommandGroupKey.Factory.asKey("domain-group"))
                .andPrevalidate(new JmpCommandPrevalidator() {
                    public JmpCommandPrevalidationResult prevalidate() {
                        JmpCommandPrevalidationResult create =
                                JmpCommandPrevalidationResult.create(
                                        JmpCommandPrevalidationResult.ResultEnum.SUCCESS,
                                        "Start Test");
                        create.setRetryTimeInMillis(200);
                        return create;
                    }
                }).andSplitStrategy(new FixedSizeListSplitStrategy(), 0).andMaxRetry(3);
        SplitAsyncClass splitAsyncClass = new SplitAsyncClass(commandBuilder);
        final AtomicInteger finalAnswer = new AtomicInteger();
        splitAsyncClass.toObservable().toBlocking().forEach(new Action1<ObservableResult<Integer>>() {
            @Override
            public void call(ObservableResult<Integer> integerObservableResult) {
                try {
                    finalAnswer.addAndGet(integerObservableResult.getResult());
                } catch (Throwable throwable) {
                    Assert.fail();
                }
            }
        });
        Assert.assertEquals(expectedAnswer, finalAnswer.get());
        Assert.assertEquals(20, timesCalled.get());
    }


    private void await(int timeinSec) {
        try {
            Thread.sleep(1000 * timeinSec);
        } catch (InterruptedException e1) {}
    }
}
