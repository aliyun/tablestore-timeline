package com.alicloud.openservices.tablestore.timeline2.model;

import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.timeline2.TimelineException;
import com.alicloud.openservices.tablestore.timeline2.utils.Preconditions;
import com.alicloud.openservices.tablestore.timeline2.utils.Utils;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TimelineCallbackImpledFuture<Message, Entry> implements Future<Entry> {
    private boolean completed = false;
    private Entry result;
    private Exception ex;
    private List<TableStoreCallback<Message, Entry>> callbackList = new LinkedList<TableStoreCallback<Message, Entry>>();


    public TimelineCallbackImpledFuture() {
    }

    @SuppressWarnings("unchecked")
    public void onCompleted(Message request, Entry result) {
        synchronized(this) {
            if (this.completed) {
                throw new IllegalStateException("onCompleted() must not be invoked twice.");
            }

            this.completed = true;
            this.result = result;
            this.notifyAll();
        }

        Iterator var3 = this.callbackList.iterator();

        while(var3.hasNext()) {
            TableStoreCallback<Message, Entry> downstream = (TableStoreCallback)var3.next();

            downstream.onCompleted(request, result);
        }

    }

    @SuppressWarnings("unchecked")
    public void onFailed(Message request, Exception ex) {
        synchronized(this) {
            if (this.completed) {
                throw new IllegalStateException("onFailed() must not be invoked twice.");
            }

            this.completed = true;
            this.ex = ex;
            this.notifyAll();
        }

        Iterator var3 = this.callbackList.iterator();
        TimelineException e = Utils.convertException(ex);

        while(var3.hasNext()) {
            TableStoreCallback<Message, Entry> downstream = (TableStoreCallback)var3.next();
            downstream.onFailed(request, e);
        }

    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    public boolean isDone() {
        synchronized(this) {
            return this.completed;
        }
    }

    public synchronized Entry get() throws InterruptedException, ExecutionException {
        while(!this.completed) {
            this.wait();
        }

        return this.getResultWithoutLock();
    }

    public Entry get(long timeout, TimeUnit unit) throws TimeoutException, InterruptedException, ExecutionException {
        Preconditions.checkNotNull(unit, "Time unit should not be null");
        long endTime = System.currentTimeMillis() + unit.toMillis(timeout);
        synchronized(this) {
            while(!this.completed) {
                long waitTime = endTime - System.currentTimeMillis();
                if (waitTime <= 0L) {
                    throw new TimeoutException();
                }

                this.wait(waitTime);
            }

            return this.getResultWithoutLock();
        }
    }

    public TimelineCallbackImpledFuture<Message, Entry> watchBy(TableStoreCallback<Message, Entry> callback) {
        Preconditions.checkNotNull(callback, "Callback must not be null.");

        this.callbackList.add(callback);
        return this;
    }

    private Entry getResultWithoutLock() throws TimelineException {
        if (ex != null) {
            throw Utils.convertException(ex);
        }
        return this.result;
    }
}
