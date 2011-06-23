package org.radargun.stressors;

//import autonomic.SelfTuner;
//import autonomic.SelfTunerFactory;
//import com.sun.corba.se.spi.orbutil.threadpool.ThreadPoolChooser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.CacheWrapperStressor;
import org.radargun.tpcc.ElementNotFoundException;
import org.radargun.tpcc.TPCCTerminal;
import org.radargun.tpcc.TPCCTools;
import org.radargun.utils.Utils;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * On multiple threads executes put and get operations against the CacheWrapper, and returns the result as an Map.
 *
 * @author Mircea.Markus@jboss.com
 */
public class PutGetStressor implements CacheWrapperStressor {

    private static Log log = LogFactory.getLog(PutGetStressor.class);

    private static AtomicLong txCounter = new AtomicLong(0);

    private int opsCountStatusLog = 5000;

    public static final String DEFAULT_BUCKET_PREFIX = "BUCKET_";
    public static final String DEFAULT_KEY_PREFIX = "KEY_";






    /**
     * Each key will be a byte[] of this size.
     */
    private int sizeOfValue = 1000;



    /**
     * the number of threads that will work on this cache wrapper.
     */
    private int numOfThreads = 1;

    private String bucketPrefix = DEFAULT_BUCKET_PREFIX;



    private CacheWrapper cacheWrapper;
    private static Random r = new Random();
    private long startTime;
    private volatile CountDownLatch startPoint;


    private boolean is_master=false;
    private int total_num_of_slaves;

    private long simulTime;



    private double lambda=0.0;
    private BlockingQueue<RequestType> queue;
    private AtomicLong completedThread;
    private Producer[] producers;
    private AtomicLong countJobs;



    public PutGetStressor(boolean b, int slaves, long simulTime, double lambda, double paymentWeight, double orderStatusWeight, int cLastMask, int olIdMask, int cIdMask){
        TPCCTerminal.PAYMENT_WEIGHT=paymentWeight;
        TPCCTerminal.ORDER_STATUS_WEIGHT=orderStatusWeight;

        TPCCTools.A_C_LAST=cLastMask;
        TPCCTools.A_OL_I_ID=olIdMask;
        TPCCTools.A_C_ID=cIdMask;


        this.is_master=b;
        this.total_num_of_slaves=slaves;      //This variable was needed for a previous test

        this.simulTime=simulTime;

        this.lambda=lambda;


        completedThread= new AtomicLong(0L);

        if(lambda!=0.0){     //Open system
            queue=new ArrayBlockingQueue<RequestType>(7000);
            countJobs = new AtomicLong(0L);
            producers =new Producer[3];
            producers[0]=new Producer(TPCCTerminal.NEW_ORDER, false, 100.0-(TPCCTerminal.PAYMENT_WEIGHT+TPCCTerminal.ORDER_STATUS_WEIGHT));
            producers[1]=new Producer(TPCCTerminal.PAYMENT, false, TPCCTerminal.PAYMENT_WEIGHT);
            producers[2]=new Producer(TPCCTerminal.ORDER_STATUS, true, TPCCTerminal.ORDER_STATUS_WEIGHT);
        }

    }


    public Map<String, String> stress(CacheWrapper wrapper) {
        this.cacheWrapper = wrapper;
        startTime = System.currentTimeMillis();
        log.info("Executing: " + this.toString());

        List<Stresser> stressers;


        try {
            if(lambda!=0){ //Open system
                for(int i=0; i<producers.length; i++){
                    producers[i].start();
                }
            }

            stressers = executeOperations();
        } catch (Exception e) {
            log.info("*****Exception in PutGetStressor.stress() during executeOperations()*****");
            throw new RuntimeException(e);
        }
        log.info("stressers ends");
        return processResults(stressers);
    }

    public void destroy() throws Exception {
        cacheWrapper.empty();
        cacheWrapper = null;
    }

    private Map<String, String> processResults(List<Stresser> stressers) {
        long duration = 0;
        int reads = 0;
        int writes = 0;
        int failures = 0;
        int rdFailures=0;
        int wrFailures =0;
        int newOrderFailures =0;
        int paymentFailures =0;

        long readsDurations = 0;
        long writesDurations = 0;

        long successCommitDurations = 0;
        long failCommitDurations = 0;
        long allCommitDurations = 0;
        long commitFails = 0;

        long successful_writesDurations=0;
        long successful_readsDurations =0;

        long not_found_failures=0;
        long newOrderTransactions=0;
        long paymentTransactions=0;
        long paymentDurations=0;
        long newOrderDurations=0;

        long writeServiceTimes=0;
        long readServiceTimes=0;
        long newOrderServiceTimes=0;
        long paymentServiceTimes=0;

        long writeInQueueTimes=0;
        long readInQueueTimes=0;
        long newOrderInQueueTimes=0;
        long paymentInQueueTimes=0;
        long numWritesDequeued=0;
        long numReadsDequeued=0;
        long numNewOrderDequeued=0;
        long numPaymentDequeued=0;

        long LL=0;
        long LR=0;
        long RL=0;
        long RR=0;

        long skew=0;
        long local_tout=0;
        long remote_tout=0;


        for (Stresser stresser : stressers) {
            duration+=stresser.delta;
            //duration += stresser.totalDuration();
            readsDurations += stresser.readDuration;
            writesDurations += stresser.writeDuration;
            successful_writesDurations += stresser.successful_writeDuration;
            successful_readsDurations += stresser.successful_readDuration;

            successCommitDurations+=stresser.commitSuccessDuration;
            failCommitDurations += stresser.commitFailDuration;
            allCommitDurations += stresser.commitTotalDuration;

            commitFails += stresser.commitFails;

            reads += stresser.reads;
            writes += stresser.writes;
            failures += stresser.nrFailures;
            rdFailures += stresser.nrRdFailures;
            wrFailures += stresser.nrWrFailuers;
            newOrderFailures += stresser.nrNewOrderFailures;
            paymentFailures += stresser.nrPaymentFailures;

            not_found_failures += stresser.notFoundFailures;
            newOrderTransactions +=stresser.newOrder;
            paymentTransactions += stresser.payment;
            newOrderDurations += stresser.newOrderDuration;
            paymentDurations += stresser.paymentDuration;

            writeServiceTimes += stresser.writeServiceTime;
            readServiceTimes += stresser.readServiceTime;
            newOrderServiceTimes += stresser.newOrderServiceTime;
            paymentServiceTimes += stresser.paymentServiceTime;

            writeInQueueTimes += stresser.writeInQueueTime;
            readInQueueTimes += stresser.readInQueueTime;
            newOrderInQueueTimes += stresser.newOrderInQueueTime;
            paymentInQueueTimes +=stresser.paymentInQueueTime;
            numWritesDequeued += stresser.numWriteDequeued;
            numReadsDequeued += stresser.numReadDequeued;
            numNewOrderDequeued += stresser.numNewOrderDequeued;
            numPaymentDequeued += stresser.numPaymentDequeued;

            skew+=stresser.numWriteSkew;
            local_tout+=stresser.numLocalTimeOut;
            remote_tout+=stresser.numRemoteTimeOut;

        }

        duration=duration/1000000;
        readsDurations=readsDurations/1000; //nanos to micros
        writesDurations=writesDurations/1000; //nanos to micros
        newOrderDurations=newOrderDurations/1000; //nanos to micros
        paymentDurations=paymentDurations/1000;//nanos to micros
        writeServiceTimes=writeServiceTimes/1000;//nanos to micros
        readServiceTimes=readServiceTimes/1000;//nanos to micros
        paymentServiceTimes=paymentServiceTimes/1000;//nanos to micros
        newOrderServiceTimes=newOrderServiceTimes/1000;//nanos to micros
        successful_readsDurations=successful_readsDurations/1000; //nanos to micros
        successful_writesDurations=successful_writesDurations/1000; //nanos to micros

        successCommitDurations=successCommitDurations/1000; //nanos to micros
        failCommitDurations = failCommitDurations/1000; //nanos to micros
        allCommitDurations = allCommitDurations/1000; //nanos to micros

        writeInQueueTimes=writeInQueueTimes/1000;//nanos to micros
        readInQueueTimes=readInQueueTimes/1000;//nanos to micros
        newOrderInQueueTimes=newOrderInQueueTimes/1000;//nanos to micros
        paymentInQueueTimes=paymentInQueueTimes/1000;//nanos to micros

        Map<String, String> results = new LinkedHashMap<String, String>();
        results.put("DURATION (msec)", str(duration/numOfThreads));
        double requestPerSec = (reads + writes)  /((duration/numOfThreads) / 1000.0);
        double wrtPerSec=0;
        double rdPerSec=0;
        double newOrderPerSec=0;
        double paymentPerSec=0;

        results.put("REQ_PER_SEC", str(requestPerSec));

        if(readsDurations+writesDurations==0)
            results.put("READS_PER_SEC",str(0));
        else{
            rdPerSec=reads   / (((readsDurations+writesDurations)/numOfThreads) / 1000000.0);
            results.put("READS_PER_SEC", str(rdPerSec));
        }
        if (writesDurations+readsDurations==0)
            results.put("WRITES_PER_SEC", str(0));
        else{
            wrtPerSec=writes / (((writesDurations+readsDurations)/numOfThreads) / 1000000.0);
            results.put("WRITES_PER_SEC", str(wrtPerSec));
        }
        if (writesDurations+readsDurations==0)
            results.put("NEW_ORDER_PER_SEC", str(0));
        else{
            newOrderPerSec=newOrderTransactions/(((writesDurations+readsDurations)/numOfThreads)/1000000.0);

            results.put("NEW_ORDER_PER_SEC", str(newOrderPerSec));
        }
        if (writesDurations+readsDurations==0)
            results.put("PAYMENT_PER_SEC", str(0));
        else{
            paymentPerSec=paymentTransactions/(((writesDurations+readsDurations)/numOfThreads)/1000000.0);

            results.put("PAYMENT_PER_SEC", str(paymentPerSec));
        }
        results.put("READ_COUNT", str(reads));
        results.put("WRITE_COUNT", str(writes));
        results.put("NEW_ORDER_COUNT", str(newOrderTransactions));
        results.put("PAYMENT_COUNT",str(paymentTransactions));
        results.put("FAILURES", str(failures));
        results.put("NOT_FOUND_FAILURES", str(not_found_failures));
        results.put("WRITE_FAILURES", str(wrFailures));
        results.put("NEW_ORDER_FAILURES", str(newOrderFailures));
        results.put("PAYMENT_FAILURES", str(paymentFailures));
        results.put("READ_FAILURES", str(rdFailures));

        if((reads+writes)!=0)
            results.put("AVG_SUCCESSFUL_TX_DURATION (usec)",str((successful_writesDurations+successful_readsDurations)/(reads+writes)));
        else
            results.put("AVG_SUCCESSFUL_TX_DURATION (usec)",str(0));


        if(reads!=0)
            results.put("AVG_SUCCESSFUL_RD_TX_DURATION (usec)",str(successful_readsDurations/reads));
        else
            results.put("AVG_SUCCESSFUL_RD_TX_DURATION (usec)",str(0));


        if(writes!=0)
            results.put("AVG_SUCCESSFUL_WR_TX_DURATION (usec)",str(successful_writesDurations/writes));
        else
            results.put("AVG_SUCCESSFUL_WR_TX_DURATION (usec)",str(0));

        if(writes != 0) {
            results.put("AVG_SUCCESS_COMMIT_DURATION (usec)",str((successCommitDurations/writes)));
        } else {
            results.put("AVG_SUCCESS_COMMIT_DURATION (usec)",str(0));
        }

        if(commitFails != 0) {
            results.put("AVG_FAILED_COMMIT_DURATION (usec)",str((failCommitDurations/commitFails)));
        } else {
            results.put("AVG_FAILED_COMMIT_DURATION (usec)",str(0));
        }

        long totalCommits = writes + reads + commitFails;
        if(totalCommits != 0) {
            results.put("AVG_ALL_COMMIT_DURATION (usec)",str((allCommitDurations/totalCommits)));
        } else {
            results.put("AVG_ALL_COMMIT_DURATION (usec)",str(0));
        }


        if((reads+rdFailures)!=0)
            results.put("AVG_RD_SERVICE_TIME (usec)",str(readServiceTimes/(reads+rdFailures)));
        else
            results.put("AVG_RD_SERVICE_TIME (usec)",str(0));

        if((writes+wrFailures)!=0)
            results.put("AVG_WR_SERVICE_TIME (usec)",str(writeServiceTimes/(writes+wrFailures)));
        else
            results.put("AVG_WR_SERVICE_TIME (usec)",str(0));

        if((newOrderTransactions+newOrderFailures)!=0)
            results.put("AVG_NEW_ORDER_SERVICE_TIME (usec)",str(newOrderServiceTimes/(newOrderTransactions+newOrderFailures)));
        else
            results.put("AVG_NEW_ORDER_SERVICE_TIME (usec)",str(0));

        if((paymentTransactions+paymentFailures)!=0)
            results.put("AVG_PAYMENT_SERVICE_TIME (usec)",str(paymentServiceTimes/(paymentTransactions+paymentFailures)));
        else
            results.put("AVG_PAYMENT_SERVICE_TIME (usec)",str(0));
        if(numWritesDequeued!=0)
            results.put("AVG_WR_INQUEUE_TIME (usec)",str(writeInQueueTimes/numWritesDequeued));
        else
            results.put("AVG_WR_INQUEUE_TIME (usec)",str(0));
        if(numReadsDequeued!=0)
            results.put("AVG_RD_INQUEUE_TIME (usec)",str(readInQueueTimes/numReadsDequeued));
        else
            results.put("AVG_RD_INQUEUE_TIME (usec)",str(0));
        if(numNewOrderDequeued!=0)
            results.put("AVG_NEW_ORDER_INQUEUE_TIME (usec)",str(newOrderInQueueTimes/numNewOrderDequeued));
        else
            results.put("AVG_NEW_ORDER_INQUEUE_TIME (usec)",str(0));
        if(numPaymentDequeued!=0)
            results.put("AVG_PAYMENT_INQUEUE_TIME (usec)",str(paymentInQueueTimes/numPaymentDequeued));
        else
            results.put("AVG_PAYMENT_INQUEUE_TIME (usec)",str(0));



        log.info("Finished generating report. Nr of failed operations on this node is: " + failures +
                ". Test duration is: " + Utils.getDurationString(System.currentTimeMillis() - startTime));
        return results;
    }

    private List<Stresser> executeOperations() throws Exception {
        List<Stresser> stressers = new ArrayList<Stresser>();

        startPoint = new CountDownLatch(1);


        long c_run=TPCCTools.randomNumber(0, TPCCTools.A_C_LAST);
        long c_id=TPCCTools.randomNumber(0, TPCCTools.A_C_ID);
        long ol_i_id=TPCCTools.randomNumber(0, TPCCTools.A_OL_I_ID);

        for (int threadIndex = 0; threadIndex < numOfThreads; threadIndex++) {
            Stresser stresser = new Stresser(threadIndex,this.is_master,this.simulTime, c_run, c_id, ol_i_id);

            stressers.add(stresser);

            try{
                stresser.start();
            }
            catch (Throwable t){

                log.info("***Eccezione nella START "+t);
            }
        }




        log.info("Cache private class Stresser extends Thread { wrapper info is: " + cacheWrapper.getInfo());
        startPoint.countDown();




        for (Stresser stresser : stressers) {
            stresser.join();
            log.info("Stresses " + stresser + " joins!");
        }




        log.info("****BARRIER JOIN PASSED****");

        return stressers;
    }

    private class Stresser extends Thread {



        private int threadIndex;
        private int nrFailures=0;
        private int nrWrFailuers=0;
        private int nrRdFailures=0;
        private int nrNewOrderFailures=0;
        private int nrPaymentFailures=0;
        private long readDuration = 0;
        private long writeDuration = 0;
        private long successful_writeDuration=0;
        private long successful_readDuration=0;
        private long newOrderDuration=0;
        private long paymentDuration=0;
        private long writeServiceTime=0;
        private long paymentServiceTime=0;
        private long newOrderServiceTime=0;
        private long readServiceTime=0;
        private long writeInQueueTime=0;
        private long readInQueueTime=0;
        private long newOrderInQueueTime=0;
        private long paymentInQueueTime=0;
        private long numWriteDequeued=0;
        private long numReadDequeued=0;
        private long numNewOrderDequeued=0;
        private long numPaymentDequeued=0;

        private long reads=0;
        private long writes=0;

        private long delta=0;

        private long notFoundFailures=0;
        private long newOrder=0;
        private long payment=0;





        private long commitSuccessDuration = 0;
        private long commitFailDuration = 0;
        private long commitFails = 0;
        private long commitTotalDuration = 0;
        private long simulTime;

        private int numWriteSkew=0;
        private int numLocalTimeOut=0;
        private int numRemoteTimeOut=0;

        private long c_run;
        private long c_id;
        private long ol_i_id;

        public Stresser(int threadIndex, boolean is_master_thread, long time, long c_run, long c_id, long ol_i_id) {

            super("Stresser-" + threadIndex);
            this.threadIndex = threadIndex;




            this.simulTime=time;


            this.c_run=c_run;
            this.c_id=c_id;
            this.ol_i_id=ol_i_id;

        }

        @Override
        public void run() {



            try {
                startPoint.await();
                log.info("Starting thread: " + getName());
            } catch (InterruptedException e) {
                log.warn(e);
            }


            boolean force_ro;
            boolean successful;
            long start, end, startService, endInQueueTime;



            TPCCTerminal terminal= new TPCCTerminal(c_run, c_id, ol_i_id);


            long init_time=System.nanoTime();
            long commit_start=0;
            int transaction_type=0;
            txCounter.set(0);

            while(delta<this.simulTime){

                successful=true;


                start = System.nanoTime();
                if(lambda!=0.0){  //Open system
                    try{
                        RequestType request=queue.take();
                        transaction_type=request.transactionType;
                        endInQueueTime=System.nanoTime();
                        if(transaction_type==TPCCTerminal.NEW_ORDER){
                            numWriteDequeued++;
                            numNewOrderDequeued++;
                            writeInQueueTime += endInQueueTime - request.timestamp;
                            newOrderInQueueTime += endInQueueTime - request.timestamp;
                        }
                        else if(transaction_type==TPCCTerminal.PAYMENT){
                            numWriteDequeued++;
                            numPaymentDequeued++;
                            writeInQueueTime += endInQueueTime - request.timestamp;
                            paymentInQueueTime += endInQueueTime - request.timestamp;

                        }
                        else if(transaction_type==TPCCTerminal.ORDER_STATUS){
                            numReadDequeued++;
                            readInQueueTime += endInQueueTime - request.timestamp;

                        }


                    }
                    catch(InterruptedException ir){
                        log.error("»»»»»»»THREAD INTERRUPTED WHILE TRYING GETTING AN OBJECT FROM THE QUEUE«««««««");
                    }
                }
                else{ //Closed system without think time
                    transaction_type=terminal.choiceTransaction(cacheWrapper.isPassiveReplication(), is_master);
                }

                force_ro=transaction_type==TPCCTerminal.ORDER_STATUS;
                long thisTx = txCounter.getAndIncrement();
                log.info("*** tx started: " + thisTx + ", type=" + transaction_type + " ***");

                startService = System.nanoTime();

                cacheWrapper.startTransaction();


                try{
                    terminal.executeTransaction(cacheWrapper, transaction_type);
                }
                catch (Throwable e) {
                    successful=false;
                    log.warn(e);
                    if(e instanceof ElementNotFoundException){
                        this.notFoundFailures++;
                    }
                    if(e instanceof NullPointerException){
                        log.error("Problem!!!!", e);
                    }

                    if(e.getClass().getName()=="org.infinispan.CacheException")
                        this.numWriteSkew++;
                    else if(e.getClass().getName()=="org.infinispan.util.concurrent.TimeoutException")
                        this.numLocalTimeOut++;
                    if(e instanceof Exception) {
                        e.printStackTrace();
                    }
                }

                //here we try to finalize the transaction
                //if any read/write has failed we abort
                boolean measureTime = false;
                try{
                    /* In our tests we are interested in the commit time spent for write txs*/
                    if(successful && !force_ro){
                        commit_start=System.nanoTime();
                        measureTime = true;
                    }
                    cacheWrapper.endTransaction(successful);
                    if(!successful){
                        nrFailures++;
                        if(!force_ro){
                            nrWrFailuers++;
                            if(transaction_type==TPCCTerminal.NEW_ORDER){
                                nrNewOrderFailures++;
                            }
                            else if(transaction_type== TPCCTerminal.PAYMENT){
                                nrPaymentFailures++;
                            }

                        }
                        else{
                            nrRdFailures++;
                        }

                    }
                }
                catch(Throwable rb){
                    log.info(this.threadIndex+"Error while committing");

                    if(successful && !force_ro) {
                        commitFails++;
                    }

                    nrFailures++;
                    if(!force_ro){
                        nrWrFailuers++;
                        if(transaction_type==TPCCTerminal.NEW_ORDER){
                            nrNewOrderFailures++;
                        }
                        else if(transaction_type== TPCCTerminal.PAYMENT){
                            nrPaymentFailures++;
                        }
                    }
                    else{
                        nrRdFailures++;
                    }
                    successful=false;
                    log.warn(rb);


                    this.numRemoteTimeOut++;
                }

                end=System.nanoTime();
                log.info("*** tx ends: " + thisTx + ", " + (successful ? "committed" : "aborted") + " ***");

                if(lambda==0.0){  //Closed system
                    start=startService;
                }

                if(!force_ro){
                    writeDuration += end - start;
                    writeServiceTime += end - startService;
                    if(transaction_type==TPCCTerminal.NEW_ORDER){
                        newOrderDuration += end - start;
                        newOrderServiceTime += end - startService;
                    }
                    else if(transaction_type== TPCCTerminal.PAYMENT){
                        paymentDuration += end - start;
                        paymentServiceTime += end - startService;
                    }
                    if(successful){
                        successful_writeDuration += end - startService;
                        writes++;
                        if(transaction_type==TPCCTerminal.PAYMENT){
                            payment++;
                        }
                        else if(transaction_type==TPCCTerminal.NEW_ORDER){
                            newOrder++;
                        }
                    }
                }
                else{    //ro transaction
                    readDuration += end - start;
                    readServiceTime += end -startService;
                    if(successful){
                        reads++;
                        successful_readDuration += end - startService;
                    }
                }
                //We are interested only in the successful commits for write transactions
                if(successful && !force_ro){
                    this.commitSuccessDuration +=end-commit_start;
                }

                if(measureTime) {
                    if(successful) {
                        this.commitSuccessDuration += end - commit_start;
                    } else {
                        this.commitFailDuration += end - commit_start;
                    }
                    this.commitTotalDuration += end - commit_start;
                }


                this.delta=end-init_time;
            }

            completedThread.incrementAndGet();
        }




    }


    private String str(Object o) {
        return String.valueOf(o);
    }

    public void setSizeOfAnAttribute(int sizeOfValue) {
        this.sizeOfValue = sizeOfValue;
    }

    public void setNumOfThreads(int numOfThreads) {
        this.numOfThreads = numOfThreads;
    }



    public void setOpsCountStatusLog(int opsCountStatusLog) {
        this.opsCountStatusLog = opsCountStatusLog;
    }

    /**
     * This will make sure that each session runs in its own thread and no collisition will take place. See
     * https://sourceforge.net/apps/trac/cachebenchfwk/ticket/14
     */
    private String getBucketId(int threadIndex) {
        return bucketPrefix + "_" + threadIndex;
    }

    private static String generateRandomString(int size) {
        // each char is 2 bytes
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size / 2; i++) sb.append((char) (64 + r.nextInt(26)));
        return sb.toString();
    }

    public String getBucketPrefix() {
        return bucketPrefix;
    }

    public void setBucketPrefix(String bucketPrefix) {
        this.bucketPrefix = bucketPrefix;


    }

    @Override
    public String toString() {
        return "PutGetStressor{" +
                "opsCountStatusLog=" + opsCountStatusLog +
                ", sizeOfValue=" + sizeOfValue +

                ", numOfThreads=" + numOfThreads +
                ", bucketPrefix=" + bucketPrefix +
                ", cacheWrapper=" + cacheWrapper +
                "}";
    }



    private class Producer extends Thread{


        private double transaction_weight;    //an integer in [0,100]
        private int transaction_type;
        private boolean read_only;
        private double mean_lambda;
        private Random random;

        public Producer(int transaction_type, boolean read_only, double transaction_weight){

            this.transaction_weight=transaction_weight;
            this.transaction_type=transaction_type;
            this.read_only=read_only;
            this.mean_lambda=(lambda/1000.0)*(this.transaction_weight/100.0);
            this.random=new Random(System.currentTimeMillis());


        }
        
        public void run(){

            long time;
            double new_mean_lambda;



            while(completedThread.get()!=numOfThreads){

                try{
                    new_mean_lambda=mean_lambda;
                    if(cacheWrapper.isPassiveReplication()){

                        if(!is_master){
                            new_mean_lambda=new_mean_lambda/(total_num_of_slaves-1);
                        }
                    }
                    else{
                        new_mean_lambda=new_mean_lambda/total_num_of_slaves;
                    }


                    if(!cacheWrapper.isPassiveReplication() || ((read_only && !is_master) || (!read_only && is_master))){
                        //log.info("Producer "+this.transaction_type+" adds an new transaction.");
                        queue.add(new RequestType(System.nanoTime(),this.transaction_type));
                        countJobs.incrementAndGet();
                    }

                    time =(long) (exp(new_mean_lambda));
                    //log.info("Producer "+this.transaction_type+" sleeps for "+time+" millisec" );
                    Thread.sleep(time);
                }
                catch(InterruptedException i){
                    log.error("»»»»»»INTERRUPTED_EXCEPTION«««««««");
                }
                catch(IllegalStateException il){
                    log.error("»»»»»»»FULL QUEUE«««««««««");

                }
            }

        }




        private double exp(double lambda) {

            return -Math.log(1.0 - random.nextDouble()) / lambda;
        }


    }

    private class RequestType{

        private long timestamp;
        private int transactionType;

        public RequestType(long timestamp, int transactionType){
            this.timestamp=timestamp;
            this.transactionType=transactionType;
        }

    }
}

