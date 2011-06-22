package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.state.MasterState;
import org.radargun.stressors.PutGetStressor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Double.parseDouble;
import static org.radargun.utils.Utils.numberFormat;

/**
 * Simulates the work with a distributed web sessions.
 *
 * @author Mircea.Markus@jboss.com
 */
public class WebSessionBenchmarkStage extends AbstractDistStage {

   private int opsCountStatusLog = 5000;

   public static final String SESSION_PREFIX = "SESSION";





   /**
    * Each attribute will be a byte[] of this size
    */
   private int sizeOfAnAttribute = 1000;


   /**
    * the number of threads that will work on this slave
    */
   private int numOfThreads = 10;

   private boolean reportNanos = false;


   private CacheWrapper cacheWrapper;


    private int total_num_of_slaves;//needed to perform another kind of test.

    private long perThreadSimulTime;  //total time (in nanosec) of simulation for each stresser thread

    private double lambda; //global mean arrival rate

    private double paymentWeight;
    private double orderStatusWeight;

    private int cLastMask;
    private int olIdMask;
    private int cIdMask;


   public DistStageAck executeOnSlave() {
      DefaultDistStageAck result = new DefaultDistStageAck(slaveIndex, slaveState.getLocalAddress());
      this.cacheWrapper = slaveState.getCacheWrapper();
      if (cacheWrapper == null) {
         log.info("Not running test on this slave as the wrapper hasn't been configured.");
         return result;
      }

      log.info("Starting WebSessionBenchmarkStage: " + this.toString());
      //Added for the  PB vs standard Infinispan test
      PutGetStressor putGetStressor = new PutGetStressor(cacheWrapper.isPrimary(), total_num_of_slaves,this.perThreadSimulTime, this.lambda, this.paymentWeight, this.orderStatusWeight, this.cLastMask, this.olIdMask, this.cIdMask);

      putGetStressor.setBucketPrefix(getSlaveIndex() + "");

      putGetStressor.setNumOfThreads(numOfThreads);
      putGetStressor.setOpsCountStatusLog(opsCountStatusLog);
      putGetStressor.setSizeOfAnAttribute(sizeOfAnAttribute);


       //ATTENTION! THE CACHEWRAPPER IS A PARAMETER FOR THE STRESS METHOD SO IT'S NOT SET UPON THE CALL TO THE
       //STRESSOR CONSTRUCTOR!

      try {
         Map<String, String> results = putGetStressor.stress(cacheWrapper);
         result.setPayload(results);
         return result;
      } catch (Exception e) {
         log.warn("Exception while initializing the test", e);
         result.setError(true);
         result.setRemoteException(e);
         return result;
      }
   }

   public boolean processAckOnMaster(List<DistStageAck> acks, MasterState masterState) {
      logDurationInfo(acks);
      boolean success = true;
      Map<Integer, Map<String, Object>> results = new HashMap<Integer, Map<String, Object>>();
      masterState.put("results", results);
      for (DistStageAck ack : acks) {
         DefaultDistStageAck wAck = (DefaultDistStageAck) ack;
         if (wAck.isError()) {
            success = false;
            log.warn("Received error ack: " + wAck);
         } else {
            if (log.isTraceEnabled())
               log.trace(wAck);
         }
         Map<String, Object> benchResult = (Map<String, Object>) wAck.getPayload();
         if (benchResult != null) {
            results.put(ack.getSlaveIndex(), benchResult);
            Object reqPerSes = benchResult.get("REQ_PER_SEC");
            if (reqPerSes == null) {
               throw new IllegalStateException("This should be there!");
            }
            log.info("On slave " + ack.getSlaveIndex() + " we had " + numberFormat(parseDouble(reqPerSes.toString())) + " requests per second");
         } else {
            log.trace("No report received from slave: " + ack.getSlaveIndex());
         }
      }
      return success;
   }


   public void setSizeOfAnAttribute(int sizeOfAnAttribute) {
      this.sizeOfAnAttribute = sizeOfAnAttribute;
   }

   public void setNumOfThreads(int numOfThreads) {
      this.numOfThreads = numOfThreads;
   }

   public void setReportNanos(boolean reportNanos) {
      this.reportNanos = reportNanos;
   }



   public void setOpsCountStatusLog(int opsCountStatusLog) {
      this.opsCountStatusLog = opsCountStatusLog;
   }

   @Override
   public String toString() {
      return "WebSessionBenchmarkStage {" +
            "opsCountStatusLog=" + opsCountStatusLog +
           // ", numberOfRequests=" + numberOfRequests +
            ", sizeOfAnAttribute=" + sizeOfAnAttribute +
            ", numOfThreads=" + numOfThreads +
            ", reportNanos=" + reportNanos +
            ", cacheWrapper=" + cacheWrapper +
            ", " + super.toString();
   }



    public void setNumSlaves(int no){

        this.total_num_of_slaves=no;
    }





    public void setPerThreadSimulTime(long l){

        this.perThreadSimulTime=l;

    }


    public long getPerThreadSimulTime(){

        return this.perThreadSimulTime;
    }

    public double getLambda(){
        return this.lambda;
    }

    public void setLambda(double lambda){
        this.lambda=lambda;
    }

    public double getPaymentWeight() {
        return paymentWeight;
    }

    public void setPaymentWeight(double paymentWeight) {
        this.paymentWeight = paymentWeight;
    }

    public double getOrderStatusWeight() {
        return orderStatusWeight;
    }

    public void setOrderStatusWeight(double orderStatusWeight) {
        this.orderStatusWeight = orderStatusWeight;
    }

    public int getCIdMask() {
        return cIdMask;
    }

    public int getCLastMask() {
        return cLastMask;
    }

    public int getOlIdMask() {
        return olIdMask;
    }

    public void setCIdMask(int cIdMask) {
        this.cIdMask = cIdMask;
    }

    public void setCLastMask(int cLastMask) {
        this.cLastMask = cLastMask;
    }

    public void setOlIdMask(int olIdMask) {
        this.olIdMask = olIdMask;
    }
}
