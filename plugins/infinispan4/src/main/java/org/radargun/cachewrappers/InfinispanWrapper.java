package org.radargun.cachewrappers;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.utils.Utils;

import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import java.awt.image.ImagingOpException;
import java.io.FileWriter;
import java.util.List;

import static java.util.concurrent.TimeUnit.MINUTES;

public class InfinispanWrapper implements CacheWrapper {

   private static Log log = LogFactory.getLog(InfinispanWrapper.class);
   DefaultCacheManager cacheManager;
   Cache<Object, Object> cache;
   TransactionManager tm;
   boolean started = false;
   String config;


   public void setUp(String config, boolean isLocal, int nodeIndex) throws Exception {
      this.config = config;


      if (!started) {
         cacheManager = new DefaultCacheManager(config);
         // use a named cache, based on the 'default'
         cacheManager.defineConfiguration("x", new Configuration());
         cache = cacheManager.getCache("x");
         tm=cache.getAdvancedCache().getTransactionManager();
         started = true;
      }

      log.info("Loading JGroups form: " + org.jgroups.Version.class.getProtectionDomain().getCodeSource().getLocation());
      log.info("JGroups version: " + org.jgroups.Version.printDescription());

      // should we be blocking until all rehashing, etc. has finished?
      long gracePeriod = MINUTES.toMillis(15);
      long giveup = System.currentTimeMillis() + gracePeriod;
      if (cache.getConfiguration().getCacheMode().isDistributed()) {
         while (!cache.getAdvancedCache().getDistributionManager().isJoinComplete() && System.currentTimeMillis() < giveup)
            Thread.sleep(200);
      }

      if (cache.getConfiguration().getCacheMode().isDistributed() && !cache.getAdvancedCache().getDistributionManager().isJoinComplete())
         throw new RuntimeException("Caches haven't discovered and joined the cluster even after " + Utils.prettyPrintTime(gracePeriod));
   }

   public void tearDown() throws Exception {
      List<Address> addressList = cacheManager.getMembers();
      if (started) {
         cacheManager.stop();
         log.trace("Stopped, previous view is " + addressList);
         started = false;
      }
   }

   public void put(String bucket, Object key, Object value) throws Exception {

      cache.put(key, value);

   }

   public Object get(String bucket, Object key) throws Exception {
      return cache.get(key);
   }

   public void empty() throws Exception {
      log.info("Cache size before clear: " + cache.size());
      cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).clear();
      log.info("Cache size after clear: " + cache.size());
   }

   public int getNumMembers() {
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      if (componentRegistry.getStatus().startingUp()) {
         log.trace("We're in the process of starting up.");
      }
      if (cacheManager.getMembers() != null) {
         log.trace("Members are: " + cacheManager.getMembers());
      }
      return cacheManager.getMembers() == null ? 0 : cacheManager.getMembers().size();
   }

   public String getInfo() {
      String clusterSizeStr = "";
      RpcManager rpcManager = cache.getAdvancedCache().getRpcManager();
      if (rpcManager != null && rpcManager.getTransport() != null) {
         clusterSizeStr = "cluster size = " + rpcManager.getTransport().getMembers().size();
      }
      return cache.getVersion() + ", " + clusterSizeStr + ", " + config + ", Size of the cache is: " + cache.size();
   }

   public Object getReplicatedData(String bucket, String key) throws Exception {
      return get(bucket, key);
   }

   public Object startTransaction() {

      if (tm==null) return null ;

      try {
         tm.begin();
         return tm.getTransaction();
      }
      catch (Exception e) {
         throw new RuntimeException(e);
      }
   }


   public void endTransaction(boolean successful)throws RuntimeException{

      if (tm == null){
         return;
      }
      try {
         if (successful)
            tm.commit();
         else
            tm.rollback();
      }


      catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

    /*
    * Method to retrieve information about the replicas policy
     */

   public boolean isPassiveReplication(){
      return false;
   }

   public boolean isPrimary(){

      return this.cacheManager.isCoordinator();
   }

   public Cache getCache(){
      return this.cache;
   }

   public String getNodeName(){
       return ((EmbeddedCacheManager)getCache().getCacheManager()) .getAddress().toString();
   }

}
