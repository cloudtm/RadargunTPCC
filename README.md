# RadargunTPCC
RadargunTPCC benchmark is an implementation of the Transaction Profiles defined in the TPC Benchmark C [specification](http://www.tpc.org/tpcc/spec/tpcc_current.pdf) within [Radargun benchamark](http://sourceforge.net/apps/trac/radargun/wiki/WikiStart), and it has been designed with the purpose of executing the TPC-C benchmark against Infinispan.

## Documentation:
Refer to Radargun's [home page](http://sourceforge.net/apps/trac/radargun/wiki/WikiStart) for documentation.

## Quick start:
The best way to get started with Radargun is [The five minutes tutorial](https://sourceforge.net/apps/trac/radargun/wiki/FiveMinutesTutorial)

## Configuration:
A complete example of configuration is in benchmark.xml file in RadrgunTPCC/framework/src/main/resources/ directory. Compared to the classical Radargun [configuration file](http://sourceforge.net/apps/trac/radargun/wiki/DistributedBenchmarks), we have additional parameters concerning the configuration of the Warmup stage and the WebSessionBenchmark stage.

<Warmup operationCount="1000" numWarehouse="1"/>
In the Warmup we perform the population of the data grid and numWarehouse specifies the number of Warehouse to be populated.
<WebSessionBenchmark
               opsCountStatusLog="500"
               numOfThreads="2"
               perThreadSimulTime="60000000000"
               lambda="0.0"
               paymentWeight="45.0"
               orderStatusWeight="5.0"
               cLastMask="255"
               olIdMask="8191"
               cIdMask="1023"/>
The other new parameters are:
 - perThreadSimulTime. The simulation duration (in nanoseconds).
 - lambda. If the value is greater than "0.0", the "open system" mode is active and the parameter represents the arrival rate (in transactions per second) of a job (a transaction to be executed) to the system; otherwise the "closed system" mode is active: this means that each Stresser generates and executes a new transaction in an iteration as soon as it has completed the previous iteration.
 - paymentWeight. Percentage of Payment transactions in the system.
 - orderStatusWeight. Percentage of Order-Status transactions in the system.
 - cLastMask, cIdMask, olIdMask. Bitmasks used for generating customer last names, customer numbers and item numbers in order to implement a non-uniform random generation. See the TPC Benchmark C specification for more details.

