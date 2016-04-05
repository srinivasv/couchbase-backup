package com.talena.agents.couchbase.benchmark;

import com.talena.agents.couchbase.benchmark.BenchmarkConfig.OP_TYPE;

public class Benchmark {

  private static final String BACKUP_FULL = "full";
  private static final String BACKUP_INCR = "incr";
  private static final String TIMED_RUN = "time";

  public static void main(String[] args) {
    final int argsCount = 4;

    if (args.length < argsCount) {
      printUsage();

      System.exit(1);
    }

    BenchmarkConfig config = parseArguments(args);
    if (config.getOperation() == OP_TYPE.USAGE) {
      printUsage();

      System.exit(1);
    }

    System.out.println(config);

    CouchbaseAgent cbAgent = new CouchbaseAgent(config);

    cbAgent.backup();
  }

  private static BenchmarkConfig parseArguments(String[] args) {
    BenchmarkConfig config = new BenchmarkConfig();

    config.setBucketName(args[0]);

    config.setOperation(OP_TYPE.USAGE);
    if (args[1].compareToIgnoreCase(BACKUP_FULL) == 0) {
      config.setOperation(OP_TYPE.BACKUP_FULL);
    } else if (args[1].compareToIgnoreCase(BACKUP_INCR) == 0) {
      config.setOperation(OP_TYPE.BACKUP_INCR);
    } else if (args[1].toLowerCase().startsWith(TIMED_RUN)) {
      try {
        long secWait = Long.parseLong(args[1].substring(TIMED_RUN.length()));

        config.setOperation(OP_TYPE.TIMED_RUN);
        config.setSecondsWait(secWait);
      } catch (NumberFormatException e) {
      }
    }

    config.setBackupPath(args[2]);

    String nodes[] = new String[args.length - 3];
    System.arraycopy(args, 3, nodes, 0, args.length - 3);
    config.setNodesIp(nodes);

    return config;
  }

  private static void printUsage() {
    System.err.println("Usage: Benchmark <bucket> <full/incr/timeXXX> <backupPath> <ipNodes>");
  }
}
