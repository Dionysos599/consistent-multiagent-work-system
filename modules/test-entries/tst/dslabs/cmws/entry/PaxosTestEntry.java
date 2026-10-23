package dslabs.cmws.entry;

import cmws.paxos.PaxosTest;

/**
 * Discovery entry point. The test framework only scans packages whose name starts with
 * "dslabs", while the tests themselves live next to the code they test.
 */
public class PaxosTestEntry extends PaxosTest {}
