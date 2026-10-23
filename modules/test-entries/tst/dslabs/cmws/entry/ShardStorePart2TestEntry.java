package dslabs.cmws.entry;

import cmws.shardkv.ShardStorePart2Test;
import dslabs.framework.testing.junit.Part;

/**
 * Discovery entry point. The test framework only scans packages whose name starts with
 * "dslabs", while the tests themselves live next to the code they test.
 */
@Part(3)
public class ShardStorePart2TestEntry extends ShardStorePart2Test {}
