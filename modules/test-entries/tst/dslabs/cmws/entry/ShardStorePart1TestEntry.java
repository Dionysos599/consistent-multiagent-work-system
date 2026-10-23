package dslabs.cmws.entry;

import cmws.shardkv.ShardStorePart1Test;
import dslabs.framework.testing.junit.Part;

/**
 * Discovery entry point. The test framework only scans packages whose name starts with
 * "dslabs", while the tests themselves live next to the code they test.
 */
@Part(2)
public class ShardStorePart1TestEntry extends ShardStorePart1Test {}
