/**
 * Serialization classes for writing primitives to and from byte arrays.
 * <P>
 * &gt;=Accumulo-1.6.0 have these included already, but &lt;Accumulo1.6 does
 * not. To ensure proper compatibility with both major versions of Accumulo, 
 * we need to 'inline' these classes.
 * <P>
 * Once support for Accumulo 1.5 is dropped, this can be removed from the Hive
 * codebase in favor of using the libraries directly from Accumulo.
 */
package org.apache.hadoop.hive.accumulo.lexicoders;