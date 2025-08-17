/*
 * =============================================================================
 *  BEFORE PR: The messy dual listener nightmare
 * =============================================================================
 *
 *                    +-------------------------------------+
 *                    |      RealmAwareZkClient             |
 *                    |                                     |
 *                    |  subscribeStateChanges(new)  <---- clean method but not used
 *                    |  subscribeStateChanges(old)  <---- everybody used this
 *                    +------------------+------------------+
 *                                       |
 *                                       | had default method that
 *                                       | created wrapper
 *                                       v
 *                      +-----------------------------------+
 *                      |   I0ItecIZkStateListenerImpl      | <-- WHY DOES THIS EXIST?!
 *                      |      (wrapper hell)               |
 *                      |                                   |
 *                      |  wraps new ---> old               |
 *                      +------------------+----------------+
 *                                         |
 *                                         | wraps to
 *                                         v
 *                +------------------------+         +------------------------+
 *                |  deprecated.           |         |   IZkStateListener     |
 *                |  IZkStateListener      |  <---+  |     (new one)          |
 *                |    (what everyone      |      |  |     (unused)           |
 *                |     actually used)     |      |  |                        |
 *                +------------------------+      |  +------------------------+
 *                          ^                    |
 *                          |                    |
 *                          | imported & used    | wrapper delegates
 *                          |                    |
 *            +----------------+    +----------------+    +----------------+
 *            | DedicatedZkClient|    |  SharedZkClient|    |FederatedZkClient|
 *            |                  |    |                |    |                |
 *            | imports:         |    | imports:       |    | imports: both  |
 *            | deprecated.      |    | deprecated.    |    | deprecated &   |
 *            | IZkStateListener |    | IZkStateListener|   | IZkStateListener|
 *            |                  |    |                |    |                |
 *            | delegates to     |    | delegates to   |    | throws         |
 *            | _rawZkClient     |    |_innerSharedZk  |    | UnsupportedOp  |
 *            +------------------+    +----------------+    +----------------+
 *
 * Developer experience: "Which listener should I use??"
 * 
 * =============================================================================
 *  AFTER PR: The beautiful cleanup
 * =============================================================================
 *
 *                    +-------------------------------------+
 *                    |      RealmAwareZkClient             |
 *                    |                                     |
 *                    |  subscribeStateChanges(IZkState    |  <-- ONLY ONE method now!
 *                    |        Listener listener)          |      No more overloads!
 *                    +------------------+------------------+
 *                                       |
 *                                       | direct interface usage
 *                                       | no wrapper needed
 *                                       v
 *                      +-----------------------------------+
 *                      |      IZkStateListener             |
 *                      |    (org.apache.helix.zookeeper    |
 *                      |     .zkclient.IZkStateListener)   |
 *                      |                                   |
 *                      |   handleStateChanged()            |
 *                      |   handleNewSession(sessionId) <-- with session awareness!
 *                      |   handleSessionEstablishmentErr   |
 *                      +-----------------------------------+
 *                                       ^
 *                                       |
 *                                       | ALL implementations import
 *                                       | the SAME interface now
 *                                       |
 *            +------------------+       |       +----------------+    +----------------+
 *            | DedicatedZkClient|       |       |  SharedZkClient|    |FederatedZkClient|
 *            |                  |       |       |                |    |                |
 *            | imports:         +-------+       | imports:       |    | imports:       |
 *            | IZkStateListener |               | IZkStateListener|   | IZkStateListener|
 *            | (new clean one)  |               | (new clean one)|    | (new clean one)|
 *            |                  |               |                |    |                |
 *            | delegates to     |               | delegates to   |    | throws         |
 *            | _rawZkClient     |               |_innerSharedZk  |    | UnsupportedOp  |
 *            |   (works!)       |               |   (works!)     |    |  (consistent!) |
 *            +------------------+               +----------------+    +----------------+
 *
 * Developer experience: "Ah, just use IZkStateListener!"
 *
 *
 * KEY CHANGES SUMMARY:
 * ====================
 * 
 * REMOVED:
 *    - deprecated.IZkStateListener interface (org.apache.helix.zookeeper.zkclient.deprecated.IZkStateListener)
 *    - I0ItecIZkStateListenerImpl wrapper class (the confusing bridge)
 *    - Default method in RealmAwareZkClient that created the wrapper
 *    - Dual method overloads for subscribeStateChanges/unsubscribeStateChanges
 *
 * CHANGED PER CLIENT TYPE:
 *    
 *    DedicatedZkClient:
 *      Before: import org.apache.helix.zookeeper.zkclient.deprecated.IZkStateListener
 *      After:  import org.apache.helix.zookeeper.zkclient.IZkStateListener
 *      Effect: Clean delegation to _rawZkClient with modern interface
 *
 *    SharedZkClient:  
 *      Before: import org.apache.helix.zookeeper.zkclient.deprecated.IZkStateListener
 *      After:  import org.apache.helix.zookeeper.zkclient.IZkStateListener
 *      Effect: Clean delegation to _innerSharedZkClient with modern interface
 *
 *    FederatedZkClient:
 *      Before: import both deprecated and new IZkStateListener, threw UnsupportedOp for both
 *      After:  import org.apache.helix.zookeeper.zkclient.IZkStateListener, throws UnsupportedOp
 *      Effect: Consistent exception throwing with single interface
 *
 * ADDED:
 *    - Direct subscribeStateChanges/unsubscribeStateChanges methods in HelixZkClient interface
 *    - Consistent org.apache.helix.zookeeper.zkclient.IZkStateListener usage everywhere
 *
 * RESULT:
 *    - No more "which listener should I use?" confusion
 *    - All three client types use the SAME interface now
 *    - Better session handling with sessionId parameter
 *    - Cleaner inheritance hierarchy with no wrapper madness
 *    - More maintainable codebase
 *
 *          Before: "Is this the right listener interface? Which import?"
 *          After:  "Just use IZkStateListener - there's only one!"
 *
 * Side note: Whoever created that I0ItecIZkStateListenerImpl wrapper class
 * probably had good intentions, but man was that confusing!
 * Good riddance to legacy baggage!
 */
