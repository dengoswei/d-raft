
## D-Raft


recv req msg -> step -> store to disk -> update mem-state -> send rsp msg


### store to disk
- log entries;
- <term, vote, commit>
  // commit need store ??

### update mem-state

#### can be discard
- leader-id // other stuff
- vote-resps // std::map


#### must update after a success disk write
- log entries hold in mem-cache;
- <term, vote, commit> hold in mem-cache;
- config ?

// stuff only for leader
- peer-status // next msg send to => replicate state




### NEW

1. recv msg; or set
2. Step => produce <state need store to disk>
   <raf-state, replicate-state, hard-state, log entries>
   // soft
   - raft-state: leader, follower, candicate
   - replicate-state: // leader only

    // hard
   - hard-state: term, vote, commited index
   - log entries:

3. optional: store to disk
4. Apply the state(// need store to disk) => produce rsp msg
5. send rsp msg
6. notify disk-read catch ? // leader only
