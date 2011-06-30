{-
Copyright (c) 2010 Alexander Bogdanov <andorn@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
-}

{-# LANGUAGE FlexibleInstances, MultiParamTypeClasses, FunctionalDependencies,
  FlexibleContexts, OverloadedStrings #-}

-- | Main Redis API and protocol implementation
module Database.Redis.Redis (
       -- * Types ans Constructors
       Redis,
       Reply(..),
       Message(..),
       LInsertDirection(..),
       Interval(..),
       IsInterval(..),
       SortOptions(..),
       Aggregate(..),
       RedisKeyType(..),
       RedisInfo,
       sortDefaults,
       fromRInline, fromRBulk, fromRBulk', fromRMulti,
       fromRMultiBulk, fromRMultiBulk', fromRInt,
       fromROk, noError, parseMessage, takeAll,

       -- * Database connection
       localhost, defaultPort,
       connect, disconnect, isConnected,
       getServer, getDatabase, renameCommand,

       -- * Redis commands
       -- ** Generic
       ping, auth, echo,  quit, shutdown,
       multi, exec, discard, run_multi,
       watch, unwatch, run_cas, exists,
       del, getType, keys, randomKey, rename,
       renameNx, dbsize, expire, expireAt,
       persist, ttl, select, move, flushDb,
       flushAll, info,

       -- ** Strings
       set, setNx, setEx, mSet, mSetNx,
       get, getSet, mGet,
       incr, incrBy, decr,
       decrBy, append, substr,
       getrange, setrange,
       getbit, setbit, strlen,

       -- ** Lists
       rpush, lpush, rpushx, lpushx,
       linsert, llen, lrange, ltrim,
       lindex, lset, lrem, lpop, rpop,
       rpoplpush, blpop, brpop, brpoplpush,

       -- ** Sets
       sadd, srem, spop, smove, scard, sismember,
       smembers, srandmember, sinter, sinterStore,
       sunion, sunionStore, sdiff, sdiffStore,

       -- ** Sorted sets
       zadd, zrem, zincrBy, zrange,
       zrevrange, zrangebyscore, zrevrangebyscore,
       zcount, zremrangebyscore, zcard, zscore,
       zrank, zrevrank, zremrangebyrank,
       zunion, zinter, zunionStore, zinterStore,

       -- ** Hashes
       hset, hget, hdel, hmset, hmget,
       hincrby, hexists, hlen,
       hkeys, hvals, hgetall,

       -- ** Sorting
       sort, listRelated,

       -- ** Publish/Subscribe
       subscribed, subscribe, unsubscribe,
       psubscribe, punsubscribe, publish,
       listen,

       -- ** Persistent control
       save, bgsave, lastsave, bgrewriteaof
)
where

import Control.Concurrent.MVar
import Data.IORef
import qualified Network.Socket as S
import qualified System.IO as IO
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8 (unpack, pack)
import Data.ByteString (ByteString)
import Data.Maybe (fromJust, isNothing)
import Data.List (intersperse)
import qualified Data.Map as Map
import Control.Monad (when)
import Control.Exception (onException)

import Database.Redis.ByteStringClass
import Database.Redis.Info
import Database.Redis.Internal

-- | default Redis port
defaultPort :: String
defaultPort = "6379"

-- | just a localhost
localhost :: String
localhost = "localhost"

-- | a (0, -1) range - takes all element from a list in lrange, zrange
-- and so on
takeAll :: (Int, Int)
takeAll = (0, -1)

-- | Unwraps RInline reply.
--
-- Throws an exception when called with something different from RInline
fromRInline :: (Monad m, BS s) => Reply s -> m s
fromRInline reply = case reply of
                      RError msg -> error msg
                      RInline s  -> return s
                      _          -> error $ "wrong reply, RInline expected: " ++ (show reply)

-- | Unwraps RBulk reply.
--
-- Throws an exception when called with something different from RBulk
fromRBulk :: (Monad m, BS s) => Reply s -> m (Maybe s)
fromRBulk reply = case reply of
                    RError msg -> error msg
                    RBulk s    -> return s
                    _          -> error $ "wrong reply, RBulk expected: " ++ (show reply)

-- | The same as fromRBulk but with fromJust applied
fromRBulk' :: (Monad m, BS s) => Reply s -> m s
fromRBulk' reply = fromRBulk reply >>= return . fromJust

-- | Unwraps RMulti reply
--
-- Throws an exception when called with something different from RMulti
fromRMulti :: (Monad m, BS s) => Reply s -> m (Maybe [Reply s])
fromRMulti reply = case reply of
                     RError msg -> error msg
                     RMulti ss  -> return ss
                     _          -> error $ "wrong reply, RMulti expected: " ++ (show reply)

-- | Unwraps RMulti reply filled with RBulk
--
-- Throws an exception when called with something different from RMulti
fromRMultiBulk :: (Monad m, BS s) => Reply s -> m (Maybe [Maybe s])
fromRMultiBulk reply = fromRMulti reply >>= return . (>>= sequence . map fromRBulk)

-- | The same as fromRMultiBulk but with fromJust applied
fromRMultiBulk' :: (Monad m, BS s) => Reply s -> m [s]
fromRMultiBulk' reply = fromRMultiBulk reply >>= return . fromJust >>= return . (map fromJust)

-- | Unwraps RInt reply
--
-- Throws an exception when called with something different from RInt
fromRInt :: (Monad m, BS s) => Reply s -> m Int
fromRInt reply = case reply of
                   RError msg -> error msg
                   RInt n     -> return n
                   _          -> error $ "wrong reply, RInt expected: " ++ (show reply)

-- | Unwraps ROk reply
--
-- Throws an exception when called with something different from ROk
fromROk :: (Monad m, BS s) => Reply s -> m ()
fromROk reply = case reply of
                   RError msg -> error msg
                   ROk        -> return ()
                   _          -> error $ "wrong reply, ROk expected: " ++ (show reply)

-- | Unwraps every non-error reply
--
-- Throws an exception when called with something different from RMulti
noError :: (Monad m, BS s) => Reply s -> m ()
noError reply = case reply of
                   RError msg -> error msg
                   _          -> return ()

-- | Parse Reply as a Message
--
-- Throws an exception on parse error
parseMessage :: (Monad m, BS s) => Reply ByteString -> m (Message s)
parseMessage reply = do rm <- fromRMulti reply
                        when (isNothing rm) $ error $ "error parsing message: " ++ (show reply)
                        let rm' = fromJust rm
                        mtype <- fromRBulk $ head rm'
                        when (isNothing mtype) $ error $ "error parsing message: " ++ (show reply)
                        return $ case fromJust mtype of
                                   "subscribe"    -> mksub MSubscribe $ tail rm'
                                   "unsubscribe"  -> mksub MUnsubscribe $ tail rm'
                                   "psubscribe"   -> mksub MPSubscribe $ tail rm'
                                   "punsubscribe" -> mksub MPUnsubscribe $ tail rm'
                                   "message"      -> mkmsg $ tail rm'
                                   "pmessage"     -> mkpmsg $ tail rm'

    where mksub f [RBulk (Just k), RInt n] = f (fromBS k) n
          mkmsg [RBulk (Just k), RBulk (Just msg)] = MMessage (fromBS k) (fromBS msg)
          mkpmsg  [RBulk (Just p), RBulk (Just c), RBulk (Just msg)] = MPMessage (fromBS p) (fromBS c) (fromBS msg)

-- | Conects to Redis server and returns connection descriptor
connect :: String               -- ^ hostname or path to the redis socket
        -> String               -- ^ port or null if unix sockets used
        -> IO Redis
connect hostname port =
    do s <- if null port
              then socket_unix hostname
              else socket_inet hostname port

       h <- S.socketToHandle s IO.ReadWriteMode
       IO.hSetBuffering h (IO.BlockBuffering Nothing)

       newRedis (hostname, port) h

socket_inet :: String -> String -> IO S.Socket
socket_inet hostname port =
    do serveraddr <- head `fmap` (S.getAddrInfo
                                  (Just S.defaultHints {S.addrFlags = [S.AI_CANONNAME],
                                                        S.addrFamily = S.AF_INET})
                                  (Just hostname) (Just port))

       s <- S.socket (S.addrFamily serveraddr) S.Stream S.defaultProtocol
       S.setSocketOption s S.KeepAlive 1
       S.connect s (S.addrAddress serveraddr)
       return s

socket_unix :: String -> IO S.Socket
socket_unix path =
    do s <- S.socket S.AF_UNIX S.Stream S.defaultProtocol
       S.setSocketOption s S.KeepAlive 0
       S.connect s (S.SockAddrUnix path)
       return s

-- | Close connection
disconnect :: Redis -> IO ()
disconnect = withState' (IO.hClose . handle)

-- | Returns True when connection handler is opened
isConnected :: Redis -> IO Bool
isConnected = withState' (IO.hIsOpen . handle)

-- | Returns connection host and port
getServer :: Redis -> IO (String, String)
getServer = withState' (return . server)

-- | Returns currently selected database
getDatabase :: Redis -> IO Int
getDatabase = withState' (return . database)

-- | Adds command to renaming map
renameCommand :: Redis
              -> ByteString        -- ^ command to rename
              -> ByteString        -- ^ new name
              -> IO ()
renameCommand r c c' = inState_ r $ \rs -> let m = renamedCommands rs
                                           in return $ rs {renamedCommands = Map.insert c c' m}

{- ============ Just commands ============= -}
-- | ping - pong
--
-- RPong returned if no errors happends
ping :: Redis -> IO (Reply ())
ping = withState' (\rs -> sendCommand rs (CInline "PING") >> recv rs)

{- UNTESTED -}
-- | Password authentication
--
-- ROk returned
auth :: BS s =>
        Redis
     -> s                       -- ^ password
     -> IO (Reply ())
auth r pwd = withState r (\rs -> sendCommand rs (CMInline ["AUTH", toBS pwd] ) >> recv rs)

-- | Echo the given string
--
-- RBulk returned
echo :: BS s =>
        Redis
     -> s                       -- ^ what to echo
     -> IO (Reply s)
echo r s = withState r (\rs -> sendCommand rs (CMBulk ["ECHO", toBS s]) >> recv rs)

{- UNTESTED -}
-- | Quit and close connection
quit :: Redis -> IO ()
quit r = withState r (sendCommand' (CInline "QUIT")) >> disconnect r

{- UNTESTED -}
-- | Stop all the clients, save the DB, then quit the server
shutdown :: Redis -> IO ()
shutdown r = withState r (sendCommand' (CInline "SHUTDOWN")) >> disconnect r

-- | Begin the multi-exec block
--
-- ROk returned
multi :: Redis -> IO (Reply ())
multi = withState' (\rs -> sendCommand rs (CInline "MULTI") >> recv rs)

-- | Execute queued commands
--
-- RMulti returned - replys for all executed commands
exec :: BS s => Redis -> IO (Reply s)
exec = withState' (\rs -> sendCommand rs (CInline "EXEC") >> recv rs)

-- | Discard queued commands without execution
--
-- ROk returned
discard :: Redis -> IO (Reply ())
discard = withState' (\rs -> sendCommand rs (CInline "DISCARD") >> recv rs)

-- | Run commands within multi-exec block
--
-- RMulti returned - replys for all executed commands
run_multi :: (BS s) =>
             Redis
          -> (Redis -> IO ())    -- ^ IO action to run
          -> IO (Reply s)
run_multi r cs = withState r (\rs -> do sendCommand rs (CInline "MULTI")
                                        (recv rs :: IO (Reply ())) >>= fromROk
                                        cs r `onException` do sendCommand rs (CInline "DISCARD")
                                                              recv rs :: IO (Reply ())
                                        sendCommand rs (CInline "EXEC")
                                        recv rs)

-- | Add keys to a watch list for Check-and-Set operation.
--
-- For more information see <http://redis.io/topics/transactions>
--
-- ROk returned
watch :: BS s =>
         Redis
      -> [s]                    -- ^ keys to watch for
      -> IO (Reply ())
watch r keys = withState r (\rs -> sendCommand rs (CMBulk ("WATCH" : map toBS keys)) >> recv rs)

-- | Force unwatch all watched keys
--
-- For more information see <http://redis.io/topics/transactions>
--
-- ROk returned
unwatch :: Redis -> IO (Reply ())
unwatch = withState' (\rs -> sendCommand rs (CInline "UNWATCH") >> recv rs)

-- | Run actions in a CAS manner
--
-- You have to explicitly add multi/exec commands to an appropriate
-- place in an action sequence. Command sequence will be explicitly
-- terminated with /unwatch/ command even if /exec/ command was sent.
--
-- Result of user-defined action returned
run_cas :: BS s1 =>
           Redis
        -> [s1]                     -- ^ keys watched
        -> (Redis -> IO a)          -- ^ action to run
        -> IO a
run_cas r keys cs = let keys' = map toBS keys
                    in withState r (\rs -> do sendCommand rs (CMBulk ("WATCH" : keys'))
                                              (recv rs :: IO (Reply ())) >>= fromROk
                                              res <- cs r `onException` do sendCommand rs (CInline "DISCARD")
                                                                           recv rs :: IO (Reply ())
                                                                           sendCommand rs (CInline "UNWATCH")
                                                                           recv rs :: IO (Reply ())
                                              sendCommand rs (CInline "UNWATCH")
                                              (recv rs :: IO (Reply ())) >>= fromROk
                                              return res)

-- | Test if the key exists
--
-- (RInt 1) returned if the key exists and (RInt 0) otherwise
exists :: BS s =>
          Redis
       -> s                     -- ^ target key
       -> IO (Reply Int)
exists r key = withState r (\rs -> sendCommand rs (CMBulk ["EXISTS", toBS key]) >> recv rs)

-- | Remove the key
--
-- (RInt 0) returned if no keys were removed or (RInt n) with removed keys count
del :: BS s =>
       Redis
    -> s                        -- ^ target key
    -> IO (Reply Int)
del r key = withState r (\rs -> sendCommand rs (CMBulk ["DEL", toBS key]) >> recv rs)


data RedisKeyType = RTNone | RTString | RTList | RTSet | RTZSet | RTHash
                    deriving (Show, Eq)

parseType :: ByteString -> RedisKeyType
parseType "none"   = RTNone
parseType "string" = RTString
parseType "list"   = RTList
parseType "set"    = RTSet
parseType "zset"   = RTZSet
parseType "hash"   = RTHash
parseType s = error $ "unknown key type: " ++ (B8.unpack s)

-- | Return the type of the value stored at key in form of a string
--
-- RedisKeyType returned
getType :: BS s =>
           Redis
        -> s                    -- ^ target key
        -> IO RedisKeyType
getType r key = withState r (\rs -> sendCommand rs (CMBulk ["TYPE", toBS key]) >> recv rs
                                    >>= fromRInline >>= return . parseType)


-- | Returns all the keys matching the glob-style pattern
--
-- RMulti filled with RBulk returned
keys :: (BS s1, BS s2) =>
        Redis
     -> s1                      -- ^ target keys pattern
     -> IO (Reply s2)
keys r pattern = withState r (\rs -> sendCommand rs (CMInline ["KEYS", toBS pattern]) >> recv rs)

-- | Return random key name
--
-- RBulk returned
randomKey :: BS s => Redis -> IO (Reply s)
randomKey r = withState r (\rs -> sendCommand rs (CInline "RANDOMKEY") >> recv rs)

-- | Rename the key. If key with that name exists it'll be overwritten.
--
-- ROk returned
rename :: (BS s1, BS s2) =>
          Redis
       -> s1                    -- ^ source key
       -> s2                    -- ^ destination key
       -> IO (Reply ())
rename r from to = withState r (\rs -> sendCommand rs (CMBulk ["RENAME", toBS from, toBS to]) >> recv rs)

-- | Rename the key if no keys with destination name exists.
--
-- (RInt 1) returned if key was renamed and (RInt 0) otherwise
renameNx :: (BS s1, BS s2) =>
            Redis
         -> s1                  -- ^ source key
         -> s2                  -- ^ destination key
         -> IO (Reply Int)
renameNx r from to = withState r (\rs -> sendCommand rs (CMBulk ["RENAMENX", toBS from, toBS to]) >> recv rs)


-- | Get the number of keys in the currently selected database
--
-- RInt returned
dbsize :: Redis -> IO (Reply Int)
dbsize r = withState r (\rs -> sendCommand rs (CInline "DBSIZE") >> recv rs)

-- | Set an expiration timeout in seconds on the specified key.
--
-- For more information see <http://redis.io/commands/expire>
--
-- (RInt 1) returned if timeout was set and (RInt 0) otherwise
expire :: BS s =>
          Redis
       -> s                     -- ^ target key
       -> Int                   -- ^ timeout in seconds
       -> IO (Reply Int)
expire r key seconds = withState r (\rs -> sendCommand rs (CMBulk ["EXPIRE", toBS key, toBS seconds]) >> recv rs)

-- | Set an expiration time in form of UNIX timestamp on the specified key
--
-- For more information see <http://redis.io/commands/expireat>
--
-- (RInt 1) returned if timeout was set and (RInt 0) otherwise
expireAt :: BS s =>
            Redis
         -> s                   -- ^ target key
         -> Int                 -- ^ expiration time
         -> IO (Reply Int)
expireAt r key timestamp = withState r (\rs -> sendCommand rs (CMBulk ["EXPIREAT", toBS key, toBS timestamp]) >> recv rs)

-- | Remove the timeout from a key
--
-- (RInt 1) returned if the timeout was removed and (RInt 0) otherwise
persist :: BS s =>
           Redis
        -> s                    -- ^ target key
        -> IO (Reply Int)
persist r key = withState r (\rs -> sendCommand rs (CMBulk ["PERSIST", toBS key]) >> recv rs)

-- | Return the remining time to live of the key or -1 if key has no
-- associated timeout
--
-- RInt returned
ttl :: BS s =>
       Redis
    -> s                        -- ^ target key
    -> IO (Reply Int)
ttl r key = withState r (\rs -> sendCommand rs (CMBulk ["TTL", toBS key]) >> recv rs)

-- | Select the DB with the specified zero-based numeric index
--
-- ROk returned
select :: Redis
       -> Int                   -- ^ database number
       -> IO (Reply ())
select r db = inState r $ \rs -> do sendCommand rs (CMInline ["SELECT", toBS db])
                                    reply <- recv rs
                                    return (rs { database = db }, reply)

-- | Move the specified key from the currently selected DB to the
-- specified destination DB. If such a key is already exists in the
-- target DB no data modification performed.
--
-- (RInt 1) returned if the key was moved and (RInt 0) otherwise
move :: BS s =>
        Redis
     -> s                       -- ^ target key
     -> Int                     -- ^ destination database number
     -> IO (Reply Int)
move r key db = withState r (\rs -> sendCommand rs (CMBulk ["MOVE", toBS key, toBS db]) >> recv rs)

-- | Delete all the keys of the currently selected DB
--
-- ROk returned
flushDb :: Redis -> IO (Reply ())
flushDb r = withState r (\rs -> sendCommand rs (CInline "FLUSHDB") >> recv rs)

-- | Delete all the keys of all the existing databases
--
-- ROk returned
flushAll :: Redis -> IO (Reply ())
flushAll r = withState r (\rs -> sendCommand rs (CInline "FLUSHALL") >> recv rs)

{- UNTESTED -}
-- | Returns different information and statistics about the server
--
-- for more information see <http://redis.io/commands/info>
--
-- 'RedisInfo' returned
info :: Redis -> IO RedisInfo
info r = withState r (\rs -> sendCommand rs (CInline "INFO") >> recv rs
                             >>= fromRBulk >>= return . fromRight . parseInfo . fromJust)
    where fromRight (Right a) = a
          fromRight _ = error "fromRight"

-- | Set the string value as value of the key
--
-- ROk returned
set :: (BS s1, BS s2) => Redis
    -> s1                   -- ^ target key
    -> s2                   -- ^ value
    -> IO (Reply ())
set r key val = withState r (\rs -> sendCommand rs (CMBulk ["SET", toBS key, toBS val]) >> recv rs)

-- | Set the key value if key does not exists
--
-- (RInt 1) returned if key was set and (RInt 0) otherwise
setNx :: (BS s1, BS s2) =>
         Redis
      -> s1                     -- ^ target key
      -> s2                     -- ^ value
      -> IO (Reply Int)
setNx r key val = withState r (\rs -> sendCommand rs (CMBulk ["SETNX", toBS key, toBS val]) >> recv rs)

-- | Atomically sets target key value and assigns expiration time. The
-- same as /multi; set key val; expire key seconds; exec/ but faster.
--
-- Arguments order is the same as in Redis protocol.
--
-- ROk returned
setEx :: (BS s1, BS s2) =>
         Redis
      -> s1                     -- ^ target key
      -> Int                    -- ^ timeout in seconds
      -> s2                     -- ^ value
      -> IO (Reply ())
setEx r key seconds val = withState r (\rs -> sendCommand rs (CMBulk ["SETEX", toBS key, toBS seconds, toBS val]) >> recv rs)

-- | Atomically set multiple keys
--
-- ROk returned
mSet :: (BS s1, BS s2) =>
        Redis
     -> [(s1, s2)]              -- ^ (key, value) pairs
     -> IO (Reply ())
mSet r ks = let interlace' [] ls = ls
                interlace' ((a, b):rest) ls = interlace' rest (toBS a : toBS b : ls)
                interlace ls = interlace' ls []
            in withState r (\rs -> sendCommand rs (CMBulk ("MSET" : interlace ks)) >> recv rs)

-- | Atomically set multiple keys if none of them exists.
--
-- (RInt 1) returned if all keys was set and (RInt 0) otherwise
mSetNx :: (BS s1, BS s2) =>
          Redis
       -> [(s1, s2)]            -- ^ (key, value) pairs
       -> IO (Reply Int)
mSetNx r ks = let interlace' [] ls = ls
                  interlace' ((a, b):rest) ls = interlace' rest (toBS a : toBS b : ls)
                  interlace ls = interlace' ls []
              in withState r (\rs -> sendCommand rs (CMBulk ("MSETNX" : interlace ks)) >> recv rs)

-- | Get the value of the specified key.
--
-- RBulk returned
get :: (BS s1, BS s2) =>
       Redis
    -> s1                       -- ^ target key
    -> IO (Reply s2)
get r key = withState r (\rs -> sendCommand rs (CMBulk ["GET", toBS key]) >> recv rs)


-- | Atomically set this value and return the old value
--
-- RBulk returned
getSet :: (BS s1, BS s2, BS s3) =>
          Redis
       -> s1                -- ^ target key
       -> s2                -- ^ value
       -> IO (Reply s3)
getSet r key val = withState r (\rs -> sendCommand rs (CMBulk ["GETSET", toBS key, toBS val]) >> recv rs)

-- | Get the values of all specified keys
--
-- RMulti filled with RBulk replys returned
mGet :: (BS s1, BS s2) =>
        Redis
     -> [s1]                    -- ^ target keys
     -> IO (Reply s2)
mGet r keys = withState r (\rs -> sendCommand rs (CMBulk ("MGET" : map toBS keys)) >> recv rs)

-- | Increment the key value by one
--
-- RInt returned with new key value
incr :: BS s =>
        Redis
     -> s                       -- ^ target key
     -> IO (Reply Int)
incr r key = withState r (\rs -> sendCommand rs (CMBulk ["INCR", toBS key]) >> recv rs)

-- | Increment the key value by N
--
-- RInt returned with new key value
incrBy :: BS s =>
          Redis
       -> s                     -- ^ target key
       -> Int                   -- ^ increment
       -> IO (Reply Int)
incrBy r key n = withState r (\rs -> sendCommand rs (CMBulk ["INCRBY", toBS key, toBS n]) >> recv rs)

-- | Decrement the key value by one
--
-- RInt returned with new key value
decr :: BS s =>
        Redis
     -> s                  -- ^ target key
     -> IO (Reply Int)
decr r key = withState r (\rs -> sendCommand rs (CMBulk ["DECR", toBS key]) >> recv rs)

-- | Decrement the key value by N
--
-- RInt returned with new key value
decrBy :: BS s =>
          Redis
       -> s                     -- ^ target key
       -> Int                   -- ^ decrement
       -> IO (Reply Int)
decrBy r key n = withState r (\rs -> sendCommand rs (CMBulk ["DECRBY", toBS key, toBS n]) >> recv rs)

-- | Append string to the string-typed key
--
-- RInt returned - the length of resulting string
append :: (BS s1, BS s2) =>
          Redis
       -> s1                    -- ^ target key
       -> s2                    -- ^ value
       -> IO (Reply Int)
append r key str = withState r (\rs -> sendCommand rs (CMBulk ["APPEND", toBS key, toBS str]) >> recv rs)

-- | Returns the substring of the string value stored at key,
-- determined by the offsets start and end (both are
-- inclusive). Negative offsets can be used in order to provide an
-- offset starting from the end of the string.
--
-- RBulk returned
substr :: (BS s1, BS s2) =>
          Redis
       -> s1                    -- ^ target key
       -> (Int, Int)            -- ^ (start, end)
       -> IO (Reply s2)
substr r key (from, to) = withState r (\rs -> sendCommand rs (CMBulk ["SUBSTR", toBS key, toBS from, toBS to]) >> recv rs)

-- | Returns the substring of the string value stored at key,
-- determined by the offsets start and end (both are
-- inclusive). Negative offsets can be used in order to provide an
-- offset starting from the end of the string.
--
-- RBulk returned
getrange :: (BS s1, BS s2) =>
            Redis
         -> s1                    -- ^ target key
         -> (Int, Int)            -- ^ (start, end)
         -> IO (Reply s2)
getrange r key (from, to) = withState r (\rs -> sendCommand rs (CMBulk ["GETRANGE", toBS key, toBS from, toBS to]) >> recv rs)

-- | Overwrites part of the string stored at key, starting at the
-- specified offset, for the entire length of value. If the offset is
-- larger than the current length of the string at key, the string is
-- padded with zero-bytes to make offset fit. Non-existing keys are
-- considered as empty strings, so this command will make sure it
-- holds a string large enough to be able to set value at offset.
--
-- RInt returned - resulting string length.
setrange :: (BS s1, BS s2) =>
            Redis
         -> s1                  -- ^ target key
         -> Int                 -- ^ offset
         -> s2                  -- ^ value
         -> IO (Reply Int)
setrange r key offset val = withState r (\rs -> sendCommand rs (CMBulk ["SETRANGE", toBS key, toBS offset, toBS val]) >> recv rs)

-- | Returns the bit value at offset in the string value stored at
-- key. When offset is beyond the string length, the string is assumed
-- to be a contiguous space with 0 bits. When key does not exist it is
-- assumed to be an empty string, so offset is always out of range and
-- the value is also assumed to be a contiguous space with 0 bits.
--
-- RInt returned
getbit :: (BS s) =>
          Redis
       -> s                     -- ^ target key
       -> Int                   -- ^ bit offset
       -> IO (Reply Int)
getbit r key offset = withState r (\rs -> sendCommand rs (CMBulk ["GETBIT", toBS key, toBS offset]) >> recv rs)

-- | Sets or clears the bit at offset in the string value stored at key.
-- For more information see <http://redis.io/commands/setbit>
--
-- RInt returned - the original bit value stored at offset.
setbit :: (BS s) =>
          Redis
       -> s                     -- ^ target key
       -> Int                   -- ^ bit offset
       -> Int                   -- ^ bit value - 0 or 1
       -> IO (Reply Int)
setbit r key offset bit = withState r (\rs -> sendCommand rs (CMBulk ["SETBIT", toBS key, toBS offset, toBS bit]) >> recv rs)

-- | Returns a length of a string-typed key
--
-- RInt returned
strlen :: BS s =>
          Redis
       -> s                     -- ^ target key
       -> IO (Reply Int)
strlen r key = withState r (\rs -> sendCommand rs (CMBulk ["STRLEN", toBS key]) >> recv rs)

-- | Add string value to the tail of the list-type key. New list
-- length returned
--
-- RInt returned
rpush :: (BS s1, BS s2) =>
         Redis
      -> s1                     -- ^ target key
      -> s2                     -- ^ value
      -> IO (Reply Int)
rpush r key val = withState r (\rs -> sendCommand rs (CMBulk ["RPUSH", toBS key, toBS val]) >> recv rs)

-- | Add string value to the head of the list-type key. New list
-- length returned
--
-- RInt returned
lpush :: (BS s1, BS s2) =>
         Redis
      -> s1                     -- ^ target key
      -> s2                     -- ^ value
      -> IO (Reply Int)
lpush r key val = withState r (\rs -> sendCommand rs (CMBulk ["LPUSH", toBS key, toBS val]) >> recv rs)


-- | Add string value to the head of existing list-type key.  New list
-- length returned. If such a key was not exists, list is not created
-- and (RInt 0) returned.
--
-- RInt returned
lpushx :: (BS s1, BS s2) =>
          Redis
       -> s1                    -- ^ target key
       -> s2                    -- ^ value to push
       -> IO (Reply Int)
lpushx r key val = withState r (\rs -> sendCommand rs (CMBulk ["LPUSHX", toBS key, toBS val]) >> recv rs)

-- | Add string value to the tail of existing list-type key. New list
-- length returned.  If such a key was not exists, list is not created
-- and (RInt 0) returned.
--
-- RInt returned
rpushx :: (BS s1, BS s2) =>
          Redis
       -> s1                    -- ^ target key
       -> s2                    -- ^ value to push
       -> IO (Reply Int)
rpushx r key val = withState r (\rs -> sendCommand rs (CMBulk ["RPUSHX", toBS key, toBS val]) >> recv rs)

data LInsertDirection = BEFORE | AFTER deriving Show

-- | Inserts value in the list stored at key either before or after
-- the reference value pivot.
--
-- RInt returned - resulting list length or (RInt -1) if target element was not found.
linsert :: (BS s1, BS s2, BS s3) =>
           Redis
        -> s1                   -- ^ target list
        -> LInsertDirection     -- ^ where to insert - before or after
        -> s2                   -- ^ target element
        -> s3                   -- ^ inserted value
        -> IO (Reply Int)
linsert r key direction anchor value = withState r (\rs -> sendCommand rs (CMBulk ["LINSERT", toBS key, toBS $ show direction, toBS anchor, toBS value]) >> recv rs)

-- | Return lenght of the list. Note that for not-existing keys it
-- returns zero length.
--
-- RInt returned or RError if key is not a list
llen :: BS s =>
        Redis
     -> s                       -- ^ target key
     -> IO (Reply Int)
llen r key = withState r (\rs -> sendCommand rs (CMBulk ["LLEN", toBS key]) >> recv rs)

-- | Return the specified range of list elements. List indexed from 0
-- to (llen - 1). lrange returns slice including \"from\" and \"to\"
-- elements, eg. lrange 0 2 will return the first three elements of
-- the list.
--
-- Parameters \"from\" and \"to\" may also be negative. If so it will counts as
-- offset from end ot the list. eg. -1 - is the last element of the
-- list, -2 - is the second from the end and so on.
--
-- RMulti filled with RBulk returned
lrange :: (BS s1, BS s2) =>
          Redis
       -> s1                    -- ^ target key
       -> (Int, Int)            -- ^ (from, to) pair
       -> IO (Reply s2)
lrange r key (from, to) = withState r (\rs -> sendCommand rs (CMBulk ["LRANGE", toBS key, toBS from, toBS to]) >> recv rs)


-- | Trim list so that it will contain only the specified range of elements.
--
-- ROk returned
ltrim :: BS s =>
         Redis
      -> s                      -- ^ target key
      -> (Int, Int)             -- ^ (from, to) pair
      -> IO (Reply ())
ltrim r key (from, to) = withState r (\rs -> sendCommand rs (CMBulk ["LTRIM", toBS key, toBS from, toBS to]) >> recv rs)

-- | Return the specified element of the list by its index
--
-- RBulk returned
lindex :: (BS s1, BS s2) =>
          Redis
       -> s1                    -- ^ target key
       -> Int                   -- ^ index
       -> IO (Reply s2)
lindex r key index = withState r (\rs -> sendCommand rs (CMBulk ["LINDEX", toBS key, toBS index]) >> recv rs)

-- | Set the list's value indexed by an /index/ to the new value
--
-- ROk returned if element was set and RError if index is out of
-- range or key is not a list
lset :: (BS s1, BS s2) =>
        Redis
     -> s1                      -- ^ target key
     -> Int                     -- ^ index
     -> s2                      -- ^ new value
     -> IO (Reply ())
lset r key index val = withState r (\rs -> sendCommand rs (CMBulk ["LSET", toBS key, toBS index, toBS val]) >> recv rs)

-- | Remove the first /count/ occurrences of the /value/ element from the list
--
-- RInt returned - the number of elements removed
lrem :: (BS s1, BS s2) =>
        Redis
     -> s1                      -- ^ target key
     -> Int                     -- ^ occurrences
     -> s2                      -- ^ value
     -> IO (Reply Int)
lrem r key count value = withState r (\rs -> sendCommand rs (CMBulk ["LREM", toBS key, toBS count, toBS value]) >> recv rs)

-- | Atomically return and remove the first element of the list
--
-- RBulk returned
lpop :: (BS s1, BS s2) =>
        Redis
     -> s1                      -- ^ target key
     -> IO (Reply s2)
lpop r key = withState r (\rs -> sendCommand rs (CMBulk ["LPOP", toBS key]) >> recv rs)

-- | Atomically return and remove the last element of the list
--
-- RBulk returned
rpop :: (BS s1, BS s2) =>
        Redis
     -> s1                      -- ^ target key
     -> IO (Reply s2)
rpop r key = withState r (\rs -> sendCommand rs (CMBulk ["RPOP", toBS key]) >> recv rs)

-- | Atomically return and remove the last (tail) element of the
-- source list, and push the element as the first (head) element of
-- the destination list
--
-- RBulk returned
rpoplpush :: (BS s1, BS s2, BS s3) =>
             Redis
          -> s1                 -- ^ source key
          -> s2                 -- ^ destination key
          -> IO (Reply s3)
rpoplpush r src dst = withState r (\rs -> sendCommand rs (CMBulk ["RPOPLPUSH", toBS src, toBS dst]) >> recv rs)

-- | Blocking lpop
--
-- For more information see <http://redis.io/commands/blpop>
--
-- Return (Just (key, value)) if /value/ was successfully popped from /key/ list or Nothing of timeout exceeded.
blpop :: (BS s1, BS s2) =>
         Redis
      -> [s1]                   -- ^ keys list
      -> Int                    -- ^ timeout
      -> IO (Maybe (s1, s2))
blpop r keys timeout = withState r $ \rs -> do sendCommand rs (CMBulk (("BLPOP" : map toBS keys) ++ [toBS timeout]))
                                               res <- recv rs >>= fromRMultiBulk
                                               return $ case res of
                                                          Nothing -> Nothing
                                                          Just [Just k, Just v] -> Just (fromBS k, fromBS v)

-- | Blocking rpop
--
-- For more information see <http://redis.io/commands/brpop>
--
-- Return (Just (key, value)) if /value/ was successfully popped from /key/ list or Nothing of timeout exceeded.
brpop :: (BS s1, BS s2) =>
         Redis
      -> [s1]                   -- ^ keys list
      -> Int                    -- ^ timeout
      -> IO (Maybe (s1, s2))
brpop r keys timeout = withState r $ \rs -> do sendCommand rs (CMBulk (("BRPOP" : map toBS keys) ++ [toBS timeout]))
                                               res <- recv rs >>= fromRMultiBulk
                                               return $ case res of
                                                          Nothing -> Nothing
                                                          Just [Just k, Just v] -> Just (fromBS k, fromBS v)

-- | Blocking rpoplpush
--
-- For more information see <http://redis.io/commands/brpoplpush>
--
-- Return (Just $ Maybe value) if value was successfully popped or Nothing if timeout exceeded.
brpoplpush :: (BS s1, BS s2, BS s3) =>
             Redis
          -> s1                 -- ^ source key
          -> s2                 -- ^ destination key
          -> Int                -- ^ timeout
          -> IO (Maybe (Maybe s3))
brpoplpush r src dst timeout = withState r $ \rs -> do sendCommand rs (CMBulk ["BRPOPLPUSH", toBS src, toBS dst, toBS timeout])
                                                       res <- recv rs
                                                       return $ case res of
                                                                  RBulk res' -> Just res'
                                                                  RMulti Nothing -> Nothing
                                                                  _ -> error $ "wrong reply, RBulk or Nil RMulti expected: " ++ show res

-- | Add the specified member to the set value stored at key
--
-- (RInt 1) returned if element was added and (RInt 0) if element was
-- already a member of the set
sadd :: (BS s1, BS s2) =>
        Redis
     -> s1                      -- ^ target key
     -> s2                      -- ^ value
     -> IO (Reply Int)
sadd r key val = withState r (\rs -> sendCommand rs (CMBulk ["SADD", toBS key, toBS val]) >> recv rs)

-- | Remove the specified member from the set value stored at key
--
-- (RInt 1) returned if element was removed and (RInt 0) if element
-- is not a member of the set
srem :: (BS s1, BS s2) =>
        Redis
     -> s1                      -- ^ target key
     -> s2                      -- ^ value
     -> IO (Reply Int)
srem r key val = withState r (\rs -> sendCommand rs (CMBulk ["SREM", toBS key, toBS val]) >> recv rs)

-- | Remove a random element from a Set returning it as return value
--
-- RBulk returned
spop :: (BS s1, BS s2) =>
        Redis
     -> s1                      -- ^ target key
     -> IO (Reply s2)
spop r key = withState r (\rs -> sendCommand rs (CMBulk ["SPOP", toBS key]) >> recv rs)

-- | Move the specifided member from one set to another
--
-- (RInt 1) returned if element was moved and (RInt 0) if element
-- is not a member of the source set
smove :: (BS s1, BS s2, BS s3) =>
         Redis
      -> s1                     -- ^ source key
      -> s2                     -- ^ destination key
      -> s3                     -- ^ value
      -> IO (Reply Int)
smove r src dst member = withState r (\rs -> sendCommand rs (CMBulk ["SMOVE", toBS src, toBS dst, toBS member]) >> recv rs)

-- | Return the number of elements of the set. If key doesn't exists 0
-- returned.
--
-- RInt returned
scard :: BS s =>
         Redis
      -> s                      -- ^ target key
      -> IO (Reply Int)
scard r key = withState r (\rs -> sendCommand rs (CMBulk ["SCARD", toBS key]) >> recv rs)

-- | Test if element is member of the set. If key doesn't exists 0
-- returned.
--
-- (RInt 1) returned if element is member of the set and (RInt 0) otherwise
sismember :: (BS s1, BS s2) =>
             Redis
          -> s1                 -- ^ target key
          -> s2                 -- ^ value to test
          -> IO (Reply Int)
sismember r key val = withState r (\rs -> sendCommand rs (CMBulk ["SISMEMBER", toBS key, toBS val]) >> recv rs)

-- | Return all the members (elements) of the set
--
-- RMulti filled with RBulk returned
smembers :: (BS s1, BS s2) =>
            Redis
         -> s1                  -- ^ target key
         -> IO (Reply s2)
smembers r key = withState r (\rs -> sendCommand rs (CMBulk ["SMEMBERS", toBS key]) >> recv rs)

-- | Return a random element from a set
--
-- RBulk returned
srandmember :: (BS s1, BS s2) =>
               Redis
            -> s1               -- ^ target key
            -> IO (Reply s2)
srandmember r key = withState r (\rs -> sendCommand rs (CMBulk ["SRANDMEMBER", toBS key]) >> recv rs)

-- | Return the members of a set resulting from the intersection of
-- all the specifided sets
--
-- RMulti filled with RBulk returned
sinter :: (BS s1, BS s2) =>
          Redis
       -> [s1]                  -- ^ keys list
       -> IO (Reply s2)
sinter r keys = withState r (\rs -> sendCommand rs (CMBulk ("SINTER" : map toBS keys)) >> recv rs)

-- | The same as 'sinter' but instead of being returned the resulting set
-- is stored
--
-- RInt returned - resulting set cardinality.
sinterStore :: (BS s1, BS s2) =>
               Redis
            -> s1               -- ^ where to store resulting set
            -> [s2]             -- ^ sets list
            -> IO (Reply ())
sinterStore r dst keys = withState r (\rs -> sendCommand rs (CMBulk ("SINTERSTORE" : toBS dst : map toBS keys)) >> recv rs)

-- | Return the members of a set resulting from the union of all the
-- specifided sets
--
-- RMulti filled with RBulk returned
sunion :: (BS s1, BS s2) =>
          Redis
       -> [s1]                  -- ^ keys list
       -> IO (Reply s2)
sunion r keys = withState r (\rs -> sendCommand rs (CMBulk ("SUNION" : map toBS keys)) >> recv rs)

-- | The same as 'sunion' but instead of being returned the resulting set
-- is stored
--
-- RInt returned - resulting set cardinality.
sunionStore :: (BS s1, BS s2) =>
               Redis
            -> s1               -- ^ where to store resulting set
            -> [s2]             -- ^ sets list
            -> IO (Reply ())
sunionStore r dst keys = withState r (\rs -> sendCommand rs (CMBulk ("SUNIONSTORE" : toBS dst : map toBS keys)) >> recv rs)

-- | Return the members of a set resulting from the difference between
-- the first set provided and all the successive sets
--
-- RMulti filled with RBulk returned
sdiff :: (BS s1, BS s2) =>
         Redis
      -> [s1]                   -- ^ keys list
      -> IO (Reply s2)
sdiff r keys = withState r (\rs -> sendCommand rs (CMBulk ("SDIFF" : map toBS keys)) >> recv rs)

-- | The same as 'sdiff' but instead of being returned the resulting
-- set is stored
--
-- RInt returned - resulting set cardinality.
sdiffStore :: (BS s1, BS s2) =>
              Redis
           -> s1                -- ^ where to store resulting set
           -> [s2]              -- ^ sets list
           -> IO (Reply ())
sdiffStore r dst keys = withState r (\rs -> sendCommand rs (CMBulk ("SDIFFSTORE" : toBS dst : map toBS keys)) >> recv rs)

-- | Add the specified member having the specifeid score to the sorted
-- set
--
-- (RInt 1) returned if new element was added and (RInt 0) if that
-- element was already a member of the sortet set and the score was
-- updated
zadd :: (BS s1, BS s2) =>
        Redis
     -> s1                      -- ^ target key
     -> Double                  -- ^ score
     -> s2                      -- ^ value
     -> IO (Reply Int)
zadd r key score member = withState r (\rs -> sendCommand rs (CMBulk ["ZADD", toBS key, toBS score, toBS member]) >> recv rs)

-- | Remove the specified member from the sorted set
--
-- (RInt 1) returned if element was removed and (RInt 0) if element
-- was not a member of the sorted set
zrem :: (BS s1, BS s2) =>
        Redis
     -> s1                      -- ^ target key
     -> s2                      -- ^ value
     -> IO (Reply Int)
zrem r key member = withState r (\rs -> sendCommand rs (CMBulk ["ZREM", toBS key, toBS member]) >> recv rs)

-- | If /member/ already in the sorted set adds the /increment/ to its
-- score and updates the position of the element in the sorted set
-- accordingly. If member does not exist in the sorted set it is added
-- with increment as score (that is, like if the previous score was
-- virtually zero). The new score of the member is returned.
--
-- RBulk returned
zincrBy :: (BS s1, BS s2, BS s3) =>
           Redis
        -> s1                   -- ^ target key
        -> Double               -- ^ increment
        -> s2                   -- ^ value
        -> IO (Reply s3)
zincrBy r key increment member = withState r (\rs -> sendCommand rs (CMBulk ["ZINCRBY", toBS key, toBS increment, toBS member]) >> recv rs)

-- | Return the specified elements of the sorted set. Start and end
-- are zero-based indexes. WITHSCORES paramenter indicates if it's
-- needed to return elements with its scores or not. If WITHSCORES is
-- True then the resulting list will be composed of value1, score1,
-- value2, score2 and so on.
--
-- RMulti filled with RBulk returned
zrange :: (BS s1, BS s2) =>
          Redis
       -> s1                    -- ^ target key
       -> (Int, Int)            -- ^ (from, to) pair
       -> Bool                  -- ^ withscores option
       -> IO (Reply s2)
zrange r key limit withscores = let cmd' = ["ZRANGE", toBS key, toBS $ fst limit, toBS $ snd limit]
                                    cmd | withscores = cmd' ++ ["WITHSCORES"]
                                        | otherwise  = cmd'
                                in withState r (\rs -> sendCommand rs (CMBulk cmd) >> recv rs)

-- | Return the specified elements of the sorted set at the specified
-- key. The elements are considered sorted from the highest to the
-- lowerest score
--
-- RMulti filled with RBulk returned
zrevrange :: (BS s1, BS s2) =>
             Redis
          -> s1                 -- ^ target key
          -> (Int, Int)         -- ^ (from, to) pair
          -> Bool               -- ^ withscores option
          -> IO (Reply s2)
zrevrange r key limit withscores = let cmd' = ["ZREVRANGE", toBS key, toBS $ fst limit, toBS $ snd limit]
                                       cmd | withscores = cmd' ++ ["WITHSCORES"]
                                           | otherwise  = cmd'
                                   in withState r (\rs -> sendCommand rs (CMBulk cmd) >> recv rs)

-- | Interval representation
data Interval a = Closed a a    -- ^ closed interval [a, b]
                | Open a a      -- ^ open interval (a, b)
                | LeftOpen a a  -- ^ left-open interval (a, b]
                | RightOpen a a -- ^ right-open interval [a, b)
                deriving Show

-- | Class for conversion value to 'Interval'
--
-- Definied instances is:
--
-- * the Interval itself
--
-- * pair (a,b) for open interval
--
-- * two-member list [a, b] for closed interval (throws runtime error if the list length is different)
--
class IsInterval i a | i -> a where
    toInterval :: i -> Interval a

-- | Trivial IsInterval instance
instance IsInterval (Interval a) a where
    toInterval = id

-- | Pair (a, b) converted to open interval
instance IsInterval (a, a) a where
    toInterval (a, b) = Open a b

-- | Two-element list [a, b] converted to closed interval. No static
-- checking of list length performed.
instance IsInterval [a] a where
    toInterval (a : b : []) = Closed a b
    toInterval _ = error "Interval cast error"

from (Closed a _) = show a
from (Open a _) = '(' : (show a)
from (LeftOpen a _) = '(' : (show a)
from (RightOpen a _) = show a

to (Closed _ a) = show a
to (Open _ a) = '(' : (show a)
to (LeftOpen _ a) = show a
to (RightOpen _ a) = '(' : (show a)

-- | Return the all the elements in the sorted set with a score that
-- lays within a given interval
--
-- RMulti filled with RBulk returned
zrangebyscore :: (IsInterval i Double, BS s1, BS s2) =>
                 Redis
              -> s1               -- ^ target key
              -> i                -- ^ scores interval
              -> Maybe (Int, Int) -- ^ limits (offset, count)
              -> Bool             -- ^ withscores option
              -> IO (Reply s2)
zrangebyscore r key i limit withscores = let cmd' = i' `seq` ["ZRANGEBYSCORE", toBS key, toBS (from i'), toBS (to i')]
                                             cmd'' = maybe cmd' (\(a, b) -> cmd' ++ ["LIMIT", toBS a, toBS b]) limit
                                             cmd | withscores = cmd'' ++ ["WITHSCORES"]
                                                 | otherwise  = cmd''
                                             i' = toInterval i
                                         in cmd `seq` withState r (\rs -> sendCommand rs (CMBulk cmd) >> recv rs)

-- | Return the all the elements in the sorted set with a score that
-- lays within a given interval. Elements is ordered from greater
-- score to lower. Interval passed into command must be reversed
-- (first value is greater then second)
--
-- RMulti filled with RBulk returned
zrevrangebyscore :: (IsInterval i Double, BS s1, BS s2) =>
                    Redis
                 -> s1
                 -> i
                 -> Maybe (Int, Int)
                 -> Bool
                 -> IO (Reply s2)
zrevrangebyscore r key i limit withscores = let cmd' = i' `seq` ["ZREVRANGEBYSCORE", toBS key, toBS (from i'), toBS (to i')]
                                                cmd'' = maybe cmd' (\(a, b) -> cmd' ++ ["LIMIT", toBS a, toBS b]) limit
                                                cmd | withscores = cmd'' ++ ["WITHSCORES"]
                                                    | otherwise  = cmd''
                                                i' = toInterval i
                                            in cmd `seq` withState r (\rs -> sendCommand rs (CMBulk cmd) >> recv rs)

-- | Count a number of elements of the sorted set with a score that
-- lays within a given interval
--
-- RInt returned
zcount :: (IsInterval i Double, BS s) =>
          Redis
       -> s                     -- ^ target key
       -> i                     -- ^ scores interval
       -> IO (Reply Int)
zcount r key i = let cmd = i' `seq` ["ZCOUNT", toBS key, toBS (from i'), toBS (to i')]
                     i' = toInterval i
                 in cmd `seq` withState r (\rs -> sendCommand rs (CMBulk cmd) >> recv rs)

-- | Remove all the elements in the sorted set with a score that lays
-- within a given interval. For now this command doesn't supports open
-- and semi-open intervals
--
-- RInt returned - the number of elements removed
zremrangebyscore :: BS s =>
                    Redis
                 -> s                -- ^ target key
                 -> (Double, Double) -- ^ (from, to) pair. zremrangebyscore
                                     -- currently doesn't supports
                                     -- open intervals
                 -> IO (Reply Int)
zremrangebyscore r key (from, to) = withState r (\rs -> sendCommand rs (CMBulk ["ZREMRANGEBYSCORE", toBS key, toBS from, toBS to]) >> recv rs)

-- | Return the sorted set cardinality (number of elements)
--
-- RInt returned
zcard :: BS s =>
         Redis
      -> s                      -- ^ target key
      -> IO (Reply Int)
zcard r key = withState r (\rs -> sendCommand rs (CMBulk ["ZCARD", toBS key]) >> recv rs)

-- | Return the score of the specified element of the sorted set
--
-- RBulk returned
zscore :: (BS s1, BS s2, BS s3) =>
          Redis
       -> s1                    -- ^ target key
       -> s2                    -- ^ value
       -> IO (Reply s3)
zscore r key member = withState r (\rs -> sendCommand rs (CMBulk ["ZSCORE", toBS key, toBS member]) >> recv rs)

-- | Returns the rank of member in the sorted set stored at key, with
-- the scores ordered from low to high.
--
-- RInt returned or (RBulk Nothing) if value is not found in set.
zrank :: (BS s1, BS s2) =>
         Redis
      -> s1                     -- ^ target key
      -> s2                     -- ^ value
      -> IO (Reply Int)
zrank r key member = withState r (\rs -> sendCommand rs (CMBulk ["ZRANK", toBS key, toBS member]) >> recv rs)

-- | Returns the rank of member in the sorted set stored at key, with
-- the scores ordered from high to low.
--
-- RInt returned or (RBulk Nothing) if value is not found in set.
zrevrank :: (BS s1, BS s2) =>
            Redis
         -> s1                  -- ^ target key
         -> s2                  -- ^ value
         -> IO (Reply Int)
zrevrank r key member = withState r (\rs -> sendCommand rs (CMBulk ["ZREVRANK", toBS key, toBS member]) >> recv rs)

-- | Remove elements from the sorted set with rank lays within a given
-- interval.
--
-- RInt returned - the number of elements removed
zremrangebyrank :: (BS s) =>
                   Redis
                -> s
                -> (Int, Int)
                -> IO (Reply Int)
zremrangebyrank r key (from, to) =
    withState r (\rs -> sendCommand rs (CMBulk ["ZREMRANGEBYRANK", toBS key, toBS from, toBS to]) >> recv rs)

data Aggregate = SUM | MIN | MAX
                 deriving (Eq, Show)

-- | Create a union of provided sorted sets and store it at /destination/ key
--
-- If /weights/ is not null then scores of sorted sets used with
-- corresponding weights. If so lenght of /weights/ must be the same
-- as length of /sources/.
--
-- /Aggregate/ is an option how to aggregate resulting scores.
--
-- RInt returned - the number of elements in the resulting set.
zunionStore :: (BS s1, BS s2) =>
               Redis
            -> s1                    -- ^ destination key
            -> [s2]                  -- ^ sources keys
            -> [Double]              -- ^ weights
            -> Aggregate             -- ^ aggregate
            -> IO (Reply Int)
zunionStore r dst src weights aggregate =
    let src_s = toBS (length src) : map toBS src

        weight_s | null weights = []
                 | otherwise    = "WEIGHTS" : map toBS weights

        aggr_s | aggregate == SUM = []
               | otherwise        = ["AGGREGATE", toBS (show aggregate)]
    in withState r (\rs -> sendCommand rs (CMBulk (("ZUNIONSTORE" : toBS dst : src_s) ++ weight_s ++ aggr_s)) >> recv rs)

{-# DEPRECATED zunion "ZUNION command was renamed to ZUNIONSTORE" #-}
zunion :: (BS s1, BS s2) => Redis -> s1 -> [s2] -> [Double] -> Aggregate -> IO (Reply Int)
zunion = zunionStore

-- | Create an intersectoin of provided sorted sets and store it at destination key
--
-- If /weights/ is not null then scores of sorted sets used with
-- corresponding weights. If so lenght of /weights/ must be the same
-- as length of /sources/.
--
-- Aggregate is an option how to aggregate resulting scores.
--
-- RInt returned - the number of elements in the resulting set.
zinterStore :: (BS s1, BS s2) =>
               Redis
            -> s1                    -- ^ destination key
            -> [s2]                  -- ^ sources keys
            -> [Double]              -- ^ weights
            -> Aggregate             -- ^ aggregate
            -> IO (Reply Int)
zinterStore r dst src weights aggregate =
    let src_s = toBS (length src) : map toBS src

        weight_s | null weights = []
                 | otherwise    = "WEIGHTS" : map toBS weights

        aggr_s | aggregate == SUM = []
               | otherwise        = ["AGGREGATE", toBS (show aggregate)]
    in withState r (\rs -> sendCommand rs (CMBulk (("ZINTERSTORE" : toBS dst : src_s) ++ weight_s ++ aggr_s)) >> recv rs)

{-# DEPRECATED zinter "ZINTER command was renamed to ZINTERSTORE" #-}
zinter :: (BS s1, BS s2) => Redis -> s1 -> [s2] -> [Double] -> Aggregate -> IO (Reply Int)
zinter = zinterStore

-- | Set the specified hash field to the specified value
--
-- (RInt 0 returned if field value was updated and (RInt 1) if new field created
hset :: (BS s1, BS s2, BS s3) =>
        Redis
     -> s1                      -- ^ target key
     -> s2                      -- ^ field name
     -> s3                      -- ^ value
     -> IO (Reply Int)
hset r key field value = withState r (\rs -> sendCommand rs (CMBulk ["HSET", toBS key, toBS field, toBS value]) >> recv rs)

-- | Return value associated with specified field from hash
--
-- RBulk returned
hget :: (BS s1, BS s2, BS s3) =>
        Redis
     -> s1                      -- ^ key
     -> s2                      -- ^ field name
     -> IO (Reply s3)
hget r key field = withState r (\rs -> sendCommand rs (CMBulk ["HGET", toBS key, toBS field]) >> recv rs)

-- | Remove field from a hash
--
-- (RInt 1) returned if field was removed and (RInt 0) otherwise
hdel :: (BS s1, BS s2) =>
        Redis
     -> s1                      -- ^ key
     -> s2                      -- ^ field name
     -> IO (Reply Int)
hdel r key field = withState r (\rs -> sendCommand rs (CMBulk ["HDEL", toBS key, toBS field]) >> recv rs)

-- | Atomically sets multiple fields within a hash-typed key
--
-- ROk returned
hmset :: (BS s1, BS s2, BS s3) =>
         Redis
      -> s1                     -- ^ target key
      -> [(s2, s3)]             -- ^ (field, value) pairs
      -> IO (Reply ())
hmset r key fields = let interlace' [] ls = ls
                         interlace' ((a, b):rest) ls = interlace' rest (toBS a : toBS b : ls)
                         interlace ls = interlace' ls []
                     in withState r (\rs -> sendCommand rs (CMBulk ("HMSET" : toBS key : interlace fields)) >> recv rs)

-- | Get the values of all specified fields from the hash-typed key
--
-- RMulti filled with RBulk replys returned
hmget :: (BS s1, BS s2, BS s3) =>
         Redis
      -> s1                     -- ^ target key
      -> [s2]                   -- ^ field names
      -> IO (Reply s3)
hmget r key fields = withState r (\rs -> sendCommand rs (CMBulk ("HMGET" : toBS key : map toBS fields)) >> recv rs)

-- | Increment the field value within a hash by N
--
-- RInt returned with new key value
hincrby :: (BS s1, BS s2) =>
           Redis
        -> s1                   -- ^ target key
        -> s2                   -- ^ field name
        -> Int                  -- ^ increment
        -> IO (Reply Int)
hincrby r key field n = withState r (\rs -> sendCommand rs (CMBulk ["HINCRBY", toBS key, toBS field, toBS n]) >> recv rs)

-- | Test if hash contains the specified field
--
-- (RInt 1) returned if fiels exists and (RInt 0) otherwise
hexists :: (BS s1, BS s2) =>
           Redis
        -> s1                   -- ^ key
        -> s2                   -- ^ field name
        -> IO (Reply Int)
hexists r key field = withState r (\rs -> sendCommand rs (CMBulk ["HEXISTS", toBS key, toBS field]) >> recv rs)

-- | Return the number of fields contained in the specified hash
--
-- RInt returned
hlen :: (BS s) =>
        Redis
     -> s
     -> IO (Reply Int)
hlen r key = withState r (\rs -> sendCommand rs (CMBulk ["HLEN", toBS key]) >> recv rs)

-- | Return all the field names the hash holding
--
-- RMulti field with RBulk returned
hkeys :: (BS s1, BS s2) =>
         Redis
      -> s1
      -> IO (Reply s2)
hkeys r key = withState r (\rs -> sendCommand rs (CMBulk ["HKEYS", toBS key]) >> recv rs)

-- | Return all the associated values the hash holding
--
-- RMulti field with RBulk returned
hvals :: (BS s1, BS s2) =>
         Redis
      -> s1
      -> IO (Reply s2)
hvals r key = withState r (\rs -> sendCommand rs (CMBulk ["HVALS", toBS key]) >> recv rs)

-- | Return all the field names and associated values the hash holding
-- in form of /[field1, value1, field2, value2...]/
--
-- RMulti field with RBulk returned. If key doesn't exists (RMulti []) returned.
hgetall :: (BS s1, BS s2) =>
           Redis
        -> s1                   -- ^ target key
        -> IO (Reply s2)
hgetall r key = withState r (\rs -> sendCommand rs (CMBulk ["HGETALL", toBS key]) >> recv rs)

-- | Options data type for the 'sort' command
data BS s => SortOptions s = SortOptions { desc       :: Bool,       -- ^ sort with descending order
                                           limit      :: (Int, Int), -- ^ return (from, to) elements
                                           alpha      :: Bool,       -- ^ sort alphabetically
                                           sort_by    :: s,          -- ^ sort by value from this key
                                           get_obj    :: [s],        -- ^ return this keys values
                                           store      :: s           -- ^ store result to this key
                                         }

-- | Default options for the 'sort' command
sortDefaults :: SortOptions ByteString
sortDefaults = SortOptions { desc = False,
                             limit = takeAll,
                             alpha = False,
                             sort_by = "",
                             get_obj = [],
                             store = "" }

-- | Sort the elements contained in the List, Set, or Sorted Set
--
-- for more information see <http://redis.io/commands/sort>
--
-- RMulti filled with RBulk returned
sort :: (BS s1, BS s2, BS s3) =>
        Redis
     -> s1                      -- ^ target key
     -> SortOptions s2          -- ^ options
     -> IO (Reply s3)
sort r key opt = let opt_s = buildOptions opt
                     buildOptions :: BS s => SortOptions s -> [ByteString]
                     buildOptions opt = let desc_s
                                                | desc opt  = ["DESC"]
                                                | otherwise = []
                                            limit_s
                                                | (limit opt) == (0, 0) = []
                                                | otherwise             = ["LIMIT", (toBS $ fst $ limit opt), (toBS $ snd $ limit opt)]
                                            alpha_s
                                                | alpha opt = ["ALPHA"]
                                                | otherwise = []
                                            sort_by_s
                                                | B.null $ toBS (sort_by opt) = []
                                                | otherwise                   = ["BY",(toBS $ sort_by opt)]
                                            get_obj_s
                                                | null $ get_obj opt = []
                                                | otherwise          = "GET" : (intersperse "GET" . map toBS $ get_obj opt)
                                            store_s
                                                | B.null $ toBS (store opt) = []
                                                | otherwise                 = ["STORE", toBS $ store opt]
                                        in concat [sort_by_s, limit_s, get_obj_s, desc_s, alpha_s, store_s]
                 in withState r (\rs -> sendCommand rs (CMBulk ("SORT" : toBS key : opt_s)) >> recv rs)

-- | Shortcut for the 'sort' with some 'get_obj' and constant
-- 'sort_by' options
--
-- RMulti filled with RBulk returned
listRelated :: (BS s1, BS s2, BS s3) =>
               Redis
            -> s1               -- ^ related key
            -> s2               -- ^ index key
            -> (Int, Int)       -- ^ range
            -> IO (Reply s3)
listRelated r related key l = let opts = sortDefaults { sort_by = "x",
                                                        get_obj = [toBS related],
                                                        limit = l }
                              in sort r key opts

-- | Get a number of subscribed channels on this connection
--
-- It doesn't run any redis commands, number of subscribtions is taken
-- from internal connection state
subscribed :: Redis -> IO Int
subscribed r = withState r $ \rs -> return $ isSubscribed rs

recv_ rs ls 0 = return ls
recv_ rs ls n = do l <- recv rs
                   ll <- recv_ rs ls (n - 1)
                   return $ l:ll

-- | Subscribe to channels
--
-- list of Message with subscribtion information returned
subscribe :: (BS s1, BS s2) =>
             Redis
          -> [s1]               -- ^ channels to subscribe
          -> IO [Message s2]
subscribe r classes = inState r $ \rs -> do sendCommand rs (CMBulk ("SUBSCRIBE" : map toBS classes))
                                            res <- recv_ rs [] (length classes) >>= mapM parseMessage
                                            let !(MSubscribe _ n) = last res
                                            return (rs {isSubscribed = n}, res)

-- | Unsubscribe from channels. If called with an empty list then
-- unsubscribe all channels
--
-- list of Message with subscribtion information returned
unsubscribe :: (BS s1, BS s2) =>
               Redis
            -> [s1]             -- ^ channels to unsubscribe
            -> IO [Message s2]
unsubscribe r [] = inState r $ \rs -> let subs = isSubscribed rs
                                      in if subs == 0
                                         then return (rs, [])
                                         else do sendCommand rs (CInline "UNSUBSCRIBE")
                                                 res <- recv_ rs [] subs >>= mapM parseMessage
                                                 let !(MUnsubscribe _ n) = last res
                                                 return (rs {isSubscribed = n}, res)

unsubscribe r classes = inState r $ \rs -> do sendCommand rs (CMBulk ("UNSUBSCRIBE" : map toBS classes))
                                              res <- recv_ rs [] (length classes) >>= mapM parseMessage
                                              let !(MUnsubscribe _ n) = last res
                                              return (rs {isSubscribed = n}, res)

-- | Subscribe to patterns
--
-- list of Message with subscribtion information returned
psubscribe :: (BS s1, BS s2) =>
              Redis
           -> [s1]               -- ^ patterns to subscribe
           -> IO [Message s2]
psubscribe r patterns = inState r $ \rs -> do sendCommand rs (CMBulk ("PSUBSCRIBE" : map toBS patterns))
                                              res <- recv_ rs [] (length patterns) >>= mapM parseMessage
                                              let !(MPSubscribe _ n) = last res
                                              return (rs {isSubscribed = n}, res)

-- | Unsubscribe from patterns. If called with an empty list then
-- unsubscribe all patterns
--
-- list of Message with subscribtion information returned
punsubscribe :: (BS s1, BS s2) =>
                Redis
             -> [s1]             -- ^ patterns to unsubscribe
             -> IO [Message s2]
punsubscribe r [] = inState r $ \rs -> let subs = isSubscribed rs
                                       in if subs == 0
                                          then return (rs, [])
                                          else do sendCommand rs (CInline "PUNSUBSCRIBE")
                                                  res <- recv_ rs [] subs >>= mapM parseMessage
                                                  let !(MPUnsubscribe _ n) = last res
                                                  return (rs {isSubscribed = n}, res)

punsubscribe r patterns = inState r $ \rs -> do sendCommand rs (CMBulk ("PUNSUBSCRIBE" : map toBS patterns))
                                                res <- recv_ rs [] (length patterns) >>= mapM parseMessage
                                                let !(MPUnsubscribe _ n) = last res
                                                return (rs {isSubscribed = n}, res)

-- | Publish message to target channel
--
-- RInt returned - a number of clients that recieves the message
publish :: (BS s1, BS s2) =>
           Redis
        -> s1                   -- ^ channel
        -> s2                   -- ^ message
        -> IO (Reply Int)
publish r klass msg = withState r $ \rs -> sendCommand rs (CMBulk ["PUBLISH", toBS klass, toBS msg]) >> recv rs

-- | Wait for a messages.
--
-- Just Message returned or Nothing if timeout exceeded
listen :: BS s =>
          Redis
       -> Int                   -- ^ timeout
       -> IO (Maybe (Message s))
listen r timeout = withState r $ \rs -> if isSubscribed rs == 0
                                        then return Nothing
                                        else do ready <- wait rs timeout
                                                if ready
                                                  then recv rs >>= parseMessage >>= return . Just
                                                  else return Nothing

{- UNTESTED -}
-- | Save the whole dataset on disk
--
-- ROk returned
save :: Redis -> IO (Reply ())
save r = withState r (\rs -> sendCommand rs (CInline "SAVE") >> recv rs)

{- UNTESTED -}
-- | Save the DB in background
--
-- ROk returned
bgsave :: Redis -> IO (Reply ())
bgsave r = withState r (\rs -> sendCommand rs (CInline "BGSAVE") >> recv rs)

{- UNTESTED -}
-- | Return the UNIX TIME of the last DB save executed with success
--
-- RInt returned
lastsave :: Redis -> IO (Reply Int)
lastsave r = withState r (\rs -> sendCommand rs (CInline "LASTSAVE") >> recv rs)

{- UNTESTED -}
-- | Rewrites the Append Only File in background
--
-- ROk returned
bgrewriteaof :: Redis -> IO (Reply ())
bgrewriteaof r = withState r (\rs -> sendCommand rs (CInline "BGREWRITEAOF") >> recv rs)
